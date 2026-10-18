"""T3-only Terminal MCP. Public tools and private control plane use separate ports."""
from __future__ import annotations

import argparse
import asyncio
import contextvars
import json
import os
import secrets
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
import uvicorn

from t3_thread_state import ThreadState

CURRENT_OPERATION = contextvars.ContextVar('t3_terminal_operation', default=None)


def build(core, state: ThreadState, control_token: str):
    if not control_token:
        raise ValueError('Private control-plane token must be configured')
    watchers: set[asyncio.Task] = set()

    class T3FastMCP(core.AccountingFastMCP):
        async def call_tool(self, name, arguments):
            try:
                ids = {str(arguments[k]) for k in ('thread_id', 'session_id') if arguments.get(k)}
                if len(ids) != 1:
                    raise ValueError('Provide exactly the T3-assigned thread/session ID')
                binding = state.require_terminal(ids.pop())
                operation = 'tool-' + uuid.uuid4().hex
                state.begin(binding['thread_id'], operation, 'tool:' + name)
            except ValueError as exc:
                return core.mcp_types.CallToolResult(isError=True, content=[core.mcp_types.TextContent(type='text', text=f'Error: {exc}')])
            token = CURRENT_OPERATION.set(operation)
            try:
                return await super().call_tool(name, arguments)
            finally:
                CURRENT_OPERATION.reset(token)
                state.finish(operation, 'tool_returned')

    @asynccontextmanager
    async def lifespan(_):
        # Recover only our locally registered operations, once at startup.
        # Live process handles are observed with pidfd exit notifications, never project scans.
        with state.connect() as db:
            rows = [dict(r) for r in db.execute('SELECT * FROM operations WHERE finished IS NULL')]
        for row in rows:
            pid = row['pid']
            if pid and core.read_process_start_ticks(pid) == row['start_ticks']:
                fd = os.pidfd_open(pid)
                async def restored_exit(row=row, fd=fd):
                    loop = asyncio.get_running_loop()
                    future = loop.create_future()
                    loop.add_reader(fd, lambda: None if future.done() else future.set_result(None))
                    try:
                        await future
                        state.finish(row['operation_id'], 'process_exited_after_recovery')
                    finally:
                        loop.remove_reader(fd)
                        os.close(fd)
                task = asyncio.create_task(restored_exit())
                watchers.add(task)
                task.add_done_callback(watchers.discard)
            else:
                state.finish(row['operation_id'], 'interrupted_by_restart')
        with state.connect() as db:
            ids = [r[0] for r in db.execute("SELECT thread_id FROM bindings WHERE state='working' AND paused=0")]
        for thread_id in ids:
            state.arm(thread_id)
        try:
            yield
        finally:
            state.close()
            for task in list(watchers):
                task.cancel()
            await asyncio.gather(*watchers, return_exceptions=True)
            await core._close_all_mcp_connections()

    instructions = '''This is the T3-specific Terminal MCP. Bootstrap using exactly the permanent Terminal MCP ID injected by T3; use it as session_id on every tool call. Unregistered IDs are rejected. Never invent IDs. T3 owns ChatGPT prompts and streaming; Terminal MCP owns task working/done and inactivity recovery. Use schedule_continue with exactly one of after_seconds or wake_at (Unix seconds or timezone-aware ISO-8601), or continue_after_process with a managed process_id, to resume this task when no tools/processes are active. These durable continuations belong to the current task epoch; pause, completion, or a new activation invalidates them. Reuse an idempotency_key to retry scheduling safely. When all requested work is verified, call thread_task(action="complete", evidence=[...]). End of model output does not finish the task. For a pending question, call thread_task(action="wait", reason="..."). There are no project scanning, watchdog, scheduled-send, or ChatGPT runtime-control tools here.'''
    mcp = T3FastMCP('T3 Terminal MCP', instructions=instructions, lifespan=lifespan,
                    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
    excluded = {'thread_goal', 'refresh_startup_context'}
    for name, tool in core.mcp._tool_manager._tools.items():
        if not name.startswith(('agent_', 'chat_runtime_')) and name not in excluded:
            mcp.add_tool(tool.fn, name=tool.name, description=tool.description)
    core.mcp = mcp
    core._set_startup_instructions = lambda: instructions
    # The context loader builds its manifest from core.mcp, which now contains only this flavor.
    core._core_behavior_prompt = lambda: instructions

    @mcp.tool()
    async def thread_task(action: str = 'get', session_id: str = '', evidence: list[str] | None = None, reason: str = '') -> str:
        """Inspect, complete with evidence, or wait for user input. Only Terminal MCP can mark T3 done."""
        binding = state.require_terminal(session_id)
        thread_id = binding['thread_id']
        try:
            if action == 'complete':
                result = state.complete(thread_id, evidence or [], excluding=CURRENT_OPERATION.get())
            elif action == 'wait':
                if not reason.strip():
                    raise ValueError('A reason for waiting is required')
                result = state.pause(thread_id, 'wait-' + uuid.uuid4().hex)
            elif action == 'get':
                result = binding
            else:
                raise ValueError('action must be get, complete, or wait')
            return json.dumps({'threadId': thread_id, 'state': result['state'], 'paused': bool(result['paused'])})
        except ValueError as exc:
            return f'Error: {exc}'

    @mcp.tool()
    async def link_pull_request(url: str, session_id: str = '') -> str:
        """Ask the bound T3 thread to register a full GitHub PR URL."""
        binding = state.require_terminal(session_id)
        return json.dumps({'eventId': state.link_pr(binding['thread_id'], url)})

    @mcp.tool()
    async def schedule_continue(session_id: str, after_seconds: float | None = None, wake_at: float | str | None = None, idempotency_key: str | None = None) -> str:
        """Schedule this task's continuation after a positive delay or at an absolute time, once tools/processes are idle."""
        binding = state.require_terminal(session_id)
        return json.dumps(state.schedule_continue(binding['thread_id'], after_seconds, wake_at, idempotency_key))

    @mcp.tool()
    async def continue_after_process(process_id: str, session_id: str, idempotency_key: str | None = None) -> str:
        """Continue this task after its managed process exits (any exit code), once tools/processes are idle."""
        binding = state.require_terminal(session_id)
        return json.dumps(state.continue_after_process(binding['thread_id'], process_id, idempotency_key))

    original_spawn = core._spawn_workload
    async def spawn(*args, **kwargs):
        process, unit_name = await original_spawn(*args, **kwargs)
        binding = state.require_terminal(kwargs['session'].session_id)
        operation_id = 'process-' + uuid.uuid4().hex
        state.begin(binding['thread_id'], operation_id, 'process', process.pid, core.read_process_start_ticks(process.pid), process_id=kwargs['request_id'])
        async def finished():
            result = await process.wait()
            state.finish(operation_id, f'exit_code={result}')
        task = asyncio.create_task(finished())
        watchers.add(task)
        task.add_done_callback(watchers.discard)
        return process, unit_name
    core._spawn_workload = spawn

    async def health(_):
        return JSONResponse({'service': 't3-terminal-mcp', 'ready': True, 'projectScanning': False})
    mcp.custom_route('/healthz', methods=['GET'])(health)

    async def control(request):
        if not secrets.compare_digest(request.headers.get('authorization', ''), 'Bearer ' + control_token):
            return JSONResponse({'error': 'Unauthorized'}, status_code=401)
        try:
            action = request.path_params['action']
            if request.method == 'GET':
                environment_id = state.identifier(request.query_params.get('environmentId'))
                provider_id = state.identifier(request.query_params.get('providerId'))
                after_seq = max(0, int(request.query_params.get('afterSeq', '0')))
                wait = min(25, max(0, float(request.query_params.get('waitSeconds', '0'))))
                state.changed.clear()
                events = state.events(environment_id, provider_id, after_seq)
                if not events and wait:
                    try:
                        await asyncio.wait_for(state.changed.wait(), wait)
                    except asyncio.TimeoutError:
                        pass
                    events = state.events(environment_id, provider_id, after_seq)
                return JSONResponse({'events': events})
            body = await request.json()
            if action == 'register':
                result = state.register(thread_id=body['threadId'], terminal_id=body.get('terminalThreadId'),
                    environment_id=body['environmentId'], provider_id=body['providerId'], cwd=body['cwd'], model=body['model'], inactivity_seconds=body.get('inactivitySeconds'))
                if body.get('inactivitySeconds') is not None:
                    state.arm(body['threadId'])
            elif action in ('claim', 'ack'):
                fn = state.claim if action == 'claim' else state.acknowledge
                result = fn(body['eventId'], body['environmentId'], body['providerId'])
            else:
                binding = state.get(body['threadId'])
                if (binding['environment_id'], binding['provider_id']) != (body['environmentId'], body['providerId']):
                    raise ValueError('Thread belongs to another T3 environment/provider')
                if action == 'bind':
                    result = state.bind(body['threadId'], body['conversationId'])
                elif action == 'activate':
                    result = state.activate(body['threadId'], body['requestId'])
                elif action == 'pause':
                    result = state.pause(body['threadId'], body['requestId'])
                elif action == 'get':
                    result = binding
                else:
                    return JSONResponse({'error': 'Unknown action'}, status_code=404)
            return JSONResponse(result or {'ok': True})
        except (KeyError, ValueError, TypeError, sqlite3.IntegrityError) as exc:
            return JSONResponse({'error': str(exc)}, status_code=409)

    import sqlite3
    private = Starlette(routes=[Route('/v1/{action}', control, methods=['GET', 'POST'])])
    return mcp, private


def build_public_app(core, mcp):
    """Build the public T3 app with the legacy usage ledger dashboard only.

    The T3 flavor deliberately has no watchdog/project scanner or agent
    orchestration surface, so those optional dashboard capabilities stay off.
    """
    app = mcp.streamable_http_app()
    core.install_usage_dashboard(app, core.GPT_STORE)
    return app


async def serve(public_port: int, control_port: int):
    import terminal_mcp as core
    state = ThreadState(Path(os.environ['T3_TERMINAL_DB']), float(os.environ.get('T3_TERMINAL_IDLE_SECONDS', '300')))
    token = Path(os.environ['T3_TERMINAL_CONTROL_TOKEN_FILE']).read_text().strip()
    mcp, private = build(core, state, token)
    public_server = uvicorn.Server(uvicorn.Config(build_public_app(core, mcp), host='127.0.0.1', port=public_port, log_level='info'))
    private_server = uvicorn.Server(uvicorn.Config(private, host='127.0.0.1', port=control_port, log_level='info'))
    await asyncio.gather(public_server.serve(), private_server.serve())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=8021)
    parser.add_argument('--control-port', type=int, default=8022)
    args = parser.parse_args()
    asyncio.run(serve(args.port, args.control_port))
