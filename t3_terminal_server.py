"""T3-only Terminal MCP. Public tools and private control plane use separate ports."""
from __future__ import annotations

import argparse
import asyncio
import contextvars
import json
import math
import time
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
from durable_process_supervisor import DurableProcessSupervisor, _job_id

FOREGROUND_DETACH_SECONDS = 180.0
CURRENT_OPERATION = contextvars.ContextVar('t3_terminal_operation', default=None)


def build(core, state: ThreadState, control_token: str):
    if not control_token:
        raise ValueError('Private control-plane token must be configured')
    watchers: set[asyncio.Task] = set()
    durable_jobs = DurableProcessSupervisor(
        Path(os.environ.get('MCP_T3_DURABLE_JOBS_DIR', str(state.path.parent / 'durable-jobs'))),
        launch_mode=os.environ.get('MCP_T3_DURABLE_LAUNCH_MODE', 'systemd'),
    )

    # The native T3 conversation ID is often longer than 128 bytes and includes
    # percent-encoded separators. Generic Terminal MCP must not truncate it.
    core._normalize_session_id = lambda value: (value or 'default').strip() or 'default'
    tracked_jobs: set[str] = set()

    async def supervise_durable(thread_id: str, job_id: str):
        operation_id = 'durable:' + job_id
        try:
            while True:
                job = durable_jobs.status(job_id, owner=thread_id)
                if job['result'] is not None:
                    state.finish(operation_id, json.dumps(job['result']), receipt={**job['result'],
                               'stdoutPath': job['stdout_path'], 'stderrPath': job['stderr_path']})
                    return
                if job['status'] == 'outcome_unknown':
                    state.finish(operation_id, 'outcome_unknown',
                                 receipt={'status': 'outcome_unknown', 'exit_code': None})
                    return
                await asyncio.sleep(.1)
        finally:
            tracked_jobs.discard(job_id)

    def watch_durable(thread_id: str, job_id: str):
        if job_id not in tracked_jobs:
            tracked_jobs.add(job_id)
            task = asyncio.create_task(supervise_durable(thread_id, job_id))
            watchers.add(task)
            task.add_done_callback(watchers.discard)

    def track_durable(thread_id: str, job: dict, *, detached: bool = False):
        state.ensure_durable_process(thread_id, job['job_id'], detached=detached)
        watch_durable(thread_id, job['job_id'])
        return job

    async def start_durable(thread_id: str, session_id: str, command: str,
                            cwd: str | None, env: dict[str, str] | None,
                            idempotency_key: str | None, timeout: float | None = None,
                            *, detached: bool = False, track_cwd: bool = False):
        session = await core._get_session(session_id)
        binding = state.require_terminal(session_id)
        workspace = Path(binding['cwd']).resolve()
        run_cwd = core._resolve_cwd(cwd, session.cwd)
        if not run_cwd.is_relative_to(workspace):
            raise ValueError('Durable job cwd must remain inside the bound T3 workspace')
        gate = core._require_project_context(session, run_cwd)
        if gate:
            raise ValueError(gate)
        # A unique MCP invocation ID is the fallback when the caller does not
        # supply a stable key for retries. Never derive identity from command.
        key = idempotency_key if idempotency_key is not None else CURRENT_OPERATION.get()
        if not key:
            key = uuid.uuid4().hex
        if track_cwd:
            marker = "__MCP_PWD_" + _job_id(thread_id, key) + "__"
            command = (f'{command}\n__mcp_status=$?\n'
                       f'printf "\\n{marker}:%s\\n" "$PWD"\n'
                       'exit "$__mcp_status"')
        launch = asyncio.create_task(asyncio.to_thread(
            durable_jobs.start, command, owner=thread_id, cwd=run_cwd,
            env=core._build_env(session, env), idempotency_key=key, hard_timeout=timeout,
        ))
        try:
            job = await asyncio.shield(launch)
        except asyncio.CancelledError:
            # The job might already have launched on another thread. A client
            # interruption must not abandon the durable SQLite association.
            async def register_late_launch():
                try:
                    launched = await launch
                    track_durable(thread_id, launched, detached=True)
                except Exception:
                    # The registry is also reconciled on MCP startup.
                    return
            task = asyncio.create_task(register_late_launch())
            watchers.add(task)
            task.add_done_callback(watchers.discard)
            raise
        return track_durable(thread_id, job, detached=detached)

    def format_command(job: dict, max_chars: int):
        process_id = job['job_id']
        result = job['result']
        lines = [f'Process ID: {process_id}', f'Status: {job["status"]}',
                 f'Working Directory: {job["cwd"]}']
        if result is not None:
            lines.append(f'Exit Code: {result.get("exit_code")}')
            lines.append(f'Result: {result.get("status")}')
        elif job['status'] == 'outcome_unknown':
            lines.append('Outcome: unknown; supervisor or exit receipt was lost.')
        else:
            lines.append('Presentation: detached after foreground budget; execution continues.')
        # Log paths must survive truncation of large command output.
        lines.append('Durable Output Logs:\n' + job['stdout_path'] + '\n' + job['stderr_path'])
        header = '\n'.join(lines)
        remaining = max_chars - len(header) - len('\n[Output truncated]') - 16
        if remaining <= 0:
            return header[:max_chars]
        output = durable_jobs.output(process_id, owner=job['owner'],
                                     max_bytes=min(262144, remaining))
        output['stdout']['text'], _ = core._strip_pwd_marker(
            output['stdout']['text'], "__MCP_PWD_" + process_id + "__")
        for name in ('stdout', 'stderr'):
            value = output[name]['text']
            if not value or remaining <= 0:
                continue
            prefix = '\n' + name.upper() + ':\n'
            if len(prefix) + len(value) > remaining:
                lines.append(prefix + value[:max(0, remaining - len(prefix))] +
                             '\n[Output truncated]')
                break
            lines.append(prefix + value)
            remaining -= len(prefix) + len(value)
        return '\n'.join(lines)[:max_chars]

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
        # Recover only this registered T3 environment's durable job registry.
        # A lost MCP response may precede the SQLite operation insert; discover
        # such launches without executing them again.
        with state.connect() as db:
            owners = [r[0] for r in db.execute('SELECT thread_id FROM bindings')]
        for owner in owners:
            for job in durable_jobs.discover(owner=owner):
                track_durable(owner, job, detached=True)
        for row in rows:
            if row['durable']:
                # The foreground presentation was lost on MCP restart.
                # Its supervisor, logs and eventual receipt are independent.
                state.mark_detached(row['thread_id'], row['process_id'])
                watch_durable(row['thread_id'], row['process_id'])
                continue
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

    instructions = '''This is the T3-specific Terminal MCP. Bootstrap using exactly the permanent Terminal MCP ID injected by T3; use it as session_id on every tool call. Unregistered IDs are rejected. Never invent IDs. T3 owns ChatGPT prompts and streaming; Terminal MCP owns task working/done and inactivity recovery. Use schedule_continue with exactly one of after_seconds or wake_at (Unix seconds or timezone-aware ISO-8601), or continue_after_process with a managed process_id, to resume this task when no blocking tools/processes are active. These durable continuations belong to the current task epoch; pause, completion, or a new activation invalidates them. Reuse an idempotency_key to retry scheduling safely. When all requested work is verified, call thread_task(action="complete", evidence=[...]). Intentionally long-lived background processes such as development servers may remain running after explicit completion; completion detaches them from thread-idleness accounting while leaving them controllable. If completion is not called, active processes keep the task working exactly as before. End of model output does not finish the task. For a pending question, call thread_task(action="wait", reason="..."). There are no project scanning, watchdog, scheduled-send, or ChatGPT runtime-control tools here.'''
    mcp = T3FastMCP('T3 Terminal MCP', instructions=instructions, lifespan=lifespan,
                    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False))
    excluded = {'thread_goal', 'refresh_startup_context'}
    for name, tool in core.mcp._tool_manager._tools.items():
        if not name.startswith(('agent_', 'chat_runtime_')) and name not in excluded:
            mcp.add_tool(tool.fn, name=tool.name, description=tool.description)
    mcp.remove_tool('run_command')

    @mcp.tool()
    async def run_command(
        command: str, timeout: float | None = None, session_id: str = '',
        cwd: str | None = None, max_output_chars: int = 24000,
        env: dict[str, str] | None = None, idempotency_key: str | None = None,
    ) -> str:
        """Run under a durable supervisor; after 180 seconds detach only the tool presentation.

        timeout is an independent hard execution deadline, enforced inside the
        surviving supervisor even after an MCP restart. Provide a stable
        idempotency_key to deduplicate client retries.
        """
        session = await core._get_session(session_id)
        async with session.lock:
            binding = state.require_terminal(session_id)
            if timeout is not None:
                timeout = float(timeout)
                if not math.isfinite(timeout) or timeout <= 0:
                    raise ValueError('timeout must be positive and finite')
            if max_output_chars < 128:
                raise ValueError('max_output_chars must be at least 128')
            deadline = time.monotonic() + FOREGROUND_DETACH_SECONDS
            job = await start_durable(binding['thread_id'], session_id, command,
                                      cwd, env, idempotency_key, timeout, track_cwd=True)
            while True:
                job = durable_jobs.status(job['job_id'], owner=binding['thread_id'])
                if job['result'] is not None or job['status'] == 'outcome_unknown':
                    if job['result'] is not None and job['result']['status'] == 'exited':
                        # A trailing shell marker captures "cd" even though
                        # each job is launched in a fresh process. It is stable
                        # across retries because it derives from the job ID.
                        end = Path(job['stdout_path']).stat().st_size
                        tail = durable_jobs.output(
                            job['job_id'], owner=binding['thread_id'],
                            stdout_offset=max(0, end - 16384),
                            max_bytes=16384)['stdout']['text']
                        _, updated = core._strip_pwd_marker(
                            tail, "__MCP_PWD_" + job['job_id'] + "__")
                        if updated:
                            candidate = Path(updated).expanduser().resolve()
                            if candidate.is_dir() and candidate.is_relative_to(
                                    Path(binding['cwd']).resolve()):
                                session.cwd = candidate
                        session.command_count += 1
                        session.updated_at = time.time()
                    if job['result'] is not None:
                        state.finish('durable:' + job['job_id'],
                                     json.dumps(job['result']), receipt={**job['result'],
                                         'stdoutPath': job['stdout_path'], 'stderrPath': job['stderr_path']})
                    else:
                        state.finish('durable:' + job['job_id'], 'outcome_unknown',
                                     receipt={'status': 'outcome_unknown', 'exit_code': None})
                    return format_command(job, max_output_chars)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if state.mark_detached(binding['thread_id'], job['job_id']):
                        return format_command(job, max_output_chars)
                    # A receipt won the finish/detach race; return its terminal status.
                    return format_command(durable_jobs.status(job['job_id'],
                                            owner=binding['thread_id']), max_output_chars)
                await asyncio.sleep(min(.04, remaining))

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
        """Continue this task after its managed process exits (any exit code), once blocking tools/processes are idle."""
        binding = state.require_terminal(session_id)
        return json.dumps(state.continue_after_process(binding['thread_id'], process_id, idempotency_key))


    @mcp.tool()
    async def start_durable_process(command: str, idempotency_key: str, session_id: str,
                                    cwd: str | None = None, env: dict[str, str] | None = None) -> str:
        """Launch one restart-discoverable background job; retries never launch a second."""
        binding = state.require_terminal(session_id)
        job = await start_durable(binding['thread_id'], session_id, command,
                                  cwd, env, idempotency_key, detached=True)
        return json.dumps(job)

    @mcp.tool()
    async def durable_process_status(job_id: str, session_id: str) -> str:
        """Read a supervised job's durable identity and exit receipt; never relaunch it."""
        binding = state.require_terminal(session_id)
        return json.dumps(durable_jobs.status(job_id, owner=binding['thread_id']))

    @mcp.tool()
    async def durable_process_output(job_id: str, session_id: str, stream: str = 'stdout',
                                     offset: int = 0, limit: int = 65536) -> str:
        """Read a bounded byte range of durable stdout/stderr after an MCP restart."""
        binding = state.require_terminal(session_id)
        if stream not in ('stdout', 'stderr'):
            raise ValueError('stream must be stdout or stderr')
        return json.dumps(durable_jobs.output(job_id, owner=binding['thread_id'],
            stdout_offset=offset if stream == 'stdout' else 0,
            stderr_offset=offset if stream == 'stderr' else 0,
            max_bytes=limit)[stream])

    @mcp.tool()
    async def list_durable_processes(session_id: str) -> str:
        """Discover this thread's persisted jobs without searching projects or spawning."""
        binding = state.require_terminal(session_id)
        return json.dumps(durable_jobs.discover(owner=binding['thread_id']))

    @mcp.tool()
    async def stop_durable_process(job_id: str, session_id: str) -> str:
        """Request identity-verified termination of a supervised job."""
        binding = state.require_terminal(session_id)
        return json.dumps(durable_jobs.stop(job_id, owner=binding['thread_id']))

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
