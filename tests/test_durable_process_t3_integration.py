from __future__ import annotations
import asyncio
import json

from t3_thread_state import ThreadState


def _result(call):
    content = call[0] if isinstance(call, tuple) else call.content
    return json.loads(content[0].text)


def test_t3_durable_tools_are_isolated_and_parent_can_complete(tmp_path, monkeypatch):
    async def scenario():
        import terminal_mcp as core
        from t3_terminal_server import build

        monkeypatch.setenv("MCP_T3_DURABLE_JOBS_DIR", str(tmp_path / "durable"))
        monkeypatch.setenv("MCP_T3_DURABLE_LAUNCH_MODE", "process")
        monkeypatch.setenv("MCP_GPT_HOME", str(tmp_path / "gpt-home"))
        for name in ("mcp", "_spawn_workload", "_set_startup_instructions", "_core_behavior_prompt"):
            monkeypatch.setattr(core, name, getattr(core, name))
        monkeypatch.setattr(core, "WORKSPACE_DIR", tmp_path)
        state = ThreadState(tmp_path / "state.sqlite", inactivity_seconds=3600)
        mcp, _private = build(core, state, "test-token")
        try:
            tools = {tool.name for tool in await mcp.list_tools()}
            assert {"start_durable_process", "durable_process_status",
                    "durable_process_output", "list_durable_processes",
                    "stop_durable_process"} <= tools

            denied = await mcp.call_tool("start_durable_process", {
                "session_id": "unregistered", "idempotency_key": "bad",
                "command": "touch ignored",
            })
            assert 'refused' in str(denied)
            assert not (tmp_path / "ignored").exists()

            state.register(thread_id="thread-one", environment_id="test-env",
                           provider_id="chatgpt", cwd=str(tmp_path), model="test-model")
            state.activate("thread-one", "request-one")
            await mcp.call_tool("bootstrap_thread", {
                "thread_id": "thread-one", "cwd": str(tmp_path), "max_chars": 0,
            })
            started = _result(await mcp.call_tool("start_durable_process", {
                "session_id": "thread-one", "idempotency_key": "job-1",
                "command": "sleep 0.3; printf done; exit 8",
            }))
            job_id = started["job_id"]
            same = _result(await mcp.call_tool("start_durable_process", {
                "session_id": "thread-one", "idempotency_key": "job-1",
                "command": "sleep 0.3; printf done; exit 8",
            }))
            assert same["job_id"] == job_id

            complete = _result(await mcp.call_tool("thread_task", {
                "session_id": "thread-one", "action": "complete",
                "evidence": ["job runs independently"],
            }))
            assert complete["state"] == "done"

            deadline = asyncio.get_running_loop().time() + 7
            while asyncio.get_running_loop().time() < deadline:
                row = _result(await mcp.call_tool("durable_process_status", {
                    "session_id": "thread-one", "job_id": job_id,
                }))
                if row["result"]:
                    break
                await asyncio.sleep(0.03)
            assert row["result"]["exit_code"] == 8
            output = _result(await mcp.call_tool("durable_process_output", {
                "session_id": "thread-one", "job_id": job_id,
            }))
            assert output["text"] == "done"
            found = _result(await mcp.call_tool("list_durable_processes", {
                "session_id": "thread-one",
            }))
            assert len(found) == 1 and found[0]["job_id"] == job_id
        finally:
            state.close()
    asyncio.run(scenario())
