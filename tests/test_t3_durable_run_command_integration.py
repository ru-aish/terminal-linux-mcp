"""T3 foreground presentation, durable receipts and native encoded thread IDs."""
from __future__ import annotations

import asyncio
import json
import re
import time

import pytest

from t3_thread_state import ThreadState


def decoded(value):
    if isinstance(value, tuple):
        return value[0][0].text
    assert not value.isError, str(value)
    return value.content[0].text


def job_id(value):
    match = re.search(r"^Process ID: ([0-9a-f]{64})$", decoded(value), re.M)
    assert match, str(value)
    return match.group(1)


@pytest.mark.parametrize("kind", ["quick", "detached", "timeout"])
def test_run_command_uses_same_supervisor_and_separate_deadlines(tmp_path, monkeypatch, kind):
    async def scenario():
        import terminal_mcp as core
        import t3_terminal_server as server

        monkeypatch.setenv("MCP_GPT_HOME", str(tmp_path / "gpt"))
        monkeypatch.setenv("MCP_T3_DURABLE_JOBS_DIR", str(tmp_path / "durable"))
        monkeypatch.setenv("MCP_T3_DURABLE_LAUNCH_MODE", "process")
        monkeypatch.setattr(server, "FOREGROUND_DETACH_SECONDS", 1.0 if kind == "quick" else .12)
        for name in ("mcp", "_spawn_workload", "_set_startup_instructions", "_core_behavior_prompt"):
            monkeypatch.setattr(core, name, getattr(core, name))
        monkeypatch.setattr(core, "WORKSPACE_DIR", tmp_path)

        state = ThreadState(tmp_path / "state.sqlite", inactivity_seconds=3600)
        # This exact format previously failed validation and was truncated to 128 chars.
        thread = "thread:delegated-task:command%3Amcp%3A" + "x" * 185
        state.register(thread_id=thread, terminal_id=thread, environment_id="test-env",
                       provider_id="chatgpt", cwd=str(tmp_path), model="gpt-6")
        state.activate(thread, "activation-1")
        mcp, _ = server.build(core, state, "test-token")
        await mcp.call_tool("bootstrap_thread", {"thread_id": thread,
                                                 "cwd": str(tmp_path), "max_chars": 0})
        command = {
            "quick": "printf FAST; exit 4",
            "detached": "sleep 0.45; printf LATE; exit 6",
            "timeout": "sleep 2; printf SHOULD_NOT_PRINT",
        }[kind]
        args = {"command": command, "session_id": thread, "idempotency_key": "idempotent",
                "cwd": str(tmp_path)}
        if kind == "timeout":
            args["timeout"] = .28
        begun = time.monotonic()
        returned = await mcp.call_tool("run_command", args)
        duration = time.monotonic() - begun
        result = decoded(returned)
        pid = job_id(returned)
        assert core._normalize_session_id(thread) == thread
        assert len(thread) > 128
        assert state.require_terminal(thread)["thread_id"] == thread
        assert duration < 1.5
        if kind == "quick":
            assert "Exit Code: 4" in result and "FAST" in result
            assert "detached after foreground budget" not in result
        else:
            assert "detached after foreground budget" in result
            assert state.active(thread)  # process is still outstanding
            done = json.loads(decoded(await mcp.call_tool("thread_task", {
                "action": "complete", "session_id": thread, "evidence": ["Detached from parent"]})))
            assert done["state"] == "done"

        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            status = json.loads(decoded(await mcp.call_tool("durable_process_status", {
                "session_id": thread, "job_id": pid})))
            if status["result"] is not None:
                break
            await asyncio.sleep(.025)
        assert status["result"] is not None
        assert status["result"]["exit_code"] == {"quick": 4, "detached": 6, "timeout": -15}[kind]
        assert status["result"]["status"] == ("timed_out" if kind == "timeout" else "exited")
        retry = await mcp.call_tool("run_command", args)
        assert job_id(retry) == pid
        await asyncio.sleep(.12)
        completed = [e for e in state.events("test-env", "chatgpt") if e["kind"] == "process.completed"]
        if kind == "quick":
            assert not completed
        else:
            assert len(completed) == 1, completed
            assert completed[0]["payload"]["processId"] == pid
            assert completed[0]["payload"]["exitCode"] == status["result"]["exit_code"]
        state.close()
    asyncio.run(scenario())


def test_restart_replays_completion_receipt_once_after_parent_done(tmp_path, monkeypatch):
    async def scenario():
        import terminal_mcp as core
        import t3_terminal_server as server

        monkeypatch.setenv("MCP_GPT_HOME", str(tmp_path / "gpt"))
        monkeypatch.setenv("MCP_T3_DURABLE_JOBS_DIR", str(tmp_path / "durable"))
        monkeypatch.setenv("MCP_T3_DURABLE_LAUNCH_MODE", "process")
        monkeypatch.setattr(server, "FOREGROUND_DETACH_SECONDS", .10)
        for name in ("mcp", "_spawn_workload", "_set_startup_instructions", "_core_behavior_prompt"):
            monkeypatch.setattr(core, name, getattr(core, name))
        monkeypatch.setattr(core, "WORKSPACE_DIR", tmp_path)

        state_path = tmp_path / "state.sqlite"
        original = ThreadState(state_path, inactivity_seconds=3600)
        thread = "thread:delegated-task:command%3Amcp%3A" + "y" * 185
        original.register(thread_id=thread, environment_id="test-env",
                          provider_id="chatgpt", cwd=str(tmp_path), model="gpt-6")
        original.activate(thread, "request-1")
        mcp, _ = server.build(core, original, "token")
        async with mcp.settings.lifespan(mcp):
            await mcp.call_tool("bootstrap_thread", {
                "thread_id": thread, "cwd": str(tmp_path), "max_chars": 0})
            returned = await mcp.call_tool("run_command", {
                "session_id": thread, "cwd": str(tmp_path,
                ), "idempotency_key": "restart", "command": "sleep .6; printf RESTARTED; exit 9"})
            pid = job_id(returned)
            assert "detached" in decoded(returned)
            assert json.loads(decoded(await mcp.call_tool("thread_task", {
                "session_id": thread, "action": "complete", "evidence": ["Detached"]})))["state"] == "done"

        # A closed MCP lifespan has no active receipt watcher. The child persists.
        await asyncio.sleep(.8)
        restarted = ThreadState(state_path, inactivity_seconds=3600)
        mcp2, _ = server.build(core, restarted, "token")
        async with mcp2.settings.lifespan(mcp2):
            for _ in range(100):
                events = [e for e in restarted.events("test-env", "chatgpt")
                          if e["kind"] == "process.completed"]
                if events:
                    break
                await asyncio.sleep(.02)
            assert len(events) == 1
            assert events[0]["payload"]["processId"] == pid
            assert events[0]["payload"]["exitCode"] == 9
        assert len([e for e in ThreadState(state_path).events("test-env", "chatgpt")
                    if e["kind"] == "process.completed"]) == 1
    asyncio.run(scenario())
