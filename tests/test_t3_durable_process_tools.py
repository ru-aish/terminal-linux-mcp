"""Focused boundary tests for T3-owned durable process tools."""
from __future__ import annotations

import asyncio
import json
import time

import pytest

from t3_thread_state import ThreadState


def decoded(result):
    # T3FastMCP returns an error result for rejected identities, but the
    # underlying MCP tool manager returns (content, structured) on success.
    if isinstance(result, tuple):
        content, _structured = result
    else:
        assert not result.isError, result
        content = result.content
    return json.loads(next(part.text for part in content if part.type == "text"))


def test_t3_durable_tools_survive_rebuild_and_reject_cross_thread_access(tmp_path, monkeypatch):
    async def run():
        monkeypatch.setenv("MCP_GPT_HOME", str(tmp_path / "gpt"))
        monkeypatch.setenv("MCP_T3_DURABLE_JOBS_DIR", str(tmp_path / "durable"))
        monkeypatch.setenv("MCP_T3_DURABLE_LAUNCH_MODE", "process")
        import terminal_mcp as core
        from t3_terminal_server import build

        for name in ("mcp", "_spawn_workload", "_set_startup_instructions", "_core_behavior_prompt"):
            monkeypatch.setattr(core, name, getattr(core, name))
        monkeypatch.setattr(core, "WORKSPACE_DIR", tmp_path)
        project = tmp_path / "project"
        project.mkdir()
        other = tmp_path / "other"
        other.mkdir()
        state = ThreadState(tmp_path / "state.db", inactivity_seconds=3600)
        state.register(thread_id="thread-one", terminal_id="thread-one", environment_id="env",
                       provider_id="chatgpt", cwd=str(project), model="test")
        state.register(thread_id="thread-two", terminal_id="thread-two", environment_id="env",
                       provider_id="chatgpt", cwd=str(other), model="test")
        state.activate("thread-one", "activation-one")
        mcp, _ = build(core, state, "test-token")
        names = {tool.name for tool in await mcp.list_tools()}
        assert {"start_durable_process", "durable_process_status", "durable_process_output",
                "list_durable_processes", "stop_durable_process"} <= names
        await mcp.call_tool("bootstrap_thread",
                            {"thread_id": "thread-one", "cwd": str(project), "max_chars": 0})
        denied = await mcp.call_tool("start_durable_process",
                                     {"command": "touch denied", "idempotency_key": "deny",
                                      "session_id": "unknown", "cwd": str(project)})
        assert denied.isError
        assert not (project / "denied").exists()
        with pytest.raises(Exception, match="cwd must remain inside"):
            await mcp.call_tool("start_durable_process",
                                {"command": "touch out-of-workspace",
                                 "idempotency_key": "outside", "session_id": "thread-one",
                                 "cwd": str(other)})

        arguments = {"command": "printf DURABLE; printf STDERR >&2; exit 11",
                     "idempotency_key": "one", "session_id": "thread-one", "cwd": str(project)}
        first = decoded(await mcp.call_tool("start_durable_process", arguments))
        assert first["job_id"]
        retry = decoded(await mcp.call_tool("start_durable_process", arguments))
        assert retry["job_id"] == first["job_id"]
        with pytest.raises(Exception, match="Unknown job for this owner"):
            await mcp.call_tool("durable_process_status",
                                {"session_id": "thread-two", "job_id": first["job_id"]})
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            item = decoded(await mcp.call_tool("durable_process_status", {
                "session_id": "thread-one", "job_id": first["job_id"]}))
            if item["result"] is not None:
                break
            await asyncio.sleep(0.025)
        else:
            pytest.fail("Durable receipt did not appear")
        assert item["result"]["exit_code"] == 11
        result = decoded(await mcp.call_tool("durable_process_output", {
            "session_id": "thread-one", "job_id": first["job_id"], "stream": "stderr"}))
        assert result["text"] == "STDERR"
        result = decoded(await mcp.call_tool("durable_process_output", {
            "session_id": "thread-one", "job_id": first["job_id"], "stream": "stdout"}))
        assert result["text"] == "DURABLE"

        # Rebuild server wrapper with the same state directory; no restart spawn.
        second, _ = build(core, state, "test-token")
        listed = decoded(await second.call_tool("list_durable_processes", {"session_id": "thread-one"}))
        assert len(listed) == 1 and listed[0]["result"]["exit_code"] == 11
        state.close()

    asyncio.run(run())
