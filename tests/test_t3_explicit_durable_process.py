"""Public process tools reuse durable jobs, without an in-memory process owner."""
from __future__ import annotations

import asyncio
import re
import time

from test_t3_durable_run_command import setup, decode, completed_events


async def _await_event(state, job_id):
    end = time.monotonic() + 6
    while time.monotonic() < end:
        matches = [e for e in completed_events(state) if e["payload"]["processId"] == job_id]
        if matches:
            return matches
        await asyncio.sleep(.025)
    raise AssertionError("Process terminal event not observed")


def test_explicit_start_survives_parent_completion_and_t3_rebuild(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, session_id = await setup(tmp_path, monkeypatch)
        args = {"session_id": session_id, "command": "sleep .15; printf EXPLICIT_DURABLE_OK",
                "idempotency_key": "explicit-once"}
        started = decode(await mcp.call_tool("start_process", args))
        job_id = re.search(r"Process ID: ([a-f0-9]{64})", started).group(1)
        assert job_id not in core.processes
        replay = decode(await mcp.call_tool("start_process", args))
        assert f"Process ID: {job_id}" in replay
        await mcp.call_tool("thread_task", {
            "session_id": session_id, "action": "complete",
            "evidence": ["Background job remains independent"],
        })
        events = await _await_event(state, job_id)
        assert len(events) == 1
        assert events[0]["payload"]["exitCode"] == 0
        replacement, _ = t3.build(core, state, "test-token")
        polled = decode(await replacement.call_tool("poll_process", {
            "session_id": session_id, "process_id": job_id}))
        assert "Exit Code: 0" in polled
        assert "EXPLICIT_DURABLE_OK" in polled
        assert state.claim(events[0]["event_id"], "test-env", "chatgpt")["valid"]
        state.close()

    asyncio.run(scenario())


def test_explicit_stop_is_owner_fenced_and_reports_a_durable_receipt(tmp_path, monkeypatch):
    async def scenario():
        core, _, state, mcp, session_id = await setup(tmp_path, monkeypatch)
        started = decode(await mcp.call_tool("start_process", {
            "session_id": session_id, "command": "sleep 5",
            "idempotency_key": "stop-once",
        }))
        job_id = re.search(r"Process ID: ([a-f0-9]{64})", started).group(1)
        invalid = await mcp.call_tool("poll_process", {
            "session_id": "unknown", "process_id": job_id})
        assert invalid.isError
        stopped = decode(await mcp.call_tool("stop_process", {
            "session_id": session_id, "process_id": job_id}))
        assert f"Process ID: {job_id}" in stopped
        events = await _await_event(state, job_id)
        assert len(events) == 1
        assert events[0]["payload"]["status"] in (
            "cancelled", "descendants_stopped", "exited")
        polled = decode(await mcp.call_tool("poll_process", {
            "session_id": session_id, "process_id": job_id}))
        assert "Exit Code:" in polled
        state.close()

    asyncio.run(scenario())
