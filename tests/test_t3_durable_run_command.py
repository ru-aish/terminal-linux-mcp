"""T3 command presentation must not own or kill a durable supervisor job."""
from __future__ import annotations

import asyncio
import json
import re
import time

from t3_thread_state import ThreadState


def decode(value):
    parts = value[0] if isinstance(value, tuple) else value.content
    return parts[0].text


def completed_events(state):
    return [event for event in state.events("test-env", "chatgpt")
            if event["kind"] == "process.completed"]


async def setup(tmp_path, monkeypatch, *, long_id=False):
    import terminal_mcp as core
    import t3_terminal_server as t3
    monkeypatch.setenv("MCP_T3_DURABLE_JOBS_DIR", str(tmp_path / "jobs"))
    monkeypatch.setenv("MCP_T3_DURABLE_LAUNCH_MODE", "process")
    monkeypatch.setenv("MCP_GPT_HOME", str(tmp_path / "gpt"))
    for name in ("mcp", "_spawn_workload", "_normalize_session_id",
                 "_set_startup_instructions", "_core_behavior_prompt"):
        monkeypatch.setattr(core, name, getattr(core, name))
    monkeypatch.setattr(core, "WORKSPACE_DIR", tmp_path)
    state = ThreadState(tmp_path / "state.db", inactivity_seconds=3600)
    terminal_id = ("thread:delegated-task:command%3Amcp%3A" + "a" * 135
                   + "%3Adelegate-task%3Aprocess-design-v4") if long_id else "terminal-one"
    state.register(thread_id=terminal_id, terminal_id=terminal_id,
                   environment_id="test-env", provider_id="chatgpt",
                   cwd=str(tmp_path), model="test-model")
    state.activate(terminal_id, "activation-one")
    mcp, _ = t3.build(core, state, "test-token")
    await mcp.call_tool("bootstrap_thread",
                        {"thread_id": terminal_id, "cwd": str(tmp_path), "max_chars": 0})
    return core, t3, state, mcp, terminal_id


def test_short_commands_retain_exit_code_and_deduplicate_with_long_id(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch, long_id=True)
        assert len(terminal_id) > 128 and "%" in terminal_id
        marker = tmp_path / "once"
        args = {"session_id": terminal_id, "idempotency_key": "one",
                "command": f"printf x >> '{marker}'; printf hello; printf error >&2; exit 9"}
        first = decode(await mcp.call_tool("run_command", args))
        second = decode(await mcp.call_tool("run_command", args))
        assert "Exit Code: 9" in first
        assert "STDOUT:\nhello" in first
        assert "STDERR:\nerror" in first
        assert "Process ID:" in first
        assert re.search(r"Process ID: ([a-f0-9]{64})", first).group(1) == re.search(
            r"Process ID: ([a-f0-9]{64})", second).group(1)
        assert marker.read_text() == "x"
        assert state.require_terminal(terminal_id)["terminal_id"] == terminal_id
        assert core._normalize_session_id(terminal_id) == terminal_id
        state.close()
    asyncio.run(scenario())


def test_detached_presentation_parent_done_then_one_durable_event(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch)
        monkeypatch.setattr(t3, "FOREGROUND_DETACH_SECONDS", 0.065)
        output = decode(await mcp.call_tool("run_command", {
            "session_id": terminal_id, "idempotency_key": "long",
            "command": "sleep 0.38; printf finished; exit 12",
        }))
        assert "Presentation: detached" in output
        assert "Process ID:" in output
        job_id = re.search(r"Process ID: ([a-f0-9]{64})", output).group(1)
        assert not completed_events(state)
        result = decode(await mcp.call_tool("thread_task", {
            "session_id": terminal_id, "action": "complete",
            "evidence": ["The detached process continues independently"],
        }))
        assert json.loads(result)["state"] == "done"
        assert state.get(terminal_id)["state"] == "done"
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline and not completed_events(state):
            await asyncio.sleep(.04)
        events = completed_events(state)
        assert len(events) == 1
        assert events[0]["payload"]["processId"] == job_id
        assert events[0]["payload"]["status"] == "exited"
        assert events[0]["payload"]["exitCode"] == 12
        assert state.claim(events[0]["event_id"], "test-env", "chatgpt")["valid"]
        state.finish("durable:" + job_id, "duplicate_result")
        assert len(completed_events(state)) == 1
        state.close()
    asyncio.run(scenario())


def test_worker_hard_timeout_survives_presentation_detachment(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch)
        monkeypatch.setattr(t3, "FOREGROUND_DETACH_SECONDS", .045)
        output = decode(await mcp.call_tool("run_command", {
            "session_id": terminal_id, "idempotency_key": "hard-timeout",
            "command": "sleep 5; echo should-not-run", "timeout": .2,
        }))
        assert "Presentation: detached" in output
        job_id = re.search(r"Process ID: ([a-f0-9]{64})", output).group(1)
        deadline = time.monotonic() + 7
        while time.monotonic() < deadline:
            status = json.loads(decode(await mcp.call_tool("durable_process_status", {
                "session_id": terminal_id, "job_id": job_id,
            })))
            if status["result"] is not None:
                break
            await asyncio.sleep(.05)
        assert status["result"]["status"] == "timed_out"
        assert status["result"]["exit_code"] != 0
        assert "should-not-run" not in (tmp_path / "jobs" / "jobs" / job_id / "stdout.log").read_text()
        state.close()
    asyncio.run(scenario())


def test_restart_recovers_job_and_delivers_after_parent_done(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch)
        monkeypatch.setattr(t3, "FOREGROUND_DETACH_SECONDS", .045)
        # Simulate the MCP lifespan shutting down while the independently
        # supervised child keeps running.
        async with mcp.settings.lifespan(mcp):
            output = decode(await mcp.call_tool("run_command", {
                "session_id": terminal_id, "idempotency_key": "restart",
                "command": "sleep 0.65; printf restored; exit 14",
            }))
            assert "Presentation: detached" in output
            job_id = re.search(r"Process ID: ([a-f0-9]{64})", output).group(1)
            done = decode(await mcp.call_tool("thread_task", {
                "session_id": terminal_id, "action": "complete",
                "evidence": ["Background job persists"],
            }))
            assert json.loads(done)["state"] == "done"

        reopened = ThreadState(state.path, inactivity_seconds=3600)
        new_mcp, _ = t3.build(core, reopened, "test-token")
        async with new_mcp.settings.lifespan(new_mcp):
            deadline = time.monotonic() + 7
            while time.monotonic() < deadline and not completed_events(reopened):
                await asyncio.sleep(.04)
            events = completed_events(reopened)
            assert len(events) == 1
            assert events[0]["payload"]["processId"] == job_id
            assert events[0]["payload"]["exitCode"] == 14
            assert reopened.claim(events[0]["event_id"], "test-env", "chatgpt")["valid"]
            job = json.loads(decode(await new_mcp.call_tool("durable_process_status", {
                "session_id": terminal_id, "job_id": job_id,
            })))
            assert job["result"]["exit_code"] == 14
        # A subsequent restart must not redeliver the same exit receipt.
        again = ThreadState(state.path, inactivity_seconds=3600)
        more, _ = t3.build(core, again, "test-token")
        async with more.settings.lifespan(more):
            assert len(completed_events(again)) == 1
    asyncio.run(scenario())


def test_worker_lost_after_detach_reports_unknown_exactly_once(tmp_path, monkeypatch):
    async def scenario():
        import os
        import signal
        core, t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch)
        monkeypatch.setattr(t3, "FOREGROUND_DETACH_SECONDS", .045)
        output = decode(await mcp.call_tool("run_command", {
            "session_id": terminal_id, "idempotency_key": "orphan",
            "command": "sleep .45; printf finished",
        }))
        job_id = re.search(r"Process ID: ([a-f0-9]{64})", output).group(1)
        registry = t3.DurableProcessSupervisor(tmp_path / "jobs", launch_mode="process")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            job = registry.status(job_id, owner=terminal_id)
            if job["child"] is not None and job["supervisor"] is not None:
                break
            await asyncio.sleep(.01)
        else:
            assert False, "worker did not record child identity"
        os.kill(job["supervisor"]["pid"], signal.SIGKILL)
        decode(await mcp.call_tool("thread_task", {
            "session_id": terminal_id, "action": "complete",
            "evidence": ["Orphan outcome remains unknown"],
        }))
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline and not completed_events(state):
            await asyncio.sleep(.05)
        events = completed_events(state)
        assert len(events) == 1
        assert events[0]["payload"]["status"] == "outcome_unknown"
        assert events[0]["payload"]["exitCode"] is None
        assert registry.status(job_id, owner=terminal_id)["result"] is None
        assert len(completed_events(state)) == 1
        state.close()
    asyncio.run(scenario())


def test_launch_without_sqlite_registration_recovered_without_resubmitting(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch)
        registry = t3.DurableProcessSupervisor(tmp_path / "jobs", launch_mode="process")
        counter = tmp_path / "counter"
        launched = registry.start(f"sleep .35; printf x >> {counter}",
                                  owner=terminal_id, cwd=tmp_path,
                                  idempotency_key="launch-before-db")
        job_id = launched["job_id"]
        assert not state.active(terminal_id)
        state.complete(terminal_id, ["Task can complete despite lost MCP launch reply"])
        state.close()
        reopened = ThreadState(tmp_path / "state.db", inactivity_seconds=3600)
        rebuilt, _ = t3.build(core, reopened, "test-token")
        async with rebuilt.service_lifespan(rebuilt):
            deadline = time.monotonic() + 7
            while time.monotonic() < deadline and not completed_events(reopened):
                await asyncio.sleep(.04)
            events = completed_events(reopened)
            assert len(events) == 1
            assert events[0]["payload"]["processId"] == job_id
            assert events[0]["payload"]["exitCode"] == 0
            assert counter.read_text() == "x"
        assert registry.discover(owner=terminal_id)[0]["job_id"] == job_id
    asyncio.run(scenario())


def test_cancel_during_launch_still_registers_one_durable_job(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch)
        real_start = t3.DurableProcessSupervisor.start
        marker = tmp_path / "started"
        def delayed_start(self, *args, **kwargs):
            time.sleep(.12)
            return real_start(self, *args, **kwargs)
        monkeypatch.setattr(t3.DurableProcessSupervisor, "start", delayed_start)
        launch = asyncio.create_task(mcp.call_tool("run_command", {
            "session_id": terminal_id, "idempotency_key": "cancel-during-launch",
            "command": f"printf x >> '{marker}'; sleep .1",
        }))
        await asyncio.sleep(.02)
        launch.cancel()
        try:
            await launch
        except asyncio.CancelledError:
            pass
        deadline = time.monotonic() + 7
        while time.monotonic() < deadline:
            with state.connect() as db:
                records = db.execute(
                    "SELECT operation_id,finished FROM operations WHERE durable=1"
                ).fetchall()
            if len(records) == 1 and records[0]["finished"] is not None:
                break
            await asyncio.sleep(.04)
        assert len(records) == 1
        assert records[0]["finished"] is not None
        assert marker.read_text() == "x"
        assert len(completed_events(state)) == 1
        state.close()
    asyncio.run(scenario())


def test_large_quick_output_keeps_durable_log_paths(tmp_path, monkeypatch):
    async def scenario():
        _core, _t3, state, mcp, terminal_id = await setup(tmp_path, monkeypatch)
        text = decode(await mcp.call_tool("run_command", {
            "session_id": terminal_id,
            "idempotency_key": "large-output",
            "command": "python3 -c \"print('Q' * 7000)\"",
            "max_output_chars": 950,
        }))
        assert len(text) <= 950
        assert "Process ID:" in text
        assert "Exit Code: 0" in text
        assert "Durable Output Logs:" in text
        assert "stdout.log" in text and "stderr.log" in text
        assert "[Output truncated]" in text
        state.close()
    asyncio.run(scenario())
