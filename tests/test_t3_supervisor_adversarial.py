"""Adversarial guarantees for process group containment and T3 session semantics."""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from durable_process_supervisor import DurableProcessSupervisor, _running_group_members
from test_t3_durable_run_command import setup, decode


def terminal(registry, job_id, owner="owner", deadline=8):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        result = registry.status(job_id, owner=owner)
        if result["result"] is not None:
            return result
        time.sleep(.03)
    raise AssertionError(f"No receipt: {result}")


def resistant_command(tmp_path, marker_name):
    marker = tmp_path / marker_name
    pid_path = tmp_path / (marker_name + ".pid")
    code = (
        "import os,signal,time;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        f"open({str(pid_path)!r},'w').write(str(os.getpid()));"
        "time.sleep(3);"
        f"open({str(marker)!r},'w').write('SURVIVED')"
    )
    return f"{sys.executable} -c {code!r} & wait", marker, pid_path


def test_timeout_kills_term_ignoring_grandchild_after_shell_exits(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "jobs")
    command, marker, pid_path = resistant_command(tmp_path, "timeout-leftover")
    job = registry.start(command, owner="owner", cwd=tmp_path,
                         idempotency_key="timeout", hard_timeout=.9)
    result = terminal(registry, job["job_id"])
    assert pid_path.exists(), "Child never started"
    assert result["result"]["status"] == "timed_out"
    assert result["result"]["exit_code"] is not None
    assert not marker.exists()
    assert not _running_group_members(result["child"]["pid"])


def test_stop_enforces_escalation_and_verified_cancellation(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "jobs")
    command, marker, pid_path = resistant_command(tmp_path, "stop-leftover")
    job = registry.start(command, owner="owner", cwd=tmp_path, idempotency_key="stop")
    end = time.monotonic() + 4
    while not pid_path.exists() and time.monotonic() < end:
        time.sleep(.02)
    assert pid_path.exists()
    registry.stop(job["job_id"], owner="owner")
    result = terminal(registry, job["job_id"])
    assert result["result"]["status"] in ("cancelled", "descendants_stopped")
    assert result["result"]["exit_code"] is not None
    assert not marker.exists()
    assert not _running_group_members(result["child"]["pid"])


def test_stop_after_exit_does_not_relabel_success_as_cancelled(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "jobs")
    job = registry.start("exit 0", owner="owner", cwd=tmp_path, idempotency_key="natural")
    finished = terminal(registry, job["job_id"])
    assert finished["result"]["status"] == "exited"
    assert registry.stop(job["job_id"], owner="owner")["result"]["status"] == "exited"


def test_systemd_worker_receives_environment_over_ephemeral_same_uid_socket(tmp_path, monkeypatch):
    import durable_process_supervisor as module

    manager = DurableProcessSupervisor(tmp_path / "jobs", launch_mode="systemd")
    override = "secret-value-not-for-argv-or-json"
    called = []

    def simulated_systemd(argv, **kwargs):
        called.append((argv, kwargs))
        assert argv[0] == "systemd-run"
        assert "--property=KillMode=control-group" in argv
        assert override not in repr(argv)
        assert "env" in kwargs and override not in repr(kwargs["env"])
        command = argv[argv.index("--") + 1:]
        subprocess.Popen(command, cwd=kwargs["cwd"],
                         env={"PATH": os.environ.get("PATH", "/usr/bin"), "HOME": str(tmp_path)},
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, start_new_session=True)
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(module.subprocess, "run", simulated_systemd)
    job = manager.start("printf '%s/%s' \"${#USER_SECRET}\" \"${#SESSION_SECRET}\"",
                        owner="owner", cwd=tmp_path, idempotency_key="broker",
                        env={"USER_SECRET": override, "SESSION_SECRET": "session-secret"})
    settled = terminal(manager, job["job_id"])
    assert settled["status"] == "exited" and settled["result"]["exit_code"] == 0
    assert manager.output(job["job_id"], owner="owner")["stdout"]["text"] == (
        f"{len(override)}/{len('session-secret')}")
    directory = manager._directory(job["job_id"])
    for name in ("request.json", "result.json", "child.json", "supervisor.json"):
        assert override not in (directory / name).read_text()
        assert "session-secret" not in (directory / name).read_text()
    assert len(called) == 1


def test_t3_cd_marker_and_session_env_and_foreground_serialization(tmp_path, monkeypatch):
    async def scenario():
        core, t3, state, mcp, session_id = await setup(tmp_path, monkeypatch)
        sub = tmp_path / "sub"
        sub.mkdir()
        session = await core._get_session(session_id)
        session.env["SESSION_SECRET"] = "abcdef"
        first = decode(await mcp.call_tool("run_command", {
            "command": "cd sub", "session_id": session_id, "idempotency_key": "cd"}))
        assert "Exit Code: 0" in first
        assert "__MCP_PWD_" not in first
        assert session.cwd == sub
        replay = decode(await mcp.call_tool("run_command", {
            "command": "cd sub", "session_id": session_id, "idempotency_key": "cd"}))
        assert first.split("Process ID: ", 1)[1].splitlines()[0] == (
            replay.split("Process ID: ", 1)[1].splitlines()[0])
        assert session.cwd == sub
        # A newly entered project root still requires its own GPT context gate.
        await mcp.call_tool("bootstrap_thread", {
            "thread_id": session_id, "cwd": str(sub), "max_chars": 0})
        second = decode(await mcp.call_tool("run_command", {
            "command": "pwd; printf '%s/%s' \"${#SESSION_SECRET}\" \"${#CALL_SECRET}\"",
            "env": {"CALL_SECRET": "123456789"}, "session_id": session_id,
            "idempotency_key": "pwd"}))
        assert str(sub) in second
        assert "6/9" in second
        assert session.cwd == sub
        order = sub / "order"
        a = asyncio.create_task(mcp.call_tool("run_command", {
            "command": "sleep .26; printf A >> order",
            "session_id": session_id, "idempotency_key": "first"}))
        await asyncio.sleep(.04)
        b = asyncio.create_task(mcp.call_tool("run_command", {
            "command": "printf B >> order",
            "session_id": session_id, "idempotency_key": "second"}))
        await asyncio.gather(a, b)
        assert order.read_text() == "AB", "Concurrent foreground commands interleaved"
        state.close()
    asyncio.run(scenario())


def test_hard_timeout_still_kills_grandchild_when_shell_exits_first(tmp_path):
    manager = DurableProcessSupervisor(tmp_path / "jobs")
    command, marker, pid_path = resistant_command(tmp_path, "background-after-shell")
    command = command.removesuffix(" & wait") + " & printf shell-done"
    job = manager.start(command, owner="owner", cwd=tmp_path,
                        idempotency_key="background-timeout", hard_timeout=.9)
    completed = terminal(manager, job["job_id"])
    assert pid_path.exists()
    assert completed["result"]["status"] == "timed_out"
    assert completed["result"]["exit_code"] == 0  # Primary shell exited first.
    assert not marker.exists()
    assert not _running_group_members(completed["child"]["pid"])


def test_stop_after_shell_exits_labels_descendant_cleanup_separately(tmp_path):
    manager = DurableProcessSupervisor(tmp_path / "jobs")
    command, marker, pid_path = resistant_command(tmp_path, "background-stop")
    command = command.removesuffix(" & wait") + " & printf shell-done"
    job = manager.start(command, owner="owner", cwd=tmp_path,
                        idempotency_key="background-stop")
    end = time.monotonic() + 5
    while not pid_path.exists() and time.monotonic() < end:
        time.sleep(.03)
    assert pid_path.exists()
    manager.stop(job["job_id"], owner="owner")
    # If the shell has already exited, descendant cleanup is distinct from
    # claiming that the primary command itself was cancelled.
    completed = terminal(manager, job["job_id"])
    assert completed["result"]["status"] in ("descendants_stopped", "cancelled")
    assert not marker.exists()
    assert not _running_group_members(completed["child"]["pid"])


def test_stop_request_ignored_until_normal_completion_is_not_cancelled(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "jobs")
    ready = tmp_path / "ready"
    marker = tmp_path / "completed_anyway"
    code = (
        "import signal,time;"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
        f"open({str(ready)!r},'w').write('ready');"
        "time.sleep(.32);"
        f"open({str(marker)!r},'w').write('normal-exit')"
    )
    job = registry.start(f"{sys.executable} -c {code!r}",
                         owner="owner", cwd=tmp_path, idempotency_key="ignore")
    end = time.monotonic() + 5
    while not ready.exists() and time.monotonic() < end:
        time.sleep(.015)
    assert ready.exists()
    registry.stop(job["job_id"], owner="owner")
    result = terminal(registry, job["job_id"])
    assert marker.read_text() == "normal-exit"
    assert result["result"]["exit_code"] == 0
    assert result["result"]["status"] == "stop_not_effective"
    assert result["result"]["stop_requested"]
    assert result["result"]["stop_effective"] is False
    assert result["result"]["group_verified"]



def test_stop_does_not_claim_cancelled_when_shell_dies_but_child_writes(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "jobs")
    ready = tmp_path / "grandchild_ready"
    marker = tmp_path / "grandchild_completed"
    code = (
        "import signal,time;"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        f"open({str(ready)!r},'w').write('ready');"
        "time.sleep(.24);"
        f"open({str(marker)!r},'w').write('finished-despite-stop')"
    )
    job = registry.start(
        f"{sys.executable} -c {code!r} & wait",
        owner="owner", cwd=tmp_path, idempotency_key="ignored-child",
    )
    limit = time.monotonic() + 6
    while time.monotonic() < limit and not ready.exists():
        time.sleep(.01)
    assert ready.exists()
    registry.stop(job["job_id"], owner="owner")
    outcome = terminal(registry, job["job_id"])
    assert marker.read_text() == "finished-despite-stop"
    assert outcome["result"]["status"] == "stop_not_effective"
    assert outcome["result"]["stop_effective"] is False
    assert outcome["result"]["group_verified"] is True


def test_producer_publishes_workspace_fence_and_reserves_under_lock(tmp_path, monkeypatch):
    import durable_process_supervisor as module
    root = tmp_path / "durable"
    manager = DurableProcessSupervisor(root)
    assert (root / "workspace-fence-version").read_text() == "1\n"
    seen = []
    original = module._atomic_json

    def check_reservation(path, value):
        if path.name == "request.json":
            assert (root / "workspace-mutation.lock").is_dir()
            seen.append(value["cwd"])
        return original(path, value)

    monkeypatch.setattr(module, "_atomic_json", check_reservation)
    job = manager.start("printf fenced", owner="owner", cwd=tmp_path,
                        idempotency_key="fenced")
    assert seen == [str(tmp_path)]
    assert not (root / "workspace-mutation.lock").exists()
    assert terminal(manager, job["job_id"])["result"]["group_verified"]


def test_held_workspace_fence_prevents_new_job_reservation(tmp_path):
    import pytest
    root = tmp_path / "durable"
    manager = DurableProcessSupervisor(root)
    (root / "workspace-mutation.lock").mkdir()
    try:
        with pytest.raises(TimeoutError, match="fence is held"):
            manager.start("printf must-not-launch", owner="owner", cwd=tmp_path,
                          idempotency_key="blocked")
        assert list((root / "jobs").iterdir()) == []
    finally:
        (root / "workspace-mutation.lock").rmdir()


def test_timeout_containes_setsided_orphan(tmp_path):
    manager = DurableProcessSupervisor(tmp_path / "durable")
    ready = tmp_path / "escaped_ready"
    escaped = tmp_path / "escaped_wrote"
    code = (
        "import os,signal,time;"
        "os.setsid();"
        "signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        f"open({str(ready)!r},'w').write('ready');"
        "time.sleep(2.5);"
        f"open({str(escaped)!r},'w').write('bad')"
    )
    job = manager.start(f"{sys.executable} -c {code!r} & wait", owner="owner",
                        cwd=tmp_path, hard_timeout=.7, idempotency_key="escape")
    result = terminal(manager, job["job_id"])
    assert ready.exists()
    assert result["result"]["status"] == "timed_out"
    assert result["result"]["group_verified"]
    assert not escaped.exists()


def test_stop_containes_setsided_orphan(tmp_path):
    manager = DurableProcessSupervisor(tmp_path / "durable")
    ready = tmp_path / "stop_escaped_ready"
    escaped = tmp_path / "stop_escaped_wrote"
    code = (
        "import os,signal,time;"
        "os.setsid();signal.signal(signal.SIGTERM,signal.SIG_IGN);"
        f"open({str(ready)!r},'w').write('ready');"
        "time.sleep(2.5);"
        f"open({str(escaped)!r},'w').write('bad')"
    )
    job = manager.start(f"{sys.executable} -c {code!r} & wait", owner="owner",
                        cwd=tmp_path, idempotency_key="stop-escape")
    until = time.monotonic() + 5
    while not ready.exists() and time.monotonic() < until:
        time.sleep(.02)
    assert ready.exists()
    manager.stop(job["job_id"], owner="owner")
    result = terminal(manager, job["job_id"])
    assert result["result"]["status"] in ("cancelled", "descendants_stopped")
    assert result["result"]["group_verified"]
    assert not escaped.exists()
