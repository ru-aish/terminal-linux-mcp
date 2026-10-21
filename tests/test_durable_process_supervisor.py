from __future__ import annotations
import json
import os
import signal
import subprocess
import time

import pytest
from durable_process_supervisor import DurableProcessSupervisor, _atomic_json, _alive, _boot_id


def settled(registry, job_id, timeout=8):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = registry.status(job_id, owner="thread-one")
        if result["result"] is not None:
            return result
        time.sleep(0.025)
    pytest.fail(f"No durable terminal result: {result}")


def test_exit_receipt_output_and_restart(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "registry")
    command = "printf 'out\\n'; printf 'err\\n' >&2; exit 7"
    first = registry.start(command, owner="thread-one", cwd=tmp_path, idempotency_key="same")
    assert registry.start(command, owner="thread-one", cwd=tmp_path, idempotency_key="same")["job_id"] == first["job_id"]
    restored = DurableProcessSupervisor(tmp_path / "registry")
    result = settled(restored, first["job_id"])
    assert result["result"]["exit_code"] == 7
    assert result["result"]["child"] == result["child"]
    assert result["child"]["boot_id"] == _boot_id()
    output = restored.output(first["job_id"], owner="thread-one")
    assert output["stdout"]["text"] == "out\n"
    assert output["stderr"]["text"] == "err\n"
    assert restored.discover(owner="thread-one")[0]["job_id"] == first["job_id"]
    assert restored.discover(owner="other") == []
    with pytest.raises(ValueError, match="owner"):
        restored.status(first["job_id"], owner="other")
    directory = tmp_path / "registry" / "jobs" / first["job_id"]
    assert (directory / "result.json").stat().st_mode & 0o777 == 0o600


def test_idempotent_retry_conflict_does_not_spawn_again(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "registry")
    marker = tmp_path / "marker"
    command = f"printf x >> {marker}"
    job_id = registry.start(command, owner="thread-one", cwd=tmp_path, idempotency_key="unique")["job_id"]
    with pytest.raises(ValueError, match="different command"):
        registry.start("echo y", owner="thread-one", cwd=tmp_path, idempotency_key="unique")
    settled(registry, job_id)
    assert marker.read_text() == "x"


def test_worker_crash_preserves_child_and_reports_unknown_outcome(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "registry")
    marker = tmp_path / "marker"
    command = f"sleep 0.7; printf once >> {marker}"
    job_id = registry.start(command, owner="thread-one", cwd=tmp_path, idempotency_key="one")["job_id"]
    deadline = time.monotonic() + 6
    while time.monotonic() < deadline:
        row = registry.status(job_id, owner="thread-one")
        if row["child"] and row["supervisor"]:
            break
        time.sleep(0.02)
    else:
        pytest.fail("No child identity")
    child = row["child"]
    os.kill(row["supervisor"]["pid"], signal.SIGKILL)
    try:
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            row = registry.status(job_id, owner="thread-one")
            if row["status"] == "supervisor_lost_child_running":
                break
            time.sleep(0.02)
        assert row["status"] == "supervisor_lost_child_running"
        new_manager = DurableProcessSupervisor(tmp_path / "registry")
        assert new_manager.start(command, owner="thread-one", cwd=tmp_path, idempotency_key="one")["job_id"] == job_id
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline and not marker.exists():
            time.sleep(0.02)
        assert marker.read_text() == "once"
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            row = new_manager.status(job_id, owner="thread-one")
            if row["status"] == "outcome_unknown":
                break
            time.sleep(0.05)
        assert row["status"] == "outcome_unknown" and row["result"] is None
    finally:
        if _alive(child):
            os.killpg(child["pid"], signal.SIGTERM)


def test_boot_identity_mismatch_is_not_a_live_job(tmp_path):
    import durable_process_supervisor as module
    registry = DurableProcessSupervisor(tmp_path / "registry")
    job_id = "a" * 64
    folder = registry.jobs / job_id
    folder.mkdir()
    _atomic_json(folder / "request.json", {"owner": "thread-one", "created_at": time.time() - 15})
    _atomic_json(folder / "supervisor.json", {
        "pid": os.getpid(), "boot_id": "different-boot-id",
        "start_ticks": module.read_process_start_ticks(os.getpid()),
    })
    assert registry.status(job_id, owner="thread-one")["status"] == "outcome_unknown"


def test_atomic_receipt_not_partially_replaced(tmp_path, monkeypatch):
    import durable_process_supervisor as module
    target = tmp_path / "result.json"
    _atomic_json(target, {"exit_code": 0})
    def fail(*args):
        raise OSError("simulated failure")
    monkeypatch.setattr(module.os, "replace", fail)
    with pytest.raises(OSError, match="simulated"):
        _atomic_json(target, {"exit_code": 1})
    assert json.loads(target.read_text()) == {"exit_code": 0}
    assert not any(p.name.startswith(".result.json.") for p in tmp_path.iterdir())


def test_uncertain_systemd_launch_keeps_reservation(tmp_path, monkeypatch):
    registry = DurableProcessSupervisor(tmp_path / "registry", launch_mode="systemd")
    attempts = []
    def fail(*args, **kwargs):
        attempts.append(args)
        raise subprocess.CalledProcessError(1, "systemd-run")
    monkeypatch.setattr(subprocess, "run", fail)
    one = registry.start("echo hello", owner="thread-one", cwd=tmp_path, idempotency_key="one")
    two = registry.start("echo hello", owner="thread-one", cwd=tmp_path, idempotency_key="one")
    assert one["job_id"] == two["job_id"]
    assert len(attempts) == 1
    assert one["result"] is None

@pytest.mark.parametrize("wait_for_child", [False, True])
def test_stop_writes_cancelled_receipt_without_recycled_pid_signal(tmp_path, wait_for_child):
    registry = DurableProcessSupervisor(tmp_path / "registry")
    marker = tmp_path / "must_not_exist"
    job_id = registry.start(
        f"sleep 2; touch {marker}", owner="thread-one", cwd=tmp_path,
        idempotency_key="early" if not wait_for_child else "late",
    )["job_id"]
    if wait_for_child:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            row = registry.status(job_id, owner="thread-one")
            if row["status"] == "running":
                break
            time.sleep(.01)
        assert row["status"] == "running"
    registry.stop(job_id, owner="thread-one")
    row = settled(registry, job_id)
    assert row["result"]["status"] == "cancelled"
    assert row["result"]["exit_code"] != 0
    assert not marker.exists()


def test_simultaneous_retries_execute_once(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    registry = DurableProcessSupervisor(tmp_path / "registry")
    marker = tmp_path / "once"
    command = f"sleep 0.12; printf x >> {marker}"

    def start():
        return registry.start(command, owner="thread-one", cwd=tmp_path,
                              idempotency_key="raced")["job_id"]

    with ThreadPoolExecutor(max_workers=6) as pool:
        job_ids = list(pool.map(lambda _: start(), range(6)))
    assert len(set(job_ids)) == 1
    settled(registry, job_ids[0])
    assert marker.read_text() == "x"
