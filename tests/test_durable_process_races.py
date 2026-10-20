"""Fault and concurrency cases not covered by the supervisor happy-path tests."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from durable_process_supervisor import DurableProcessSupervisor, _atomic_json, _job_id


def test_simultaneous_idempotent_starts_only_execute_once(tmp_path):
    store = tmp_path / "store"
    counter = tmp_path / "counter"
    managers = [DurableProcessSupervisor(store) for _ in range(8)]
    command = f"printf x >> {counter}; sleep .15"

    def start(i):
        return managers[i].start(command, owner="same-thread", cwd=tmp_path, idempotency_key="same")

    with ThreadPoolExecutor(max_workers=8) as pool:
        requests = list(pool.map(start, range(8)))
    assert len({row["job_id"] for row in requests}) == 1
    job_id = requests[0]["job_id"]
    for _ in range(200):
        row = managers[0].status(job_id, owner="same-thread")
        if row["result"] is not None:
            break
        time.sleep(.025)
    assert row["status"] == "exited"
    assert row["result"]["exit_code"] == 0
    assert counter.read_text() == "x"


def test_missing_gate_grant_never_executes_shell(tmp_path):
    directory = tmp_path / "job"
    directory.mkdir()
    marker = tmp_path / "should-not-exist"
    _atomic_json(directory / "request.json", {
        "shell": "/bin/bash", "command": f"touch {marker}",
    })
    rfd, wfd = os.pipe()
    try:
        child = subprocess.Popen(
            [sys.executable, str(Path(__file__).parents[1] / "durable_process_supervisor.py"),
             "_exec", str(directory), str(rfd)],
            pass_fds=(rfd,), close_fds=True,
        )
    finally:
        os.close(rfd)
        os.close(wfd)
    assert child.wait(timeout=3) == 125
    assert not marker.exists()


def test_existing_nonprivate_root_not_chmodded(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir(mode=0o755)
    os.chmod(shared, 0o755)
    with pytest.raises(PermissionError, match="private"):
        DurableProcessSupervisor(shared)
    assert shared.stat().st_mode & 0o777 == 0o755


def test_crashed_reservation_does_not_relaunch_job(tmp_path):
    manager = DurableProcessSupervisor(tmp_path / "store")
    key = _job_id("owner", "crashed")
    manager._directory(key).mkdir()
    with pytest.raises(RuntimeError, match="refusing duplicate spawn"):
        manager.start("touch should-not-run", owner="owner", cwd=tmp_path, idempotency_key="crashed")
    assert not (tmp_path / "should-not-run").exists()

def test_job_survives_launcher_process_exit(tmp_path):
    """The caller can exit before the independent worker finishes the command."""
    root = tmp_path / "persist"
    launch_script = (
        "from durable_process_supervisor import DurableProcessSupervisor; "
        f"result=DurableProcessSupervisor({str(root)!r}).start("
        "'sleep .3; printf survived', owner='survivor', "
        f"cwd={str(tmp_path)!r}, idempotency_key='once'); "
        "print(result['job_id'])"
    )
    parent = subprocess.run(
        [sys.executable, "-c", launch_script],
        cwd=Path(__file__).parents[1],
        capture_output=True, text=True, check=True, timeout=6,
    )
    assert parent.returncode == 0
    registry = DurableProcessSupervisor(root)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        row = registry.status(parent.stdout.strip(), owner="survivor")
        if row["result"] is not None:
            break
        time.sleep(0.025)
    assert row["status"] == "exited"
    assert row["result"]["exit_code"] == 0
    assert registry.output(parent.stdout.strip(), owner="survivor")["stdout"]["text"] == "survived"


def test_stop_through_supervisor_records_cancelled_result(tmp_path):
    registry = DurableProcessSupervisor(tmp_path / "registry")
    job = registry.start("sleep 10", owner="owner", cwd=tmp_path, idempotency_key="stop")
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        row = registry.status(job["job_id"], owner="owner")
        if row["status"] == "running":
            break
        time.sleep(0.02)
    assert row["status"] == "running"
    registry.stop(job["job_id"], owner="owner")
    while time.monotonic() < deadline:
        row = registry.status(job["job_id"], owner="owner")
        if row["result"] is not None:
            break
        time.sleep(0.02)
    assert row["status"] == "cancelled"
    assert row["result"]["exit_code"] != 0
