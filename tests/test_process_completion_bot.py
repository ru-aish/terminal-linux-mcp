from __future__ import annotations

import os
import subprocess
import sys
import time

from process_completion_bot import read_process_start_ticks, wait_for_exact_process_exit


def test_zombie_process_counts_as_finished():
    process = subprocess.Popen([sys.executable, "-c", "pass"])
    try:
        start_ticks = read_process_start_ticks(process.pid)
        assert start_ticks is not None
        time.sleep(0.1)
        assert read_process_start_ticks(process.pid) is None
    finally:
        process.wait(timeout=1)


def test_start_ticks_prevent_pid_reuse_false_match():
    start_ticks = read_process_start_ticks(os.getpid())
    assert start_ticks is not None
    began = time.monotonic()
    wait_for_exact_process_exit(os.getpid(), start_ticks + 1, 0.05)
    assert time.monotonic() - began < 0.2
