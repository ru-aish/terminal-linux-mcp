from __future__ import annotations

import argparse
import time
from pathlib import Path


def read_process_start_ticks(pid: int) -> int | None:
    """Return Linux /proc start ticks, or None when the process no longer exists."""
    stat_path = Path("/proc") / str(pid) / "stat"
    try:
        text = stat_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    right_paren = text.rfind(")")
    if right_paren < 0:
        raise RuntimeError(f"malformed process stat for pid {pid}")
    fields_after_comm = text[right_paren + 2 :].split()
    if len(fields_after_comm) <= 19:
        raise RuntimeError(f"incomplete process stat for pid {pid}")
    if fields_after_comm[0] == "Z":
        return None
    return int(fields_after_comm[19])


def wait_for_exact_process_exit(pid: int, start_ticks: int, interval_seconds: float) -> None:
    """Sleep between checks until this exact PID execution is gone.

    Matching Linux start ticks prevents PID reuse from making the watcher wait on
    an unrelated later process.
    """
    interval = max(0.05, float(interval_seconds))
    while True:
        current_start_ticks = read_process_start_ticks(pid)
        if current_start_ticks is None or current_start_ticks != start_ticks:
            return
        time.sleep(interval)


def main() -> int:
    parser = argparse.ArgumentParser(description="Wait for one exact Linux process execution to finish.")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--start-ticks", type=int, required=True)
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    args = parser.parse_args()
    if args.pid <= 0:
        parser.error("--pid must be greater than zero")
    if args.start_ticks <= 0:
        parser.error("--start-ticks must be greater than zero")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be greater than zero")
    wait_for_exact_process_exit(args.pid, args.start_ticks, args.interval_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
