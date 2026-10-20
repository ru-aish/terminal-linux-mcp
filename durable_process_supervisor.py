"""Disk-backed command jobs owned by per-job supervisors, independent of MCP requests.

The caller never starts the shell: a detached helper does so after persisting the
child's Linux execution identity. Job directories and terminal receipts are immutable
once written. A missing receipt is never mistaken for a successful command.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import signal
import socket
import struct
import subprocess
import sys
import time
import uuid

from process_completion_bot import read_process_start_ticks

_MAX_READ = 262144
_JOB_DIR = "jobs"


def _boot_id() -> str:
    return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()


def _identity(pid: int) -> dict | None:
    start = read_process_start_ticks(pid)
    if start is None:
        return None
    return {"pid": pid, "boot_id": _boot_id(), "start_ticks": start}


def _alive(identity: dict | None) -> bool:
    return (
        identity is not None
        and identity.get("boot_id") == _boot_id()
        and read_process_start_ticks(identity["pid"]) == identity.get("start_ticks")
    )


def _atomic_json(path: Path, value: dict) -> None:
    """Replace one complete JSON record, synchronizing the file and directory."""
    payload = json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")
    temporary = path.with_name("." + path.name + "." + secrets.token_hex(8))
    try:
        with temporary.open("xb") as output:
            os.chmod(temporary, 0o600)
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_text(path: Path, value: str) -> None:
    temporary = path.with_name("." + path.name + "." + secrets.token_hex(8))
    try:
        with temporary.open("x", encoding="utf-8") as output:
            os.chmod(temporary, 0o600)
            output.write(value)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path) -> dict | None:
    try:
        with path.open("r", encoding="utf-8") as source:
            return json.load(source)
    except FileNotFoundError:
        return None


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _job_id(owner: str, key: str | None) -> str:
    return _digest([owner, key]) if key is not None else uuid.uuid4().hex


def _exec_after_gate(directory: Path, fd: int) -> None:
    """The shell cannot execute until the supervisor writes its PID identity."""
    spec = _read_json(directory / "request.json")
    if spec is None:
        os._exit(125)
    try:
        granted = os.read(fd, 1)
    finally:
        os.close(fd)
    if granted != b"1":
        os._exit(125)
    os.execv(spec["shell"], [spec["shell"], "-lc", spec["command"]])


def _recv_exact(connection: socket.socket, count: int) -> bytes:
    output = bytearray()
    while len(output) < count:
        chunk = connection.recv(count - len(output))
        if not chunk:
            raise ConnectionError("Environment broker disconnected")
        output.extend(chunk)
    return bytes(output)


def _receive_launch_env(token: str) -> dict[str, str]:
    """Receive intended environment through a one-time Unix socket, not argv/disk."""
    if not token.startswith("@") or len(token) != 33:
        raise ValueError("Invalid environment broker identity")
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(10)
        connection.connect("\0mcp-env-" + token[1:])
        size = struct.unpack("!I", _recv_exact(connection, 4))[0]
        if size > 1_048_576:
            raise ValueError("Environment payload exceeds limit")
        values = json.loads(_recv_exact(connection, size))
        if not isinstance(values, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in values.items()
        ):
            raise ValueError("Invalid environment payload")
        connection.sendall(b"1")
        return values


def _send_launch_env(listener: socket.socket, values: dict[str, str]) -> None:
    payload = json.dumps(values, separators=(",", ":")).encode()
    if len(payload) > 1_048_576:
        raise ValueError("Environment payload exceeds limit")
    listener.settimeout(10)
    with listener.accept()[0] as connection:
        connection.settimeout(10)
        credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        _, uid, _ = struct.unpack("3i", credentials)
        if uid != os.getuid():
            raise PermissionError("Environment broker peer UID mismatch")
        connection.sendall(struct.pack("!I", len(payload)) + payload)
        if connection.recv(1) != b"1":
            raise ConnectionError("Worker did not acknowledge environment delivery")


def _child_exited_unreaped(pid: int) -> bool:
    """Never reap the process-group leader until descendant cleanup completes."""
    result = os.waitid(os.P_PID, pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    return result is not None


def _running_group_members(pgid: int) -> list[int]:
    """Count non-zombie members in the child's original process group."""
    members = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            stat = (entry / "stat").read_text()
            # comm is parenthesized and may itself contain spaces.
            tail = stat.rsplit(")", 1)[1].split()
            if int(tail[2]) == pgid and tail[0] not in ("Z", "X", "x"):
                members.append(int(entry.name))
        except (OSError, IndexError, ValueError):
            continue
    return members


def _signal_group(pgid: int, sig: int) -> None:
    try:
        os.killpg(pgid, sig)
    except ProcessLookupError:
        pass


def _worker(directory: Path, env_token: str | None = None) -> int:
    # A worker owns exactly one launch. An accidentally repeated invocation
    # cannot start a second command, even after the first worker has died.
    try:
        fd = os.open(directory / "worker.lock", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return 125
    with os.fdopen(fd, "w") as lock:
        lock.write(str(os.getpid()))
        lock.flush()
        os.fsync(lock.fileno())
    directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)
    spec = _read_json(directory / "request.json")
    if spec is None:
        return 125
    if env_token is not None:
        try:
            received = _receive_launch_env(env_token)
        except Exception:
            # Missing broker means no execution: never fall back to an
            # incomplete environment or expose secrets in an error log.
            return 125
        os.environ.clear()
        os.environ.update(received)
    child = None
    released = False
    stop_requested = False
    stop_effective = False
    timed_out = False

    def on_stop(_signal, _frame):
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, on_stop)
    _atomic_json(directory / "supervisor.json", _identity(os.getpid()))
    read_gate, write_gate = os.pipe()
    try:
        with (directory / "stdout.log").open("ab", buffering=0) as stdout, (directory / "stderr.log").open("ab", buffering=0) as stderr:
            child = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "_exec", str(directory), str(read_gate)],
                cwd=spec["cwd"], env=os.environ.copy(),
                stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr,
                pass_fds=(read_gate,), start_new_session=True, close_fds=True,
            )
            os.close(read_gate)
            read_gate = -1
            # The gate prevents the command from running before this record is durable.
            identity = _identity(child.pid)
            if identity is None:
                raise RuntimeError("Cannot identify supervised process before execution")
            _atomic_json(directory / "child.json", identity)
            if not stop_requested and not (directory / "stop.request").exists():
                os.write(write_gate, b"1")
                released = True
            else:
                stop_effective = True  # The execution gate stays closed.
            os.close(write_gate)
            write_gate = -1
            gate_stopped = stop_effective
            # SIGTERM alone is insufficient: shells often exit on TERM while
            # a descendant ignores it. Keep the group leader unreaped, so its
            # PGID cannot be recycled before hard-killing the entire group.
            reason = None
            deadline = (time.monotonic() + spec["hard_timeout"]
                        if spec.get("hard_timeout") is not None else None)
            term_at = None
            hard_killed = False
            descendant_outlived_shell_after_stop = False
            while True:
                now = time.monotonic()
                exited = _child_exited_unreaped(child.pid)
                members = (_running_group_members(child.pid)
                           if exited or reason is not None or stop_requested else [])
                if reason is None:
                    if exited and not members:
                        break
                    if stop_requested or (directory / "stop.request").exists():
                        reason = "cancelled"
                    elif deadline is not None and now >= deadline:
                        reason = "timed_out"
                    if reason is not None:
                        term_at = now
                        _signal_group(child.pid, signal.SIGTERM)
                else:
                    if exited and members and reason == "cancelled":
                        descendant_outlived_shell_after_stop = True
                    if not members:
                        break
                    if not hard_killed and now - term_at >= 0.5:
                        _signal_group(child.pid, signal.SIGKILL)
                        hard_killed = True
                    if hard_killed and now - term_at >= 4:
                        # Do not claim successful cancellation when unable to
                        # verify that the descendants actually terminated.
                        break
                time.sleep(0.025)
            remaining = _running_group_members(child.pid) if reason else []
            verified = not remaining
            if not verified:
                code = None
            else:
                code = child.wait()
            timed_out = reason == "timed_out" and verified
            # A requested stop is not proof of cancellation: the command may
            # ignore TERM and write its final output during the grace period.
            stop_effective = verified and (gate_stopped or (
                reason == "cancelled" and code is not None and code < 0
                and not descendant_outlived_shell_after_stop))
            descendants_stopped = (reason == "cancelled" and verified and
                                   not stop_effective and hard_killed)
            ineffective_stop = (reason == "cancelled" and verified and
                                not stop_effective and not hard_killed)
            os.fsync(stdout.fileno())
            os.fsync(stderr.fileno())
        _atomic_json(directory / "result.json", {
            "status": ("termination_unverified" if not verified else
                       "timed_out" if timed_out else
                       "descendants_stopped" if descendants_stopped else
                       "cancelled" if stop_effective else
                       "stop_not_effective" if ineffective_stop else
                       "exited"), "exit_code": code,
            "stop_requested": bool(stop_requested or (directory / "stop.request").exists()),
            "stop_effective": bool(stop_effective),
            "hard_killed": bool(hard_killed), "group_verified": bool(verified),
            "finished_at": time.time(), "child": identity,
            "stdout_bytes": (directory / "stdout.log").stat().st_size,
            "stderr_bytes": (directory / "stderr.log").stat().st_size,
        })
        return 0
    except Exception as error:
        # Never publish a successful exit when the supervisor lost its child.
        if child is not None:
            if not released:
                # Gate closure prevents execution of an unregistered child.
                if write_gate >= 0:
                    os.close(write_gate)
                    write_gate = -1
                child.wait()
            else:
                child.wait()
        try:
            _atomic_json(directory / "result.json", {
                "status": "supervisor_error",
                "error_type": type(error).__name__,
                "finished_at": time.time(),
                "child": _read_json(directory / "child.json"),
            })
        except OSError:
            pass
        return 1
    finally:
        if read_gate >= 0:
            os.close(read_gate)
        if write_gate >= 0:
            os.close(write_gate)


@contextmanager
def _workspace_mutation_lock(root: Path):
    """Share the consumer's fail-closed atomic-directory workspace fence."""
    path = root / "workspace-mutation.lock"
    deadline = time.monotonic() + 3
    while True:
        try:
            path.mkdir(mode=0o700)
            break
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TimeoutError("Durable workspace mutation fence is held")
            time.sleep(.025)
    try:
        yield
    finally:
        path.rmdir()


class DurableProcessSupervisor:
    """Filesystem facade; every job is independently owned by a detached worker.

    launch_mode='systemd' creates an independent user service (recommended when
    Terminal MCP itself is managed by systemd). launch_mode='process' starts a
    new session but cannot protect jobs from an enclosing service-cgroup kill.
    """

    def __init__(self, root: str | Path, *, launch_mode: str = "process"):
        if launch_mode not in ("process", "systemd"):
            raise ValueError("launch_mode must be process or systemd")
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        # Never chmod an arbitrary preexisting directory supplied by a caller.
        # Commands and their output may be confidential.
        if self.root.stat().st_uid != os.getuid() or self.root.stat().st_mode & 0o077:
            raise PermissionError('Durable job root must be owned by the current user and private')
        self.jobs = self.root / _JOB_DIR
        self.jobs.mkdir(mode=0o700, exist_ok=True)
        if self.jobs.stat().st_uid != os.getuid() or self.jobs.stat().st_mode & 0o077:
            raise PermissionError('Durable jobs directory must be owned by the current user and private')
        self.launch_mode = launch_mode
        # Publish only for roots served by this fence-aware producer.
        with _workspace_mutation_lock(self.root):
            marker = self.root / "workspace-fence-version"
            if marker.exists() and marker.read_text().strip() != "1":
                raise RuntimeError("Unsupported durable workspace fence version")
            if not marker.exists():
                _atomic_text(marker, "1\n")

    def _directory(self, job_id: str) -> Path:
        if len(job_id) not in (32, 64) or any(c not in "0123456789abcdef" for c in job_id):
            raise ValueError("Invalid job ID")
        return self.jobs / job_id

    def start(
        self,
        command: str,
        *,
        owner: str,
        cwd: str | Path,
        env: dict[str, str] | None = None,
        idempotency_key: str | None = None,
        shell: str = "/bin/bash",
        hard_timeout: float | None = None,
    ) -> dict:
        if not command or not command.strip() or not owner or not owner.strip():
            raise ValueError("Command and owner must be nonempty")
        if idempotency_key is not None and not idempotency_key.strip():
            raise ValueError("Idempotency key must be nonempty if given")
        if env is not None and not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()):
            raise ValueError("Environment overrides must be strings")
        if hard_timeout is not None:
            hard_timeout = float(hard_timeout)
            if not math.isfinite(hard_timeout) or hard_timeout <= 0:
                raise ValueError("hard_timeout must be positive and finite")
        workdir = str(Path(cwd).expanduser().resolve(strict=True))
        if not Path(workdir).is_dir() or not Path(shell).is_file():
            raise ValueError("Working directory and shell must exist")
        job_id = _job_id(owner, idempotency_key)
        directory = self._directory(job_id)
        fingerprint = _digest({
            "owner": owner, "command": command, "cwd": workdir, "shell": shell,
            "env": env or {}, "launch_mode": self.launch_mode, "hard_timeout": hard_timeout,
        })
        with _workspace_mutation_lock(self.root):
            try:
                directory.mkdir(mode=0o700)
            except FileExistsError:
                # Another caller may have created the reservation but not committed
                # request.json yet. Never launch a second worker for the same key.
                deadline = time.monotonic() + 1
                existing = _read_json(directory / "request.json")
                while existing is None and time.monotonic() < deadline:
                    time.sleep(0.01)
                    existing = _read_json(directory / "request.json")
                if existing is None:
                    raise RuntimeError("Existing reserved job is not yet recoverable; refusing duplicate spawn")
                if existing["fingerprint"] != fingerprint or existing["owner"] != owner:
                    raise ValueError("Idempotency key was reused for a different command")
                return self.status(job_id, owner=owner)
            request = {
                "job_id": job_id, "owner": owner, "fingerprint": fingerprint,
                "command": command, "cwd": workdir, "shell": shell,
                "created_at": time.time(), "launch_mode": self.launch_mode, "hard_timeout": hard_timeout,
            }
            # No environment secrets are persisted; only a digest participates in matching.
            _atomic_json(directory / "request.json", request)
        (directory / "stdout.log").touch(mode=0o600)
        (directory / "stderr.log").touch(mode=0o600)
        launcher_env = os.environ.copy()
        if env:
            launcher_env.update(env)
        args = [sys.executable, str(Path(__file__).resolve()), "_worker", str(directory)]
        try:
            if self.launch_mode == "systemd":
                # systemd user services do NOT inherit the subprocess caller's
                # environment. --setenv/EnvironmentFile leak credentials via
                # argv or persistent files, so use a short-lived same-UID socket.
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                    token = secrets.token_hex(16)
                    listener.bind("\0mcp-env-" + token)
                    listener.listen(1)
                    unit = "mcp-supervised-" + job_id[:32]
                    subprocess.run(
                        ["systemd-run", "--user", "--quiet", "--collect",
                         "--property=KillMode=control-group",
                         f"--unit={unit}", "--", *args, "@" + token],
                        cwd=workdir, env=os.environ.copy(), capture_output=True,
                        check=True, timeout=10,
                    )
                    _send_launch_env(listener, launcher_env)
            else:
                subprocess.Popen(
                    args, cwd=workdir, env=launcher_env,
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, start_new_session=True, close_fds=True,
                )
        except (OSError, subprocess.SubprocessError) as error:
            # The launch may have succeeded before the caller lost its response.
            # Keep the reservation, and never automatically retry this job.
            _atomic_json(directory / "launch_error.json", {"error_type": type(error).__name__})
        return self.status(job_id, owner=owner)

    def status(self, job_id: str, *, owner: str) -> dict:
        directory = self._directory(job_id)
        request = _read_json(directory / "request.json")
        if request is None or request["owner"] != owner:
            raise ValueError("Unknown job for this owner")
        result = _read_json(directory / "result.json")
        supervisor = _read_json(directory / "supervisor.json")
        child = _read_json(directory / "child.json")
        if result is not None:
            status = result["status"]
        elif _alive(supervisor):
            status = "running" if child is not None else "starting"
        elif _alive(child):
            status = "supervisor_lost_child_running"
        elif supervisor is None and not (directory / "launch_error.json").exists() and time.time() - request["created_at"] < 15:
            status = "starting"
        else:
            status = "outcome_unknown"
        return {
            "job_id": job_id, "owner": owner, "status": status, "cwd": request.get("cwd"),
            "supervisor": supervisor, "child": child, "result": result,
            "stdout_path": str(directory / "stdout.log"),
            "stderr_path": str(directory / "stderr.log"),
        }

    def discover(self, *, owner: str) -> list[dict]:
        """Rediscover only recorded local jobs after an MCP restart; never respawn."""
        rows = []
        for directory in sorted(self.jobs.iterdir()):
            if not directory.is_dir():
                continue
            try:
                rows.append(self.status(directory.name, owner=owner))
            except (ValueError, KeyError):
                continue
        return rows

    def output(
        self, job_id: str, *, owner: str, stdout_offset: int = 0,
        stderr_offset: int = 0, max_bytes: int = 65536,
    ) -> dict:
        row = self.status(job_id, owner=owner)
        if max_bytes < 1 or max_bytes > _MAX_READ or stdout_offset < 0 or stderr_offset < 0:
            raise ValueError("Invalid output offset or limit")
        streams = {}
        for name, offset in (("stdout", stdout_offset), ("stderr", stderr_offset)):
            with open(row[f"{name}_path"], "rb") as log:
                log.seek(offset)
                chunk = log.read(max_bytes)
            streams[name] = {"text": chunk.decode("utf-8", errors="replace"), "next_offset": offset + len(chunk)}
        return {"job_id": job_id, "status": row["status"], **streams, "result": row["result"]}

    def stop(self, job_id: str, *, owner: str, sig: int = signal.SIGTERM) -> dict:
        """Ask the original supervisor to stop its child; never signal a recycled PID."""
        if sig != signal.SIGTERM:
            raise ValueError("Only graceful SIGTERM is supported")
        row = self.status(job_id, owner=owner)
        if row["result"] is not None:
            return row
        directory = self._directory(job_id)
        _atomic_json(directory / "stop.request", {"requested_at": time.time()})
        supervisor = row["supervisor"]
        if _alive(supervisor):
            try:
                # pidfd pins the exact supervisor across validation and signal.
                fd = os.pidfd_open(supervisor["pid"])
                try:
                    if _alive(supervisor):
                        signal.pidfd_send_signal(fd, signal.SIGTERM)
                finally:
                    os.close(fd)
            except ProcessLookupError:
                pass
        # A dead supervisor's orphan cannot yield a trustworthy exit receipt.
        return self.status(job_id, owner=owner)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["_worker", "_exec"])
    parser.add_argument("directory", type=Path)
    parser.add_argument("gate_fd", nargs="?")
    args = parser.parse_args()
    if args.mode == "_worker":
        return _worker(args.directory, args.gate_fd)
    if args.gate_fd is None:
        parser.error("Execution gate fd is required")
    _exec_after_gate(args.directory, int(args.gate_fd))
    return 125


if __name__ == "__main__":
    raise SystemExit(main())
