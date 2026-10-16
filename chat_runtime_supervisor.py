from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import re
import shlex
import signal
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

from chat_runtime_controller import (
    ChatRuntimeController,
    ChatRuntimeControllerConfig,
)

HealthProbe = Callable[[], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class SupervisorConfig:
    command: tuple[str, ...]
    app_id: str
    state_dir: Path
    runtime_dir: Path
    cdp_endpoint: str
    webview_url: str
    startup_grace_seconds: float = 45.0
    probe_interval_seconds: float = 10.0
    health_timeout_seconds: float = 90.0
    failure_threshold: int = 3
    restart_backoff_seconds: float = 5.0
    stop_timeout_seconds: float = 20.0
    status_path: Path | None = None

    @classmethod
    def from_env(cls) -> "SupervisorConfig":
        command = tuple(
            shlex.split(
                os.environ.get("CHAT_RUNTIME_COMMAND", "/usr/bin/codex-desktop").strip()
            )
        )
        app_id = os.environ.get("CHAT_RUNTIME_APP_ID", "codex-desktop").strip()
        state_dir = Path(
            os.environ.get("CHAT_RUNTIME_STATE_DIR", "~/.local/state/codex-desktop")
        ).expanduser()
        runtime_dir = Path(
            os.environ.get(
                "CHAT_RUNTIME_SOCKET_DIR",
                f"${{XDG_RUNTIME_DIR:-/tmp}}/{app_id}",
            )
        ).expanduser()
        raw_runtime = str(runtime_dir)
        if "${XDG_RUNTIME_DIR:-/tmp}" in raw_runtime:
            runtime_dir = Path(os.environ.get("XDG_RUNTIME_DIR", "/tmp")) / app_id
        status_path = Path(
            os.environ.get(
                "CHAT_RUNTIME_STATUS_PATH", str(state_dir / "supervisor-status.json")
            )
        ).expanduser()
        config = cls(
            command=command,
            app_id=app_id,
            state_dir=state_dir,
            runtime_dir=runtime_dir,
            cdp_endpoint=os.environ.get(
                "CHAT_RUNTIME_CDP_ENDPOINT", "http://127.0.0.1:9222"
            ).strip(),
            webview_url=os.environ.get(
                "CHAT_RUNTIME_WEBVIEW_URL", "http://127.0.0.1:5175/index.html"
            ).strip(),
            startup_grace_seconds=max(
                1.0, float(os.environ.get("CHAT_RUNTIME_STARTUP_GRACE", "45"))
            ),
            probe_interval_seconds=max(
                0.5, float(os.environ.get("CHAT_RUNTIME_PROBE_INTERVAL", "10"))
            ),
            health_timeout_seconds=max(
                5.0, float(os.environ.get("CHAT_RUNTIME_HEALTH_TIMEOUT", "90"))
            ),
            failure_threshold=max(
                1, int(os.environ.get("CHAT_RUNTIME_FAILURE_THRESHOLD", "3"))
            ),
            restart_backoff_seconds=max(
                0.0, float(os.environ.get("CHAT_RUNTIME_RESTART_BACKOFF", "5"))
            ),
            stop_timeout_seconds=max(
                1.0, float(os.environ.get("CHAT_RUNTIME_STOP_TIMEOUT", "20"))
            ),
            status_path=status_path,
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not self.command or not Path(self.command[0]).is_absolute():
            raise ValueError(
                "CHAT_RUNTIME_COMMAND must start with an absolute executable"
            )
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", self.app_id):
            raise ValueError("invalid CHAT_RUNTIME_APP_ID")
        for path in (self.state_dir, self.runtime_dir, self.status_path):
            if path is not None and not path.is_absolute():
                raise ValueError("runtime state paths must be absolute")


class ChatRuntimeSupervisor:
    """Own and recover the complete ChatGPT desktop process tree."""

    def __init__(
        self,
        config: SupervisorConfig,
        *,
        health_probe: HealthProbe | None = None,
    ) -> None:
        self.config = config
        self.config.validate()
        self._stop = asyncio.Event()
        self._process: asyncio.subprocess.Process | None = None
        self._generation = 0
        self._restart_count = 0
        self._health_probe = health_probe or self._default_health_probe
        self._last_health: dict[str, Any] = {"ready": False, "reason": "not started"}

    async def run(self) -> int:
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self.config.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._install_signal_handlers()
        await self._write_status("starting")
        try:
            while not self._stop.is_set():
                await asyncio.to_thread(self._prepare_generation)
                await self._launch()
                reason = await self._monitor_generation()
                await self._stop_generation(reason)
                if self._stop.is_set():
                    break
                self._restart_count += 1
                await self._write_status("backoff", reason=reason)
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.config.restart_backoff_seconds
                    )
                except TimeoutError:
                    pass
            return 0
        finally:
            await self._stop_generation("supervisor shutdown")
            await self._write_status("stopped")

    def request_stop(self) -> None:
        self._stop.set()

    def _prepare_generation(self) -> None:
        """Remove stale ownership artifacts left by an ungraceful prior exit."""
        self._stop_generated_scopes()
        self._clear_stale_runtime_markers()

    async def _launch(self) -> None:
        self._generation += 1
        environment = os.environ.copy()
        environment.update(
            {
                "KDE_APPLICATIONS_AS_SCOPE": "0",
                "CODEX_LINUX_APP_ID": self.config.app_id,
                "CODEX_APP_ID": self.config.app_id,
                "CODEX_LINUX_APP_STATE_DIR": str(self.config.state_dir),
            }
        )
        self._process = await asyncio.create_subprocess_exec(
            *self.config.command,
            env=environment,
            start_new_session=True,
        )
        await self._write_status(
            "starting",
            process_pid=self._process.pid,
            generation=self._generation,
        )

    async def _monitor_generation(self) -> str:
        assert self._process is not None
        process = self._process
        started = time.monotonic()
        consecutive_failures = 0
        while not self._stop.is_set():
            if process.returncode is not None:
                return f"launcher exited with status {process.returncode}"
            if time.monotonic() - started < self.config.startup_grace_seconds:
                await self._wait_or_stop(min(1.0, self.config.probe_interval_seconds))
                continue
            try:
                health = await self._health_probe()
            except Exception as error:
                health = {
                    "ready": False,
                    "reason": f"{type(error).__name__}: {error}",
                }
            self._last_health = health
            if health.get("ready"):
                consecutive_failures = 0
                await self._write_status(
                    "ready",
                    process_pid=process.pid,
                    generation=self._generation,
                    health=health,
                )
            else:
                consecutive_failures += 1
                await self._write_status(
                    "degraded",
                    process_pid=process.pid,
                    generation=self._generation,
                    consecutive_failures=consecutive_failures,
                    health=health,
                )
                if consecutive_failures >= self.config.failure_threshold:
                    return (
                        "health failed "
                        f"{consecutive_failures} consecutive probes: "
                        f"{str(health.get('reason') or health)[:500]}"
                    )
            await self._wait_or_stop(self.config.probe_interval_seconds)
        return "supervisor stop requested"

    async def _stop_generation(self, reason: str) -> None:
        process, self._process = self._process, None
        await self._write_status("stopping", reason=reason)
        if process is not None and process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(
                    process.wait(), timeout=self.config.stop_timeout_seconds
                )
            except TimeoutError:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(process.wait(), timeout=5.0)
        await asyncio.to_thread(self._stop_generated_scopes)
        self._clear_stale_runtime_markers()

    async def _default_health_probe(self) -> dict[str, Any]:
        controller = ChatRuntimeController(
            ChatRuntimeControllerConfig(
                service_name="codex-desktop-runtime.service",
                cdp_endpoint=self.config.cdp_endpoint,
                webview_url=self.config.webview_url,
                internal_timeout_seconds=self.config.health_timeout_seconds,
            )
        )
        return await controller.health()

    async def _wait_or_stop(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        except TimeoutError:
            pass

    def cleanup(self) -> None:
        """Remove every runtime artifact owned by this application identity."""
        self._stop_generated_scopes()
        self._clear_stale_runtime_markers()

    def _stop_generated_scopes(self) -> None:
        pattern = re.compile(rf"^app-{re.escape(self.config.app_id)}-[0-9]+\.scope$")
        empty_passes = 0
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and empty_passes < 2:
            completed = subprocess.run(
                [
                    "systemctl",
                    "--user",
                    "list-units",
                    "--type=scope",
                    "--all",
                    "--plain",
                    "--no-legend",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            units = [
                line.split(maxsplit=1)[0]
                for line in completed.stdout.splitlines()
                if line.strip() and pattern.fullmatch(line.split(maxsplit=1)[0])
            ]
            if not units:
                empty_passes += 1
                time.sleep(0.1)
                continue
            empty_passes = 0
            for unit in units:
                subprocess.run(
                    ["systemctl", "--user", "stop", unit],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=15,
                    check=False,
                )

    def _clear_stale_runtime_markers(self) -> None:
        for path in (
            self.config.state_dir / "app.pid",
            self.config.state_dir / "webview.pid",
            self.config.runtime_dir / "launch-action.sock",
        ):
            with contextlib.suppress(FileNotFoundError):
                path.unlink()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(signum, self.request_stop)

    async def _write_status(self, state: str, **details: Any) -> None:
        path = self.config.status_path
        if path is None:
            return
        payload = {
            "state": state,
            "updated_at": time.time(),
            "generation": self._generation,
            "restart_count": self._restart_count,
            "configuration": {
                **asdict(self.config),
                "command": list(self.config.command),
                "state_dir": str(self.config.state_dir),
                "runtime_dir": str(self.config.runtime_dir),
                "status_path": str(path),
            },
            **details,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Own and recover one ChatGPT desktop runtime"
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="validate environment configuration and exit",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="stop adopted application scopes and remove stale runtime markers",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    config = SupervisorConfig.from_env()
    supervisor = ChatRuntimeSupervisor(config)
    if args.check_config:
        print(json.dumps(asdict(config), sort_keys=True, default=str))
        return 0
    if args.cleanup:
        supervisor.cleanup()
        return 0
    return asyncio.run(supervisor.run())


if __name__ == "__main__":
    raise SystemExit(main())
