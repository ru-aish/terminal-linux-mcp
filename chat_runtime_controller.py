from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import time
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit

import httpx

from chat_internal_client import (
    DEFAULT_CODEX_CDP_ENDPOINT,
    InternalChatClient,
    RuntimeProbe,
    probe_runtime,
    sanitize_runtime_error,
)

_SERVICE_NAME = re.compile(r"^[A-Za-z0-9_.@-]+\.service$")


@dataclass(frozen=True)
class ChatRuntimeControllerConfig:
    service_name: str = "codex-desktop-runtime.service"
    cdp_endpoint: str = DEFAULT_CODEX_CDP_ENDPOINT
    webview_url: str = "http://127.0.0.1:5175/index.html"
    internal_timeout_seconds: float = 10.0
    recovery_timeout_seconds: float = 90.0
    poll_interval_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> "ChatRuntimeControllerConfig":
        internal_timeout = os.environ.get(
            "MCP_CHAT_RUNTIME_INTERNAL_TIMEOUT",
            os.environ.get("MCP_CHAT_WATCHDOG_INTERNAL_TIMEOUT_SECONDS", "90"),
        )
        return cls(
            service_name=os.environ.get(
                "MCP_CHAT_RUNTIME_SERVICE", "codex-desktop-runtime.service"
            ).strip()
            or "codex-desktop-runtime.service",
            cdp_endpoint=os.environ.get(
                "MCP_CHAT_WATCHDOG_CDP", DEFAULT_CODEX_CDP_ENDPOINT
            ).strip()
            or DEFAULT_CODEX_CDP_ENDPOINT,
            webview_url=os.environ.get(
                "MCP_CHAT_RUNTIME_WEBVIEW_URL",
                "http://127.0.0.1:5175/index.html",
            ).strip()
            or "http://127.0.0.1:5175/index.html",
            internal_timeout_seconds=max(
                1.0,
                float(internal_timeout),
            ),
            recovery_timeout_seconds=max(
                5.0,
                float(os.environ.get("MCP_CHAT_RUNTIME_RECOVERY_TIMEOUT", "90")),
            ),
            poll_interval_seconds=max(
                0.2,
                float(os.environ.get("MCP_CHAT_RUNTIME_POLL_INTERVAL", "1")),
            ),
        )

    def validate(self) -> None:
        if not _SERVICE_NAME.fullmatch(self.service_name):
            raise ValueError("invalid ChatGPT runtime systemd service name")
        for value in (self.cdp_endpoint, self.webview_url):
            parsed = urlsplit(value)
            if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
                "127.0.0.1",
                "localhost",
                "::1",
            }:
                raise ValueError("ChatGPT runtime endpoints must be loopback HTTP URLs")


class ChatRuntimeController:
    """Local operator boundary for one dedicated ChatGPT desktop user service."""

    def __init__(self, config: ChatRuntimeControllerConfig | None = None) -> None:
        self.config = config or ChatRuntimeControllerConfig.from_env()
        self.config.validate()
        self._operation_lock = asyncio.Lock()

    async def status(self) -> dict[str, Any]:
        service = await asyncio.to_thread(self._service_properties)
        health = await self.health()
        return {
            "service": service,
            "health": health,
            "configuration": asdict(self.config),
        }

    async def health(self) -> dict[str, Any]:
        webview = await self._webview_health()
        webview_port = urlsplit(self.config.webview_url).port or 80
        cdp: RuntimeProbe = await probe_runtime(
            self.config.cdp_endpoint,
            timeout=min(3.0, self.config.internal_timeout_seconds),
            expected_webview_port=webview_port,
        )
        client: dict[str, Any] = {
            "ready": False,
            "reason": cdp.reason or "CDP renderer is not ready",
        }
        if cdp.ready:
            try:
                async with InternalChatClient(
                    self.config.cdp_endpoint,
                    timeout=self.config.internal_timeout_seconds,
                    webview_port=webview_port,
                ) as runtime:
                    result = await runtime.health()
                client = {"ready": bool(result.get("ready")), **result}
            except Exception as error:
                client = {
                    "ready": False,
                    "reason": sanitize_runtime_error(error),
                    "error_class": type(error).__name__,
                }
        ready = bool(webview["ready"] and cdp.ready and client.get("ready"))
        return {
            "ready": ready,
            "webview": webview,
            "cdp": {
                "available": cdp.available,
                "ready": cdp.ready,
                "reason": cdp.reason,
                "target_id": cdp.target.target_id if cdp.target else "",
                "target_url": cdp.target.url if cdp.target else "",
            },
            "client": client,
            "checked_at": time.time(),
        }

    async def start(self, *, wait: bool = True) -> dict[str, Any]:
        return await self._operate("start", wait=wait)

    async def stop(self) -> dict[str, Any]:
        async with self._operation_lock:
            await asyncio.to_thread(self._systemctl, "stop")
            return await self.status()

    async def restart(self, *, wait: bool = True) -> dict[str, Any]:
        return await self._operate("restart", wait=wait)

    async def recover(self) -> dict[str, Any]:
        health = await self.health()
        if health["ready"]:
            return {"action": "none", "recovered": False, "health": health}
        result = await self.restart(wait=True)
        return {"action": "restart", "recovered": True, **result}

    async def ensure_ready(self) -> dict[str, Any]:
        health = await self.health()
        if health["ready"]:
            return health
        result = await self.recover()
        recovered = result.get("health")
        if not isinstance(recovered, dict) or not recovered.get("ready"):
            raise RuntimeError(
                "ChatGPT runtime service did not recover: "
                + json.dumps(recovered or result, sort_keys=True)[:1000]
            )
        return recovered

    async def logs(self, *, lines: int = 200) -> dict[str, Any]:
        lines = min(max(int(lines), 1), 2000)
        completed = await asyncio.to_thread(
            subprocess.run,
            [
                "journalctl",
                "--user",
                "-u",
                self.config.service_name,
                "--no-pager",
                "-n",
                str(lines),
                "-o",
                "short-iso",
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        return {
            "service_name": self.config.service_name,
            "lines": lines,
            "returncode": completed.returncode,
            "text": (completed.stdout + completed.stderr)[-100_000:],
        }

    async def _operate(self, action: str, *, wait: bool) -> dict[str, Any]:
        async with self._operation_lock:
            await asyncio.to_thread(self._systemctl, action)
            if not wait:
                return {"action": action, "health": await self.health()}
            deadline = time.monotonic() + self.config.recovery_timeout_seconds
            last = await self.health()
            while not last["ready"] and time.monotonic() < deadline:
                await asyncio.sleep(self.config.poll_interval_seconds)
                last = await self.health()
            if not last["ready"]:
                raise RuntimeError(
                    f"ChatGPT runtime service {action} did not become ready: "
                    + json.dumps(last, sort_keys=True)[:1000]
                )
            return {"action": action, "health": last}

    async def _webview_health(self) -> dict[str, Any]:
        try:
            async with httpx.AsyncClient(
                timeout=min(3.0, self.config.internal_timeout_seconds),
                trust_env=False,
            ) as client:
                response = await client.get(self.config.webview_url)
                response.raise_for_status()
            text = response.text
            ready = "startup-loader" in text and (
                "<title>Codex</title>" in text or "<title>ChatGPT</title>" in text
            )
            return {
                "ready": ready,
                "status_code": response.status_code,
                "reason": ""
                if ready
                else "expected ChatGPT webview markers are absent",
            }
        except Exception as error:
            return {
                "ready": False,
                "status_code": None,
                "reason": sanitize_runtime_error(error),
                "error_class": type(error).__name__,
            }

    def _systemctl(self, action: str) -> None:
        completed = subprocess.run(
            ["systemctl", "--user", action, self.config.service_name],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if completed.returncode != 0:
            detail = sanitize_runtime_error(completed.stderr or completed.stdout)
            raise RuntimeError(
                f"systemctl --user {action} {self.config.service_name} failed: {detail}"
            )

    def _service_properties(self) -> dict[str, Any]:
        completed = subprocess.run(
            [
                "systemctl",
                "--user",
                "show",
                self.config.service_name,
                "--property=Id,LoadState,ActiveState,SubState,MainPID,NRestarts,Result,ControlGroup,ExecMainStartTimestamp",
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        values: dict[str, Any] = {"returncode": completed.returncode}
        for line in completed.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                values[key] = value
        if completed.stderr.strip():
            values["error"] = sanitize_runtime_error(completed.stderr)
        return values
