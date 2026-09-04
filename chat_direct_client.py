from __future__ import annotations

import asyncio
import contextlib
import json
import os
import uuid
from pathlib import Path
from typing import Any

from chat_internal_client import (
    RuntimeProtocolError,
    RuntimeUnavailableError,
    build_thread_context,
    build_thread_tail,
    normalize_conversation_payload,
    normalize_project,
    normalize_project_list,
    normalize_project_threads,
    sanitize_runtime_error,
)


class DirectChatTransport:
    """Lazy JSON-lines bridge to a Node worker owned by this MCP process."""

    def __init__(self, *, worker_path: Path | None = None, timeout: float = 45.0) -> None:
        self.worker_path = worker_path or Path(__file__).with_name("chat_direct_worker.mjs")
        self.timeout = max(1.0, timeout)
        self._process: asyncio.subprocess.Process | None = None
        self._lock = asyncio.Lock()
        self._sequence = 0
        self._paused = False
        self._active_read: asyncio.Task[bytes] | None = None

    async def _start(self) -> asyncio.subprocess.Process:
        current = self._process
        if current is not None and current.returncode is None:
            return current
        try:
            self._process = await asyncio.create_subprocess_exec(
                os.environ.get("MCP_CHAT_DIRECT_NODE", "node"),
                str(self.worker_path),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=False,
            )
        except (OSError, ValueError) as exc:
            raise RuntimeUnavailableError(
                f"direct ChatGPT worker could not start: {sanitize_runtime_error(exc)}"
            ) from exc
        return self._process

    async def request(self, method: str, **params: Any) -> Any:
        request_timeout = float(params.pop("_request_timeout", self.timeout))
        if self._paused:
            raise RuntimeUnavailableError("direct ChatGPT transport is stopped")
        async with self._lock:
            for attempt in range(2):
                if self._paused:
                    raise RuntimeUnavailableError("direct ChatGPT transport is stopped")
                process = await self._start()
                if self._paused:
                    await self._discard_process()
                    raise RuntimeUnavailableError("direct ChatGPT transport is stopped")
                submission_started = False
                self._sequence += 1
                identifier = self._sequence
                message = json.dumps(
                    {"id": identifier, "method": method, "params": params},
                    separators=(",", ":"),
                )
                try:
                    assert process.stdin is not None and process.stdout is not None
                    process.stdin.write((message + "\n").encode())
                    await process.stdin.drain()
                    while True:
                        reading = asyncio.create_task(process.stdout.readline())
                        self._active_read = reading
                        try:
                            raw = await asyncio.wait_for(
                                reading,
                                timeout=max(self.timeout, request_timeout),
                            )
                        finally:
                            if self._active_read is reading:
                                self._active_read = None
                        if not raw:
                            raise ConnectionError("direct worker exited")
                        response = json.loads(raw)
                        if response.get("id") != identifier:
                            raise RuntimeProtocolError("direct worker response id mismatch")
                        if response.get("event") == "submission_started":
                            submission_started = True
                            continue
                        break
                    if response.get("ok") is not True:
                        error = response.get("error") or {}
                        raise DirectBackendResponseError(
                            sanitize_runtime_error(error.get("message") or "direct backend request failed"),
                            status_code=int(error["status"]) if error.get("status") else None,
                        )
                    return response.get("result")
                except asyncio.CancelledError as exc:
                    await self._discard_process()
                    if self._paused:
                        raise RuntimeUnavailableError(
                            "direct ChatGPT request was stopped"
                        ) from exc
                    raise
                except RuntimeProtocolError:
                    raise
                except (OSError, ConnectionError, asyncio.TimeoutError, json.JSONDecodeError) as exc:
                    await self._discard_process()
                    if self._paused:
                        raise RuntimeUnavailableError(
                            "direct ChatGPT request was stopped"
                        ) from exc
                    if attempt == 0 and method in {
                        "health", "models", "list_projects", "get_project",
                        "list_project_threads", "get_thread",
                    }:
                        continue
                    if method in {"create_thread", "continue_thread"} and submission_started:
                        raise DirectSubmissionUncertainError(
                            f"direct ChatGPT submission became uncertain: {sanitize_runtime_error(exc)}"
                        ) from exc
                    raise RuntimeUnavailableError(
                        f"direct ChatGPT worker unavailable: {sanitize_runtime_error(exc)}"
                    ) from exc
        raise RuntimeUnavailableError("direct ChatGPT worker unavailable")

    async def _discard_process(self) -> None:
        process, self._process = self._process, None
        await self._terminate_process(process)

    @staticmethod
    async def _terminate_process(process: asyncio.subprocess.Process | None) -> None:
        if process is None or process.returncode is not None:
            return
        process.terminate()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(process.wait(), timeout=2)
        if process.returncode is None:
            process.kill()
            with contextlib.suppress(Exception):
                await process.wait()

    async def close(self) -> None:
        async with self._lock:
            process = self._process
            if process is not None and process.returncode is None:
                with contextlib.suppress(Exception):
                    assert process.stdin is not None
                    process.stdin.write(b'{"id":0,"method":"shutdown","params":{}}\n')
                    await process.stdin.drain()
            await self._discard_process()

    async def pause(self) -> None:
        self._paused = True
        # Do not wait behind a request holding _lock for a long completion.
        process, self._process = self._process, None
        await self._terminate_process(process)
        reading = self._active_read
        if reading is not None:
            reading.cancel()

    def resume(self) -> None:
        self._paused = False

    @property
    def paused(self) -> bool:
        return self._paused


class DirectBackendResponseError(RuntimeProtocolError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class DirectSubmissionUncertainError(RuntimeUnavailableError):
    """The worker began the provider POST but lost its final acknowledgement."""


_SHARED_TRANSPORT: DirectChatTransport | None = None


def get_direct_chat_transport() -> DirectChatTransport:
    global _SHARED_TRANSPORT
    if _SHARED_TRANSPORT is None:
        _SHARED_TRANSPORT = DirectChatTransport(
            timeout=float(os.environ.get("MCP_CHAT_DIRECT_TIMEOUT_SECONDS", "45"))
        )
    return _SHARED_TRANSPORT


class DirectChatRuntimeController:
    """Compatibility control surface for the in-process direct transport."""

    def __init__(self) -> None:
        self.transport = get_direct_chat_transport()

    async def health(self) -> dict[str, Any]:
        if self.transport.paused:
            return {
                "ready": False,
                "managed_service": False,
                "transport": "direct",
                "stopped": True,
                "reason": "direct ChatGPT transport is stopped",
            }
        try:
            payload = await self.transport.request("health")
            return {**payload, "ready": bool(payload.get("ready")), "managed_service": False}
        except Exception as exc:
            return {
                "ready": False,
                "managed_service": False,
                "transport": "direct",
                "reason": sanitize_runtime_error(exc),
                "error_class": type(exc).__name__,
            }

    async def status(self) -> dict[str, Any]:
        health = await self.health()
        return {
            "service": {"managed": False, "name": "terminal-mcp-owned-direct-worker"},
            "health": health,
            "ready": bool(health.get("ready")),
        }

    async def ensure_ready(self) -> dict[str, Any]:
        health = await self.health()
        if not health.get("ready"):
            raise RuntimeUnavailableError(
                str(health.get("reason") or "direct ChatGPT transport is not ready")
            )
        return health

    async def start(self, *, wait: bool = True) -> dict[str, Any]:
        del wait
        self.transport.resume()
        return {"action": "start", "health": await self.ensure_ready()}

    async def stop(self) -> dict[str, Any]:
        await self.transport.pause()
        return {"action": "stop", "ready": False, "managed_service": False}

    async def restart(self, *, wait: bool = True) -> dict[str, Any]:
        del wait
        await self.transport.close()
        self.transport.resume()
        return {"action": "restart", "health": await self.ensure_ready()}

    async def recover(self) -> dict[str, Any]:
        await self.transport.close()
        self.transport.resume()
        return {"action": "restart", "recovered": True, "health": await self.ensure_ready()}

    async def logs(self, *, lines: int = 200) -> dict[str, Any]:
        return {
            "managed_service": False,
            "lines": min(max(int(lines), 1), 2000),
            "text": "Direct transport is owned by Terminal MCP; secrets and worker stderr are not retained.",
        }


class DirectChatClient:
    """Normal ChatGPT client that never attaches to or launches a desktop app."""

    def __init__(
        self,
        *,
        transport: DirectChatTransport | Any | None = None,
        preferred_model: str = "",
        thinking_effort: str = "extended",
        require_high_reasoning: bool = True,
        stream_timeout_seconds: int = 3600,
    ) -> None:
        self.transport = transport or get_direct_chat_transport()
        self.preferred_model = preferred_model.strip()
        self.thinking_effort = thinking_effort.strip()
        self.require_high_reasoning = bool(require_high_reasoning)
        self.stream_timeout_seconds = max(30, int(stream_timeout_seconds))

    async def __aenter__(self) -> "DirectChatClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        # Shared transport intentionally remains warm for the Terminal MCP lifetime.
        return None

    async def connect(self) -> None:
        await self.health()

    async def close(self) -> None:
        return None

    async def health(self) -> dict[str, Any]:
        result = await self.transport.request("health")
        if not isinstance(result, dict) or result.get("ready") is not True:
            raise RuntimeProtocolError("direct ChatGPT readiness returned false")
        return result

    async def models(self) -> dict[str, Any]:
        result = await self.transport.request("models")
        if not isinstance(result, dict):
            raise RuntimeProtocolError("model catalogue returned an invalid result")
        return result

    async def list_projects(self, *, limit: int = 20, cursor: str | None = None, owned_only: bool = True) -> dict[str, Any]:
        raw = await self.transport.request("list_projects", limit=min(max(limit, 1), 50), cursor=cursor, owned_only=owned_only)
        return normalize_project_list(raw)

    async def get_project(self, project_id: str) -> dict[str, Any]:
        raw = await self.transport.request("get_project", project_id=project_id)
        return normalize_project(raw)

    async def list_project_threads(self, project_id: str, *, limit: int = 20, cursor: str | None = None, owned_only: bool = True) -> dict[str, Any]:
        raw = await self.transport.request("list_project_threads", project_id=project_id, limit=min(max(limit, 1), 50), cursor=cursor, owned_only=owned_only)
        return normalize_project_threads(raw, project_id)

    async def find_message(self, message_id: str, *, project_id: str = "") -> str:
        payload = await self.transport.request(
            "find_message", message_id=message_id, project_id=project_id
        )
        if not isinstance(payload, dict) or not payload.get("found"):
            return ""
        return str(payload.get("conversation_id") or "")

    async def create_thread(self, prompt: str, *, project_id: str | None = None, title: str | None = None, wait_for_completion: bool = False, user_message_id: str | None = None) -> dict[str, Any]:
        payload = await self.transport.request(
            "create_thread", prompt=prompt, project_id=project_id or "", title=title or "",
            wait_for_completion=wait_for_completion, user_message_id=user_message_id or str(uuid.uuid4()),
            preferred_model=self.preferred_model, thinking_effort=self.thinking_effort,
            require_high_reasoning=self.require_high_reasoning,
            stream_timeout_seconds=self.stream_timeout_seconds,
            _request_timeout=(self.stream_timeout_seconds + 15 if wait_for_completion else 130),
        )
        if isinstance(payload, dict) and payload.get("after_raw") is not None:
            conversation_id = str(payload.get("conversation_id") or "")
            after = normalize_conversation_payload(
                payload.pop("after_raw"), conversation_id,
                owned_stream=bool(payload.get("running")),
            )
            payload["current_node"] = str(after.get("current_node") or "")
        return payload

    async def get_thread(self, conversation_id: str) -> dict[str, Any]:
        raw = await self.transport.request("get_thread", conversation_id=conversation_id)
        return normalize_conversation_payload(raw, conversation_id, owned_stream=bool(raw.get("owned_stream")) if isinstance(raw, dict) else False)

    async def thread_context(self, conversation_id: str, *, max_events: int = 60, max_chars: int = 12000) -> dict[str, Any]:
        raw = await self.transport.request("get_thread", conversation_id=conversation_id)
        return build_thread_context(raw, conversation_id, max_events=max_events, max_chars=max_chars)

    async def thread_tail(self, conversation_id: str, *, lines: int = 60, max_chars: int = 12000) -> dict[str, Any]:
        raw = await self.transport.request("get_thread", conversation_id=conversation_id)
        return build_thread_tail(raw, conversation_id, lines=lines, max_chars=max_chars)

    async def continue_thread(self, conversation_id: str, message: str, *, expected_current_node: str, wait_for_completion: bool = True, user_message_id: str | None = None, force: bool = False) -> dict[str, Any]:
        payload = await self.transport.request(
            "continue_thread", conversation_id=conversation_id, message=message,
            expected_current_node=expected_current_node, wait_for_completion=wait_for_completion,
            user_message_id=user_message_id or str(uuid.uuid4()), force=force,
            preferred_model=self.preferred_model, thinking_effort=self.thinking_effort,
            require_high_reasoning=self.require_high_reasoning,
            stream_timeout_seconds=self.stream_timeout_seconds,
            _request_timeout=(self.stream_timeout_seconds + 15 if wait_for_completion else 130),
        )
        if isinstance(payload, dict):
            payload.pop("after_raw", None)
            payload.setdefault("parent_message_id", expected_current_node)
        return payload

    async def cancel(self, conversation_id: str) -> dict[str, Any]:
        return await self.transport.request("cancel_thread", conversation_id=conversation_id)

    async def delete_thread(self, conversation_id: str) -> dict[str, Any]:
        return await self.transport.request("delete_thread", conversation_id=conversation_id)
