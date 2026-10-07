from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
import websockets

from chat_gateway.renderer_bridge import CHAT_RENDERER_BRIDGE_JS
from chat_gateway.renderer_target import (
    RendererTarget as RuntimeTarget,
    RendererTargetError,
    select_main_renderer_target as _select_main_renderer_target,
)


DEFAULT_CODEX_CDP_ENDPOINT = "http://127.0.0.1:9222"
_RUNNING_STATUSES = frozenset({"in_progress", "streaming", "queued", "pending"})


def sanitize_runtime_error(value: Any) -> str:
    text = str(value or "").replace("\n", " ")
    text = re.sub(r"Bearer\s+\S+", "Bearer [redacted]", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(authorization|cookie|token)\s*[:=]\s*[^\s,;]+",
        r"\1=[redacted]",
        text,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s+", " ", text).strip()[:500]


class InternalRuntimeError(RuntimeError):
    pass


class RuntimeUnavailableError(InternalRuntimeError):
    pass


class RuntimeNotReadyError(InternalRuntimeError):
    pass


class RuntimeProtocolError(InternalRuntimeError):
    pass


_TRANSPORT_EXCEPTIONS = (
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
    websockets.ConnectionClosed,
)
_MUTATION_EXCEPTIONS = (InternalRuntimeError,) + _TRANSPORT_EXCEPTIONS


def _javascript_exception_description(
    result: dict[str, Any], remote: dict[str, Any]
) -> str:
    details = result.get("exceptionDetails")
    candidates: list[str] = []
    if isinstance(remote.get("description"), str):
        candidates.append(remote["description"])
    if isinstance(details, dict):
        exception = details.get("exception")
        if isinstance(exception, dict) and isinstance(exception.get("description"), str):
            candidates.append(exception["description"])
        if isinstance(details.get("text"), str):
            candidates.append(details["text"])
        line = details.get("lineNumber")
        column = details.get("columnNumber")
        if isinstance(line, int):
            location = f"line {line + 1}"
            if isinstance(column, int):
                location += f", column {column + 1}"
            candidates.append(location)
    description = ": ".join(value.strip() for value in candidates if value.strip())
    return sanitize_runtime_error(description or "JavaScript evaluation failed")


@dataclass(frozen=True)
class RuntimeProbe:
    available: bool
    ready: bool
    reason: str = ""
    target: RuntimeTarget | None = None


def select_main_renderer_target(
    targets: list[dict[str, Any]], *, expected_webview_port: int = 5175
) -> RuntimeTarget:
    try:
        return _select_main_renderer_target(
            targets, expected_webview_port=expected_webview_port
        )
    except RendererTargetError as error:
        raise RuntimeNotReadyError(str(error)) from error


async def probe_runtime(
    endpoint: str,
    *,
    timeout: float = 1.5,
    expected_webview_port: int = 5175,
) -> RuntimeProbe:
    try:
        async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
            response = await client.get(f"{endpoint.rstrip('/')}/json/list")
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, OSError, ValueError) as exc:
        return RuntimeProbe(False, False, f"{type(exc).__name__}: {exc}")
    if not isinstance(payload, list):
        return RuntimeProbe(True, False, "CDP target list is not an array")
    try:
        target = select_main_renderer_target(
            payload, expected_webview_port=expected_webview_port
        )
    except RuntimeNotReadyError as exc:
        return RuntimeProbe(True, False, str(exc))
    return RuntimeProbe(True, True, target=target)


def _plain_conversation(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    for key in ("conversation", "data"):
        nested = raw.get(key)
        if isinstance(nested, dict) and isinstance(nested.get("mapping"), dict):
            return nested
    return raw


def _part_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("text", "content", "value"):
            if isinstance(value.get(key), str):
                return value[key]
    return ""


def _message_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts")
    if isinstance(parts, list):
        return "\n".join(text for text in (_part_text(part) for part in parts) if text)
    return content.get("text") if isinstance(content.get("text"), str) else ""


def normalize_conversation_payload(
    raw: Any,
    conversation_id: str,
    *,
    owned_stream: bool = False,
) -> dict[str, Any]:
    conversation = _plain_conversation(raw)
    mapping = conversation.get("mapping")
    if not isinstance(mapping, dict):
        return {
            "found": False,
            "conversation_id": conversation_id,
            "canonical": True,
            "state_verified": False,
            "reason": "canonical conversation mapping is missing",
            "turns": [],
        }

    current_node = str(conversation.get("current_node") or conversation.get("currentNode") or "")
    if not current_node or current_node not in mapping:
        return {
            "found": True,
            "conversation_id": str(conversation.get("id") or conversation_id),
            "title": str(conversation.get("title") or ""),
            "current_node": current_node,
            "canonical": True,
            "state_verified": False,
            "reason": "canonical current_node is missing from the mapping",
            "turns": [],
            "active_stream": owned_stream,
            "running": owned_stream,
        }

    reverse: list[dict[str, Any]] = []
    seen: set[str] = set()
    cursor = current_node
    valid = True
    while cursor:
        if cursor in seen or len(reverse) >= 10_000:
            valid = False
            break
        seen.add(cursor)
        node = mapping.get(cursor)
        if not isinstance(node, dict):
            valid = False
            break
        message = node.get("message")
        if isinstance(message, dict):
            author = message.get("author")
            metadata = message.get("metadata")
            hidden = bool(
                isinstance(metadata, dict)
                and metadata.get("is_visually_hidden_from_conversation")
            )
            finish_details = metadata.get("finish_details") if isinstance(metadata, dict) else None
            if not hidden:
                reverse.append(
                    {
                    "node_id": cursor,
                    "key": str(message.get("id") or cursor),
                    "role": str(author.get("role") if isinstance(author, dict) else ""),
                    "text": _message_text(message),
                    "parent_id": str(node.get("parent") or ""),
                    "status": str(message.get("status") or ""),
                    "end_turn": message.get("end_turn")
                    if isinstance(message.get("end_turn"), bool)
                    else None,
                    "create_time": float(message["create_time"])
                    if isinstance(message.get("create_time"), (int, float))
                    else None,
                    "model_slug": str(
                        metadata.get("model_slug")
                        or metadata.get("default_model_slug")
                        or ""
                    )
                    if isinstance(metadata, dict)
                    else "",
                    "finish_details": finish_details if isinstance(finish_details, dict) else None,
                }
            )
        cursor = str(node.get("parent") or "")

    active_branch = list(reversed(reverse))
    # Keep branch-wide IDs separately from the active transcript.  A send can
    # create a sibling branch while the user (or the app) moves current_node;
    # reconciliation must be able to prove that exact generated message without
    # treating the side branch as canonical for task classification.
    branch_message_ids = {
        str(message.get("id") or node_id)
        for node_id, node in mapping.items()
        if isinstance(node, dict)
        and isinstance((message := node.get("message")), dict)
        and not (
            isinstance(message.get("metadata"), dict)
            and message["metadata"].get("is_visually_hidden_from_conversation")
        )
    }
    visible_turns = [turn for turn in active_branch if turn["role"] in {"user", "assistant"}]
    visible_current_node = (
        str(visible_turns[-1].get("node_id") or "") if visible_turns else ""
    )
    latest = visible_turns[-1] if visible_turns else None
    status = str(latest.get("status") or "").casefold() if latest else ""
    canonical_running = bool(
        latest
        and latest.get("role") == "assistant"
        and (status in _RUNNING_STATUSES or latest.get("end_turn") is False)
    )
    update_time = conversation.get("update_time")
    return {
        "found": True,
        "conversation_id": str(
            conversation.get("id") or conversation.get("conversation_id") or conversation_id
        ),
        "title": str(conversation.get("title") or ""),
        "project_id": str(conversation.get("gizmo_id") or "") or None,
        "current_node": current_node,
        "visible_current_node": visible_current_node,
        "canonical": True,
        "state_verified": valid and bool(active_branch),
        "reason": "" if valid else "active branch is cyclic or incomplete",
        "update_time": float(update_time) if isinstance(update_time, (int, float)) else None,
        "active_stream": owned_stream,
        "running": owned_stream or canonical_running,
        "turns": visible_turns,
        "branch_message_ids": sorted(branch_message_ids),
        "all_message_ids": sorted(branch_message_ids),
    }


def _sanitize_context_text(value: Any, *, limit: int = 2000) -> str:
    text = _part_text(value) if not isinstance(value, str) else value
    text = str(text or "")
    text = re.sub(r"Bearer\s+\S+", "Bearer [redacted]", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(?i)(authorization|cookie|api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|session(?:[_-]?id)?|token|secret|password)\s*[:=]\s*[^\s,;]+",
        r"\1=[redacted]",
        text,
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text[: max(0, limit)]


def _project_payload(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    candidate = raw
    for key in ("resource", "project", "gizmo"):
        nested = candidate.get(key)
        if isinstance(nested, dict):
            candidate = nested
    nested = candidate.get("gizmo")
    if isinstance(nested, dict):
        candidate = nested
    return candidate if isinstance(candidate, dict) else {}


def normalize_project(raw: Any) -> dict[str, Any]:
    project = _project_payload(raw)
    display = project.get("display") if isinstance(project.get("display"), dict) else {}
    permissions = (
        project.get("current_user_permission")
        if isinstance(project.get("current_user_permission"), dict)
        else {}
    )
    project_id = str(project.get("id") or project.get("gizmo_id") or "")
    return {
        "id": project_id,
        "name": str(display.get("name") or project.get("name") or ""),
        "description": str(display.get("description") or project.get("description") or ""),
        "archived": bool(project.get("is_archived") or project.get("archived_at")),
        "updated_at": str(project.get("updated_at") or ""),
        "permissions": {
            "can_read": bool(permissions.get("can_read", True if project_id else False)),
            "can_write": bool(permissions.get("can_write", False)),
            "can_delete": bool(permissions.get("can_delete", False)),
        },
    }


def normalize_project_list(raw: Any) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    normalized = []
    for item in items:
        project = normalize_project(item)
        if not project["id"]:
            continue
        conversations = item.get("conversations") if isinstance(item, dict) else None
        conversation_items = (
            conversations.get("items")
            if isinstance(conversations, dict) and isinstance(conversations.get("items"), list)
            else []
        )
        project["conversation_count"] = len(conversation_items)
        normalized.append(project)
    return {"items": normalized, "cursor": payload.get("cursor")}


def normalize_project_threads(raw: Any, project_id: str) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    items = payload.get("items") if isinstance(payload.get("items"), list) else []
    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        conversation_id = str(item.get("id") or item.get("conversation_id") or "")
        if not conversation_id:
            continue
        result.append(
            {
                "conversation_id": conversation_id,
                "title": str(item.get("title") or ""),
                "project_id": str(item.get("gizmo_id") or project_id),
                "current_node": str(item.get("current_node") or ""),
                "snippet": str(item.get("snippet") or ""),
                "create_time": item.get("create_time"),
                "update_time": item.get("update_time")
                if isinstance(item.get("update_time"), (int, float, str))
                else None,
                "archived": bool(item.get("is_archived")),
            }
        )
    return {"items": result, "cursor": payload.get("cursor")}


def _raw_active_branch(raw: Any, conversation_id: str) -> tuple[dict[str, Any], list[dict[str, Any]], bool]:
    conversation = _plain_conversation(raw)
    mapping = conversation.get("mapping")
    if not isinstance(mapping, dict):
        return conversation, [], False
    current_node = str(conversation.get("current_node") or conversation.get("currentNode") or "")
    if not current_node or current_node not in mapping:
        return conversation, [], False
    reverse: list[dict[str, Any]] = []
    seen: set[str] = set()
    cursor = current_node
    valid = True
    while cursor:
        if cursor in seen or len(reverse) >= 10_000:
            valid = False
            break
        seen.add(cursor)
        node = mapping.get(cursor)
        if not isinstance(node, dict):
            valid = False
            break
        message = node.get("message")
        if isinstance(message, dict):
            author = message.get("author") if isinstance(message.get("author"), dict) else {}
            content = message.get("content") if isinstance(message.get("content"), dict) else {}
            metadata = message.get("metadata") if isinstance(message.get("metadata"), dict) else {}
            text = _message_text(message)
            if not text and isinstance(content.get("content"), str):
                text = content["content"]
            reverse.append(
                {
                    "node_id": cursor,
                    "message_id": str(message.get("id") or cursor),
                    "parent_id": str(node.get("parent") or ""),
                    "role": str(author.get("role") or ""),
                    "author_name": str(author.get("name") or ""),
                    "recipient": str(message.get("recipient") or "all"),
                    "content_type": str(content.get("content_type") or ""),
                    "text": text,
                    "status": str(message.get("status") or ""),
                    "end_turn": message.get("end_turn")
                    if isinstance(message.get("end_turn"), bool)
                    else None,
                    "hidden": bool(metadata.get("is_visually_hidden_from_conversation")),
                    "thinking_preamble": bool(metadata.get("is_thinking_preamble_message")),
                    "create_time": message.get("create_time")
                    if isinstance(message.get("create_time"), (int, float))
                    else None,
                }
            )
        cursor = str(node.get("parent") or "")
    return conversation, list(reversed(reverse)), valid


def _context_event_cursor(item: dict[str, Any], event: dict[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            {
                "node_id": item["node_id"],
                "message_id": item["message_id"],
                "kind": event.get("kind"),
                "tool": event.get("tool"),
                "status": event.get("status"),
                "summary": event.get("summary"),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:16]
    return f"{item['node_id']}:{digest}"


def build_thread_context(
    raw: Any,
    conversation_id: str,
    *,
    since_cursor: str | None = None,
    max_events: int = 60,
    max_chars: int = 12000,
) -> dict[str, Any]:
    conversation, branch, valid = _raw_active_branch(raw, conversation_id)
    events: list[dict[str, Any]] = []
    for item in branch:
        role = item["role"]
        content_type = item["content_type"]
        recipient = item["recipient"]
        event: dict[str, Any] | None = None
        if content_type == "reasoning_recap" and role == "assistant":
            event = {
                "kind": "reasoning_recap",
                "summary": _sanitize_context_text(item["text"], limit=1600),
            }
        elif role == "assistant" and recipient not in {"", "all"}:
            event = {
                "kind": "tool_call",
                "tool": recipient,
                "status": item["status"],
            }
        elif role == "tool":
            event = {
                "kind": "tool_result",
                "tool": item["author_name"] or recipient,
                "status": item["status"],
                "summary": _sanitize_context_text(item["text"], limit=1200),
            }
        elif role == "assistant" and content_type in {"text", "multimodal_text"}:
            if item["end_turn"] is True:
                event = {
                    "kind": "final",
                    "summary": _sanitize_context_text(item["text"], limit=2400),
                    "status": item["status"],
                }
            elif item["text"] and (
                not item["hidden"] or item["thinking_preamble"]
            ):
                event = {
                    "kind": "progress",
                    "summary": _sanitize_context_text(item["text"], limit=1600),
                    "status": item["status"],
                }
        if event is None:
            continue
        event.update(
            {
                "node_id": item["node_id"],
                "message_id": item["message_id"],
                "create_time": item["create_time"],
            }
        )
        event["cursor"] = _context_event_cursor(item, event)
        events.append(event)

    event_limit = min(max(max_events, 1), 200)
    cursor_reset = False
    cursor_updated = False
    selected: list[dict[str, Any]]
    if since_cursor:
        exact_index = next(
            (
                index
                for index, event in enumerate(events)
                if event["cursor"] == since_cursor
            ),
            None,
        )
        if exact_index is not None:
            selected = events[exact_index + 1 : exact_index + 1 + event_limit]
        else:
            node_hint = since_cursor.split(":", 1)[0]
            node_index = next(
                (
                    index
                    for index, event in enumerate(events)
                    if event["node_id"] == node_hint
                ),
                None,
            )
            if node_index is not None:
                cursor_updated = True
                selected = events[node_index : node_index + event_limit]
            else:
                cursor_reset = True
                selected = events[-event_limit:]
    else:
        selected = events[-event_limit:]

    bounded: list[dict[str, Any]] = []
    used = 0
    for event in selected:
        encoded = json.dumps(event, ensure_ascii=False, sort_keys=True)
        if bounded and used + len(encoded) > max_chars:
            break
        if not bounded and len(encoded) > max_chars:
            event = {
                **event,
                "summary": str(event.get("summary") or "")[
                    : max(0, max_chars // 2)
                ],
            }
            encoded = json.dumps(event, ensure_ascii=False, sort_keys=True)
        bounded.append(event)
        used += len(encoded)

    current_node = str(
        conversation.get("current_node") or conversation.get("currentNode") or ""
    )
    latest = branch[-1] if branch else {}
    latest_status = str(latest.get("status") or "").casefold()
    running = bool(
        latest.get("role") == "assistant"
        and (
            latest_status in _RUNNING_STATUSES
            or latest.get("end_turn") is False
        )
    )
    next_cursor = (
        bounded[-1]["cursor"]
        if bounded
        else (since_cursor or current_node)
    )
    return {
        "conversation_id": str(conversation.get("id") or conversation_id),
        "title": str(conversation.get("title") or ""),
        "project_id": str(conversation.get("gizmo_id") or "") or None,
        "current_node": current_node,
        "state_verified": valid and bool(branch),
        "running": running,
        "latest_status": latest_status,
        "events": bounded,
        "since_cursor": since_cursor,
        "next_cursor": next_cursor,
        "cursor_reset": cursor_reset,
        "cursor_updated": cursor_updated,
    }


def build_thread_tail(
    raw: Any, conversation_id: str, *, lines: int = 60, max_chars: int = 12000
) -> dict[str, Any]:
    conversation, branch, valid = _raw_active_branch(raw, conversation_id)
    rendered: list[str] = []
    for item in branch:
        role = item["role"]
        if item["content_type"] == "reasoning_recap" and role == "assistant":
            recap = _sanitize_context_text(item["text"], limit=1600)
            if recap:
                rendered.append(f"reasoning_recap: {recap}")
        elif role in {"user", "assistant"} and item["recipient"] in {"", "all"}:
            if item["hidden"] and not item["thinking_preamble"]:
                continue
            text = _sanitize_context_text(item["text"], limit=2400)
            if text:
                rendered.append(f"{role}: {text}")
        elif role == "assistant" and item["recipient"] not in {"", "all"}:
            rendered.append(f"tool_call: {item['recipient']} [{item['status']}]" )
        elif role == "tool":
            summary = _sanitize_context_text(item["text"], limit=1000)
            rendered.append(f"tool_result: {item['author_name'] or item['recipient']} [{item['status']}] {summary}".strip())
    rendered = rendered[-min(max(lines, 1), 200) :]
    while rendered and len("\n".join(rendered)) > max_chars:
        rendered.pop(0)
    return {
        "conversation_id": str(conversation.get("id") or conversation_id),
        "title": str(conversation.get("title") or ""),
        "state_verified": valid and bool(branch),
        "current_node": str(conversation.get("current_node") or ""),
        "lines": rendered,
        "text": "\n".join(rendered),
    }


_BRIDGE_JS = CHAT_RENDERER_BRIDGE_JS


class InternalChatClient:
    """Sequential bridge to the normal-chat client already loaded by Codex desktop."""

    _locks: dict[tuple[str, int], asyncio.Lock] = {}

    def __init__(
        self,
        endpoint: str = DEFAULT_CODEX_CDP_ENDPOINT,
        *,
        timeout: float = 10.0,
        webview_port: int = 5175,
        stream_timeout_seconds: int = 3600,
        preferred_model: str = "",
        thinking_effort: str = "extended",
        require_high_reasoning: bool = True,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = max(1.0, timeout)
        self.webview_port = int(webview_port)
        if not 1 <= self.webview_port <= 65535:
            raise ValueError("webview_port must be between 1 and 65535")
        self.stream_timeout_seconds = max(30, stream_timeout_seconds)
        self.preferred_model = preferred_model.strip()
        self.thinking_effort = thinking_effort.strip()
        self.require_high_reasoning = require_high_reasoning
        self._socket: Any = None
        self._sequence = 0
        self._target: RuntimeTarget | None = None

    @classmethod
    def _lock_for(cls, endpoint: str) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        return cls._locks.setdefault((endpoint, id(loop)), asyncio.Lock())

    async def __aenter__(self) -> "InternalChatClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.close()

    async def connect(self) -> None:
        probe = await probe_runtime(
            self.endpoint,
            timeout=self.timeout,
            expected_webview_port=self.webview_port,
        )
        if not probe.available:
            raise RuntimeUnavailableError(probe.reason or "Codex CDP endpoint is unavailable")
        if not probe.ready or probe.target is None:
            raise RuntimeNotReadyError(probe.reason or "Codex main renderer is not ready")
        self._target = probe.target
        try:
            self._socket = await websockets.connect(
                probe.target.websocket_url,
                origin=None,
                open_timeout=self.timeout,
                close_timeout=1,
                max_size=16 * 1024 * 1024,
            )
            await self._call("Runtime.enable")
        except Exception as exc:
            await self.close()
            raise RuntimeUnavailableError(
                f"could not attach to Codex renderer: {sanitize_runtime_error(exc)}"
            ) from exc

    async def close(self) -> None:
        if self._socket is not None:
            with contextlib.suppress(Exception):
                await self._socket.close()
        self._socket = None
        self._target = None

    async def reconnect(self) -> None:
        await self.close()
        await self.connect()

    async def _call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        if self._socket is None:
            raise RuntimeUnavailableError("Codex renderer is not connected")
        self._sequence += 1
        identifier = self._sequence
        await self._socket.send(
            json.dumps({"id": identifier, "method": method, "params": params or {}})
        )
        response_timeout = self.timeout if timeout is None else max(1.0, float(timeout))
        while True:
            raw = await asyncio.wait_for(
                self._socket.recv(), timeout=response_timeout
            )
            message = json.loads(raw)
            if message.get("id") != identifier:
                continue
            if "error" in message:
                raise RuntimeProtocolError(f"CDP {method} failed")
            return message.get("result", {})

    async def _evaluate_raw(
        self, expression: str, *, timeout: float | None = None
    ) -> Any:
        result = await self._call(
            "Runtime.evaluate",
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": True,
                "userGesture": False,
            },
            timeout=timeout,
        )
        remote = result.get("result", {})
        if remote.get("subtype") == "error" or result.get("exceptionDetails"):
            raise RuntimeProtocolError(
                _javascript_exception_description(result, remote)
            )
        return remote.get("value")

    async def _evaluate(
        self,
        body: str,
        *,
        retry_on_transport: bool = True,
        timeout: float | None = None,
    ) -> Any:
        expression = f"(async () => {{ {_BRIDGE_JS} {body} }})()"
        async with self._lock_for(self.endpoint):
            try:
                return await self._evaluate_raw(expression, timeout=timeout)
            except _TRANSPORT_EXCEPTIONS:
                if not retry_on_transport:
                    raise
                await self.reconnect()
                return await self._evaluate_raw(expression, timeout=timeout)

    async def health(self) -> dict[str, Any]:
        payload = await self._evaluate("const resolved = await resolveClient(); const catalog = collectModels(await resolved.client.models()); return {ready:true, model_count:catalog.models.length, diagnostics:resolved.diagnostics};")
        if not isinstance(payload, dict) or not payload.get("ready"):
            raise RuntimeProtocolError("internal client readiness returned an invalid result")
        return {
            "ready": True,
            "model_count": int(payload.get("model_count", 0) or 0),
            "target_id": self._target.target_id if self._target else "",
            "target_url": self._target.url if self._target else "",
            "diagnostics": payload.get("diagnostics") if isinstance(payload.get("diagnostics"), dict) else {},
        }

    async def models(self) -> dict[str, Any]:
        payload = await self._evaluate("const resolved = await resolveClient(); return collectModels(await resolved.client.models());")
        if not isinstance(payload, dict):
            raise RuntimeProtocolError("model catalogue returned an invalid result")
        return {"models": payload.get("models") if isinstance(payload.get("models"), list) else [], "default_slug": str(payload.get("default_slug") or "")}

    async def list_projects(
        self,
        *,
        limit: int = 20,
        cursor: str | None = None,
        owned_only: bool = True,
    ) -> dict[str, Any]:
        limit = min(max(int(limit), 1), 50)
        payload = await self._evaluate(
            "const resolved = await resolveClient(); "
            f"return plain(await resolved.client.listProjects({{limit:{limit},cursor:{json.dumps(cursor)},ownedOnly:{json.dumps(bool(owned_only))},conversationsPerProject:0}}));"
        )
        return normalize_project_list(payload)

    async def get_project(self, project_id: str) -> dict[str, Any]:
        project_id = project_id.strip()
        if not project_id:
            raise ValueError("project_id is required")
        payload = await self._evaluate(
            f"const resolved = await resolveClient(); return plain(await resolved.client.getProject({json.dumps(project_id)}));"
        )
        return normalize_project(payload)

    async def list_project_threads(
        self,
        project_id: str,
        *,
        limit: int = 20,
        cursor: str | None = None,
        owned_only: bool = True,
    ) -> dict[str, Any]:
        project_id = project_id.strip()
        if not project_id:
            raise ValueError("project_id is required")
        limit = min(max(int(limit), 1), 50)
        payload = await self._evaluate(
            "const resolved = await resolveClient(); "
            f"return plain(await resolved.client.listProjectConversations({{projectId:{json.dumps(project_id)},limit:{limit},cursor:{json.dumps(cursor)},ownedOnly:{json.dumps(bool(owned_only))}}}));"
        )
        return normalize_project_threads(payload, project_id)

    async def _get_thread_raw(self, conversation_id: str) -> Any:
        return await self._evaluate(
            f"const resolved = await resolveClient(); return plain(await resolved.client.get({json.dumps(conversation_id)}));",
            timeout=max(self.timeout, 30.0),
        )

    async def create_thread(
        self,
        prompt: str,
        *,
        project_id: str | None = None,
        title: str | None = None,
    ) -> dict[str, Any]:
        prompt = prompt.strip()
        if not prompt:
            raise ValueError("prompt is required")
        project = project_id.strip() if project_id else ""
        user_message_id = str(uuid.uuid4())
        body = f"""
        const resolved = await resolveClient();
        const client = resolved.client;
        const text = {json.dumps(prompt)};
        const projectId = {json.dumps(project)};
        const model = chooseModel(await client.models(), {json.dumps(self.preferred_model)}, '', {json.dumps(self.thinking_effort)}, {json.dumps(self.require_high_reasoning)});
        const userMessageId = {json.dumps(user_message_id)};
        const request = {{
          action:'next', model:model.slug,
          messages:[{{id:userMessageId,author:{{role:'user'}},content:{{content_type:'text',parts:[text]}},create_time:Date.now()/1000,end_turn:null,metadata:{{}},recipient:'all',status:'finished_successfully',weight:1}}],
          supported_encodings:['v1'],
          timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,
          timezone_offset_min:new Date().getTimezoneOffset(),
        }};
        if (model.effort) request.thinking_effort = model.effort;
        if (projectId) {{
          request.gizmo_id = projectId;
          request.conversation_mode = {{kind:'gizmo_interaction',gizmo_id:projectId}};
        }}
        let conversationId = '', requestId = '', terminalEvent = '', finalMessageId = '', finalStatus = '';
        let handle = null, timedOut = false, streamFinished = false;
        const started = await new Promise((resolvePromise, rejectPromise) => {{
          let settled = false;
          const finishStart = () => {{ if (!settled && conversationId) {{ settled = true; clearTimeout(timer); resolvePromise(true); }} }};
          const fail = (error) => {{
            streamFinished = true;
            if (conversationId) {{ streams.delete(conversationId); handles.delete(conversationId); }}
            clearTimeout(timer);
            if (settled) return;
            settled = true;
            rejectPromise(error instanceof Error ? error : new Error(String(error)));
          }};
          const timer = setTimeout(() => {{
            if (settled) return;
            settled = true;
            timedOut = true;
            streamFinished = true;
            try {{
              if (handle && typeof handle.cancel === 'function') Promise.resolve(handle.cancel()).catch(() => {{}});
            }} catch {{}}
            resolvePromise(false);
          }}, Math.min({self.stream_timeout_seconds * 1000}, 30000));
          const observe = (value) => {{
            const candidate = value && (value.message || value.data || value);
            const possibleId = value && (value.conversation_id || value.conversationId) || candidate && (candidate.conversation_id || candidate.conversationId);
            if (possibleId) conversationId = String(possibleId);
            if (candidate && candidate.id) finalMessageId = String(candidate.id);
            if (candidate && candidate.status) finalStatus = String(candidate.status);
            if (value && value.request_id) requestId = String(value.request_id);
            if (value && value.streamRequestId) requestId = String(value.streamRequestId);
            const eventType = String(value && (value.type || value.event_type || value.event) || '');
            if (eventType === 'message_stream_complete') terminalEvent = eventType;
            if (conversationId) {{
              if (!streamFinished) {{
                streams.set(conversationId, {{started_at:Date.now(),user_message_id:userMessageId}});
                if (handle) handles.set(conversationId, handle);
              }}
              finishStart();
            }}
          }};
          const complete = (value) => {{ streamFinished = true; observe(value); terminalEvent = terminalEvent || 'onComplete'; if (conversationId) {{ streams.delete(conversationId); handles.delete(conversationId); }} finishStart(); }};
          try {{
            const starting = client.startCompletionStream({{request,onEvent:observe,onUpdate:observe,onComplete:complete,onError:fail,onRecoverableError:()=>{{}}}});
            if (starting && typeof starting.then === 'function') {{
              starting.then((value) => {{ handle = value; observe(value); if (conversationId && !streamFinished) handles.set(conversationId,value); }}, fail);
            }} else {{ handle = starting; observe(starting); }}
          }} catch (error) {{ fail(error); }}
        }});
        if (!conversationId) return {{sent:true,observed:false,running:true,reason:timedOut?'new conversation id was not observed before timeout':'new conversation id was not observed',request_id:requestId,user_message_id:userMessageId,project_id:projectId||null,terminal_event:terminalEvent}};
        let afterRaw = null;
        try {{ afterRaw = await client.get(conversationId); }} catch {{}}
        const finalSummary = summary(afterRaw);
        const ownedStream = ownedStreamActive(conversationId) && !streamFinished;
        return {{sent:true,observed:!!afterRaw,running:ownedStream||finalSummary.running,owned_stream:ownedStream,reason:afterRaw?'':'created conversation could not be read back',conversation_id:conversationId,request_id:requestId,user_message_id:userMessageId,final_message_id:finalMessageId,final_status:finalStatus,project_id:projectId||null,terminal_event:terminalEvent,after_raw:afterRaw?plain(afterRaw):null,title_requested:{json.dumps(title or '')}}};
        """
        payload = await self._evaluate(
            body,
            retry_on_transport=False,
            timeout=max(self.timeout, 35.0),
        )
        if not isinstance(payload, dict):
            raise RuntimeProtocolError("new conversation creation returned an invalid result")
        conversation_id = str(payload.get("conversation_id") or "")
        after_raw = payload.pop("after_raw", None)
        if after_raw is not None and conversation_id:
            after = normalize_conversation_payload(
                after_raw,
                conversation_id,
                owned_stream=bool(payload.get("owned_stream")),
            )
            user_id = str(payload.get("user_message_id") or "")
            payload["observed"] = any(turn.get("key") == user_id for turn in after.get("turns", []))
            payload["current_node"] = str(after.get("current_node") or "")
            payload["running"] = bool(after.get("running", True))
            if not payload["observed"] and not payload.get("reason"):
                payload["reason"] = "created user message was not found in canonical conversation state"
        if conversation_id:
            payload["chat_url"] = (
                f"https://chatgpt.com/g/{project}/c/{conversation_id}"
                if project
                else f"https://chatgpt.com/c/{conversation_id}"
            )
        return payload

    async def thread_context(
        self,
        conversation_id: str,
        *,
        since_cursor: str | None = None,
        max_events: int = 60,
        max_chars: int = 12000,
    ) -> dict[str, Any]:
        raw = await self._get_thread_raw(conversation_id)
        return build_thread_context(
            raw,
            conversation_id,
            since_cursor=since_cursor,
            max_events=max_events,
            max_chars=max_chars,
        )

    async def thread_tail(
        self,
        conversation_id: str,
        *,
        lines: int = 60,
        max_chars: int = 12000,
    ) -> dict[str, Any]:
        raw = await self._get_thread_raw(conversation_id)
        return build_thread_tail(raw, conversation_id, lines=lines, max_chars=max_chars)

    async def get_thread(self, conversation_id: str) -> dict[str, Any]:
        payload = await self._evaluate(
            f"const resolved = await resolveClient(); const raw = await resolved.client.get({json.dumps(conversation_id)}); return {{raw:plain(raw), owned_stream:ownedStreamActive({json.dumps(conversation_id)})}};",
            timeout=max(self.timeout, 30.0),
        )
        if not isinstance(payload, dict):
            raise RuntimeProtocolError("conversation read returned an invalid result")
        return normalize_conversation_payload(payload.get("raw"), conversation_id, owned_stream=bool(payload.get("owned_stream")))

    async def continue_thread(
        self,
        conversation_id: str,
        message: str,
        *,
        expected_current_node: str,
        wait_for_completion: bool = True,
        user_message_id: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        if not expected_current_node:
            raise ValueError("expected_current_node is required")
        delivery_message_id = (
            user_message_id.strip() if user_message_id else str(uuid.uuid4())
        )
        if not delivery_message_id:
            raise ValueError("user_message_id must not be empty")
        body = f"""
        const resolved = await resolveClient();
        const client = resolved.client;
        const conversationId = {json.dumps(conversation_id)};
        const expectedNode = {json.dumps(expected_current_node)};
        const text = {json.dumps(message)};
        const waitForCompletion = {json.dumps(bool(wait_for_completion))};
        const force = {json.dumps(bool(force))};
        const beforeRaw = await client.get(conversationId);
        const before = summary(beforeRaw);
        if (!before.valid || before.currentNode !== expectedNode) return {{sent:false, running:false, reason:'canonical current_node changed before send'}};
        if (!force && (ownedStreamActive(conversationId) || before.running)) return {{sent:false, running:true, reason:'thread is running'}};
        const latest = before.latest;
        const status = String(latest && latest.status || '').toLowerCase();
        const terminal = new Set(['finished_successfully','finished_error','failed','cancelled','canceled','interrupted','incomplete']);
        if (!force && (!latest || String(latest.author && latest.author.role || '') !== 'assistant')) return {{sent:false, running:false, reason:'latest canonical message is not an assistant response'}};
        if (!force && (!terminal.has(status) || latest.end_turn !== true)) return {{sent:false, running:false, reason:'latest assistant response is not terminal'}};
        const metadata = latest.metadata || {{}};
        const model = chooseModel(await client.models(), {json.dumps(self.preferred_model)}, String(metadata.model_slug || metadata.default_model_slug || ''), {json.dumps(self.thinking_effort)}, {json.dumps(self.require_high_reasoning)});
        const userMessageId = {json.dumps(delivery_message_id)};
        const request = {{
          action:'next', conversation_id:conversationId, parent_message_id:expectedNode, model:model.slug,
          messages:[{{id:userMessageId, author:{{role:'user'}}, content:{{content_type:'text', parts:[text]}}, create_time:Date.now()/1000, end_turn:null, metadata:{{}}, recipient:'all', status:'finished_successfully', weight:1}}],
          supported_encodings:['v1'], timezone:Intl.DateTimeFormat().resolvedOptions().timeZone, timezone_offset_min:new Date().getTimezoneOffset(),
        }};
        if (model.effort) request.thinking_effort = model.effort;
        if (before.conversation.gizmo_id) request.gizmo_id = before.conversation.gizmo_id;
        let requestId = '', finalMessageId = '', finalStatus = '', terminalEvent = '';
        let timedOut = false, streamFinished = false;
        streams.set(conversationId, {{started_at:Date.now(), user_message_id:userMessageId}});
        await new Promise((resolvePromise, rejectPromise) => {{
          let settled = false;
          const cleanup = () => {{ streamFinished = true; streams.delete(conversationId); handles.delete(conversationId); clearTimeout(timer); }};
          const finish = (eventType='') => {{ cleanup(); if (settled) return; settled = true; terminalEvent = eventType || terminalEvent; resolvePromise(); }};
          const finishDispatch = () => {{
            if (waitForCompletion || settled) return;
            settled = true;
            clearTimeout(timer);
            resolvePromise();
          }};
          const fail = (error) => {{ cleanup(); if (settled) return; settled = true; rejectPromise(error instanceof Error ? error : new Error(String(error))); }};
          const timeoutMs = waitForCompletion ? {self.stream_timeout_seconds * 1000} : Math.min({self.stream_timeout_seconds * 1000}, 10000);
          const timer = setTimeout(() => {{ if (settled) return; settled = true; timedOut = true; resolvePromise(); }}, timeoutMs);
          const observe = (value) => {{
            const candidate = value && (value.message || value.data || value);
            if (candidate && candidate.id) finalMessageId = String(candidate.id);
            if (candidate && candidate.status) finalStatus = String(candidate.status);
            if (value && value.request_id) requestId = String(value.request_id);
            if (value && value.streamRequestId) requestId = String(value.streamRequestId);
            const eventType = String(value && (value.type || value.event_type || value.event) || '');
            if (eventType === 'message_stream_complete') finish(eventType);
            else finishDispatch();
          }};
          try {{
            const starting = client.startCompletionStream({{request, onEvent:observe, onUpdate:observe, onComplete:(value) => {{ observe(value); finish('onComplete'); }}, onError:fail, onRecoverableError:() => {{}}}});
            handles.set(conversationId, starting);
            if (starting && typeof starting.then === 'function') {{
              starting.then((handle) => {{
                if (!streamFinished) handles.set(conversationId, handle);
                observe(handle);
                finishDispatch();
              }}, fail);
            }} else {{
              if (!streamFinished) handles.set(conversationId, starting);
              observe(starting);
              finishDispatch();
            }}
          }} catch (error) {{ fail(error); }}
        }});
        let afterRaw = null;
        try {{ afterRaw = await client.get(conversationId); }} catch {{}}
        const afterSummary = summary(afterRaw);
        const running = ownedStreamActive(conversationId) || afterSummary.running;
        if (timedOut) {{
          return {{sent:true, observed:false, running:true, reason:waitForCompletion?'completion stream exceeded the configured timeout':'completion stream did not start before the dispatch timeout', request_id:requestId, user_message_id:userMessageId, final_message_id:finalMessageId, final_status:finalStatus, parent_message_id:expectedNode, terminal_event:terminalEvent, after_raw:afterRaw ? plain(afterRaw) : null}};
        }}
        return {{sent:true, observed:!!afterRaw, running:running, reason:afterRaw?'':'submitted conversation could not be read back', request_id:requestId, user_message_id:userMessageId, final_message_id:finalMessageId, final_status:finalStatus, parent_message_id:expectedNode, terminal_event:terminalEvent, after_raw:afterRaw ? plain(afterRaw) : null}};
        """
        dispatch_timeout = (
            max(self.timeout, float(self.stream_timeout_seconds) + 5.0)
            if wait_for_completion
            else max(self.timeout, 30.0)
        )
        try:
            payload = await self._evaluate(
                body,
                retry_on_transport=False,
                timeout=dispatch_timeout,
            )
        except _MUTATION_EXCEPTIONS as exc:
            return {
                "sent": True,
                "observed": False,
                "running": False,
                "reason": sanitize_runtime_error(
                    f"{type(exc).__name__}: {exc}"
                ),
                "user_message_id": delivery_message_id,
                "parent_message_id": expected_current_node,
                "submission_uncertain": True,
            }
        if not isinstance(payload, dict):
            raise RuntimeProtocolError("continuation returned an invalid result")
        if payload.get("after_raw") is not None:
            after = normalize_conversation_payload(payload.pop("after_raw"), conversation_id)
            user_id = str(payload.get("user_message_id") or "")
            verified = bool(
                after.get("found")
                and after.get("canonical")
                and after.get("state_verified")
            )
            persisted = verified and any(
                turn.get("key") == user_id for turn in after.get("turns", [])
            )
            payload["observed"] = persisted
            payload["running"] = bool(payload.get("running")) or bool(after.get("running"))
            if not persisted and not payload.get("reason"):
                payload["reason"] = "submitted user message was not found in canonical conversation state"
            turns = after.get("turns", [])
            final = next(
                (turn for turn in reversed(turns) if turn.get("role") == "assistant"),
                None,
            )
            if final is not None:
                payload["final_message_id"] = str(
                    final.get("key") or payload.get("final_message_id") or ""
                )
                payload["final_status"] = str(
                    final.get("status") or payload.get("final_status") or ""
                )
        return payload

    async def cancel(self, conversation_id: str) -> dict[str, Any]:
        payload = await self._evaluate(
            f"const resolved = await resolveClient(); const id = {json.dumps(conversation_id)};"
            "const handle = handles.get(id);"
            "try {"
            " if (handle && typeof handle.cancel === 'function') await handle.cancel();"
            " else if (typeof resolved.client.stopCompletion === 'function') await resolved.client.stopCompletion({conversationId:id});"
            " else if (typeof resolved.client.cancelStream === 'function') await resolved.client.cancelStream(id);"
            " else return {cancelled:false, reason:'cancel is unavailable'};"
            " streams.delete(id); handles.delete(id); return {cancelled:true, reason:''};"
            "} catch { return {cancelled:false, reason:'cancel failed'}; }",
            retry_on_transport=False,
            timeout=max(self.timeout, 15.0),
        )
        return payload if isinstance(payload, dict) else {"cancelled": False, "reason": "invalid result"}
