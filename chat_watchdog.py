from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
import shlex
import secrets
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from enum import Enum
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Protocol
from urllib.parse import urlsplit, urlunsplit


from conversation_gateway import ConversationGateway, DeliveryState
from durable_ledger import DurableLedger

from chat_internal_client import (
    DEFAULT_CODEX_CDP_ENDPOINT,
    InternalChatClient,
    RuntimeNotReadyError,
    RuntimeProbe,
    RuntimeProtocolError,
    RuntimeUnavailableError,
    probe_runtime,
    sanitize_runtime_error,
)
from chat_direct_client import DirectChatClient


DEFAULT_COMPLETION_MARKER = "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"
DEFAULT_CONTINUE_MESSAGE = (
    "Continue from where you stopped. Do not stop until the entire task is finished. "
    "Only output DONE_I_HAVE_COMPLETED_ALL_THE_STEPS after every required step is complete."
)
CODEX_INTERNAL_ADAPTER_NAME = "codex-internal"
DIRECT_ADAPTER_NAME = "direct"
CONVERSATION_ID_RE = re.compile(r"^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$")
PROJECT_ID_RE = re.compile(r"^g-p-[0-9a-fA-F]{32}$")
MAX_QUEUE_BYTES = 128 * 1024
MAX_QUEUE_ENTRIES = 500
MAX_WATCHED_PROJECTS = 50
MAX_AVAILABLE_PROJECTS = 500
MAX_PROJECT_DISCOVERY_PAGES = 100
PROJECT_WATCH_MODE_NEW_THREADS = "new_threads_only"
PROJECT_WATCH_MODE_EXISTING_WORKING = "existing_working"
PROJECT_WATCH_MODES = frozenset(
    {PROJECT_WATCH_MODE_NEW_THREADS, PROJECT_WATCH_MODE_EXISTING_WORKING}
)

def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


_DESKTOP_ENV_KEYS = (
    "DISPLAY",
    "WAYLAND_DISPLAY",
    "XDG_RUNTIME_DIR",
    "DBUS_SESSION_BUS_ADDRESS",
    "XAUTHORITY",
    "XDG_SESSION_TYPE",
)
_DESKTOP_SESSION_PROCESSES = (
    "plasmashell",
    "gnome-shell",
    "Xwayland",
    "kwin_wayland",
    "weston",
)


def desktop_launch_environment(
    base: dict[str, str] | None = None,
    *,
    proc_root: Path = Path("/proc"),
) -> dict[str, str]:
    """Return an app-launch environment with same-user graphical session fields."""
    environment = dict(os.environ if base is None else base)
    needs_display = not (environment.get("DISPLAY") or environment.get("WAYLAND_DISPLAY"))
    if needs_display:
        candidates: list[tuple[int, Path]] = []
        with contextlib.suppress(OSError):
            for entry in proc_root.iterdir():
                if not entry.name.isdigit():
                    continue
                with contextlib.suppress(OSError, ValueError):
                    if entry.stat().st_uid != os.getuid():
                        continue
                    name = (entry / "comm").read_text(encoding="utf-8").strip()
                    if name in _DESKTOP_SESSION_PROCESSES:
                        candidates.append((_DESKTOP_SESSION_PROCESSES.index(name), entry))
        for _, entry in sorted(candidates, key=lambda item: (item[0], int(item[1].name))):
            try:
                raw = (entry / "environ").read_bytes()
            except OSError:
                continue
            values: dict[str, str] = {}
            for chunk in raw.split(b"\0"):
                if b"=" not in chunk:
                    continue
                key, value = chunk.split(b"=", 1)
                decoded_key = key.decode("utf-8", errors="ignore")
                if decoded_key in _DESKTOP_ENV_KEYS:
                    values[decoded_key] = value.decode("utf-8", errors="ignore")
            if values.get("DISPLAY") or values.get("WAYLAND_DISPLAY"):
                for key in _DESKTOP_ENV_KEYS:
                    if values.get(key) and not environment.get(key):
                        environment[key] = values[key]
                break

    runtime_dir = environment.get("XDG_RUNTIME_DIR")
    if not runtime_dir:
        candidate = Path(f"/run/user/{os.getuid()}")
        if candidate.is_dir():
            runtime_dir = str(candidate)
            environment["XDG_RUNTIME_DIR"] = runtime_dir
    if runtime_dir and not environment.get("DBUS_SESSION_BUS_ADDRESS"):
        bus = Path(runtime_dir) / "bus"
        if bus.exists():
            environment["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path={bus}"
    return environment


@dataclass(frozen=True)
class ChatLink:
    url: str
    conversation_id: str
    project_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class InvalidQueueLine:
    line: int
    value: str
    error: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ConversationTurn:
    key: str
    role: str
    text: str
    parent_id: str = ""
    status: str = ""
    end_turn: bool | None = None
    create_time: float | None = None
    model_slug: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ThreadSnapshot:
    found: bool
    conversation_id: str
    title: str = ""
    running: bool = False
    assistant_messages: tuple[str, ...] = ()
    user_messages: tuple[str, ...] = ()
    composer_text: str = ""
    reason: str = ""
    turns: tuple[ConversationTurn, ...] = ()
    current_node: str = ""
    visible_current_node: str = ""
    canonical: bool = False
    state_verified: bool = False
    update_time: float | None = None
    active_stream: bool = False

    @property
    def assistant_hash(self) -> str:
        payload = "\n\n---assistant-turn---\n\n".join(self.assistant_messages)
        return hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()

    @property
    def transcript_hash(self) -> str:
        payload = json.dumps(
            [turn.as_dict() for turn in self.turns],
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()

    @property
    def latest_turn(self) -> ConversationTurn | None:
        return next((turn for turn in reversed(self.turns) if turn.text.strip()), None)


@dataclass(frozen=True)
class SendResult:
    clicked: bool
    observed: bool
    running: bool
    reason: str = ""
    quality_verified: bool = False
    quality_changed: bool = False
    quality_label: str = ""
    request_id: str = ""
    user_message_id: str = ""
    final_message_id: str = ""
    final_status: str = ""
    parent_message_id: str = ""


@dataclass(frozen=True)
class RefreshResult:
    refreshed: bool
    hard_reload: bool = False
    reason: str = ""


@dataclass(frozen=True)
class ModelOption:
    slug: str
    is_default: bool = False
    is_available: bool = True
    thinking_efforts: tuple[str, ...] = ()


@dataclass(frozen=True)
class ModelSelection:
    slug: str
    thinking_effort: str
    source: str


def select_model_option(
    options: tuple[ModelOption, ...] | list[ModelOption],
    *,
    preferred_model: str = "",
    latest_model: str = "",
    thinking_effort: str = "",
    require_high_reasoning: bool = False,
) -> ModelSelection:
    """Choose an available model without silently lowering required reasoning."""
    available = [option for option in options if option.slug and option.is_available]
    if not available:
        raise ValueError("model catalogue contains no available models")
    by_slug = {option.slug: option for option in available}
    preferred = preferred_model.strip()
    latest = latest_model.strip()
    requested_input = thinking_effort.strip()

    def supported_efforts(option: ModelOption) -> dict[str, str]:
        return {value.casefold(): value for value in option.thinking_efforts if value}

    def supports_requirement(option: ModelOption) -> bool:
        supported = supported_efforts(option)
        if requested_input:
            return requested_input.casefold() in supported
        if require_high_reasoning:
            return "extended" in supported or "high" in supported
        return True

    if preferred:
        selected = by_slug.get(preferred)
        if selected is None:
            raise ValueError(f"configured model is unavailable: {preferred}")
        source = "preferred"
    elif latest and latest in by_slug and supports_requirement(by_slug[latest]):
        selected = by_slug[latest]
        source = "latest-thread"
    elif require_high_reasoning or requested_input:
        catalog_default = next((option for option in available if option.is_default), None)
        selected = (
            catalog_default
            if catalog_default is not None and supports_requirement(catalog_default)
            else next((option for option in available if supports_requirement(option)), None)
        )
        if selected is None:
            required = requested_input or "extended/high"
            raise ValueError(
                f"model catalogue contains no available model supporting reasoning effort {required!r}"
            )
        source = "catalog-default" if selected.is_default else "required-reasoning"
    else:
        selected = next((option for option in available if option.is_default), None)
        if selected is None:
            selected = by_slug.get(latest) if latest else None
        if selected is None:
            selected = available[0]
        source = "catalog-default" if selected.is_default else "catalog-first"

    supported = supported_efforts(selected)
    requested = requested_input
    if require_high_reasoning and not requested:
        requested = supported.get("extended") or supported.get("high") or ""
        if not requested:
            raise ValueError(
                f"high reasoning is required but no extended/high effort is advertised for {selected.slug}"
            )
    if requested and requested.casefold() not in supported:
        if require_high_reasoning or preferred:
            raise ValueError(
                f"reasoning effort {requested!r} is unavailable for {selected.slug}"
            )
        requested = ""
    return ModelSelection(selected.slug, requested, source)


def model_options_from_payload(payload: Any) -> tuple[ModelOption, ...]:
    raw_models = payload.get("models") if isinstance(payload, dict) else payload
    if not isinstance(raw_models, list):
        return ()
    options: list[ModelOption] = []
    seen: set[str] = set()
    for item in raw_models:
        if not isinstance(item, dict):
            continue
        slug = str(item.get("slug") or "").strip()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        efforts = item.get("thinking_efforts") or item.get("thinkingEfforts")
        if isinstance(efforts, list):
            normalized_efforts: list[str] = []
            for value in efforts:
                if isinstance(value, dict):
                    value = value.get("thinking_effort") or value.get("thinkingEffort")
                effort = str(value or "").strip()
                if effort:
                    normalized_efforts.append(effort)
            thinking_efforts = tuple(normalized_efforts)
        else:
            inferred: list[str] = []
            if item.get("supports_extended"):
                inferred.append("extended")
            if item.get("supports_high"):
                inferred.append("high")
            thinking_efforts = tuple(inferred)
        options.append(
            ModelOption(
                slug=slug,
                is_default=bool(item.get("is_default") or item.get("isDefault")),
                is_available=not bool(item.get("disabled"))
                and bool(item.get("is_available", item.get("isAvailable", True))),
                thinking_efforts=thinking_efforts,
            )
        )
    return tuple(options)


class ThreadDecisionState(str, Enum):
    RUNTIME_UNAVAILABLE = "runtime_unavailable"
    RUNTIME_NOT_READY = "runtime_not_ready"
    RUNNING_OWNED_STREAM = "running_owned_stream"
    RUNNING_CANONICAL = "running_canonical"
    RUNNING = "running_canonical"  # compatibility alias
    COMPLETED = "completed"
    COMPLETE = "completed"  # compatibility alias
    DUPLICATE_PENDING = "duplicate_pending"
    INTERRUPTED_OR_STALE = "interrupted_or_stale"
    STOPPED_INCOMPLETE = "stopped_incomplete"
    AWAITING_ASSISTANT = "awaiting_assistant"
    RETRYABLE_ERROR = "retryable_error"
    TERMINAL_ERROR = "terminal_error"
    UNKNOWN = "unknown"
    INDETERMINATE = "unknown"  # compatibility alias


@dataclass(frozen=True)
class ThreadDecision:
    state: ThreadDecisionState
    reason: str = ""
    can_continue: bool = False
    completion_turn: ConversationTurn | None = None


@dataclass(frozen=True)
class ContinuationCandidate:
    link: ChatLink
    snapshot: ThreadSnapshot
    previous: dict[str, Any]
    common: dict[str, Any]
    task_start_key: str
    task_start_index: int
    fairness_epoch: float
    forced: bool = False


@dataclass(frozen=True)
class LinkObservation:
    candidate: ContinuationCandidate | None = None
    owned_stream_active: bool = False


RUNNING_MESSAGE_STATUSES = frozenset({"in_progress", "streaming", "queued", "pending"})
TERMINAL_SUCCESS_MESSAGE_STATUSES = frozenset({"finished_successfully"})
TERMINAL_FAILURE_MESSAGE_STATUSES = frozenset(
    {"finished_error", "failed", "cancelled", "canceled", "interrupted", "incomplete"}
)


def classify_runtime_error(error: BaseException) -> ThreadDecision:
    """Map internal-runtime failures to non-destructive watchdog states."""
    reason = sanitize_runtime_error(f"{type(error).__name__}: {error}")
    if isinstance(error, RuntimeUnavailableError):
        return ThreadDecision(ThreadDecisionState.RUNTIME_UNAVAILABLE, reason)
    if isinstance(error, RuntimeNotReadyError):
        return ThreadDecision(ThreadDecisionState.RUNTIME_NOT_READY, reason)
    if isinstance(error, (RuntimeProtocolError, TimeoutError, asyncio.TimeoutError, OSError, ConnectionError)):
        return ThreadDecision(ThreadDecisionState.RETRYABLE_ERROR, reason)
    if isinstance(error, (ValueError, PermissionError, FileNotFoundError)):
        return ThreadDecision(ThreadDecisionState.TERMINAL_ERROR, reason)
    return ThreadDecision(ThreadDecisionState.RETRYABLE_ERROR, reason)


def classify_thread_state(
    snapshot: ThreadSnapshot,
    *,
    task_start_index: int,
    completion_marker: str,
    continuation_message: str = "",
    now: float | None = None,
    stale_after_seconds: float = 600.0,
    progress_observed_at: float | None = None,
) -> ThreadDecision:
    """Compatibility adapter over the canonical pure state reducer."""
    from state_reducer import ReducedState, reduce_snapshot

    meaningful = [(index, turn) for index, turn in enumerate(snapshot.turns) if turn.text.strip()]
    latest_index = meaningful[-1][0] if meaningful else task_start_index + 1
    latest = meaningful[-1][1] if meaningful else None
    if snapshot.active_stream:
        return ThreadDecision(ThreadDecisionState.RUNNING_OWNED_STREAM, "a watchdog-owned completion stream is active")
    if latest is not None and (snapshot.running or latest.role == "assistant" and (latest.status.casefold() in RUNNING_MESSAGE_STATUSES or latest.end_turn is False)) and now is not None:
        reference = progress_observed_at if progress_observed_at is not None else snapshot.update_time
        if reference is not None and stale_after_seconds > 0 and now - reference >= stale_after_seconds:
            return ThreadDecision(ThreadDecisionState.INTERRUPTED_OR_STALE, "canonical generation is stale; continuation is blocked")
    turns = [turn.as_dict() for turn in snapshot.turns]
    if not snapshot.canonical:
        for item in turns:
            if item.get("role") == "assistant":
                item["status"] = item.get("status") or "finished_successfully"
                if item.get("end_turn") is None:
                    item["end_turn"] = True
    # The legacy watchdog adapter's non-canonical snapshots are already the
    # result of its adapter-level inspection.  Canonical gateway snapshots must
    # carry an explicit verification bit and remain fail-closed.
    verified = snapshot.state_verified or not snapshot.canonical
    if not turns and not snapshot.canonical:
        # Older adapters expose bounded role-specific arrays.  Convert that
        # compatibility shape once, then let the reducer make the decision.
        users = list(snapshot.user_messages)
        assistants = list(snapshot.assistant_messages)
        if users and len(users) > len(assistants):
            turns = [{"key": f"user-{index}", "role": "user", "text": text,
                      "status": "finished_successfully", "end_turn": True}
                     for index, text in enumerate(users)]
        else:
            turns = [{"key": f"assistant-{index}", "role": "assistant", "text": text,
                      "status": "finished_successfully", "end_turn": True}
                     for index, text in enumerate(assistants)]
        if not turns:
            return ThreadDecision(
                ThreadDecisionState.UNKNOWN,
                "conversation transcript is empty; continuation is unsafe",
            )
        verified = True
        meaningful = [(index, item) for index, item in enumerate(turns) if str(item.get("text") or "").strip()]
        latest_index, latest_item = meaningful[-1] if meaningful else (task_start_index + 1, turns[-1])
        latest = ConversationTurn(
            str(latest_item.get("key") or ""), str(latest_item.get("role") or ""),
            str(latest_item.get("text") or ""), status=str(latest_item.get("status") or ""),
            end_turn=latest_item.get("end_turn"),
        )
    if latest_index <= task_start_index:
        return ThreadDecision(ThreadDecisionState.AWAITING_ASSISTANT, "no assistant response exists after the current task start")
    reduction = reduce_snapshot(
        {"completion_marker": completion_marker,
         "status": "stopped_incomplete" if not snapshot.canonical else ""},
        {"state_verified": verified, "canonical": snapshot.canonical,
         "current_node": snapshot.current_node,
         "visible_current_node": snapshot.visible_current_node,
         "running": snapshot.running, "active_stream": snapshot.active_stream,
         "turns": turns},
    )
    if reduction.state is ReducedState.COMPLETED:
        return ThreadDecision(ThreadDecisionState.COMPLETED, reduction.reason, completion_turn=latest)
    if reduction.state is ReducedState.RUNNING:
        return ThreadDecision(ThreadDecisionState.RUNNING_CANONICAL, reduction.reason)
    if reduction.state is ReducedState.STALE:
        return ThreadDecision(ThreadDecisionState.INTERRUPTED_OR_STALE, reduction.reason)
    if reduction.state is ReducedState.AWAITING_ASSISTANT:
        if (
            latest is not None
            and continuation_message
            and latest.text.strip() == continuation_message.strip()
        ):
            return ThreadDecision(ThreadDecisionState.DUPLICATE_PENDING, "the continuation prompt is already the latest user turn")
        return ThreadDecision(ThreadDecisionState.AWAITING_ASSISTANT, reduction.reason)
    if reduction.state is ReducedState.STOPPED_INCOMPLETE and any(action.value == "send_continuation" for action in reduction.actions):
        return ThreadDecision(ThreadDecisionState.STOPPED_INCOMPLETE, reduction.reason, can_continue=True)
    if reduction.state is ReducedState.UNKNOWN and not snapshot.state_verified:
        return ThreadDecision(ThreadDecisionState.UNKNOWN, reduction.reason)
    return ThreadDecision(ThreadDecisionState.UNKNOWN, reduction.reason)



class ChatAdapter(Protocol):
    async def __aenter__(self) -> "ChatAdapter": ...

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None: ...

    async def current_conversation_id(self) -> str | None: ...

    async def refresh_catalog(self) -> RefreshResult: ...

    async def list_projects(
        self,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]: ...

    async def list_project_threads(
        self,
        project_id: str,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]: ...

    async def inspect(self, link: ChatLink) -> ThreadSnapshot: ...

    async def send_continue(
        self,
        link: ChatLink,
        message: str,
        *,
        expected_current_node: str = "",
        force: bool = False,
    ) -> SendResult: ...

    async def restore(self, conversation_id: str) -> bool: ...


class QueueValidationError(ValueError):
    def __init__(self, invalid_lines: list[InvalidQueueLine]):
        self.invalid_lines = invalid_lines
        detail = "; ".join(f"line {item.line}: {item.error}" for item in invalid_lines[:5])
        super().__init__(detail or "invalid watchdog queue")


class QueueConflictError(RuntimeError):
    """The queue changed on disk after the web editor loaded it."""


def parse_chat_link(raw: str) -> ChatLink:
    value = raw.strip()
    if not value:
        raise ValueError("empty URL")
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("URL must use http or https")
    hostname = (parsed.hostname or "").lower()
    if hostname not in {"chatgpt.com", "www.chatgpt.com"}:
        raise ValueError("URL must use chatgpt.com")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("credentials are not allowed in the URL")
    if parsed.query or parsed.fragment:
        raise ValueError("query strings and fragments are not allowed")

    segments = [segment for segment in parsed.path.split("/") if segment]
    conversation_id: str | None = None
    project_id: str | None = None
    for index, segment in enumerate(segments):
        if segment == "g" and index + 1 < len(segments):
            candidate = segments[index + 1]
            if PROJECT_ID_RE.fullmatch(candidate):
                project_id = candidate
        if segment == "c" and index + 1 < len(segments):
            candidate = segments[index + 1]
            if CONVERSATION_ID_RE.fullmatch(candidate):
                conversation_id = candidate.lower()
                break
    if conversation_id is None:
        raise ValueError("URL does not contain a valid /c/<conversation-id> path")
    normalized_segments = [segment.lower() for segment in segments]
    valid_path = normalized_segments == ["c", conversation_id] or (
        len(normalized_segments) == 4
        and normalized_segments[0] == "g"
        and PROJECT_ID_RE.fullmatch(segments[1] or "") is not None
        and normalized_segments[2] == "c"
        and normalized_segments[3] == conversation_id
    )
    if not valid_path:
        raise ValueError("URL path must be /c/<conversation-id> or /g/<project-id>/c/<conversation-id>")

    canonical = urlunsplit(("https", "chatgpt.com", parsed.path.rstrip("/"), "", ""))
    return ChatLink(url=canonical, conversation_id=conversation_id, project_id=project_id)


class ChatWatchdogQueue:
    """One active task per URL line, with atomic edits and a completion log."""

    def __init__(self, queue_path: Path, completed_path: Path):
        self.queue_path = queue_path.expanduser().resolve()
        self.completed_path = completed_path.expanduser().resolve()
        self._lock = threading.RLock()
        self.ensure_layout()

    def ensure_layout(self) -> None:
        with self._lock:
            self.queue_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.queue_path.exists():
                _atomic_write(
                    self.queue_path,
                    "# Add one active ChatGPT task URL per line.\n"
                    "# A completed task is removed; re-add the URL for a new task generation.\n",
                )
            if not self.completed_path.exists():
                _atomic_write(self.completed_path, "")

    @staticmethod
    def parse_document(text: str) -> tuple[list[ChatLink], list[InvalidQueueLine]]:
        entries: list[ChatLink] = []
        invalid: list[InvalidQueueLine] = []
        seen: set[str] = set()
        for line_number, raw_line in enumerate(text.splitlines(), start=1):
            value = raw_line.strip()
            if not value or value.startswith("#"):
                continue
            try:
                link = parse_chat_link(value)
            except ValueError as exc:
                invalid.append(InvalidQueueLine(line_number, value, str(exc)))
                continue
            if link.conversation_id in seen:
                continue
            seen.add(link.conversation_id)
            entries.append(link)
        if len(entries) > MAX_QUEUE_ENTRIES:
            invalid.append(
                InvalidQueueLine(0, "", f"queue exceeds the {MAX_QUEUE_ENTRIES}-task limit")
            )
        return entries, invalid

    def read_text(self) -> str:
        with self._lock:
            self.ensure_layout()
            return self.queue_path.read_text(encoding="utf-8")

    def entries(self) -> tuple[list[ChatLink], list[InvalidQueueLine]]:
        return self.parse_document(self.read_text())

    def replace_text(self, text: str, expected_version: str | None = None) -> dict[str, Any]:
        encoded = text.encode("utf-8")
        if len(encoded) > MAX_QUEUE_BYTES:
            raise ValueError(f"queue file exceeds {MAX_QUEUE_BYTES} bytes")
        entries, invalid = self.parse_document(text)
        if invalid:
            raise QueueValidationError(invalid)
        normalized = text
        if normalized and not normalized.endswith("\n"):
            normalized += "\n"
        with self._lock:
            current = self.read_text()
            current_version = hashlib.sha256(current.encode("utf-8")).hexdigest()
            if expected_version and not secrets.compare_digest(expected_version, current_version):
                raise QueueConflictError("queue changed on disk; reload the editor before saving")
            _atomic_write(self.queue_path, normalized)
        return self.snapshot()

    def add(self, raw_url: str) -> dict[str, Any]:
        link = parse_chat_link(raw_url)
        with self._lock:
            text = self.read_text()
            entries, _ = self.parse_document(text)
            if any(item.conversation_id == link.conversation_id for item in entries):
                return self.snapshot()
            if len(entries) >= MAX_QUEUE_ENTRIES:
                raise ValueError(f"queue already contains {MAX_QUEUE_ENTRIES} tasks")
            separator = "" if not text or text.endswith("\n") else "\n"
            _atomic_write(self.queue_path, f"{text}{separator}{link.url}\n")
        return self.snapshot()

    def remove(self, conversation_id: str) -> bool:
        normalized = conversation_id.strip().lower()
        removed = False
        with self._lock:
            lines = self.read_text().splitlines(keepends=True)
            retained: list[str] = []
            for line in lines:
                value = line.strip()
                try:
                    link = parse_chat_link(value) if value and not value.startswith("#") else None
                except ValueError:
                    link = None
                if link is not None and link.conversation_id == normalized:
                    removed = True
                    continue
                retained.append(line)
            if removed:
                _atomic_write(self.queue_path, "".join(retained))
        return removed

    def complete(self, link: ChatLink, metadata: dict[str, Any]) -> None:
        record = {
            "completed_at": _utc_now(),
            **link.as_dict(),
            **metadata,
        }
        with self._lock:
            with self.completed_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(self.completed_path, 0o600)
            self.remove(link.conversation_id)

    def snapshot(self) -> dict[str, Any]:
        text = self.read_text()
        entries, invalid = self.parse_document(text)
        stat = self.queue_path.stat()
        return {
            "path": str(self.queue_path),
            "completed_path": str(self.completed_path),
            "text": text,
            "version": hashlib.sha256(text.encode("utf-8")).hexdigest(),
            "updated_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(timespec="seconds"),
            "entries": [entry.as_dict() for entry in entries],
            "invalid_lines": [item.as_dict() for item in invalid],
        }


class WatchdogProjectStore:
    """Persistent allowlist and per-project discovery state."""

    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self._write_unlocked({"version": 1, "projects": {}})

    def _load_unlocked(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {"version": 1, "projects": {}}
        projects = payload.get("projects") if isinstance(payload, dict) else None
        return {
            "version": 1,
            "projects": dict(projects) if isinstance(projects, dict) else {},
        }

    def _write_unlocked(self, payload: dict[str, Any]) -> None:
        _atomic_write(
            self.path,
            json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            payload = self._load_unlocked()
            items: list[dict[str, Any]] = []
            for project_id, raw in payload["projects"].items():
                if not isinstance(raw, dict):
                    continue
                seen = raw.get("seen_thread_ids")
                items.append(
                    {
                        "project_id": project_id,
                        "name": str(raw.get("name") or project_id),
                        "selected_at": str(raw.get("selected_at") or ""),
                        "initialized": bool(raw.get("initialized")),
                        "watch_mode": (
                            str(raw.get("watch_mode") or PROJECT_WATCH_MODE_NEW_THREADS)
                            if str(raw.get("watch_mode") or PROJECT_WATCH_MODE_NEW_THREADS)
                            in PROJECT_WATCH_MODES
                            else PROJECT_WATCH_MODE_NEW_THREADS
                        ),
                        "existing_working_initialized": bool(
                            raw.get("existing_working_initialized")
                        ),
                        "seen_thread_count": len(seen) if isinstance(seen, list) else 0,
                        "last_scan_at": str(raw.get("last_scan_at") or ""),
                        "last_error": str(raw.get("last_error") or ""),
                        "last_new_thread_ids": list(raw.get("last_new_thread_ids") or []),
                    }
                )
            items.sort(key=lambda item: (item["name"].casefold(), item["project_id"]))
            return {"path": str(self.path), "items": items, "count": len(items)}

    def records(self) -> list[dict[str, Any]]:
        with self._lock:
            payload = self._load_unlocked()
            result: list[dict[str, Any]] = []
            for project_id, raw in payload["projects"].items():
                if not isinstance(raw, dict):
                    continue
                result.append({"project_id": project_id, **dict(raw)})
            return result

    def select(
        self,
        project_id: str,
        *,
        name: str,
        seen_thread_ids: list[str],
        watch_mode: str = PROJECT_WATCH_MODE_NEW_THREADS,
    ) -> None:
        if watch_mode not in PROJECT_WATCH_MODES:
            raise ValueError("watch_mode must be new_threads_only or existing_working")
        with self._lock:
            payload = self._load_unlocked()
            projects = payload["projects"]
            if project_id in projects:
                current = dict(projects[project_id]) if isinstance(projects[project_id], dict) else {}
                if name:
                    current["name"] = name[:200]
                current.setdefault("watch_mode", PROJECT_WATCH_MODE_NEW_THREADS)
                current.setdefault("existing_working_initialized", False)
                current.setdefault("checked_thread_fingerprints", {})
                projects[project_id] = current
                self._write_unlocked(payload)
                return
            if len(projects) >= MAX_WATCHED_PROJECTS:
                raise ValueError(f"watchdog already contains {MAX_WATCHED_PROJECTS} projects")
            projects[project_id] = {
                "name": (name or project_id)[:200],
                "selected_at": _utc_now(),
                "initialized": True,
                "watch_mode": watch_mode,
                "existing_working_initialized": False,
                "checked_thread_fingerprints": {},
                "seen_thread_ids": sorted(set(seen_thread_ids)),
                "last_scan_at": _utc_now(),
                "last_error": "",
                "last_new_thread_ids": [],
            }
            self._write_unlocked(payload)

    def set_mode(self, project_id: str, watch_mode: str) -> None:
        if watch_mode not in PROJECT_WATCH_MODES:
            raise ValueError("watch_mode must be new_threads_only or existing_working")
        with self._lock:
            payload = self._load_unlocked()
            raw = payload["projects"].get(project_id)
            if not isinstance(raw, dict):
                raise ValueError("watchdog project was not found")
            previous = str(raw.get("watch_mode") or PROJECT_WATCH_MODE_NEW_THREADS)
            raw["watch_mode"] = watch_mode
            if watch_mode == PROJECT_WATCH_MODE_EXISTING_WORKING and previous != watch_mode:
                raw["existing_working_initialized"] = False
                raw["checked_thread_fingerprints"] = {}
            payload["projects"][project_id] = raw
            self._write_unlocked(payload)

    def remove(self, project_id: str) -> bool:
        with self._lock:
            payload = self._load_unlocked()
            removed = payload["projects"].pop(project_id, None) is not None
            if removed:
                self._write_unlocked(payload)
            return removed

    def record_scan(
        self,
        project_id: str,
        *,
        observed_thread_ids: list[str],
        new_thread_ids: list[str],
        error: str = "",
        checked_thread_fingerprints: dict[str, str] | None = None,
        existing_working_initialized: bool | None = None,
    ) -> None:
        with self._lock:
            payload = self._load_unlocked()
            raw = payload["projects"].get(project_id)
            if not isinstance(raw, dict):
                return
            seen = {str(item) for item in raw.get("seen_thread_ids", []) if str(item)}
            seen.update(observed_thread_ids)
            checked = raw.get("checked_thread_fingerprints")
            checked = dict(checked) if isinstance(checked, dict) else {}
            if checked_thread_fingerprints:
                checked.update(
                    {
                        str(conversation_id): str(fingerprint)
                        for conversation_id, fingerprint in checked_thread_fingerprints.items()
                        if str(conversation_id) and str(fingerprint)
                    }
                )
            raw.update(
                initialized=True,
                seen_thread_ids=sorted(seen),
                checked_thread_fingerprints=checked,
                last_scan_at=_utc_now(),
                last_error=error,
                last_new_thread_ids=list(new_thread_ids),
            )
            if existing_working_initialized is not None:
                raw["existing_working_initialized"] = bool(existing_working_initialized)
            payload["projects"][project_id] = raw
            self._write_unlocked(payload)


class WatchdogStateStore:
    """Compatibility facade backed authoritatively by ``DurableLedger``.

    The JSON path remains a read-only compatibility mirror for the dashboard
    and existing operators. Existing JSON content is imported once when a
    conversation has no SQLite state yet; all later reads and writes use the
    shared ledger.
    """

    def __init__(self, path: Path, ledger: DurableLedger):
        self.path = path.expanduser().resolve()
        self.ledger = ledger
        self._lock = threading.RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            for conversation_id, state in self._load_legacy_unlocked().items():
                if not self.ledger.watchdog_state(conversation_id):
                    self.ledger.put_watchdog_state(conversation_id, state)
            self._mirror_unlocked()

    def _load_legacy_unlocked(self) -> dict[str, dict[str, Any]]:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        if not isinstance(payload, dict):
            return {}
        return {
            str(conversation_id): dict(state)
            for conversation_id, state in payload.items()
            if isinstance(state, dict)
        }

    def _mirror_unlocked(self) -> None:
        payload = self.ledger.watchdog_states()
        _atomic_write(
            self.path,
            json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )

    def get(self, conversation_id: str) -> dict[str, Any]:
        with self._lock:
            return self.ledger.watchdog_state(conversation_id)

    def update(self, conversation_id: str, **values: Any) -> dict[str, Any]:
        with self._lock:
            current = self.ledger.watchdog_state(conversation_id)
            current.update(values)
            current["updated_at"] = _utc_now()
            self.ledger.put_watchdog_state(conversation_id, current)
            self._mirror_unlocked()
            return dict(current)

    def start_task(self, link: ChatLink, *, source: str) -> dict[str, Any]:
        """Create a fresh task generation for one conversation URL."""
        with self._lock:
            previous = self.ledger.watchdog_state(link.conversation_id)
            generation = int(previous.get("task_generation", 0) or 0) + 1
            current = {
                "url": link.url,
                "task_id": secrets.token_hex(16),
                "task_generation": generation,
                "task_source": source,
                "queued": True,
                "queued_at": _utc_now(),
                "status": "queued",
                "task_start_user_turn_key": "",
                "last_turn_key": "",
                "last_assistant_hash": "",
                "last_transcript_hash": "",
                "last_continue_at": None,
                "last_continue_epoch": 0,
                "last_scheduler_selected_at": None,
                "last_scheduler_selected_epoch": 0,
                "last_progress_at": None,
                "last_progress_epoch": 0,
                "continue_attempts": 0,
                "continue_observed": False,
                "quality_verified": False,
                "quality_changed": False,
                "quality_label": "",
                "completed_at": None,
                "completion_turn_key": "",
                "last_error": "",
                "previous_task_id": previous.get("task_id", ""),
                "previous_completion_turn_key": previous.get(
                    "completion_turn_key", ""
                ),
                "updated_at": _utc_now(),
            }
            self.ledger.put_watchdog_state(link.conversation_id, current)
            self._mirror_unlocked()
            return dict(current)

    def mark_not_queued(
        self,
        conversation_id: str,
        *,
        status: str = "removed",
    ) -> dict[str, Any]:
        with self._lock:
            current = self.ledger.watchdog_state(conversation_id)
            if not current:
                return {}
            current["queued"] = False
            if current.get("status") != "completed":
                current["status"] = status
            current["updated_at"] = _utc_now()
            self.ledger.put_watchdog_state(conversation_id, current)
            self._mirror_unlocked()
            return dict(current)

    def all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return self.ledger.watchdog_states()


def select_cdp_target(
    targets: list[dict[str, Any]],
    preferred_url_prefixes: tuple[str, ...] = (),
) -> dict[str, Any] | None:
    """Choose one page deterministically and never attach to the MCP sandbox target."""
    pages = [item for item in targets if item.get("type") == "page"]
    if preferred_url_prefixes:
        preferred = [
            item
            for item in pages
            if any(str(item.get("url", "")).startswith(prefix) for prefix in preferred_url_prefixes)
        ]
        return preferred[0] if preferred else None

    def target_rank(item: dict[str, Any]) -> tuple[int, int]:
        url = str(item.get("url", ""))
        title = str(item.get("title", "")).strip().casefold()
        is_local_renderer = url.startswith("http://127.0.0.1:5175/")
        is_sandbox_devtools = "mcpAppSandboxDevtools=1" in url
        return (
            0
            if is_local_renderer and not is_sandbox_devtools
            else 1
            if title == "codex" and not is_sandbox_devtools
            else 2,
            0 if "initialRoute=" in url else 1,
        )

    return min(pages, key=target_rank) if pages else None


class CodexInternalChatAdapter:
    """Typed adapter for the app-owned normal-chat client inside Codex desktop.

    The adapter never navigates, reads rendered transcript content, or triggers UI
    controls. CDP is used only to call the already-loaded first-party client.
    """

    def __init__(
        self,
        endpoint: str = DEFAULT_CODEX_CDP_ENDPOINT,
        *,
        preferred_model: str = "",
        thinking_effort: str = "extended",
        require_high_reasoning: bool = True,
        timeout_seconds: float = 10.0,
        stream_timeout_seconds: int = 3600,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.preferred_model = preferred_model.strip()
        self.thinking_effort = thinking_effort.strip()
        self.require_high_reasoning = require_high_reasoning
        self._client = InternalChatClient(
            self.endpoint,
            timeout=timeout_seconds,
            stream_timeout_seconds=stream_timeout_seconds,
            preferred_model=preferred_model,
            thinking_effort=self.thinking_effort,
            require_high_reasoning=require_high_reasoning,
        )
        self._gateway = ConversationGateway(self._client_context)
        self._health: dict[str, Any] = {}

    async def __aenter__(self) -> "CodexInternalChatAdapter":
        await self._client.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self._client.close()

    @asynccontextmanager
    async def _client_context(self):
        """Yield the adapter-owned connected client without reconnecting it."""
        yield self._client

    async def current_conversation_id(self) -> str | None:
        # Internal operations do not depend on or alter the visible conversation.
        return None

    async def refresh_catalog(self) -> RefreshResult:
        self._health = await self._client.health()
        model_payload = await self._client.models()
        options = model_options_from_payload(model_payload)
        try:
            selection = select_model_option(
                options,
                preferred_model=self.preferred_model,
                thinking_effort=self.thinking_effort,
                require_high_reasoning=self.require_high_reasoning,
            )
        except ValueError as exc:
            return RefreshResult(False, reason=str(exc))
        self._health.update(
            model_count=len(options),
            selected_model=selection.slug,
            selected_thinking_effort=selection.thinking_effort,
            model_source=selection.source,
        )
        return RefreshResult(
            True,
            reason=f"Codex internal client ready ({len(options)} models; {selection.slug})",
        )

    async def list_projects(
        self,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return await self._client.list_projects(limit=limit, cursor=cursor)

    async def list_project_threads(
        self,
        project_id: str,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        return await self._client.list_project_threads(
            project_id,
            limit=limit,
            cursor=cursor,
        )

    async def inspect(self, link: ChatLink) -> ThreadSnapshot:
        payload = await self._gateway.read(link.conversation_id)
        if not payload.get("found"):
            return ThreadSnapshot(
                found=False,
                conversation_id=link.conversation_id,
                reason=str(payload.get("reason") or "canonical conversation was not found"),
                canonical=True,
                state_verified=False,
            )
        turns = tuple(
            ConversationTurn(
                key=str(item.get("node_id") or item.get("key") or ""),
                role=str(item.get("role") or ""),
                text=str(item.get("text") or ""),
                parent_id=str(item.get("parent_id") or ""),
                status=str(item.get("status") or ""),
                end_turn=item.get("end_turn") if isinstance(item.get("end_turn"), bool) else None,
                create_time=float(item["create_time"])
                if isinstance(item.get("create_time"), (int, float))
                else None,
                model_slug=str(item.get("model_slug") or ""),
            )
            for item in payload.get("turns", [])
            if isinstance(item, dict)
            and item.get("role") in {"user", "assistant"}
            and (item.get("node_id") or item.get("key"))
        )
        return ThreadSnapshot(
            found=True,
            conversation_id=str(payload.get("conversation_id") or link.conversation_id),
            title=str(payload.get("title") or ""),
            running=bool(payload.get("running")),
            assistant_messages=tuple(
                turn.text for turn in turns if turn.role == "assistant" and turn.text
            ),
            user_messages=tuple(turn.text for turn in turns if turn.role == "user" and turn.text),
            reason=str(payload.get("reason") or ""),
            turns=turns,
            current_node=str(payload.get("current_node") or ""),
            visible_current_node=str(payload.get("visible_current_node") or ""),
            canonical=True,
            state_verified=bool(payload.get("state_verified")),
            update_time=float(payload["update_time"])
            if isinstance(payload.get("update_time"), (int, float))
            else None,
            active_stream=bool(payload.get("active_stream")),
        )

    async def send_continue(
        self,
        link: ChatLink,
        message: str,
        *,
        expected_current_node: str = "",
        force: bool = False,
    ) -> SendResult:
        delivery = await self._gateway.send(
            link.conversation_id,
            message,
            expected_current_node=expected_current_node,
            wait_for_completion=not force,
            force=force,
        )
        clicked = delivery.state in {
            DeliveryState.DELIVERED,
            DeliveryState.SENT_UNCONFIRMED,
        }
        return SendResult(
            clicked=clicked,
            observed=delivery.state is DeliveryState.DELIVERED,
            running=delivery.running,
            reason=delivery.reason,
            quality_verified=True,
            quality_label=self.thinking_effort,
            request_id=delivery.request_id,
            user_message_id=delivery.user_message_id,
            final_message_id=delivery.final_message_id,
            final_status=delivery.final_status,
            parent_message_id=delivery.parent_message_id or expected_current_node,
        )

    async def restore(self, conversation_id: str) -> bool:
        # The internal adapter never changes the visible app route.
        return True


class DirectChatAdapter(CodexInternalChatAdapter):
    """Watchdog adapter backed by Terminal MCP's direct ChatGPT transport."""

    def __init__(
        self,
        *,
        preferred_model: str = "",
        thinking_effort: str = "extended",
        require_high_reasoning: bool = True,
        timeout_seconds: float = 10.0,
        stream_timeout_seconds: int = 3600,
    ) -> None:
        del timeout_seconds
        self.endpoint = "direct://chatgpt"
        self.preferred_model = preferred_model.strip()
        self.thinking_effort = thinking_effort.strip()
        self.require_high_reasoning = require_high_reasoning
        self._client = DirectChatClient(
            preferred_model=self.preferred_model,
            thinking_effort=self.thinking_effort,
            require_high_reasoning=self.require_high_reasoning,
            stream_timeout_seconds=stream_timeout_seconds,
        )
        self._gateway = ConversationGateway(self._client_context)
        self._health: dict[str, Any] = {}


@dataclass(frozen=True)
class ChatWatchdogConfig:
    enabled: bool
    queue_path: Path
    state_path: Path
    completed_path: Path
    adapter_mode: str = DIRECT_ADAPTER_NAME
    cdp_endpoint: str = DEFAULT_CODEX_CDP_ENDPOINT
    scan_interval_seconds: int = 600
    retry_cooldown_seconds: int = 300
    initial_delay_seconds: int = 15
    completion_marker: str = DEFAULT_COMPLETION_MARKER
    continue_message: str = DEFAULT_CONTINUE_MESSAGE
    dry_run: bool = False
    auto_start_app: bool = True
    app_command: str = "/usr/bin/codex-desktop"
    app_start_timeout_seconds: int = 30
    internal_timeout_seconds: float = 10.0
    require_high_reasoning: bool = True
    preferred_model: str = ""
    thinking_effort: str = "extended"
    stream_timeout_seconds: int = 3600
    stale_generation_seconds: int = 600
    max_continue_attempts: int = 20
    pre_send_confirmation_seconds: float = 1.25
    ledger_path: Path | None = None
    forced_continue_conversation_ids: tuple[str, ...] = ()
    forced_continue_interval_seconds: int = 1200
    projects_path: Path | None = None

    @classmethod
    def from_env(cls) -> "ChatWatchdogConfig":
        root = Path(os.environ.get("MCP_CHAT_WATCHDOG_DIR", "~/.GPT/chat-watchdog")).expanduser()
        enabled = os.environ.get("MCP_CHAT_WATCHDOG_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
        adapter_mode = (
            os.environ.get("MCP_CHAT_WATCHDOG_ADAPTER", DIRECT_ADAPTER_NAME).strip().lower()
            or DIRECT_ADAPTER_NAME
        )
        return cls(
            enabled=enabled,
            queue_path=Path(os.environ.get("MCP_CHAT_WATCHDOG_QUEUE", str(root / "threads.txt"))),
            state_path=Path(os.environ.get("MCP_CHAT_WATCHDOG_STATE", str(root / "state.json"))),
            completed_path=Path(os.environ.get("MCP_CHAT_WATCHDOG_COMPLETED", str(root / "completed.jsonl"))),
            projects_path=Path(os.environ.get("MCP_CHAT_WATCHDOG_PROJECTS", str(root / "projects.json"))),
            adapter_mode=adapter_mode,
            cdp_endpoint=os.environ.get("MCP_CHAT_WATCHDOG_CDP", DEFAULT_CODEX_CDP_ENDPOINT).strip()
            or DEFAULT_CODEX_CDP_ENDPOINT,
            scan_interval_seconds=max(30, int(os.environ.get("MCP_CHAT_WATCHDOG_INTERVAL_SECONDS", "600"))),
            retry_cooldown_seconds=max(30, int(os.environ.get("MCP_CHAT_WATCHDOG_RETRY_SECONDS", "300"))),
            initial_delay_seconds=max(0, int(os.environ.get("MCP_CHAT_WATCHDOG_INITIAL_DELAY_SECONDS", "15"))),
            completion_marker=os.environ.get("MCP_CHAT_WATCHDOG_COMPLETION_MARKER", DEFAULT_COMPLETION_MARKER).strip()
            or DEFAULT_COMPLETION_MARKER,
            continue_message=os.environ.get("MCP_CHAT_WATCHDOG_CONTINUE_MESSAGE", DEFAULT_CONTINUE_MESSAGE).strip()
            or DEFAULT_CONTINUE_MESSAGE,
            dry_run=os.environ.get("MCP_CHAT_WATCHDOG_DRY_RUN", "0").strip().lower()
            in {"1", "true", "yes", "on"},
            auto_start_app=os.environ.get("MCP_CHAT_WATCHDOG_AUTO_START_APP", "1").strip().lower()
            not in {"0", "false", "no", "off"},
            app_command=os.environ.get("MCP_CHAT_WATCHDOG_APP_COMMAND", "/usr/bin/codex-desktop").strip()
            or "/usr/bin/codex-desktop",
            app_start_timeout_seconds=max(
                1, int(os.environ.get("MCP_CHAT_WATCHDOG_APP_START_TIMEOUT_SECONDS", "30"))
            ),
            internal_timeout_seconds=max(
                1.0, float(os.environ.get("MCP_CHAT_WATCHDOG_INTERNAL_TIMEOUT_SECONDS", "10"))
            ),
            require_high_reasoning=os.environ.get("MCP_CHAT_WATCHDOG_REQUIRE_HIGH", "1").strip().lower()
            not in {"0", "false", "no", "off"},
            preferred_model=os.environ.get("MCP_CHAT_WATCHDOG_MODEL", "").strip(),
            thinking_effort=os.environ.get("MCP_CHAT_WATCHDOG_THINKING_EFFORT", "extended").strip()
            or "extended",
            stream_timeout_seconds=max(
                30, int(os.environ.get("MCP_CHAT_WATCHDOG_STREAM_TIMEOUT_SECONDS", "3600"))
            ),
            stale_generation_seconds=max(
                60, int(os.environ.get("MCP_CHAT_WATCHDOG_STALE_GENERATION_SECONDS", "600"))
            ),
            max_continue_attempts=max(
                1, int(os.environ.get("MCP_CHAT_WATCHDOG_MAX_CONTINUE_ATTEMPTS", "20"))
            ),
            pre_send_confirmation_seconds=max(
                0.0, float(os.environ.get("MCP_CHAT_WATCHDOG_PRE_SEND_CONFIRM_SECONDS", "1.25"))
            ),
            ledger_path=Path(
                os.environ.get(
                    "MCP_CHAT_AGENT_DB",
                    "~/.GPT/chat-agent-orchestrator.db",
                )
            ).expanduser(),
            forced_continue_conversation_ids=tuple(
                conversation_id.strip()
                for conversation_id in os.environ.get(
                    "MCP_CHAT_WATCHDOG_FORCED_CONTINUE_CONVERSATION_IDS", ""
                ).split(",")
                if conversation_id.strip()
            ),
            forced_continue_interval_seconds=max(
                30,
                int(
                    os.environ.get(
                        "MCP_CHAT_WATCHDOG_FORCED_CONTINUE_INTERVAL_SECONDS", "1200"
                    )
                ),
            ),
        )


class ChatWatchdog:
    def __init__(
        self,
        config: ChatWatchdogConfig,
        *,
        adapter_factory: Callable[[], ChatAdapter] | None = None,
        runtime_ensure: Callable[[], Awaitable[Any]] | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.config = config
        self.queue = ChatWatchdogQueue(config.queue_path, config.completed_path)
        self.projects = WatchdogProjectStore(
            config.projects_path or config.queue_path.with_name("projects.json")
        )
        ledger_path = config.ledger_path or config.state_path.with_suffix(".sqlite")
        self.ledger = DurableLedger(ledger_path)
        self.state = WatchdogStateStore(config.state_path, self.ledger)
        self._uses_managed_runtime = (
            adapter_factory is None and config.adapter_mode == CODEX_INTERNAL_ADAPTER_NAME
        )
        if adapter_factory is not None:
            self.adapter_factory = adapter_factory
        elif config.adapter_mode == CODEX_INTERNAL_ADAPTER_NAME:
            self.adapter_factory = lambda: CodexInternalChatAdapter(
                config.cdp_endpoint,
                preferred_model=config.preferred_model,
                thinking_effort=config.thinking_effort,
                require_high_reasoning=config.require_high_reasoning,
                timeout_seconds=config.internal_timeout_seconds,
                stream_timeout_seconds=config.stream_timeout_seconds,
            )
        elif config.adapter_mode == DIRECT_ADAPTER_NAME:
            self.adapter_factory = lambda: DirectChatAdapter(
                preferred_model=config.preferred_model,
                thinking_effort=config.thinking_effort,
                require_high_reasoning=config.require_high_reasoning,
                timeout_seconds=config.internal_timeout_seconds,
                stream_timeout_seconds=config.stream_timeout_seconds,
            )
        else:
            raise ValueError(
                f"unsupported chat watchdog adapter: {config.adapter_mode}; "
                f"supported adapters are {DIRECT_ADAPTER_NAME!r} and {CODEX_INTERNAL_ADAPTER_NAME!r}"
            )
        self.clock = clock
        self.identity_listing_observer = None
        self._runtime_ensure = runtime_ensure
        self._task: asyncio.Task[None] | None = None
        self._scan_lock = asyncio.Lock()
        self._runtime_launch_lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()
        self._runtime: dict[str, Any] = {
            "started_at": None,
            "last_scan_started_at": None,
            "last_scan_finished_at": None,
            "next_scan_at": None,
            "last_error": "",
            "runtime_state": "idle",
            "scanning": False,
            "scan_count": 0,
        }

    async def start(self) -> None:
        if not self.config.enabled or self._task is not None:
            return
        self._stop.clear()
        self._runtime["started_at"] = _utc_now()
        self._task = asyncio.create_task(self._run_forever(), name="terminal-mcp-chat-watchdog")

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    def wake(self) -> None:
        self._wake.set()

    async def available_projects(self) -> dict[str, Any]:
        if self._uses_managed_runtime:
            await self.ensure_background_runtime()
        selected = {item["project_id"] for item in self.projects.snapshot()["items"]}
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        requests = 0
        async with self.adapter_factory() as adapter:
            for _ in range(MAX_PROJECT_DISCOVERY_PAGES):
                payload = await adapter.list_projects(limit=50, cursor=cursor)
                requests += 1
                page = payload.get("items") if isinstance(payload, dict) else []
                items.extend(
                    {**item, "selected": str(item.get("id") or "") in selected}
                    for item in page
                    if isinstance(item, dict) and item.get("id")
                )
                if len(items) >= MAX_AVAILABLE_PROJECTS:
                    items = items[:MAX_AVAILABLE_PROJECTS]
                    cursor = None
                    break
                next_cursor = str(payload.get("cursor") or "") if isinstance(payload, dict) else ""
                if not next_cursor or next_cursor in seen_cursors:
                    cursor = None
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
        return {"items": items, "cursor": cursor, "physical_list_requests": requests}

    async def _project_thread_pages(
        self,
        adapter: ChatAdapter,
        project_id: str,
        *,
        stop_after_seen: set[str] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        requests = 0
        next_cursor = ""
        for _ in range(MAX_PROJECT_DISCOVERY_PAGES):
            payload = await adapter.list_project_threads(project_id, limit=50, cursor=cursor)
            requests += 1
            page = [item for item in payload.get("items", []) if isinstance(item, dict)] if isinstance(payload, dict) else []
            items.extend(page)
            if stop_after_seen is not None:
                page_ids = {
                    str(item.get("conversation_id") or "").lower()
                    for item in page
                    if CONVERSATION_ID_RE.fullmatch(str(item.get("conversation_id") or ""))
                }
                if page_ids & stop_after_seen:
                    break
            next_cursor = str(payload.get("cursor") or "") if isinstance(payload, dict) else ""
            if not next_cursor or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        if self.identity_listing_observer is not None:
            # Complete listings may be reused for identity matching without
            # another backend request. Prefix/partial scans cannot set a baseline.
            complete = not next_cursor and stop_after_seen is None
            self.identity_listing_observer(project_id, items, complete=complete)
        return items, requests

    @staticmethod
    def _project_thread_fingerprint(item: dict[str, Any]) -> str:
        payload = {
            "current_node": str(item.get("current_node") or ""),
            "update_time": item.get("update_time")
            if isinstance(item.get("update_time"), (int, float))
            else None,
            "archived": bool(item.get("archived")),
        }
        return hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    async def select_project(
        self,
        project_id: str,
        *,
        name: str = "",
        watch_mode: str = PROJECT_WATCH_MODE_NEW_THREADS,
    ) -> dict[str, Any]:
        normalized = project_id.strip()
        if PROJECT_ID_RE.fullmatch(normalized) is None:
            raise ValueError("project_id must be a valid ChatGPT Project id")
        if watch_mode not in PROJECT_WATCH_MODES:
            raise ValueError("watch_mode must be new_threads_only or existing_working")
        existing = {item["project_id"]: item for item in self.projects.snapshot()["items"]}
        if normalized in existing:
            self.projects.select(
                normalized,
                name=name,
                seen_thread_ids=[],
                watch_mode=watch_mode,
            )
            return self.snapshot()
        if self._uses_managed_runtime:
            await self.ensure_background_runtime()
        async with self.adapter_factory() as adapter:
            items, requests = await self._project_thread_pages(adapter, normalized)
        baseline = [
            str(item.get("conversation_id") or "").lower()
            for item in items
            if CONVERSATION_ID_RE.fullmatch(str(item.get("conversation_id") or ""))
        ]
        self.projects.select(
            normalized,
            name=name,
            seen_thread_ids=baseline,
            watch_mode=watch_mode,
        )
        self._runtime["project_baseline"] = {
            "at": _utc_now(),
            "project_id": normalized,
            "physical_list_requests": requests,
            "thread_count": len(set(baseline)),
        }
        return self.snapshot()

    def set_project_mode(self, project_id: str, watch_mode: str) -> dict[str, Any]:
        normalized = project_id.strip()
        if PROJECT_ID_RE.fullmatch(normalized) is None:
            raise ValueError("project_id must be a valid ChatGPT Project id")
        self.projects.set_mode(normalized, watch_mode)
        self.wake()
        return self.snapshot()

    def remove_project(self, project_id: str) -> bool:
        return self.projects.remove(project_id.strip())

    async def _discover_selected_project_threads(
        self,
        adapter: ChatAdapter,
    ) -> tuple[list[str], dict[str, ThreadSnapshot]]:
        records = self.projects.records()
        added_all: list[str] = []
        new_added_ids: list[str] = []
        existing_added_ids: list[str] = []
        preloaded_snapshots: dict[str, ThreadSnapshot] = {}
        list_requests = 0
        inspect_requests = 0
        existing_checked = 0
        failures = 0
        queued_entries, _ = self.queue.entries()
        queued_ids = {item.conversation_id for item in queued_entries}
        for project in records:
            project_id = str(project.get("project_id") or "")
            if PROJECT_ID_RE.fullmatch(project_id) is None:
                continue
            try:
                seen = {
                    str(item).lower()
                    for item in project.get("seen_thread_ids", [])
                    if str(item)
                }
                watch_mode = str(
                    project.get("watch_mode") or PROJECT_WATCH_MODE_NEW_THREADS
                )
                if watch_mode not in PROJECT_WATCH_MODES:
                    watch_mode = PROJECT_WATCH_MODE_NEW_THREADS
                include_existing = watch_mode == PROJECT_WATCH_MODE_EXISTING_WORKING
                existing_initialized = bool(project.get("existing_working_initialized"))
                full_existing_scan = include_existing and not existing_initialized
                items, project_requests = await self._project_thread_pages(
                    adapter,
                    project_id,
                    stop_after_seen=(
                        None
                        if full_existing_scan
                        else seen if project.get("initialized") else None
                    ),
                )
                list_requests += project_requests
                current_ids: list[str] = []
                unseen: list[tuple[str, str, str]] = []
                checked = project.get("checked_thread_fingerprints")
                checked = dict(checked) if isinstance(checked, dict) else {}
                checked_updates: dict[str, str] = {}
                inspection_failures = 0
                added_here: list[str] = []
                discovered_here: list[str] = []

                for item in items:
                    conversation_id = str(item.get("conversation_id") or "").lower()
                    if CONVERSATION_ID_RE.fullmatch(conversation_id) is None:
                        continue
                    current_ids.append(conversation_id)
                    title = str(item.get("title") or "")
                    archived = bool(item.get("archived"))
                    fingerprint = self._project_thread_fingerprint(item)

                    if conversation_id not in seen and not archived:
                        unseen.append((conversation_id, title, fingerprint))
                        continue

                    if not include_existing:
                        continue
                    if conversation_id in queued_ids:
                        checked_updates[conversation_id] = fingerprint
                        continue
                    if archived:
                        checked_updates[conversation_id] = fingerprint
                        continue
                    if not full_existing_scan and checked.get(conversation_id) == fingerprint:
                        continue

                    link = parse_chat_link(
                        f"https://chatgpt.com/g/{project_id}/c/{conversation_id}"
                    )
                    try:
                        inspect_requests += 1
                        snapshot = await adapter.inspect(link)
                    except Exception:
                        inspection_failures += 1
                        continue
                    existing_checked += 1
                    if not snapshot.found or not snapshot.state_verified:
                        inspection_failures += 1
                        continue
                    checked_updates[conversation_id] = fingerprint
                    if not (snapshot.running or snapshot.active_stream):
                        continue

                    self.queue.add(link.url)
                    self.state.start_task(
                        link,
                        source=f"project-watch-existing:{project_id}",
                    )
                    self.state.update(
                        conversation_id,
                        title=snapshot.title or title,
                        auto_project_id=project_id,
                        auto_project_mode=PROJECT_WATCH_MODE_EXISTING_WORKING,
                    )
                    queued_ids.add(conversation_id)
                    added_here.append(conversation_id)
                    preloaded_snapshots[conversation_id] = snapshot
                    existing_added_ids.append(conversation_id)

                if not project.get("initialized"):
                    self.projects.record_scan(
                        project_id,
                        observed_thread_ids=current_ids,
                        new_thread_ids=[],
                    )
                    continue

                for conversation_id, title, fingerprint in unseen:
                    discovered_here.append(conversation_id)
                    if conversation_id in queued_ids:
                        continue
                    link = parse_chat_link(
                        f"https://chatgpt.com/g/{project_id}/c/{conversation_id}"
                    )
                    self.queue.add(link.url)
                    self.state.start_task(link, source=f"project-watch:{project_id}")
                    self.state.update(
                        conversation_id,
                        title=title,
                        auto_project_id=project_id,
                        auto_project_mode=watch_mode,
                    )
                    queued_ids.add(conversation_id)
                    added_here.append(conversation_id)
                    new_added_ids.append(conversation_id)
                    if include_existing:
                        checked_updates[conversation_id] = fingerprint

                project_error = (
                    f"could not verify {inspection_failures} existing thread(s)"
                    if inspection_failures
                    else ""
                )
                self.projects.record_scan(
                    project_id,
                    observed_thread_ids=current_ids,
                    new_thread_ids=discovered_here,
                    error=project_error,
                    checked_thread_fingerprints=checked_updates,
                    existing_working_initialized=(
                        True if full_existing_scan else None
                    ),
                )
                added_all.extend(added_here)
            except Exception as exc:
                failures += 1
                self.projects.record_scan(
                    project_id,
                    observed_thread_ids=[],
                    new_thread_ids=[],
                    error=sanitize_runtime_error(f"{type(exc).__name__}: {exc}"),
                )
        self._runtime["project_scan"] = {
            "at": _utc_now(),
            "selected_projects": len(records),
            "physical_list_requests": list_requests,
            "physical_existing_inspect_requests": inspect_requests,
            "existing_threads_checked": existing_checked,
            "existing_working_threads_added": len(existing_added_ids),
            "existing_working_thread_ids": existing_added_ids,
            "new_threads_added": len(new_added_ids),
            "new_thread_ids": new_added_ids,
            "threads_added": len(added_all),
            "added_thread_ids": added_all,
            "failed_projects": failures,
        }
        return added_all, preloaded_snapshots

    def _reconcile_tasks(self, entries: list[ChatLink], *, source: str) -> None:
        """Synchronize persistent task generations with the editable URL file."""
        active_ids = {entry.conversation_id for entry in entries}
        states = self.state.all()
        for entry in entries:
            current = states.get(entry.conversation_id, {})
            if not current.get("queued") or not current.get("task_id"):
                self.state.start_task(entry, source=source)
        for conversation_id, current in states.items():
            if current.get("queued") and conversation_id not in active_ids:
                self.state.mark_not_queued(conversation_id)

    async def _cdp_available(self) -> bool:
        probe = await probe_runtime(
            self.config.cdp_endpoint,
            timeout=min(3.0, max(1.0, self.config.internal_timeout_seconds)),
        )
        return probe.available

    async def _probe_managed_runtime(self) -> RuntimeProbe:
        probe = await probe_runtime(
            self.config.cdp_endpoint,
            timeout=min(3.0, max(1.0, self.config.internal_timeout_seconds)),
        )
        if not probe.ready:
            return probe
        try:
            async with InternalChatClient(
                self.config.cdp_endpoint,
                timeout=self.config.internal_timeout_seconds,
                stream_timeout_seconds=self.config.stream_timeout_seconds,
                preferred_model=self.config.preferred_model,
                thinking_effort=self.config.thinking_effort,
                require_high_reasoning=self.config.require_high_reasoning,
            ) as client:
                health = await client.health()
            if not health.get("ready"):
                return RuntimeProbe(
                    True,
                    False,
                    "normal-chat client readiness returned false",
                    probe.target,
                )
        except (RuntimeUnavailableError, RuntimeNotReadyError, RuntimeProtocolError) as exc:
            return RuntimeProbe(
                True,
                False,
                sanitize_runtime_error(exc),
                probe.target,
            )
        return probe

    async def ensure_background_runtime(self) -> bool:
        """Ensure one usable internal runtime, serializing start/reopen attempts."""
        async with self._runtime_launch_lock:
            return await self._ensure_background_runtime()

    async def _ensure_background_runtime(self) -> bool:
        """Ensure the internal ChatGPT client has a live primary renderer."""
        if not self._uses_managed_runtime:
            return False
        initial = await self._probe_managed_runtime()
        if self._runtime_ensure is not None:
            if initial.ready:
                self._runtime["runtime_state"] = "ready"
                return False
            attempt = {
                "at": _utc_now(),
                "attempted": True,
                "started": False,
                "action": "service-recover",
                "initial_reason": initial.reason,
                "reason": "",
            }
            self._runtime["last_app_start"] = attempt
            self._runtime["runtime_state"] = "recovering"
            try:
                await self._runtime_ensure()
            except Exception as exc:
                attempt["reason"] = sanitize_runtime_error(exc)
                self._runtime["runtime_state"] = "runtime_unavailable"
                raise RuntimeUnavailableError(attempt["reason"]) from exc
            final = await self._probe_managed_runtime()
            if not final.ready:
                attempt["reason"] = final.reason or "runtime service recovered without a ready renderer"
                self._runtime["runtime_state"] = "runtime_not_ready"
                raise RuntimeNotReadyError(attempt["reason"])
            attempt["started"] = True
            attempt["ready_at"] = _utc_now()
            self._runtime["runtime_state"] = "ready"
            return True
        if initial.ready:
            self._runtime["runtime_state"] = "ready"
            return False

        attempt = {
            "at": _utc_now(),
            "attempted": False,
            "started": False,
            "action": "reopen" if initial.available else "start",
            "command": self.config.app_command,
            "initial_reason": initial.reason,
            "reason": "",
        }
        self._runtime["last_app_start"] = attempt
        self._runtime["runtime_state"] = (
            "runtime_not_ready" if initial.available else "runtime_unavailable"
        )
        if not self.config.auto_start_app:
            attempt["reason"] = "automatic ChatGPT startup is disabled"
            error_type = RuntimeNotReadyError if initial.available else RuntimeUnavailableError
            raise error_type(attempt["reason"])

        argv = shlex.split(self.config.app_command)
        if not argv:
            attempt["reason"] = "ChatGPT desktop command is empty"
            raise RuntimeUnavailableError(attempt["reason"])
        launch_argv = list(argv)
        if initial.available and not any(
            argument in {"--new-chat", "--quick-chat", "--prompt-chat", "--hotkey-window"}
            for argument in launch_argv
        ):
            launch_argv.append("--new-chat")
        attempt["attempted"] = True
        attempt["argv"] = launch_argv
        try:
            process = await asyncio.create_subprocess_exec(
                *launch_argv,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                start_new_session=True,
                env=desktop_launch_environment(),
            )
            asyncio.create_task(process.wait(), name="chatgpt-watchdog-app-launch-reaper")
        except (OSError, ValueError) as exc:
            attempt["reason"] = f"{type(exc).__name__}: {sanitize_runtime_error(exc)}"
            raise RuntimeUnavailableError(
                f"could not launch ChatGPT desktop: {sanitize_runtime_error(exc)}"
            ) from exc

        deadline = time.monotonic() + self.config.app_start_timeout_seconds
        last_probe = initial
        while time.monotonic() < deadline:
            last_probe = await self._probe_managed_runtime()
            if last_probe.ready:
                attempt["started"] = True
                attempt["ready_at"] = _utc_now()
                attempt["reason"] = ""
                self._runtime["runtime_state"] = "ready"
                return True
            attempt["reason"] = last_probe.reason
            await asyncio.sleep(0.5)

        reason = last_probe.reason or (
            f"internal runtime did not become ready within "
            f"{self.config.app_start_timeout_seconds} seconds"
        )
        attempt["reason"] = reason
        error_type = RuntimeNotReadyError if last_probe.available else RuntimeUnavailableError
        raise error_type(reason)

    async def _wait(self, seconds: int) -> None:
        self._runtime["next_scan_at"] = datetime.fromtimestamp(self.clock() + seconds, UTC).isoformat(timespec="seconds")
        self._wake.clear()
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=seconds)
        except TimeoutError:
            pass

    async def _run_forever(self) -> None:
        if self.config.initial_delay_seconds:
            await self._wait(self.config.initial_delay_seconds)
        while not self._stop.is_set():
            await self.scan_once(trigger="scheduled")
            if self._stop.is_set():
                break
            await self._wait(self.config.scan_interval_seconds)

    def _marker_in_text(self, text: str) -> bool:
        return any(line.strip() == self.config.completion_marker for line in text.splitlines())

    def _resolve_task_start(
        self,
        snapshot: ThreadSnapshot,
        previous: dict[str, Any],
    ) -> tuple[str | None, int | None, str]:
        """Resolve the user turn that started this task generation."""
        turns = list(snapshot.turns)
        if not turns:
            return None, None, "conversation transcript is unavailable"
        existing = str(previous.get("task_start_user_turn_key") or "")
        if existing:
            for index, turn in enumerate(turns):
                if turn.key == existing and turn.role == "user":
                    return existing, index, ""
        continue_message = self.config.continue_message.strip()
        user_turns = [
            (index, turn)
            for index, turn in enumerate(turns)
            if turn.role == "user" and turn.text.strip() != continue_message
        ]
        if not user_turns:
            return None, None, "conversation has no user task turn yet"

        previous_completion_key = str(previous.get("previous_completion_turn_key") or "")
        if previous_completion_key:
            boundary_index = next(
                (index for index, turn in enumerate(turns) if turn.key == previous_completion_key),
                None,
            )
            if boundary_index is not None:
                candidate_users = [(index, turn) for index, turn in user_turns if index > boundary_index]
                if not candidate_users:
                    return None, None, "waiting for a new user task after the previous completion marker"
                index, turn = candidate_users[-1]
                return turn.key, index, ""

        if previous.get("previous_task_id"):
            marker_indices = [
                index
                for index, turn in enumerate(turns)
                if turn.role == "assistant" and self._marker_in_text(turn.text)
            ]
            if marker_indices:
                candidate_users = [
                    (index, turn)
                    for index, turn in user_turns
                    if any(marker_index < index for marker_index in marker_indices)
                ]
                if not candidate_users:
                    return None, None, "waiting for a new user task after the previous completion marker"
                index, turn = candidate_users[-1]
                return turn.key, index, ""

        index, turn = user_turns[-1]
        return turn.key, index, ""

    def _completion_turn(
        self,
        snapshot: ThreadSnapshot,
        task_start_key: str,
        task_start_index: int,
    ) -> ConversationTurn | None:
        turns = list(snapshot.turns)
        meaningful = [(index, turn) for index, turn in enumerate(turns) if turn.text.strip()]
        if not meaningful:
            return None
        latest_index, latest = meaningful[-1]
        if latest.role != "assistant" or latest_index <= task_start_index:
            return None
        return latest if self._marker_in_text(latest.text) else None

    def _record_active_generation(self, conversation_id: str) -> None:
        active = self._runtime.setdefault("active_generation_conversation_ids", [])
        if conversation_id not in active:
            active.append(conversation_id)
        if not self._runtime.get("active_generation_conversation_id"):
            self._runtime["active_generation_conversation_id"] = conversation_id

    def _snapshot_state(
        self,
        link: ChatLink,
        snapshot: ThreadSnapshot,
        previous: dict[str, Any],
        *,
        now: float,
    ) -> dict[str, Any]:
        last_turn_key = snapshot.turns[-1].key if snapshot.turns else ""
        previous_progress_epoch = float(previous.get("last_progress_epoch", 0) or 0)
        structurally_unchanged = bool(previous.get("last_transcript_hash")) and all(
            (
                str(previous.get("last_transcript_hash") or "") == snapshot.transcript_hash,
                str(previous.get("current_node") or "") == snapshot.current_node,
                str(previous.get("last_turn_key") or "") == last_turn_key,
            )
        )
        progress_epoch = (
            previous_progress_epoch
            if structurally_unchanged and previous_progress_epoch > 0
            else now
        )
        return {
            "url": link.url,
            "title": snapshot.title or str(previous.get("title") or ""),
            "last_checked_at": _utc_now(),
            "last_turn_key": last_turn_key,
            "last_assistant_hash": snapshot.assistant_hash,
            "last_transcript_hash": snapshot.transcript_hash,
            "current_node": snapshot.current_node,
            "visible_current_node": snapshot.visible_current_node,
            "canonical": snapshot.canonical,
            "state_verified": snapshot.state_verified,
            "conversation_update_time": snapshot.update_time,
            "active_stream": snapshot.active_stream,
            "last_progress_epoch": progress_epoch,
            "last_progress_at": datetime.fromtimestamp(progress_epoch, UTC).isoformat(
                timespec="seconds"
            ),
            "last_error": "",
        }

    async def scan_once(self, *, trigger: str = "manual") -> dict[str, Any]:
        if not self.config.enabled:
            return self.snapshot()
        if self._scan_lock.locked():
            return self.snapshot()
        async with self._scan_lock:
            self._runtime.update(
                scanning=True,
                last_scan_started_at=_utc_now(),
                last_error="",
                trigger=trigger,
            )
            entries, invalid = await asyncio.to_thread(self.queue.entries)
            self._reconcile_tasks(entries, source=f"scan:{trigger}")
            orchestrated = self.ledger.active_task_conversation_ids()
            skipped = [
                link.conversation_id
                for link in entries
                if link.conversation_id in orchestrated
            ]
            entries = [
                link
                for link in entries
                if link.conversation_id not in orchestrated
            ]
            self._runtime["skipped_orchestrated_conversation_ids"] = skipped
            if invalid:
                self._runtime["last_error"] = f"queue contains {len(invalid)} invalid line(s)"
            if not entries and self.projects.snapshot()["count"] == 0:
                self._finish_scan()
                return self.snapshot()

            original_conversation_id: str | None = None
            restore_needed = False
            try:
                if self._uses_managed_runtime:
                    await self.ensure_background_runtime()
                async with self.adapter_factory() as adapter:
                    original_conversation_id = await adapter.current_conversation_id()
                    try:
                        refresh = await adapter.refresh_catalog()
                        self._runtime["last_refresh"] = {
                            "at": _utc_now(),
                            "refreshed": refresh.refreshed,
                            "hard_reload": refresh.hard_reload,
                            "reason": refresh.reason,
                        }
                        restore_needed = refresh.refreshed or refresh.hard_reload
                        self._runtime["runtime_state"] = (
                            "ready" if refresh.refreshed else ThreadDecisionState.RUNTIME_NOT_READY.value
                        )
                        if not refresh.refreshed:
                            self._runtime["last_error"] = f"refresh deferred: {refresh.reason}"
                            for link in entries:
                                self.state.update(
                                    link.conversation_id,
                                    url=link.url,
                                    status="waiting_refresh",
                                    last_error=refresh.reason,
                                    last_checked_at=_utc_now(),
                                )
                        else:
                            _, project_snapshots = await self._discover_selected_project_threads(
                                adapter
                            )
                            entries, discovered_invalid = await asyncio.to_thread(self.queue.entries)
                            self._reconcile_tasks(entries, source=f"scan:{trigger}")
                            if discovered_invalid and not self._runtime.get("last_error"):
                                self._runtime["last_error"] = (
                                    f"queue contains {len(discovered_invalid)} invalid line(s)"
                                )
                            orchestrated = self.ledger.active_task_conversation_ids()
                            skipped = [
                                link.conversation_id
                                for link in entries
                                if link.conversation_id in orchestrated
                            ]
                            entries = [
                                link
                                for link in entries
                                if link.conversation_id not in orchestrated
                            ]
                            self._runtime["skipped_orchestrated_conversation_ids"] = skipped
                            self._runtime["active_generation_conversation_id"] = ""
                            self._runtime["active_generation_conversation_ids"] = []
                            self._runtime["selected_continue_conversation_id"] = ""
                            self._runtime["write_deferred_reason"] = ""
                            candidates: list[tuple[float, int, ContinuationCandidate]] = []
                            owned_stream_active = False
                            for position, link in enumerate(entries):
                                observation = await self._process_link(
                                    adapter,
                                    link,
                                    snapshot=project_snapshots.get(link.conversation_id),
                                )
                                owned_stream_active = (
                                    owned_stream_active or observation.owned_stream_active
                                )
                                if observation.candidate is not None:
                                    candidates.append(
                                        (
                                            observation.candidate.fairness_epoch,
                                            position,
                                            observation.candidate,
                                        )
                                    )

                            if owned_stream_active:
                                self._runtime["write_deferred_reason"] = (
                                    "a watchdog-owned completion stream is already active"
                                )
                            elif candidates:
                                _, _, candidate = min(candidates, key=lambda item: (item[0], item[1]))
                                current_entries, _ = await asyncio.to_thread(self.queue.entries)
                                still_queued = any(
                                    item.conversation_id == candidate.link.conversation_id
                                    for item in current_entries
                                )
                                if still_queued:
                                    selected_epoch = self.clock()
                                    self.state.update(
                                        candidate.link.conversation_id,
                                        last_scheduler_selected_at=_utc_now(),
                                        last_scheduler_selected_epoch=selected_epoch,
                                        status="selected_for_continue",
                                    )
                                    self._runtime["selected_continue_conversation_id"] = (
                                        candidate.link.conversation_id
                                    )
                                    await self._continue_candidate(adapter, candidate)
                                else:
                                    self._runtime["write_deferred_reason"] = (
                                        "selected task was removed from the queue before continuation"
                                    )
                    finally:
                        if restore_needed and original_conversation_id:
                            try:
                                restored = await adapter.restore(original_conversation_id)
                                self._runtime["last_restore"] = {
                                    "at": _utc_now(),
                                    "conversation_id": original_conversation_id,
                                    "restored": restored,
                                    "reason": "" if restored else "original conversation was not restored",
                                }
                                if not restored and not self._runtime.get("last_error"):
                                    self._runtime["last_error"] = "original conversation was not restored"
                            except Exception as restore_exc:
                                self._runtime["last_restore"] = {
                                    "at": _utc_now(),
                                    "conversation_id": original_conversation_id,
                                    "restored": False,
                                    "reason": f"{type(restore_exc).__name__}: {restore_exc}",
                                }
                                if not self._runtime.get("last_error"):
                                    self._runtime["last_error"] = f"restore failed: {type(restore_exc).__name__}: {restore_exc}"
            except Exception as exc:
                failure = classify_runtime_error(exc)
                self._runtime["last_error"] = failure.reason
                self._runtime["runtime_state"] = failure.state.value
                for link in entries:
                    self.state.update(
                        link.conversation_id,
                        url=link.url,
                        status=failure.state.value,
                        last_error=failure.reason,
                        last_checked_at=_utc_now(),
                    )
            finally:
                self._finish_scan()
            return self.snapshot()

    def _finish_scan(self) -> None:
        self._runtime["scanning"] = False
        self._runtime["last_scan_finished_at"] = _utc_now()
        self._runtime["scan_count"] = int(self._runtime.get("scan_count", 0)) + 1
        self._runtime["next_scan_at"] = datetime.fromtimestamp(
            self.clock() + self.config.scan_interval_seconds,
            UTC,
        ).isoformat(timespec="seconds")

    async def _record_completion(
        self,
        link: ChatLink,
        snapshot: ThreadSnapshot,
        previous: dict[str, Any],
        task_start_key: str,
        completion_turn: ConversationTurn,
        common: dict[str, Any],
    ) -> None:
        task_id = str(previous.get("task_id") or "")
        task_generation = int(previous.get("task_generation", 0) or 0)
        await asyncio.to_thread(
            self.queue.complete,
            link,
            {
                "title": snapshot.title or str(previous.get("title") or ""),
                "assistant_hash": snapshot.assistant_hash,
                "transcript_hash": snapshot.transcript_hash,
                "completion_marker": self.config.completion_marker,
                "task_id": task_id,
                "task_generation": task_generation,
                "task_start_user_turn_key": task_start_key,
                "completion_turn_key": completion_turn.key,
            },
        )
        self.state.update(
            link.conversation_id,
            **common,
            status="completed",
            queued=False,
            completed_at=_utc_now(),
            completion_turn_key=completion_turn.key,
        )

    async def _process_link(
        self,
        adapter: ChatAdapter,
        link: ChatLink,
        *,
        snapshot: ThreadSnapshot | None = None,
    ) -> LinkObservation:
        """Inspect one queued thread and return a write candidate without sending."""
        now = self.clock()
        previous = self.state.get(link.conversation_id)
        if snapshot is None:
            try:
                snapshot = await adapter.inspect(link)
            except Exception as exc:
                failure = classify_runtime_error(exc)
                self.state.update(
                    link.conversation_id,
                    url=link.url,
                    status=failure.state.value,
                    last_error=failure.reason,
                    last_checked_at=_utc_now(),
                )
                return LinkObservation()

        common = self._snapshot_state(link, snapshot, previous, now=now)
        if not snapshot.found:
            if snapshot.reason.startswith("conversation transcript"):
                status = "waiting_transcript"
            elif snapshot.reason.startswith("refresh deferred"):
                status = "waiting_refresh"
            else:
                status = "not_found"
            self.state.update(
                link.conversation_id,
                **{**common, "status": status, "last_error": snapshot.reason},
            )
            return LinkObservation()
        if not snapshot.turns:
            self.state.update(
                link.conversation_id,
                **{
                    **common,
                    "status": "waiting_transcript",
                    "last_error": "conversation transcript is empty; continuation was not sent",
                },
            )
            return LinkObservation()

        if link.conversation_id in self.config.forced_continue_conversation_ids:
            last_continue_at = float(previous.get("last_continue_epoch", 0) or 0)
            same_transcript = bool(previous.get("last_transcript_hash")) and (
                str(previous.get("last_transcript_hash")) == snapshot.transcript_hash
            )
            if same_transcript and previous.get("status") in {
                "continue_unconfirmed",
                "waiting_unconfirmed_submission",
            }:
                self.state.update(
                    link.conversation_id,
                    **{
                        **common,
                        "status": "waiting_unconfirmed_submission",
                        "last_error": (
                            "the previous forced send was unconfirmed; retry is blocked "
                            "until the transcript changes"
                        ),
                    },
                )
                return LinkObservation()
            if (
                last_continue_at
                and now - last_continue_at
                < self.config.forced_continue_interval_seconds
            ):
                self.state.update(
                    link.conversation_id,
                    **common,
                    status="waiting_forced_interval",
                )
                return LinkObservation()
            if self.config.dry_run:
                self.state.update(
                    link.conversation_id,
                    **common,
                    status="would_force_continue",
                )
                return LinkObservation()
            candidate = ContinuationCandidate(
                link=link,
                snapshot=snapshot,
                previous=previous,
                common=common,
                task_start_key="",
                task_start_index=0,
                fairness_epoch=last_continue_at,
                forced=True,
            )
            self.state.update(
                link.conversation_id,
                **common,
                status="ready_forced_continue",
            )
            return LinkObservation(candidate=candidate)

        task_start_key, task_start_index, boundary_reason = self._resolve_task_start(
            snapshot,
            previous,
        )
        if task_start_key is None or task_start_index is None:
            self.state.update(
                link.conversation_id,
                **{**common, "status": "awaiting_new_task", "last_error": boundary_reason},
            )
            return LinkObservation()
        if previous.get("task_start_user_turn_key") != task_start_key:
            previous = self.state.update(
                link.conversation_id,
                task_start_user_turn_key=task_start_key,
                task_started_observed_at=_utc_now(),
            )
        common["task_start_user_turn_key"] = task_start_key

        latest_turn = snapshot.latest_turn
        if (
            latest_turn is not None
            and latest_turn.role == "user"
            and latest_turn.text.strip() == self.config.continue_message.strip()
        ):
            self.state.update(
                link.conversation_id,
                **{
                    **common,
                    "status": "waiting_after_continue",
                    "last_error": "continuation prompt is already the latest user turn",
                },
            )
            return LinkObservation()

        decision = classify_thread_state(
            snapshot,
            task_start_index=task_start_index,
            completion_marker=self.config.completion_marker,
            continuation_message=self.config.continue_message,
            now=now,
            stale_after_seconds=self.config.stale_generation_seconds,
            progress_observed_at=float(common["last_progress_epoch"]),
        )
        if decision.state is ThreadDecisionState.COMPLETE and decision.completion_turn is not None:
            await self._record_completion(
                link,
                snapshot,
                previous,
                task_start_key,
                decision.completion_turn,
                common,
            )
            return LinkObservation()
        if decision.state in {
            ThreadDecisionState.RUNNING_OWNED_STREAM,
            ThreadDecisionState.RUNNING_CANONICAL,
        }:
            self._record_active_generation(link.conversation_id)
            self.state.update(
                link.conversation_id,
                **{**common, "status": decision.state.value, "last_error": decision.reason},
            )
            return LinkObservation(
                owned_stream_active=decision.state is ThreadDecisionState.RUNNING_OWNED_STREAM
            )
        if decision.state in {
            ThreadDecisionState.DUPLICATE_PENDING,
            ThreadDecisionState.INTERRUPTED_OR_STALE,
            ThreadDecisionState.AWAITING_ASSISTANT,
            ThreadDecisionState.UNKNOWN,
        }:
            self.state.update(
                link.conversation_id,
                **{**common, "status": decision.state.value, "last_error": decision.reason},
            )
            return LinkObservation()
        if not decision.can_continue:
            self.state.update(
                link.conversation_id,
                **{
                    **common,
                    "status": ThreadDecisionState.UNKNOWN.value,
                    "last_error": decision.reason or "thread is not eligible for continuation",
                },
            )
            return LinkObservation()
        if snapshot.composer_text.strip():
            self.state.update(link.conversation_id, **common, status="draft_present")
            return LinkObservation()

        last_continue_at = float(previous.get("last_continue_epoch", 0) or 0)
        previous_transcript_hash = str(previous.get("last_transcript_hash") or "")
        same_transcript = (
            bool(previous_transcript_hash)
            and previous_transcript_hash == snapshot.transcript_hash
        )
        previous_attempts = int(previous.get("continue_attempts", 0) or 0)
        if previous_attempts >= self.config.max_continue_attempts:
            self.state.update(
                link.conversation_id,
                **{
                    **common,
                    "status": ThreadDecisionState.TERMINAL_ERROR.value,
                    "last_error": (
                        f"maximum continuation attempts reached "
                        f"({self.config.max_continue_attempts})"
                    ),
                },
            )
            return LinkObservation()
        if same_transcript and last_continue_at and previous_attempts > 0:
            unconfirmed = previous.get("status") in {
                "continue_unconfirmed",
                "waiting_unconfirmed_submission",
            }
            self.state.update(
                link.conversation_id,
                **{
                    **common,
                    "status": (
                        "waiting_unconfirmed_submission"
                        if unconfirmed
                        else "waiting_after_continue"
                    ),
                    "last_error": (
                        "the previous send was unconfirmed; automatic retry is blocked "
                        "until the transcript changes"
                        if unconfirmed
                        else "a continuation was already sent for this exact transcript; "
                        "waiting for the transcript to change"
                    ),
                },
            )
            return LinkObservation()
        if (
            same_transcript
            and last_continue_at
            and now - last_continue_at < self.config.retry_cooldown_seconds
        ):
            self.state.update(link.conversation_id, **common, status="waiting_after_continue")
            return LinkObservation()
        if self.config.dry_run:
            self.state.update(link.conversation_id, **common, status="would_continue")
            return LinkObservation()

        fairness_epoch = max(
            last_continue_at,
            float(previous.get("last_scheduler_selected_epoch", 0) or 0),
        )
        candidate = ContinuationCandidate(
            link=link,
            snapshot=snapshot,
            previous=previous,
            common=common,
            task_start_key=task_start_key,
            task_start_index=task_start_index,
            fairness_epoch=fairness_epoch,
        )
        self.state.update(link.conversation_id, **common, status="ready_to_continue")
        return LinkObservation(candidate=candidate)

    async def _continue_candidate(
        self,
        adapter: ChatAdapter,
        candidate: ContinuationCandidate,
    ) -> None:
        """Revalidate and continue one fairly selected candidate."""
        link = candidate.link
        snapshot = candidate.snapshot
        previous = candidate.previous
        common = candidate.common

        if self.config.pre_send_confirmation_seconds:
            await asyncio.sleep(self.config.pre_send_confirmation_seconds)
        try:
            confirmation = await adapter.inspect(link)
        except Exception as exc:
            failure = classify_runtime_error(exc)
            self.state.update(
                link.conversation_id,
                **{
                    **common,
                    "status": failure.state.value,
                    "last_error": f"pre-send inspection failed: {failure.reason}",
                },
            )
            return

        now = self.clock()
        confirmation_common = self._snapshot_state(
            link,
            confirmation,
            {**previous, **common},
            now=now,
        )
        if not confirmation.found or not confirmation.turns:
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": "waiting_transcript",
                    "last_error": (
                        confirmation.reason
                        or "conversation transcript disappeared before send"
                    ),
                },
            )
            return

        if candidate.forced:
            await self._send_forced_continue(
                adapter,
                candidate,
                confirmation,
                confirmation_common,
                now,
            )
            return

        confirmation_start_key, confirmation_start_index, confirmation_reason = (
            self._resolve_task_start(confirmation, previous)
        )
        if confirmation_start_key is None or confirmation_start_index is None:
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": "awaiting_new_task",
                    "last_error": confirmation_reason,
                },
            )
            return
        confirmation_common["task_start_user_turn_key"] = confirmation_start_key

        confirmation_latest = confirmation.latest_turn
        if (
            confirmation_latest is not None
            and confirmation_latest.role == "user"
            and confirmation_latest.text.strip() == self.config.continue_message.strip()
        ):
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": "waiting_after_continue",
                    "last_error": "continuation prompt is already the latest user turn",
                },
            )
            return

        confirmation_decision = classify_thread_state(
            confirmation,
            task_start_index=confirmation_start_index,
            completion_marker=self.config.completion_marker,
            continuation_message=self.config.continue_message,
            now=now,
            stale_after_seconds=self.config.stale_generation_seconds,
            progress_observed_at=float(confirmation_common["last_progress_epoch"]),
        )
        if (
            confirmation_decision.state is ThreadDecisionState.COMPLETE
            and confirmation_decision.completion_turn is not None
        ):
            await self._record_completion(
                link,
                confirmation,
                previous,
                confirmation_start_key,
                confirmation_decision.completion_turn,
                confirmation_common,
            )
            return
        if confirmation_decision.state in {
            ThreadDecisionState.RUNNING_OWNED_STREAM,
            ThreadDecisionState.RUNNING_CANONICAL,
        }:
            self._record_active_generation(link.conversation_id)
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": confirmation_decision.state.value,
                    "last_error": confirmation_decision.reason,
                },
            )
            return
        if confirmation_decision.state in {
            ThreadDecisionState.DUPLICATE_PENDING,
            ThreadDecisionState.INTERRUPTED_OR_STALE,
            ThreadDecisionState.AWAITING_ASSISTANT,
            ThreadDecisionState.UNKNOWN,
        }:
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": confirmation_decision.state.value,
                    "last_error": confirmation_decision.reason,
                },
            )
            return
        if not confirmation_decision.can_continue:
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": ThreadDecisionState.UNKNOWN.value,
                    "last_error": (
                        confirmation_decision.reason
                        or "thread is not eligible for continuation"
                    ),
                },
            )
            return
        if confirmation.composer_text.strip():
            self.state.update(
                link.conversation_id,
                **confirmation_common,
                status="draft_present",
            )
            return
        if snapshot.canonical or confirmation.canonical:
            if not snapshot.current_node or confirmation.current_node != snapshot.current_node:
                self.state.update(
                    link.conversation_id,
                    **{
                        **confirmation_common,
                        "status": "waiting_current_node_change",
                        "last_error": (
                            "canonical current_node changed during pre-send confirmation; "
                            "continuation was not sent"
                        ),
                    },
                )
                return
        if confirmation.transcript_hash != snapshot.transcript_hash:
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": "waiting_transcript_change",
                    "last_error": (
                        "conversation changed during pre-send confirmation; "
                        "continuation was not sent"
                    ),
                },
            )
            return

        current_entries, _ = await asyncio.to_thread(self.queue.entries)
        if not any(item.conversation_id == link.conversation_id for item in current_entries):
            self.state.mark_not_queued(link.conversation_id)
            return

        last_continue_parent = str(previous.get("last_continue_parent_node") or "")
        if confirmation.current_node and last_continue_parent == confirmation.current_node:
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": "waiting_after_continue",
                    "last_error": (
                        "a continuation was already submitted for this canonical parent node"
                    ),
                },
            )
            return

        try:
            result = await adapter.send_continue(
                link,
                self.config.continue_message,
                expected_current_node=confirmation.current_node,
            )
        except Exception as exc:
            failure = classify_runtime_error(exc)
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": failure.state.value,
                    "last_error": failure.reason,
                    "continue_attempts": int(previous.get("continue_attempts", 0) or 0),
                },
            )
            return

        attempts = int(previous.get("continue_attempts", 0) or 0) + (
            1 if result.clicked else 0
        )
        if result.running:
            self._record_active_generation(link.conversation_id)
        if result.clicked and result.running:
            status = ThreadDecisionState.RUNNING_OWNED_STREAM.value
        elif result.clicked:
            status = "continue_sent"
        elif result.running:
            status = ThreadDecisionState.RUNNING_CANONICAL.value
        else:
            status = "continue_blocked"
        if result.clicked and not result.observed and not result.running:
            status = "continue_unconfirmed"
        if result.clicked and result.reason.startswith("chooser blocked"):
            status = "chooser_blocked"
        if not result.clicked and result.reason.startswith("transcript is unavailable"):
            status = "waiting_transcript"
        elif not result.clicked and result.reason.startswith("completion marker appeared"):
            status = "waiting_completion_confirmation"
        elif not result.clicked and result.reason.startswith("continuation prompt is already"):
            status = "waiting_after_continue"

        self.state.update(
            link.conversation_id,
            **{
                **confirmation_common,
                "status": status,
                "last_continue_at": _utc_now() if result.clicked else previous.get("last_continue_at"),
                "last_continue_epoch": now
                if result.clicked
                else float(previous.get("last_continue_epoch", 0) or 0),
                "continue_attempts": attempts,
                "continue_observed": result.observed,
                "quality_verified": result.quality_verified,
                "quality_changed": result.quality_changed,
                "quality_label": result.quality_label,
                "last_continue_parent_node": (
                    result.parent_message_id
                    if result.clicked
                    else previous.get("last_continue_parent_node", "")
                ),
                "last_continue_request_id": (
                    result.request_id
                    if result.clicked
                    else previous.get("last_continue_request_id", "")
                ),
                "last_continue_user_message_id": (
                    result.user_message_id
                    if result.clicked
                    else previous.get("last_continue_user_message_id", "")
                ),
                "last_continue_final_message_id": (
                    result.final_message_id
                    if result.clicked
                    else previous.get("last_continue_final_message_id", "")
                ),
                "last_continue_final_status": (
                    result.final_status
                    if result.clicked
                    else previous.get("last_continue_final_status", "")
                ),
                "last_error": result.reason,
            },
        )

    async def _send_forced_continue(
        self,
        adapter: ChatAdapter,
        candidate: ContinuationCandidate,
        confirmation: ThreadSnapshot,
        confirmation_common: dict[str, Any],
        now: float,
    ) -> None:
        """Send an interval-driven continuation without consulting thread status."""
        link = candidate.link
        previous = candidate.previous
        current_entries, _ = await asyncio.to_thread(self.queue.entries)
        if not any(item.conversation_id == link.conversation_id for item in current_entries):
            self.state.mark_not_queued(link.conversation_id)
            return
        try:
            result = await adapter.send_continue(
                link,
                self.config.continue_message,
                expected_current_node=confirmation.current_node,
                force=True,
            )
        except Exception as exc:
            failure = classify_runtime_error(exc)
            self.state.update(
                link.conversation_id,
                **{
                    **confirmation_common,
                    "status": failure.state.value,
                    "last_error": failure.reason,
                    "continue_attempts": int(previous.get("continue_attempts", 0) or 0),
                },
            )
            return

        attempts = int(previous.get("continue_attempts", 0) or 0) + (
            1 if result.clicked else 0
        )
        if result.clicked and not result.observed and not result.running:
            status = "continue_unconfirmed"
        elif result.clicked:
            status = "forced_continue_sent"
        else:
            status = "forced_continue_blocked"
        self.state.update(
            link.conversation_id,
            **{
                **confirmation_common,
                "status": status,
                "last_continue_at": (
                    _utc_now() if result.clicked else previous.get("last_continue_at")
                ),
                "last_continue_epoch": (
                    now
                    if result.clicked
                    else float(previous.get("last_continue_epoch", 0) or 0)
                ),
                "continue_attempts": attempts,
                "continue_observed": result.observed,
                "quality_verified": result.quality_verified,
                "quality_changed": result.quality_changed,
                "quality_label": result.quality_label,
                "last_continue_parent_node": (
                    result.parent_message_id
                    if result.clicked
                    else previous.get("last_continue_parent_node", "")
                ),
                "last_continue_request_id": (
                    result.request_id
                    if result.clicked
                    else previous.get("last_continue_request_id", "")
                ),
                "last_continue_user_message_id": (
                    result.user_message_id
                    if result.clicked
                    else previous.get("last_continue_user_message_id", "")
                ),
                "last_error": result.reason,
            },
        )

    def replace_queue(self, text: str, expected_version: str | None = None) -> dict[str, Any]:
        self.queue.replace_text(text, expected_version)
        entries, _ = self.queue.entries()
        self._reconcile_tasks(entries, source="queue-replace")
        self.wake()
        return self.snapshot()

    def add_url(self, url: str) -> dict[str, Any]:
        link = parse_chat_link(url)
        before, _ = self.queue.entries()
        was_present = any(item.conversation_id == link.conversation_id for item in before)
        self.queue.add(url)
        if not was_present:
            self.state.start_task(link, source="queue-add")
        self.wake()
        return self.snapshot()

    def remove_url(self, conversation_id: str) -> bool:
        removed = self.queue.remove(conversation_id)
        if removed:
            self.state.mark_not_queued(conversation_id)
            self.wake()
        return removed

    def snapshot(self) -> dict[str, Any]:
        queue = self.queue.snapshot()
        states = self.state.all()
        entries: list[dict[str, Any]] = []
        for entry in queue["entries"]:
            state = states.get(entry["conversation_id"], {})
            entries.append({**entry, "state": state})
        return {
            "enabled": self.config.enabled,
            "dry_run": self.config.dry_run,
            "scan_interval_seconds": self.config.scan_interval_seconds,
            "retry_cooldown_seconds": self.config.retry_cooldown_seconds,
            "completion_marker": self.config.completion_marker,
            "adapter_mode": self.config.adapter_mode,
            "cdp_endpoint": self.config.cdp_endpoint,
            "auto_start_app": self.config.auto_start_app,
            "app_command": self.config.app_command,
            "app_start_timeout_seconds": self.config.app_start_timeout_seconds,
            "internal_timeout_seconds": self.config.internal_timeout_seconds,
            "require_high_reasoning": self.config.require_high_reasoning,
            "preferred_model": self.config.preferred_model,
            "thinking_effort": self.config.thinking_effort,
            "stream_timeout_seconds": self.config.stream_timeout_seconds,
            "stale_generation_seconds": self.config.stale_generation_seconds,
            "max_continue_attempts": self.config.max_continue_attempts,
            "pre_send_confirmation_seconds": self.config.pre_send_confirmation_seconds,
            "runtime": dict(self._runtime),
            "projects": self.projects.snapshot(),
            "queue": {**queue, "entries": entries},
        }


def install_chat_watchdog_lifespan(app: Any, watchdog: ChatWatchdog) -> None:
    """Compose the FastMCP lifespan with the watchdog's background task."""

    original = app.router.lifespan_context

    @asynccontextmanager
    async def combined(application: Any) -> AsyncIterator[Any]:
        async with original(application) as state:
            await watchdog.start()
            try:
                yield state
            finally:
                await watchdog.stop()

    app.router.lifespan_context = combined
