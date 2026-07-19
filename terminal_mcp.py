from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import hashlib
import json
import os
import re
import secrets
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import tomllib
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit, urlunsplit

from mcp import types as mcp_types
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from gpt_thread_store import GPTThreadStore
from usage_dashboard import install_usage_dashboard
from chat_watchdog import ChatWatchdog, ChatWatchdogConfig, install_chat_watchdog_lifespan

WORKSPACE_DIR = Path(os.environ.get("MCP_WORKSPACE", "~/mcp_workspace")).expanduser().resolve()
WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = Path(os.environ.get("MCP_LOG_DIR", "~/.gpt_terminal_mcp_logs")).expanduser().resolve()
LOG_DIR.mkdir(parents=True, exist_ok=True)
SHELL = os.environ.get("MCP_SHELL", "/bin/bash")
DEFAULT_TIMEOUT = int(os.environ.get("MCP_DEFAULT_TIMEOUT", "30"))
DEFAULT_MAX_OUTPUT_CHARS = int(os.environ.get("MCP_MAX_OUTPUT_CHARS", "24000"))
PROCESS_BUFFER_LINES = int(os.environ.get("MCP_PROCESS_BUFFER_LINES", "1000"))
CAPTURE_MEMORY_CHARS = int(os.environ.get("MCP_CAPTURE_MEMORY_CHARS", "65536"))
WORKLOAD_ISOLATION = os.environ.get("MCP_WORKLOAD_ISOLATION", "auto").lower()
WORKLOAD_MEMORY_MAX = os.environ.get("MCP_WORKLOAD_MEMORY_MAX", "")
WORKLOAD_CPU_QUOTA = os.environ.get("MCP_WORKLOAD_CPU_QUOTA", "")
WORKLOAD_TASKS_MAX = os.environ.get("MCP_WORKLOAD_TASKS_MAX", "")
AGENT_DIR_NAME = os.environ.get("MCP_AGENT_DIR_NAME", ".agent")
AGENT_CONTEXT_MAX_CHARS = int(os.environ.get("MCP_AGENT_CONTEXT_MAX_CHARS", "12000"))
MCP_PROXY_IDLE_TIMEOUT = int(os.environ.get("MCP_PROXY_IDLE_TIMEOUT", "1800"))
DEFAULT_BOOTSTRAP_MAX_CHARS = int(os.environ.get("MCP_BOOTSTRAP_MAX_CHARS", "100000"))
WATCH_IMAGE_MAX_BYTES = int(os.environ.get("MCP_WATCH_IMAGE_MAX_BYTES", str(20 * 1024 * 1024)))
TOKEN_ACCOUNTING_MAX_CHARS = int(os.environ.get("MCP_TOKEN_ACCOUNTING_MAX_CHARS", "1000000"))
GOAL_REMINDER_SECONDS = int(os.environ.get("MCP_GOAL_REMINDER_SECONDS", "900"))
NODE_REPL_MCP_SERVER = os.environ.get("MCP_NODE_REPL_SERVER", "node_repl")
GPT_STORE = GPTThreadStore(lambda: WORKSPACE_DIR)


def _format_goal_context(goal: dict[str, Any], *, reminder: bool = False) -> str:
    heading = "Periodic active-goal reminder" if reminder else "Thread goal context"
    conditions = "\n".join(
        f"{index}. {condition}"
        for index, condition in enumerate(goal["finish_conditions"], start=1)
    )
    lines = [
        "<goal_context>",
        heading,
        f"Status: {goal['status']}",
        f"Objective: {goal['objective']}",
        "Finish conditions:",
        conditions or "(none)",
    ]
    if goal["status"] == "active":
        lines.append(
            "Keep working toward the full objective; do not stop for partial success, and call thread_goal(action='complete') only with one evidence entry per condition."
        )
    elif goal["status"] == "technical_error":
        lines.append(f"Technical error: {goal['technical_error_reason']}")
    lines.append("</goal_context>")
    return "\n".join(lines)


def _append_goal_context(result: Any, goal: dict[str, Any]) -> Any:
    reminder = mcp_types.TextContent(
        type="text",
        text=_format_goal_context(goal, reminder=True),
    )
    if isinstance(result, mcp_types.CallToolResult):
        return result.model_copy(update={"content": [*result.content, reminder]})
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[0], list):
        return ([*result[0], reminder], result[1])
    if isinstance(result, list):
        return [*result, reminder]
    if isinstance(result, dict):
        return mcp_types.CallToolResult(
            content=[reminder],
            structuredContent=result,
            isError=False,
        )
    return [mcp_types.TextContent(type="text", text=str(result)), reminder]


class AccountingFastMCP(FastMCP):
    """Record the two text legs of each MCP tool loop without affecting tools.

    A model's tool-call payload is text it emitted; the tool result is text the
    host may provide back to the model.  This proxy sees both, but not the rest
    of the host prompt, provider cache accounting, or token costs for images.
    Accounting failures are deliberately non-fatal: observability must never
    make a usable terminal tool fail.
    """

    @staticmethod
    def _thread_id(arguments: dict[str, Any]) -> str:
        candidate = arguments.get("thread_id") or arguments.get("session_id") or ""
        return str(candidate).strip()[:128]

    @staticmethod
    def _text_for_token_accounting(value: Any) -> tuple[str, dict[str, Any]]:
        """Serialize textual content only; never tokenize image/audio base64."""
        media_blocks = 0

        def sanitize(item: Any) -> Any:
            nonlocal media_blocks
            if hasattr(item, "model_dump"):
                item = item.model_dump(mode="json")
            if isinstance(item, dict):
                kind = str(item.get("type", ""))
                if kind in {"image", "audio"} or "data" in item and kind:
                    media_blocks += 1
                    return {
                        "type": kind or "binary",
                        "mimeType": item.get("mimeType", ""),
                        "token_accounting": "non-text payload excluded",
                    }
                return {str(key): sanitize(child) for key, child in item.items()}
            if isinstance(item, (list, tuple)):
                return [sanitize(child) for child in item]
            if isinstance(item, (str, int, float, bool)) or item is None:
                return item
            return str(item)

        text = json.dumps(sanitize(value), ensure_ascii=False, sort_keys=True, default=str)
        truncated = len(text) > TOKEN_ACCOUNTING_MAX_CHARS
        if truncated:
            text = text[:TOKEN_ACCOUNTING_MAX_CHARS]
        return text, {
            "media_blocks_excluded": media_blocks,
            "truncated": truncated,
            "counted_chars": len(text),
            "tokenizer": "o200k_base",
        }

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        # FastMCP's own conversion/validation remains authoritative.
        result = await super().call_tool(name, arguments)
        thread_id = self._thread_id(arguments)
        if not thread_id or thread_id == "default":
            return result
        try:
            if name != "thread_goal":
                goal = GPT_STORE.claim_goal_reminder(
                    thread_id,
                    interval_seconds=GOAL_REMINDER_SECONDS,
                )
                if goal is not None:
                    result = _append_goal_context(result, goal)
        except Exception:
            # Goal reminders are persistence aids and must never break a tool.
            pass
        try:
            call_text, call_metadata = self._text_for_token_accounting(
                {"name": name, "arguments": arguments}
            )
            result_text, result_metadata = self._text_for_token_accounting(result)
            _record_usage_event(
                thread_id,
                event_type="mcp_tool_loop",
                source="proxy_estimate",
                input_tokens=_estimate_tokens(result_text),
                output_tokens=_estimate_tokens(call_text),
                is_exact=False,
                request_id=f"mcp-{_now_ms()}-{uuid.uuid4().hex[:12]}",
                metadata={
                    "tool_name": name,
                    "output": call_metadata,
                    "input": result_metadata,
                    "meaning": {
                        "output_tokens": "estimated text emitted by the model as this MCP tool call",
                        "input_tokens": "estimated textual tool result available to the next model turn",
                    },
                },
            )
        except Exception:
            # Token accounting is best-effort diagnostics, never an execution gate.
            pass
        return result


mcp = AccountingFastMCP(
    "Terminal GPT Experimental",
    instructions="Startup context is initialized after tool registration.",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=(os.environ.get("MCP_DNS_REBINDING_PROTECTION", "0") == "1")
    ),
)

@dataclass
class SessionState:
    session_id: str
    cwd: Path
    env: dict[str, str] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    command_count: int = 0
    context_fingerprints: dict[str, str] = field(default_factory=dict)
    bootstrapped_at: float | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

@dataclass
class ProcessState:
    process_id: str
    session_id: str
    command: str
    cwd: Path
    process: asyncio.subprocess.Process
    started_at: float = field(default_factory=time.time)
    stdout_lines: deque[str] = field(default_factory=lambda: deque(maxlen=PROCESS_BUFFER_LINES))
    stderr_lines: deque[str] = field(default_factory=lambda: deque(maxlen=PROCESS_BUFFER_LINES))
    stdout_closed: bool = False
    stderr_closed: bool = False
    unit_name: str | None = None


@dataclass
class StreamCapture:
    """Bound output in RAM; spill only large output while preserving a useful tail."""
    request_id: str
    label: str
    memory_limit: int
    _chunks: list[str] = field(default_factory=list)
    _chars: int = 0
    _total: int = 0
    _tail: deque[str] = field(default_factory=deque)
    _tail_chars: int = 0
    _path: str | None = None
    _file: Any = None

    def append(self, value: str) -> None:
        if not value:
            return
        self._total += len(value)
        self._tail.append(value)
        self._tail_chars += len(value)
        while self._tail and self._tail_chars > self.memory_limit:
            removed = self._tail.popleft()
            self._tail_chars -= len(removed)
        if self._file is None and self._chars + len(value) <= self.memory_limit:
            self._chunks.append(value)
            self._chars += len(value)
            return
        if self._file is None:
            self._path = _write_log_file(self.request_id, self.label.lower(), "".join(self._chunks))
            self._file = open(self._path, "a", encoding="utf-8", errors="replace")
            self._chunks.clear()
            self._chars = 0
        self._file.write(value)

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def text(self, max_chars: int) -> tuple[str, str | None]:
        self.close()
        if self._path is None:
            return "".join(self._chunks), None
        kept = "".join(self._tail)[-max(1, max_chars):]
        return (f"[output streamed to disk: kept last {min(len(kept), max_chars)} of {self._total} chars; full {self.label} saved to {self._path}]\n{kept}", self._path)


@dataclass
class McpActorRequest:
    operation: str
    tool_name: str
    arguments: dict[str, Any] | None
    meta: dict[str, Any] | None
    elicitation_callback: Callable[[Any], Awaitable[Any]] | None
    future: asyncio.Future[Any]


@dataclass
class PersistentMcpConnection:
    key: tuple[str, str]
    config_fingerprint: str
    queue: asyncio.Queue[McpActorRequest | None]
    ready: asyncio.Future[None]
    task: asyncio.Task[None] | None = None
    exclusive_resource: str | None = None
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    active_calls: int = 0
    closed: bool = False
    failure: BaseException | None = None


sessions: dict[str, SessionState] = {"default": SessionState("default", WORKSPACE_DIR)}
processes: dict[str, ProcessState] = {}
state_lock = asyncio.Lock()
agent_state_lock = asyncio.Lock()
mcp_connections: dict[tuple[str, str], PersistentMcpConnection] = {}
mcp_connection_key_locks: dict[tuple[str, str], asyncio.Lock] = {}
mcp_resource_owners: dict[str, tuple[str, str]] = {}
mcp_connections_lock = asyncio.Lock()


class McpResourceBusyError(RuntimeError):
    pass


class BearerAuthMiddleware:
    def __init__(self, app: Any, token: str):
        self.app = app
        self.token = token

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        path = str(scope.get("path", ""))
        if scope.get("type") != "http" or path == "/dashboard" or path.startswith("/dashboard/"):
            await self.app(scope, receive, send)
            return

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        supplied = headers.get("authorization", "")
        expected = f"Bearer {self.token}"
        if secrets.compare_digest(supplied, expected):
            await self.app(scope, receive, send)
            return

        body = b'{"error":"unauthorized"}'
        await send({
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
                (b"www-authenticate", b"Bearer"),
            ],
        })
        await send({"type": "http.response.body", "body": body})


class PrivacyCloneProxyMiddleware:
    """Stream one dedicated public path to the offline privacy terminal."""

    _HOP_BY_HOP_HEADERS = {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    }

    def __init__(self, app: Any, path: str, upstream: str):
        self.app = app
        self.path = "/" + path.strip("/")
        self.upstream = upstream.rstrip("/")

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        request_path = str(scope.get("path", "")).rstrip("/") or "/"
        if scope.get("type") != "http" or request_path != self.path:
            await self.app(scope, receive, send)
            return

        body_chunks: list[bytes] = []
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "http.disconnect":
                return
            if message_type != "http.request":
                continue
            body_chunks.append(message.get("body", b""))
            if not message.get("more_body", False):
                break

        query = scope.get("query_string", b"").decode("latin-1")
        upstream_url = self.upstream
        if query:
            upstream_url += ("&" if "?" in upstream_url else "?") + query

        request_headers = [
            (key.decode("latin-1"), value.decode("latin-1"))
            for key, value in scope.get("headers", [])
            if key.decode("latin-1").lower() not in self._HOP_BY_HOP_HEADERS | {"host", "content-length"}
        ]

        response_started = False
        try:
            import httpx

            async with httpx.AsyncClient(timeout=None, follow_redirects=False, trust_env=False) as client:
                async with client.stream(
                    str(scope.get("method", "GET")),
                    upstream_url,
                    headers=request_headers,
                    content=b"".join(body_chunks),
                ) as response:
                    response_headers = [
                        (key, value)
                        for key, value in response.headers.raw
                        if key.decode("latin-1").lower() not in self._HOP_BY_HOP_HEADERS | {"date", "server"}
                    ]
                    await send({
                        "type": "http.response.start",
                        "status": response.status_code,
                        "headers": response_headers,
                    })
                    response_started = True
                    async for chunk in response.aiter_raw():
                        await send({"type": "http.response.body", "body": chunk, "more_body": True})
                    await send({"type": "http.response.body", "body": b"", "more_body": False})
        except Exception as exc:
            if response_started:
                with contextlib.suppress(Exception):
                    await send({"type": "http.response.body", "body": b"", "more_body": False})
                return
            body = json.dumps({
                "error": "privacy terminal unavailable",
                "type": type(exc).__name__,
            }).encode("utf-8")
            await send({
                "type": "http.response.start",
                "status": 502,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            })
            await send({"type": "http.response.body", "body": body})


def _hidden_tool() -> Any:
    """Keep retired helpers callable internally without publishing MCP tools."""
    return lambda function: function

def _now_ms() -> int:
    return int(time.time() * 1000)

def _normalize_session_id(session_id: str | None) -> str:
    normalized = (session_id or "default").strip()
    return (normalized or "default")[:128]


def _gpt_home() -> Path:
    return GPT_STORE.home()


def _gpt_agents_path() -> Path:
    return GPT_STORE.agents_path()


def _thread_db_path() -> Path:
    return GPT_STORE.db_path()


def _ensure_gpt_layout() -> Path:
    return GPT_STORE.ensure_layout()


def _estimate_tokens(text: str) -> int:
    return GPT_STORE.estimate_tokens(text)


def _upsert_thread_record(thread_id: str, cwd: Path, fingerprint: str = "", loaded: bool = False) -> None:
    GPT_STORE.upsert_thread(thread_id, cwd, fingerprint, loaded=loaded)


def _record_usage_event(
    thread_id: str,
    *,
    event_type: str,
    source: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cached_input_tokens: int = 0,
    is_exact: bool = False,
    model: str = "",
    request_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> int:
    return GPT_STORE.record_usage(
        thread_id,
        event_type=event_type,
        source=source,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cached_input_tokens=cached_input_tokens,
        is_exact=is_exact,
        model=model,
        request_id=request_id,
        metadata=metadata,
    )


def _usage_summary(thread_id: str | None = None, limit: int = 100) -> dict[str, Any]:
    return GPT_STORE.usage_summary(thread_id, limit)


def _format_timestamp(value: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))

async def _get_session(session_id: str | None = None) -> SessionState:
    normalized = _normalize_session_id(session_id)
    async with state_lock:
        session = sessions.get(normalized)
        if session is None:
            session = SessionState(normalized, WORKSPACE_DIR)
            sessions[normalized] = session
        return session

def _resolve_cwd(cwd: str | None, fallback: Path) -> Path:
    candidate = Path(cwd).expanduser() if cwd else fallback
    candidate = (fallback / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    if not candidate.exists():
        raise FileNotFoundError(f"Working directory does not exist: {candidate}")
    if not candidate.is_dir():
        raise NotADirectoryError(f"Working directory is not a directory: {candidate}")
    return candidate

def _resolve_path(path: str, session: SessionState, cwd: str | None = None) -> Path:
    base = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd
    candidate = Path(path).expanduser()
    return (base / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()

def _line_slice(text: str, start_line: int, max_lines: int) -> tuple[list[str], int, int]:
    lines = text.splitlines()
    start = max(1, start_line)
    limit = max(1, max_lines)
    begin = start - 1
    end = min(len(lines), begin + limit)
    return lines[begin:end], len(lines), start

def _format_file_lines(lines: list[str], start_line: int) -> str:
    return "\n".join(f"{idx:>6} | {line}" for idx, line in enumerate(lines, start=start_line))

def _build_env(session: SessionState, env: dict[str, str] | None = None) -> dict[str, str]:
    merged = os.environ.copy()
    merged.update(session.env)
    if env:
        merged.update({str(k): str(v) for k, v in env.items()})
    return merged

def _write_log_file(request_id: str, name: str, content: str) -> str:
    path = LOG_DIR / f"{request_id}.{name}.log"
    path.write_text(content, encoding="utf-8", errors="replace")
    return str(path)

def _truncate_output(label: str, content: str, request_id: str, max_chars: int) -> tuple[str, str | None]:
    if max_chars <= 0 or len(content) <= max_chars:
        return content, None
    log_path = _write_log_file(request_id, label.lower(), content)
    kept = content[-max_chars:]
    return f"[output truncated: kept last {max_chars} of {len(content)} chars; full {label} saved to {log_path}]\n{kept}", log_path

def _strip_pwd_marker(stdout: str, marker: str) -> tuple[str, str | None]:
    new_cwd = None
    cleaned = []
    prefix = f"{marker}:"
    for line in stdout.splitlines():
        if line.startswith(prefix):
            new_cwd = line[len(prefix):].strip()
        else:
            cleaned.append(line)
    return "\n".join(cleaned).rstrip(), new_cwd

async def _terminate_process_group(process: asyncio.subprocess.Process, grace_seconds: float = 2.0) -> None:
    if process.returncode is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except Exception:
        try:
            process.terminate()
        except ProcessLookupError:
            return
    try:
        await asyncio.wait_for(process.wait(), timeout=grace_seconds)
        return
    except asyncio.TimeoutError:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except Exception:
        try:
            process.kill()
        except ProcessLookupError:
            return
    await process.wait()


def _safe_unit_component(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-.")[:48] or "task"


def _systemd_workload_prefix(session_id: str, request_id: str) -> tuple[list[str], str | None]:
    """Launch workload commands outside the MCP service cgroup when supported."""
    if WORKLOAD_ISOLATION == "off" or shutil.which("systemd-run") is None:
        return [], None
    unit = f"mcp-workload-{_safe_unit_component(session_id)}-{_safe_unit_component(request_id)}.scope"
    args = ["systemd-run", "--user", "--scope", "--quiet", "--collect", f"--unit={unit}", "--slice=mcp-workloads.slice"]
    if WORKLOAD_MEMORY_MAX:
        args.extend(["-p", f"MemoryMax={WORKLOAD_MEMORY_MAX}"])
    if WORKLOAD_CPU_QUOTA:
        args.extend(["-p", f"CPUQuota={WORKLOAD_CPU_QUOTA}"])
    if WORKLOAD_TASKS_MAX:
        args.extend(["-p", f"TasksMax={WORKLOAD_TASKS_MAX}"])
    return args + ["--"], unit


async def _spawn_workload(
    args: list[str], *, shell: bool, session: SessionState, request_id: str,
    cwd: Path, env: dict[str, str] | None, stdin: int | None = None,
) -> tuple[asyncio.subprocess.Process, str | None]:
    prefix, unit = _systemd_workload_prefix(session.session_id, request_id)
    if prefix:
        try:
            return await asyncio.create_subprocess_exec(
                *prefix, *args, cwd=str(cwd), env=_build_env(session, env), stdin=stdin,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                start_new_session=True,
            ), unit
        except (FileNotFoundError, OSError):
            if WORKLOAD_ISOLATION == "required":
                raise
    if WORKLOAD_ISOLATION == "required":
        raise RuntimeError("MCP workload isolation is required but systemd-run is unavailable")
    if shell:
        return await asyncio.create_subprocess_shell(
            args[-1], cwd=str(cwd), env=_build_env(session, env), stdin=stdin,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            executable=SHELL, start_new_session=True,
        ), None
    return await asyncio.create_subprocess_exec(
        *args, cwd=str(cwd), env=_build_env(session, env), stdin=stdin,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    ), None


async def _stop_workload(process: asyncio.subprocess.Process, unit_name: str | None) -> None:
    if unit_name and shutil.which("systemctl"):
        control = await asyncio.create_subprocess_exec("systemctl", "--user", "stop", unit_name, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await control.wait()
    await _terminate_process_group(process)


async def _capture_stream(stream: asyncio.StreamReader | None, capture: StreamCapture) -> None:
    if stream is None:
        return
    while chunk := await stream.read(16 * 1024):
        capture.append(chunk.decode("utf-8", errors="replace"))


async def _collect_bounded_output(process: asyncio.subprocess.Process, request_id: str, timeout: int | None, unit_name: str | None = None) -> tuple[StreamCapture, StreamCapture, bool]:
    stdout = StreamCapture(request_id, "STDOUT", CAPTURE_MEMORY_CHARS)
    stderr = StreamCapture(request_id, "STDERR", CAPTURE_MEMORY_CHARS)
    drains = [asyncio.create_task(_capture_stream(process.stdout, stdout)), asyncio.create_task(_capture_stream(process.stderr, stderr))]
    timed_out = False
    try:
        if timeout and timeout > 0:
            await asyncio.wait_for(process.wait(), timeout=timeout)
        else:
            await process.wait()
    except asyncio.TimeoutError:
        timed_out = True
    finally:
        if timed_out:
            await _stop_workload(process, unit_name)
        await asyncio.gather(*drains, return_exceptions=True)
        stdout.close()
        stderr.close()
    return stdout, stderr, timed_out

async def _drain_stream(stream: asyncio.StreamReader | None, sink: deque[str], close_attr: str, state: ProcessState) -> None:
    if stream is None:
        setattr(state, close_attr, True)
        return
    try:
        while True:
            chunk = await stream.readline()
            if not chunk:
                break
            sink.append(chunk.decode("utf-8", errors="replace").rstrip("\n"))
    finally:
        setattr(state, close_attr, True)

def _process_status(state: ProcessState) -> str:
    code = state.process.returncode
    return "running" if code is None else f"exited({code})"

def _format_process(state: ProcessState, max_lines: int = 200) -> str:
    stdout = list(state.stdout_lines)[-max_lines:]
    stderr = list(state.stderr_lines)[-max_lines:]
    parts = [
        f"Process ID: {state.process_id}",
        f"Session ID: {state.session_id}",
        f"Status: {_process_status(state)}",
        *([f"Exit Code: {state.process.returncode}"] if state.process.returncode is not None else []),
        f"PID: {state.process.pid}",
        f"Command: {state.command}",
        f"Working Directory: {state.cwd}",
        f"Started At: {_format_timestamp(state.started_at)}",
    ]
    if stdout:
        parts.append("STDOUT:\n" + "\n".join(stdout))
    if stderr:
        parts.append("STDERR:\n" + "\n".join(stderr))
    return "\n\n".join(parts)

def _tmux_name(name: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in (name or "default"))
    return (safe or "default")[:80]

def _run_tmux(args: list[str], timeout: int = 10) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["tmux", *args], text=True, capture_output=True, timeout=timeout)

def _tmux_available() -> bool:
    return shutil.which("tmux") is not None

def _tmux_error() -> str:
    return "Error: tmux is not installed or not available in PATH. Install tmux for reconnect-safe persistent terminal sessions."

def _utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _slug(value: str, fallback: str = "default", limit: int = 80) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in "._-" else "-" for ch in (value or fallback).strip())
    cleaned = "-".join(part for part in cleaned.split("-") if part)
    return (cleaned or fallback)[:limit]

def _read_text_cap(path: Path, max_chars: int = 6000) -> str:
    if not path.exists() or not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8", errors="replace")
    if len(text) <= max_chars:
        return text.rstrip()
    head = text[: max_chars // 2]
    tail = text[-(max_chars // 2) :]
    return f"{head}\n\n[...truncated {len(text) - len(head) - len(tail)} chars...]\n\n{tail}".rstrip()

def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}.{uuid.uuid4().hex[:8]}")
    tmp.write_text(content, encoding="utf-8", errors="replace")
    tmp.replace(path)

@contextlib.contextmanager
def _file_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+")
    try:
        try:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        except Exception:
            pass
        yield
    finally:
        try:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        handle.close()

def _agent_id(agent_id: str | None, session_id: str | None) -> str:
    return _slug(agent_id or session_id or "default", "default")

def _project_root(session: SessionState, project_root: str | None = None, cwd: str | None = None) -> Path:
    if project_root:
        return _resolve_cwd(project_root, session.cwd)
    if cwd:
        return _resolve_cwd(cwd, session.cwd)
    return session.cwd

def _agent_paths(root: Path, agent_id: str) -> dict[str, Path]:
    base = root / AGENT_DIR_NAME
    agent_dir = base / "agents" / _slug(agent_id)
    return {
        "root": root,
        "base": base,
        "project": base / "PROJECT.md",
        "queue": base / "QUEUE.md",
        "agents": base / "agents",
        "agent_dir": agent_dir,
        "task": agent_dir / "TASK.md",
        "handoff": agent_dir / "HANDOFF.md",
        "state": agent_dir / "STATE.json",
        "lock": base / ".lock",
        "agents_md": root / ".GPT" / "AGENTS.md",
    }

def _ensure_agent_files(paths: dict[str, Path], agent_id: str) -> None:
    paths["base"].mkdir(parents=True, exist_ok=True)
    paths["agent_dir"].mkdir(parents=True, exist_ok=True)
    if not paths["project"].exists():
        _atomic_write(paths["project"], "# Project\n\nGoal:\n- Not set yet.\n\nRules:\n- Keep work scoped to the active task.\n- Update handoff before stopping.\n")
    if not paths["queue"].exists():
        _atomic_write(paths["queue"], "# Queue\n\n| id | owner | status | title |\n| --- | --- | --- | --- |\n")
    if not paths["task"].exists():
        _atomic_write(paths["task"], f"# Task for {agent_id}\n\nStatus: idle\n\nActive Task:\n- None\n\nChecklist:\n- [ ] Pick or claim a task.\n")
    if not paths["handoff"].exists():
        _atomic_write(paths["handoff"], f"# Handoff for {agent_id}\n\nNo handoff yet.\n")
    if not paths["state"].exists():
        state = {
            "agent_id": agent_id,
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "turn_count": 0,
            "last_user_request": "",
            "last_classification": "",
            "last_context_hash": "",
            "active_task_id": "",
        }
        _atomic_write(paths["state"], json.dumps(state, indent=2, sort_keys=True) + "\n")

def _load_agent_state(paths: dict[str, Path], agent_id: str) -> dict:
    try:
        state = json.loads(paths["state"].read_text(encoding="utf-8"))
        if not isinstance(state, dict):
            raise ValueError("STATE.json is not an object")
    except Exception:
        state = {"agent_id": agent_id, "created_at": _utc_now(), "turn_count": 0}
    state.setdefault("agent_id", agent_id)
    state.setdefault("turn_count", 0)
    state.setdefault("active_task_id", "")
    return state

def _save_agent_state(paths: dict[str, Path], state: dict) -> None:
    state["updated_at"] = _utc_now()
    _atomic_write(paths["state"], json.dumps(state, indent=2, sort_keys=True) + "\n")

def _context_hash(parts: list[str]) -> str:
    digest = hashlib.sha256()
    for part in parts:
        digest.update(part.encode("utf-8", errors="replace"))
        digest.update(b"\0")
    return digest.hexdigest()[:16]

def _word_set(text: str) -> set[str]:
    stop = {"the", "and", "for", "this", "that", "with", "from", "your", "you", "are", "was", "were", "have", "has", "had", "into", "then", "than", "what", "when", "where", "how", "why", "can", "could", "should", "would"}
    out = set()
    token = []
    for ch in text.lower():
        if ch.isalnum() or ch in "_-":
            token.append(ch)
        else:
            if len(token) >= 3:
                word = "".join(token)
                if word not in stop:
                    out.add(word)
            token = []
    if len(token) >= 3:
        word = "".join(token)
        if word not in stop:
            out.add(word)
    return out

def _classify_request(user_request: str, project_text: str, task_text: str, handoff_text: str) -> tuple[str, str]:
    req = user_request.strip().lower()
    if not req:
        return "meta_question", "Empty request."
    goal_change_markers = ["forget", "new goal", "replace the goal", "switch goal", "stop current", "instead of", "ignore previous", "start over", "different project"]
    if any(marker in req for marker in goal_change_markers):
        return "goal_change", "Request contains explicit goal-change wording."
    meta_markers = ["what is", "what are", "explain", "why", "how should", "structure", "architecture", "for record", "tell me"]
    action_markers = ["change", "edit", "fix", "run", "create", "replace", "push", "commit", "merge", "deploy", "restart", "install", "remove", "add"]
    if any(marker in req for marker in meta_markers) and not any(marker in req for marker in action_markers):
        return "meta_question", "Looks like a planning/explanation request, not execution work."
    continuation_markers = ["continue", "retry", "again", "that", "it", "this", "same", "now", "next", "then", "after that", "fix it"]
    if any(marker in req for marker in continuation_markers):
        return "continue_current", "Request uses continuation wording."
    context_words = _word_set(project_text + "\n" + task_text + "\n" + handoff_text)
    request_words = _word_set(user_request)
    overlap = len(context_words & request_words)
    if overlap >= 3:
        return "new_subtask_same_goal", f"Request overlaps active context with {overlap} keywords."
    if overlap >= 1:
        return "continue_current", f"Request has small overlap with active context ({overlap} keyword)."
    return "side_task", "Request has little overlap with current goal/task."

def _build_context_capsule(
    agent_id: str,
    root: Path,
    classification: str,
    reason: str,
    agents_md: str,
    project_text: str,
    queue_text: str,
    task_text: str,
    handoff_text: str,
    state: dict,
    changed: bool,
) -> str:
    parts = [
        "[Agent Context Capsule]",
        f"project_root: {root}",
        f"agent_id: {agent_id}",
        f"classification: {classification}",
        f"classification_reason: {reason}",
        f"context_changed_since_last_turn: {changed}",
        f"context_gate: {'reblock' if changed else 'load'}",
        f"agents_fingerprint: {_context_hash([agents_md])}",
        f"active_task_id: {state.get('active_task_id', '') or 'none'}",
        "",
    ]
    if agents_md:
        parts.extend(["AGENTS.md:", agents_md, ""])
    parts.extend([
        "PROJECT.md:", project_text or "(empty)", "",
        "QUEUE.md:", queue_text or "(empty)", "",
        "TASK.md:", task_text or "(empty)", "",
        "HANDOFF.md:", handoff_text or "(empty)",
    ])
    text = "\n".join(parts).rstrip()
    if len(text) > AGENT_CONTEXT_MAX_CHARS:
        section_start = text.find("\n\nAGENTS.md:")
        if section_start < 0:
            section_start = text.find("\n\nPROJECT.md:")
        header = text[:section_start] if section_start >= 0 else ""
        body = text[section_start:] if section_start >= 0 else text
        remaining = max(80, AGENT_CONTEXT_MAX_CHARS - len(header))
        keep_head = remaining // 2
        keep_tail = remaining - keep_head
        text = header + body[:keep_head] + f"\n\n[...context capsule truncated to {AGENT_CONTEXT_MAX_CHARS} chars...]\n\n" + body[-keep_tail:]
    return text


def _find_project_root(cwd: Path) -> Path:
    cwd = cwd.resolve()
    for candidate in (cwd, *cwd.parents):
        if (candidate / ".git").exists():
            return candidate
    return cwd


def _applicable_agents_files(cwd: Path) -> tuple[Path, list[Path]]:
    return GPT_STORE.applicable_agents_files(cwd, _find_project_root)


def _agents_snapshot(cwd: Path) -> tuple[Path, list[tuple[Path, str]], str]:
    return GPT_STORE.agents_snapshot(cwd, _find_project_root)


def _context_key(cwd: Path) -> str:
    return str(cwd.resolve())


def _context_gate_error(
    root: Path,
    cwd: Path,
    rows: list[tuple[Path, str]],
    session_id: str,
) -> str:
    listed = "\n".join(f"- {path}" for path, _ in rows) or "- none"
    identity_note = (
        "The shared session_id='default' is intentionally rejected by this experimental server.\n"
        "Choose a unique stable thread_id, call bootstrap_thread(thread_id=...), and reuse it as session_id.\n\n"
        if session_id == "default"
        else ""
    )
    return (
        "GPT thread context has not been loaded, or an applicable .GPT/AGENTS.md changed.\n"
        "No action was executed.\n\n"
        + identity_note
        + "Call bootstrap_thread or get_thread_context with this cwd, follow every returned instruction, then retry.\n\n"
        + f"Thread/Session ID: {session_id}\nProject Root: {root}\nWorking Directory: {cwd}\nApplicable Files:\n{listed}"
    )


def _require_project_context(session: SessionState, cwd: Path) -> str | None:
    cwd = cwd.resolve()
    root, rows, fingerprint = _agents_snapshot(cwd)
    key = _context_key(cwd)
    if session.session_id == "default":
        return _context_gate_error(root, cwd, rows, session.session_id)
    if session.context_fingerprints.get(key) != fingerprint:
        return _context_gate_error(root, cwd, rows, session.session_id)
    return None


def _operation_cwd(session: SessionState, cwd: str | None) -> Path:
    return _resolve_cwd(cwd, session.cwd) if cwd else session.cwd.resolve()


def _limit_text(value: str, max_chars: int, label: str = "output") -> str:
    if max_chars <= 0 or len(value) <= max_chars:
        return value
    return value[:max_chars] + f"\n[...{label} truncated to {max_chars} of {len(value)} chars...]"


def _frontmatter_fields(content: str) -> dict[str, str]:
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}
    fields: dict[str, str] = {}
    for line in lines[1:]:
        if line.strip() == "---":
            break
        if ":" in line:
            key, value = line.split(":", 1)
            if key.strip() in {"name", "description"}:
                fields[key.strip()] = value.strip().strip('"\'')
    return fields


def _skill_summary(content: str, fields: dict[str, str]) -> str:
    if fields.get("description"):
        return fields["description"][:300]
    in_frontmatter = bool(content.startswith("---"))
    frontmatter_done = not in_frontmatter
    for line in content.splitlines():
        stripped = line.strip()
        if in_frontmatter and stripped == "---":
            if frontmatter_done:
                continue
            frontmatter_done = True
            continue
        if not frontmatter_done or not stripped or stripped.startswith("#"):
            continue
        return stripped[:300]
    return ""


def _skill_roots(root: Path) -> list[tuple[str, Path]]:
    candidates = [
        ("project-gpt", root / ".GPT" / "skills"),
        ("user-gpt", _gpt_home() / "skills"),
        ("project-codex", root / ".codex" / "skills"),
        ("project-agents", root / ".agents" / "skills"),
        ("project-skills", root / "skills"),
        ("project-skill", root / "skill"),
        ("user-codex", Path.home() / ".codex" / "skills"),
        ("user-agents", Path.home() / ".agents" / "skills"),
        ("grok-user", Path.home() / ".grok" / "skills"),
        ("grok-bundled", Path.home() / ".grok" / "bundled" / "skills"),
        ("gemini-antigravity", Path.home() / ".gemini" / "antigravity" / "skills"),
        ("gemini-antigravity-cli", Path.home() / ".gemini" / "antigravity-cli" / "skills"),
    ]
    seen: set[Path] = set()
    output: list[tuple[str, Path]] = []
    for source, candidate in candidates:
        candidate = candidate.expanduser().resolve()
        if candidate in seen or not candidate.is_dir():
            continue
        seen.add(candidate)
        output.append((source, candidate))
    return output


def _discover_skills(root: Path, limit: int = 1000) -> list[dict[str, Any]]:
    chosen: dict[str, dict[str, Any]] = {}
    for source, skill_root in _skill_roots(root):
        try:
            paths = sorted(skill_root.rglob("SKILL.md"), key=lambda item: str(item))
        except OSError:
            continue
        for item in paths[:limit]:
            if not item.is_file():
                continue
            try:
                content = item.read_text(encoding="utf-8", errors="replace")
            except OSError:
                continue
            fields = _frontmatter_fields(content)
            name = (fields.get("name") or item.parent.name).strip()
            normalized = name.casefold()
            if normalized in chosen:
                continue
            chosen[normalized] = {
                "name": name,
                "source": source,
                "description": _skill_summary(content, fields),
                "path": str(item),
                "content": content,
            }
    return sorted(chosen.values(), key=lambda row: row["name"].casefold())


@mcp.tool()
async def local_skills(
    action: str = "list",
    name: str = "",
    query: str = "",
    session_id: str = "default",
    cwd: str | None = None,
    max_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
) -> str:
    """Dynamically list, read, or search project and machine-local SKILL.md files."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    root = _find_project_root(run_cwd)
    skills = _discover_skills(root)
    action = action.strip().lower()
    if action == "list":
        public = [{key: row[key] for key in ("name", "source", "description", "path")} for row in skills]
        return _limit_text(json.dumps({"project_root": str(root), "skills": public}, indent=2), max_chars, "skills list")
    if action == "read":
        needle = (name or query).strip()
        if not needle:
            return "Error: name is required for action=read."
        exact = [row for row in skills if row["name"].casefold() == needle.casefold() or row["path"] == str(Path(needle).expanduser())]
        matches = exact or [row for row in skills if needle.casefold() in row["name"].casefold()]
        if not matches:
            return f"Error: no skill matched: {needle}"
        if len(matches) > 1:
            return "Error: skill name is ambiguous:\n" + "\n".join(f"- {row['name']} ({row['path']})" for row in matches[:20])
        row = matches[0]
        return _limit_text(json.dumps({"name": row["name"], "source": row["source"], "path": row["path"], "content": row["content"]}, indent=2), max_chars, "skill")
    if action == "search":
        needle = query.strip().casefold()
        if not needle:
            return "Error: query is required for action=search."
        matches: list[dict[str, str]] = []
        for row in skills:
            haystack = "\n".join([row["name"], row["description"], row["path"], row["content"]])
            index = haystack.casefold().find(needle)
            if index < 0:
                continue
            start = max(0, index - 160)
            end = min(len(haystack), index + len(needle) + 240)
            matches.append({
                "name": row["name"], "source": row["source"], "path": row["path"],
                "description": row["description"], "snippet": haystack[start:end].replace("\n", " "),
            })
        return _limit_text(json.dumps({"query": query, "matches": matches}, indent=2), max_chars, "skills search")
    return "Error: action must be list, read, or search."


def _read_mcp_config(path: Path) -> dict[str, Any]:
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
        parsed = tomllib.loads(content) if path.suffix.lower() == ".toml" else json.loads(content)
        return parsed if isinstance(parsed, dict) else {}
    except (OSError, ValueError, tomllib.TOMLDecodeError):
        return {}


def _extract_mcp_server_map(raw: dict[str, Any]) -> dict[str, Any]:
    for key in ("mcp_servers", "mcpServers", "servers"):
        value = raw.get(key)
        if isinstance(value, dict):
            return value
    mcp_section = raw.get("mcp")
    if isinstance(mcp_section, dict):
        for key in ("servers", "mcpServers", "mcp_servers"):
            value = mcp_section.get(key)
            if isinstance(value, dict):
                return value
    return {}


def _mcp_config_files(root: Path) -> list[Path]:
    home = Path.home()
    candidates = [
        home / ".codex" / "config.toml",
        home / ".gemini" / "config" / "mcp_config.json",
        home / ".config" / "Claude" / "claude_desktop_config.json",
        root / ".codex" / "config.toml",
        root / ".mcp.json",
        root / "mcp.json",
        root / ".vscode" / "mcp.json",
    ]
    seen: set[Path] = set()
    return [item.resolve() for item in candidates if item.is_file() and not (item.resolve() in seen or seen.add(item.resolve()))]


def _normalize_mcp_config(name: str, raw: Any, source: Path) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    if raw.get("enabled") is False or raw.get("disabled") is True:
        return None
    command = raw.get("command")
    args = raw.get("args", raw.get("arguments", []))
    url = raw.get("url", raw.get("serverUrl", raw.get("endpoint")))
    transport = str(raw.get("transport", raw.get("type", ""))).lower().replace("_", "-")
    if not transport:
        transport = "streamable-http" if url else "stdio"
    if transport in {"http", "streamablehttp"}:
        transport = "streamable-http"
    if transport in {"server-sent-events", "eventsource"}:
        transport = "sse"
    config = {
        "name": name,
        "source": str(source),
        "config_dir": str(source.parent),
        "transport": transport,
        "command": str(command) if isinstance(command, str) else "",
        "args": [str(value) for value in args] if isinstance(args, list) else [],
        "url": str(url) if isinstance(url, str) else "",
        "env": raw.get("env") if isinstance(raw.get("env"), dict) else {},
        "headers": raw.get("headers") if isinstance(raw.get("headers"), dict) else {},
        "cwd": str(raw.get("cwd", "")) if raw.get("cwd") is not None else "",
    }
    if not config["command"] and not config["url"]:
        return None
    return config


def _discover_mcp_servers(root: Path) -> dict[str, dict[str, Any]]:
    servers: dict[str, dict[str, Any]] = {}
    for source in _mcp_config_files(root):
        mapping = _extract_mcp_server_map(_read_mcp_config(source))
        for name, raw in mapping.items():
            normalized = _normalize_mcp_config(str(name), raw, source)
            if normalized is not None:
                servers[str(name)] = normalized
            elif str(name) in servers and isinstance(raw, dict) and (raw.get("enabled") is False or raw.get("disabled") is True):
                servers.pop(str(name), None)
    return servers


def _expand_config_string(value: str) -> str:
    def vscode_env(match: re.Match[str]) -> str:
        return os.environ.get(match.group(1), "")
    value = re.sub(r"\$\{env:([^}]+)\}", vscode_env, value)
    return os.path.expandvars(value)


def _runtime_mcp_config(config: dict[str, Any]) -> dict[str, Any]:
    config_dir = Path(config["config_dir"])
    command = _expand_config_string(config.get("command", ""))
    if command and "/" in command and not Path(command).expanduser().is_absolute():
        command = str((config_dir / command).resolve())
    cwd = _expand_config_string(config.get("cwd", ""))
    if cwd and not Path(cwd).expanduser().is_absolute():
        cwd = str((config_dir / cwd).resolve())
    return {
        **config,
        "command": command,
        "args": [_expand_config_string(value) for value in config.get("args", [])],
        "url": _expand_config_string(config.get("url", "")),
        "cwd": cwd or None,
        "env": {str(key): _expand_config_string(str(value)) for key, value in config.get("env", {}).items()},
        "headers": {str(key): _expand_config_string(str(value)) for key, value in config.get("headers", {}).items()},
    }


def _safe_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
        host = parsed.hostname or ""
        if parsed.port:
            host += f":{parsed.port}"
        return urlunsplit((parsed.scheme, host, parsed.path, "", ""))
    except Exception:
        return "configured-url"


def _is_recursive_terminal_server(name: str, config: dict[str, Any]) -> bool:
    target = " ".join([
        name, config.get("command", ""), " ".join(config.get("args", [])), config.get("url", "")
    ]).casefold()
    return "terminal_mcp.py" in target or "/lead/terminal" in target or ("terminal" in name.casefold() and "mcp" in name.casefold())


def _public_mcp_summary(name: str, config: dict[str, Any]) -> dict[str, str]:
    return {
        "name": name,
        "transport": config["transport"],
        "endpoint": _safe_url(config["url"]) if config.get("url") else Path(config.get("command", "")).name,
        "source": config["source"],
        "status": "blocked_recursive" if _is_recursive_terminal_server(name, config) else "configured",
    }


def _mcp_config_fingerprint(config: dict[str, Any]) -> str:
    runtime = _runtime_mcp_config(config)
    payload = json.dumps(runtime, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()


def _mcp_exclusive_resource(config: dict[str, Any]) -> str | None:
    runtime = _runtime_mcp_config(config)
    env = {str(key).upper(): str(value) for key, value in runtime.get("env", {}).items()}
    exact_keys = (
        "SAB_USER_DATA_DIR",
        "USER_DATA_DIR",
        "BROWSER_USER_DATA_DIR",
        "BROWSER_PROFILE",
        "BROWSER_PROFILE_DIR",
        "PROFILE_PATH",
        "PROFILE_DIR",
    )
    value = next((env[key] for key in exact_keys if env.get(key)), "")
    if not value:
        for key, candidate in env.items():
            if candidate and ("USER_DATA_DIR" in key or "PROFILE_DIR" in key or "PROFILE_PATH" in key):
                value = candidate
                break
    if not value:
        return None
    normalized = str(Path(value).expanduser().resolve())
    return hashlib.sha256(normalized.encode("utf-8", errors="replace")).hexdigest()


def _forwardable_mcp_request_meta(
    *,
    fallback_session_id: str | None = None,
) -> dict[str, Any] | None:
    """Copy per-call MCP metadata, optionally adding Node REPL turn identity.

    Progress tokens belong to the current client/server hop. Forwarding one
    without also bridging downstream progress notifications would be misleading.
    Hosts that do not provide Codex turn metadata still need a scoped identity
    for the Chrome bridge, so the Node REPL path can request a synthetic fallback.
    """
    request_meta: Any = None
    try:
        request_meta = mcp.get_context().request_context.meta
    except (LookupError, ValueError):
        pass

    forwarded = (
        request_meta.model_dump(mode="json", by_alias=True, exclude_none=True)
        if request_meta is not None
        else {}
    )
    forwarded.pop("progressToken", None)
    if fallback_session_id and "x-codex-turn-metadata" not in forwarded:
        forwarded["x-codex-turn-metadata"] = {
            "session_id": fallback_session_id,
            "turn_id": f"terminal-mcp-{uuid.uuid4().hex}",
            "thread_id": fallback_session_id,
            "thread_source": "terminal_mcp",
        }
    return forwarded or None


def _normalize_approved_browser_origins(origins: list[str] | None) -> set[str]:
    approved: set[str] = set()
    for raw_origin in origins or []:
        value = raw_origin.strip()
        parsed = urlsplit(value)
        scheme = parsed.scheme.lower()
        if (
            scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(f"invalid approved browser origin: {raw_origin}")
        approved.add(urlunsplit((scheme, parsed.netloc.lower(), "", "", "")))
    return approved


def _browser_origin_elicitation_is_approved(params: Any, approved_origins: set[str]) -> bool:
    if not approved_origins or not hasattr(params, "model_dump"):
        return False
    payload = params.model_dump(mode="json", by_alias=True, exclude_none=True)
    meta = payload.get("_meta")
    if not isinstance(meta, dict):
        return False
    origin = meta.get("origin")
    if not isinstance(origin, str):
        return False
    try:
        normalized_origin = next(iter(_normalize_approved_browser_origins([origin])))
    except (StopIteration, ValueError):
        return False
    return (
        payload.get("mode") == "form"
        and meta.get("codex_approval_kind") == "mcp_tool_call"
        and meta.get("connector_id") == "browser-use"
        and meta.get("tool_name") == "access_browser_origin"
        and normalized_origin in approved_origins
    )


def _mcp_elicitation_relay(
    approved_browser_origins: set[str] | None = None,
) -> Callable[[Any], Awaitable[Any]] | None:
    """Relay downstream prompts, with exact per-call browser-origin consent."""
    approved_origins = approved_browser_origins or set()
    outer_session: Any = None
    outer_supports_elicitation = False
    try:
        context = mcp.get_context()
        outer_session = context.request_context.session
        client_params = getattr(outer_session, "client_params", None)
        capabilities = getattr(client_params, "capabilities", None)
        outer_supports_elicitation = getattr(capabilities, "elicitation", None) is not None
    except (LookupError, ValueError):
        pass

    if not approved_origins and not outer_supports_elicitation:
        return None

    async def relay(params: Any) -> Any:
        if _browser_origin_elicitation_is_approved(params, approved_origins):
            return mcp_types.ElicitResult(action="accept", content={})
        if not outer_supports_elicitation or outer_session is None:
            return mcp_types.ErrorData(
                code=mcp_types.INVALID_REQUEST,
                message="The outer MCP client does not support this elicitation request.",
            )
        try:
            return await outer_session.send_request(
                mcp_types.ServerRequest(mcp_types.ElicitRequest(params=params)),
                mcp_types.ElicitResult,
            )
        except Exception as exc:
            return mcp_types.ErrorData(
                code=mcp_types.INTERNAL_ERROR,
                message=f"Failed to relay downstream elicitation: {type(exc).__name__}",
            )

    return relay


async def _mcp_connection_actor(
    connection: PersistentMcpConnection,
    config: dict[str, Any],
) -> None:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    from mcp.client.stdio import StdioServerParameters, stdio_client
    from mcp.client.streamable_http import streamable_http_client

    runtime = _runtime_mcp_config(config)
    stack = contextlib.AsyncExitStack()
    client: Any = None
    current_request: McpActorRequest | None = None

    async def relay_elicitation(_context: Any, params: Any) -> Any:
        callback = current_request.elicitation_callback if current_request is not None else None
        if callback is None:
            return mcp_types.ErrorData(
                code=mcp_types.INVALID_REQUEST,
                message="The outer MCP client does not support elicitation for this request.",
            )
        return await callback(params)

    try:
        if runtime.get("url"):
            if runtime["transport"] == "sse":
                streams = await stack.enter_async_context(
                    sse_client(runtime["url"], headers=runtime["headers"])
                )
            else:
                http_client = None
                if runtime["headers"]:
                    import httpx
                    http_client = await stack.enter_async_context(
                        httpx.AsyncClient(headers=runtime["headers"])
                    )
                streams = await stack.enter_async_context(
                    streamable_http_client(runtime["url"], http_client=http_client)
                )
        else:
            env = os.environ.copy()
            env.update(runtime["env"])
            params = StdioServerParameters(
                command=runtime["command"],
                args=runtime["args"],
                env=env,
                cwd=runtime["cwd"],
            )
            streams = await stack.enter_async_context(stdio_client(params))

        client = await stack.enter_async_context(
            ClientSession(
                streams[0],
                streams[1],
                elicitation_callback=relay_elicitation,
            )
        )
        await client.initialize()
        if not connection.ready.done():
            connection.ready.set_result(None)

        while True:
            request = await connection.queue.get()
            if request is None:
                break
            connection.last_used = time.time()
            current_request = request
            try:
                result = await (
                    client.list_tools()
                    if request.operation == "tools"
                    else client.call_tool(
                        request.tool_name,
                        request.arguments or {},
                        meta=request.meta,
                    )
                )
            except asyncio.CancelledError:
                if not request.future.done():
                    request.future.cancel()
                raise
            except BaseException as exc:
                connection.failure = exc
                if not request.future.done():
                    request.future.set_exception(exc)
                break
            else:
                if not request.future.done():
                    request.future.set_result(result)
                connection.last_used = time.time()
            finally:
                current_request = None
    except asyncio.CancelledError:
        if not connection.ready.done():
            connection.ready.cancel()
        raise
    except BaseException as exc:
        connection.failure = exc
        if not connection.ready.done():
            connection.ready.set_exception(exc)
    finally:
        connection.closed = True
        failure = connection.failure or RuntimeError("MCP connection closed")
        while True:
            try:
                pending = connection.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if pending is not None and not pending.future.done():
                pending.future.set_exception(failure)
        with contextlib.suppress(BaseException):
            await stack.aclose()


async def _open_mcp_connection(
    session_id: str,
    server_name: str,
    config: dict[str, Any],
) -> PersistentMcpConnection:
    loop = asyncio.get_running_loop()
    connection = PersistentMcpConnection(
        key=(session_id, server_name),
        config_fingerprint=_mcp_config_fingerprint(config),
        queue=asyncio.Queue(),
        ready=loop.create_future(),
        exclusive_resource=_mcp_exclusive_resource(config),
    )
    connection.task = loop.create_task(
        _mcp_connection_actor(connection, config),
        name=f"mcp-proxy:{session_id}:{server_name}",
    )
    try:
        await connection.ready
    except BaseException:
        await _close_mcp_connection(connection)
        raise
    return connection


async def _close_mcp_connection(connection: PersistentMcpConnection) -> None:
    task = connection.task
    if task is None:
        connection.closed = True
        return
    if task.done():
        connection.closed = True
        with contextlib.suppress(BaseException):
            task.result()
        return

    connection.closed = True
    await connection.queue.put(None)
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=5)
    except asyncio.TimeoutError:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
    except BaseException:
        with contextlib.suppress(BaseException):
            await task


def _unregister_mcp_connection_locked(
    connection: PersistentMcpConnection,
    *,
    release_resource: bool = True,
) -> None:
    if mcp_connections.get(connection.key) is connection:
        mcp_connections.pop(connection.key, None)
    resource = connection.exclusive_resource
    if release_resource and resource and mcp_resource_owners.get(resource) == connection.key:
        mcp_resource_owners.pop(resource, None)


async def _release_mcp_resource(connection: PersistentMcpConnection) -> None:
    resource = connection.exclusive_resource
    if not resource:
        return
    async with mcp_connections_lock:
        current = mcp_connections.get(connection.key)
        replacement_uses_resource = (
            current is not None
            and current is not connection
            and current.exclusive_resource == resource
            and not current.closed
        )
        if not replacement_uses_resource and mcp_resource_owners.get(resource) == connection.key:
            mcp_resource_owners.pop(resource, None)


async def _mcp_key_lock(key: tuple[str, str]) -> asyncio.Lock:
    async with mcp_connections_lock:
        lock = mcp_connection_key_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            mcp_connection_key_locks[key] = lock
        return lock


async def _close_idle_mcp_connections() -> None:
    if MCP_PROXY_IDLE_TIMEOUT <= 0:
        return
    cutoff = time.time() - MCP_PROXY_IDLE_TIMEOUT
    stale: list[PersistentMcpConnection] = []
    async with mcp_connections_lock:
        for connection in list(mcp_connections.values()):
            task_done = connection.task is not None and connection.task.done()
            if task_done or (connection.active_calls == 0 and connection.last_used <= cutoff):
                _unregister_mcp_connection_locked(connection, release_resource=False)
                stale.append(connection)
    for connection in stale:
        await _close_mcp_connection(connection)
        await _release_mcp_resource(connection)


async def _acquire_mcp_connection(
    session_id: str,
    server_name: str,
    config: dict[str, Any],
) -> PersistentMcpConnection:
    await _close_idle_mcp_connections()
    key = (session_id, server_name)
    fingerprint = _mcp_config_fingerprint(config)
    exclusive_resource = _mcp_exclusive_resource(config)
    key_lock = await _mcp_key_lock(key)

    async with key_lock:
        replaced: PersistentMcpConnection | None = None
        reserved_resource = False
        async with mcp_connections_lock:
            connection = mcp_connections.get(key)
            task_done = connection is not None and connection.task is not None and connection.task.done()
            if connection is not None and (
                connection.closed or task_done or connection.config_fingerprint != fingerprint
            ):
                _unregister_mcp_connection_locked(connection)
                replaced = connection
                connection = None

            if connection is None and exclusive_resource:
                owner = mcp_resource_owners.get(exclusive_resource)
                if owner is not None and owner != key:
                    raise McpResourceBusyError(
                        f"exclusive MCP profile/resource is already owned by session {owner[0]!r} "
                        f"through server {owner[1]!r}"
                    )
                mcp_resource_owners[exclusive_resource] = key
                reserved_resource = True

        if replaced is not None:
            await _close_mcp_connection(replaced)

        if connection is None:
            try:
                connection = await _open_mcp_connection(session_id, server_name, config)
            except BaseException:
                if reserved_resource and exclusive_resource:
                    async with mcp_connections_lock:
                        if mcp_resource_owners.get(exclusive_resource) == key:
                            mcp_resource_owners.pop(exclusive_resource, None)
                raise
            async with mcp_connections_lock:
                mcp_connections[key] = connection
                if connection.exclusive_resource:
                    mcp_resource_owners[connection.exclusive_resource] = key

        async with mcp_connections_lock:
            connection.active_calls += 1
            connection.last_used = time.time()
        return connection


async def _release_mcp_connection(connection: PersistentMcpConnection) -> None:
    async with mcp_connections_lock:
        connection.active_calls = max(0, connection.active_calls - 1)
        connection.last_used = time.time()


async def _discard_mcp_connection(connection: PersistentMcpConnection) -> None:
    async with mcp_connections_lock:
        _unregister_mcp_connection_locked(connection, release_resource=False)
    await _close_mcp_connection(connection)
    await _release_mcp_resource(connection)


async def _reset_mcp_connections(session_id: str, server_name: str | None = None) -> int:
    selected: list[PersistentMcpConnection] = []
    async with mcp_connections_lock:
        for key, connection in list(mcp_connections.items()):
            if key[0] != session_id:
                continue
            if server_name is not None and key[1] != server_name:
                continue
            _unregister_mcp_connection_locked(connection, release_resource=False)
            selected.append(connection)
    for connection in selected:
        await _close_mcp_connection(connection)
        await _release_mcp_resource(connection)
    return len(selected)


async def _close_all_mcp_connections() -> None:
    async with mcp_connections_lock:
        selected = list(mcp_connections.values())
        mcp_connections.clear()
        mcp_resource_owners.clear()
    for connection in selected:
        await _close_mcp_connection(connection)


async def _with_mcp_session(
    session_id: str,
    server_name: str,
    config: dict[str, Any],
    operation: str,
    tool_name: str = "",
    arguments: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
    elicitation_callback: Callable[[Any], Awaitable[Any]] | None = None,
) -> Any:
    last_error: BaseException | None = None
    for attempt in range(2):
        connection = await _acquire_mcp_connection(session_id, server_name, config)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        request = McpActorRequest(
            operation,
            tool_name,
            arguments,
            meta,
            elicitation_callback,
            future,
        )
        try:
            task_done = connection.task is not None and connection.task.done()
            if connection.closed or task_done:
                raise RuntimeError("MCP connection closed before the request could run")
            await connection.queue.put(request)
            result = await future
            connection.last_used = time.time()
            return result
        except asyncio.CancelledError:
            future.cancel()
            await _discard_mcp_connection(connection)
            raise
        except BaseException as exc:
            last_error = exc
            await _discard_mcp_connection(connection)
            if attempt == 0:
                continue
            raise
        finally:
            await _release_mcp_connection(connection)
    if last_error is not None:
        raise last_error
    raise RuntimeError("MCP connection closed before the request could run")


def _mcp_result_json(result: Any, max_chars: int) -> str:
    if hasattr(result, "model_dump"):
        result = result.model_dump(mode="json")
    return _limit_text(json.dumps(result, indent=2, default=str), max_chars, "MCP result")


@mcp.tool()
async def local_mcp(
    action: str = "list",
    server: str = "",
    tool: str = "",
    arguments: dict[str, Any] | None = None,
    session_id: str = "default",
    cwd: str | None = None,
    timeout: int = 60,
    max_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
    approved_browser_origins: list[str] | None = None,
) -> Any:
    """List, inspect, call, or reset configured MCP servers through persistent downstream connections."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    root = _find_project_root(run_cwd)
    servers = _discover_mcp_servers(root)
    action = action.strip().lower().replace("_", "-")
    forwarded_arguments = arguments
    compatibility_origins: list[str] | None = None
    if isinstance(arguments, dict) and "__terminal_mcp_approved_browser_origins" in arguments:
        if action != "call" or server != NODE_REPL_MCP_SERVER:
            return "Error: __terminal_mcp_approved_browser_origins is only supported for calls to the configured Node REPL server."
        forwarded_arguments = dict(arguments)
        raw_compatibility_origins = forwarded_arguments.pop("__terminal_mcp_approved_browser_origins")
        if not isinstance(raw_compatibility_origins, list) or not all(
            isinstance(origin, str) for origin in raw_compatibility_origins
        ):
            return "Error: __terminal_mcp_approved_browser_origins must be a list of origin strings."
        compatibility_origins = raw_compatibility_origins
    combined_approved_origins = [
        *(approved_browser_origins or []),
        *(compatibility_origins or []),
    ]
    try:
        approved_origin_set = _normalize_approved_browser_origins(combined_approved_origins)
    except ValueError as exc:
        return f"Error: {exc}"
    if approved_origin_set and (action != "call" or server != NODE_REPL_MCP_SERVER):
        return "Error: approved_browser_origins is only supported for calls to the configured Node REPL server."
    await _close_idle_mcp_connections()

    if action == "list":
        async with mcp_connections_lock:
            connected = {
                key[1]
                for key, connection in mcp_connections.items()
                if key[0] == session.session_id and not connection.closed
            }
        rows = []
        for name, config in sorted(servers.items()):
            row = _public_mcp_summary(name, config)
            if name in connected and row["status"] == "configured":
                row["status"] = "connected"
            rows.append(row)
        return _limit_text(
            json.dumps({"project_root": str(root), "session_id": session.session_id, "servers": rows}, indent=2),
            max_chars,
            "MCP list",
        )

    if action in {"reset", "close"}:
        if not server:
            return "Error: server is required for action=reset."
        closed = await _reset_mcp_connections(session.session_id, server)
        return f"Reset MCP connection for {server}: closed {closed} connection(s)."
    if action in {"reset-all", "close-all"}:
        closed = await _reset_mcp_connections(session.session_id)
        return f"Reset all MCP connections for session {session.session_id}: closed {closed} connection(s)."

    config = servers.get(server)
    if config is None:
        return f"Error: unknown configured MCP server: {server}"
    if _is_recursive_terminal_server(server, config):
        return "Error: recursive terminal MCP proxying is blocked."
    if action not in {"tools", "call"}:
        return "Error: action must be list, tools, call, reset, or reset-all."
    if action == "call" and not tool:
        return "Error: tool is required for action=call."

    request_meta = (
        _forwardable_mcp_request_meta(
            fallback_session_id=session.session_id if server == NODE_REPL_MCP_SERVER else None
        )
        if action == "call"
        else None
    )
    elicitation_callback = (
        _mcp_elicitation_relay(approved_origin_set)
        if action == "call"
        else None
    )
    try:
        result = await asyncio.wait_for(
            _with_mcp_session(
                session.session_id,
                server,
                config,
                action,
                tool,
                forwarded_arguments,
                request_meta,
                elicitation_callback,
            ),
            timeout=max(1, timeout),
        )
        # Preserve multimodal content from downstream MCP tools. Returning the
        # CallToolResult directly lets FastMCP emit ImageContent, AudioContent,
        # embedded resources, and resource links as native MCP blocks instead
        # of flattening them into JSON text/base64 that the model cannot see.
        if action == "call":
            return result
        return _mcp_result_json(result, max_chars)
    except asyncio.TimeoutError:
        return f"Error: MCP {action} timed out for {server}; the cached connection was discarded."
    except McpResourceBusyError as exc:
        return f"Error: cannot start {server}: {exc}. Use the owning session_id or reset that connection first."
    except Exception as exc:
        return f"Error: MCP {action} failed for {server}: {type(exc).__name__}"


@mcp.tool()
async def node_repl_js(
    code: str,
    timeout_ms: int | None = None,
    title: str = "",
    session_id: str = "default",
    cwd: str | None = None,
    approved_browser_origins: list[str] | None = None,
) -> Any:
    """Run JavaScript through the configured Node REPL MCP. Browser-session APIs are available from this JavaScript when the Codex Chrome bridge is connected."""
    arguments: dict[str, Any] = {"code": code}
    if timeout_ms is not None:
        arguments["timeout_ms"] = timeout_ms
    if title:
        arguments["title"] = title
    return await local_mcp(
        action="call",
        server=NODE_REPL_MCP_SERVER,
        tool="js",
        arguments=arguments,
        session_id=session_id,
        cwd=cwd,
        timeout=max(60, (timeout_ms or 0) // 1000 + 15),
        approved_browser_origins=approved_browser_origins,
    )


@mcp.tool()
async def node_repl_js_reset(
    session_id: str = "default",
    cwd: str | None = None,
) -> Any:
    """Reset the configured Node REPL MCP kernel and clear its JavaScript bindings."""
    return await local_mcp(
        action="call",
        server=NODE_REPL_MCP_SERVER,
        tool="js_reset",
        arguments={},
        session_id=session_id,
        cwd=cwd,
    )


@mcp.tool()
async def run_command(
    command: str,
    timeout: int = DEFAULT_TIMEOUT,
    session_id: str = "default",
    cwd: str | None = None,
    max_output_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
    env: dict[str, str] | None = None,
) -> str:
    """Execute a bash command in a terminal session. Commands in the same session_id are serialized; different session_id values can run independently."""
    request_id = f"cmd-{_now_ms()}-{uuid.uuid4().hex[:8]}"
    started = time.time()
    session = await _get_session(session_id)
    async with session.lock:
        if not session.cwd.exists():
            session.cwd = WORKSPACE_DIR
        try:
            run_cwd = _resolve_cwd(cwd, session.cwd)
            gate = _require_project_context(session, run_cwd)
            if gate:
                return gate
            marker = f"__MCP_PWD_{uuid.uuid4().hex}__"
            modified = (
                f"{command}\n"
                "__mcp_status=$?\n"
                f"printf '\\n{marker}:%s\\n' \"$PWD\"\n"
                "exit \"$__mcp_status\""
            )
            process, unit_name = await _spawn_workload(
                [SHELL, "-lc", modified], shell=True, session=session, request_id=request_id,
                cwd=run_cwd, env=env,
            )
            stdout_capture, stderr_capture, timed_out = await _collect_bounded_output(process, request_id, timeout, unit_name)
            if timed_out:
                return "\n\n".join([
                    f"Request ID: {request_id}",
                    f"Session ID: {session.session_id}",
                    f"Error: Command timed out after {timeout} seconds.",
                    f"Duration: {time.time() - started:.2f}s",
                    f"Working Directory: {session.cwd}",
                ])
            stdout_raw, stdout_log = stdout_capture.text(max_output_chars)
            stderr_raw, stderr_log = stderr_capture.text(max_output_chars)
            stdout_clean, new_cwd = _strip_pwd_marker(stdout_raw, marker)
            if new_cwd:
                candidate = Path(new_cwd).expanduser()
                if candidate.is_dir():
                    session.cwd = candidate.resolve()
            session.command_count += 1
            session.updated_at = time.time()
            if stdout_log:
                extra_stdout_log = None
            else:
                stdout_clean, extra_stdout_log = _truncate_output("STDOUT", stdout_clean, request_id, max_output_chars)
            if stderr_log:
                extra_stderr_log = None
            else:
                stderr_clean, extra_stderr_log = _truncate_output("STDERR", stderr_raw.rstrip(), request_id, max_output_chars)
            response = [f"Request ID: {request_id}", f"Session ID: {session.session_id}"]
            if stdout_clean.strip():
                response.append(f"STDOUT:\n{stdout_clean}")
            if stderr_clean.strip():
                response.append(f"STDERR:\n{stderr_clean}")
            response.extend([
                f"Exit Code: {process.returncode}",
                f"Duration: {time.time() - started:.2f}s",
                f"Working Directory: {session.cwd}",
            ])
            logs = [path for path in [stdout_log, stderr_log, extra_stdout_log, extra_stderr_log] if path]
            if logs:
                response.append("Full Output Logs:\n" + "\n".join(logs))
            return "\n\n".join(response)
        except Exception as exc:
            return "\n\n".join([
                f"Request ID: {request_id}",
                f"Session ID: {session.session_id}",
                f"Error: {type(exc).__name__}: {exc}",
                f"Duration: {time.time() - started:.2f}s",
                f"Working Directory: {session.cwd}",
            ])

@_hidden_tool()
async def set_cwd(path: str, session_id: str = "default") -> str:
    """Set the tracked working directory for a session."""
    session = await _get_session(session_id)
    async with session.lock:
        session.cwd = _resolve_cwd(path, session.cwd)
        session.updated_at = time.time()
        return f"Session ID: {session.session_id}\nWorking Directory: {session.cwd}"

@_hidden_tool()
async def get_session(session_id: str = "default") -> str:
    """Show working directory and metadata for one session."""
    session = await _get_session(session_id)
    return "\n".join([
        f"Session ID: {session.session_id}",
        f"Working Directory: {session.cwd}",
        f"Created At: {_format_timestamp(session.created_at)}",
        f"Updated At: {_format_timestamp(session.updated_at)}",
        f"Command Count: {session.command_count}",
        f"Environment Overrides: {len(session.env)}",
    ])

@_hidden_tool()
async def list_sessions() -> str:
    """List known terminal sessions."""
    async with state_lock:
        rows = [f"{sid}\t{st.cwd}\tcommands={st.command_count}\tupdated={_format_timestamp(st.updated_at)}" for sid, st in sorted(sessions.items())]
    return "No sessions." if not rows else "\n".join(rows)

@_hidden_tool()
async def reset_session(session_id: str = "default") -> str:
    """Reset a session's working directory and environment overrides."""
    session = await _get_session(session_id)
    async with session.lock:
        session.cwd = WORKSPACE_DIR
        session.env.clear()
        session.context_fingerprints.clear()
        session.updated_at = time.time()
        return f"Session reset.\nSession ID: {session.session_id}\nWorking Directory: {session.cwd}"

@mcp.tool()
async def set_session_env(name: str, value: str, session_id: str = "default") -> str:
    """Set an environment variable override for future commands in a session."""
    session = await _get_session(session_id)
    async with session.lock:
        gate = _require_project_context(session, session.cwd.resolve())
        if gate:
            return gate
        session.env[str(name)] = str(value)
        session.updated_at = time.time()
        return f"Set {name} for session {session.session_id}."

@_hidden_tool()
async def unset_session_env(name: str, session_id: str = "default") -> str:
    """Remove an environment variable override from a session."""
    session = await _get_session(session_id)
    async with session.lock:
        existed = session.env.pop(str(name), None) is not None
        session.updated_at = time.time()
        return f"Unset {name} for session {session.session_id}. existed={existed}"

@mcp.tool()
async def start_process(command: str, session_id: str = "default", cwd: str | None = None, env: dict[str, str] | None = None) -> str:
    """Start a long-running background bash process and return a process_id."""
    session = await _get_session(session_id)
    async with session.lock:
        run_cwd = _resolve_cwd(cwd, session.cwd)
        gate = _require_project_context(session, run_cwd)
        if gate:
            return gate
        process_id = f"proc-{_now_ms()}-{uuid.uuid4().hex[:8]}"
        process, unit_name = await _spawn_workload(
            [SHELL, "-lc", command], shell=True, session=session, request_id=process_id,
            cwd=run_cwd, env=env, stdin=asyncio.subprocess.PIPE,
        )
        state = ProcessState(process_id, session.session_id, command, run_cwd, process, unit_name=unit_name)
        async with state_lock:
            processes[process_id] = state
        asyncio.create_task(_drain_stream(process.stdout, state.stdout_lines, "stdout_closed", state))
        asyncio.create_task(_drain_stream(process.stderr, state.stderr_lines, "stderr_closed", state))
        session.updated_at = time.time()
        return "\n".join([f"Process ID: {process_id}", f"Session ID: {session.session_id}", f"PID: {process.pid}", f"Workload Unit: {unit_name or 'process-group'}", f"Command: {command}", f"Working Directory: {run_cwd}", "Status: running"])

@mcp.tool()
async def poll_process(
    process_id: str,
    max_lines: int = 200,
    session_id: str = "default",
) -> str:
    """Read buffered output from a background process owned by this bootstrapped thread."""
    session = await _get_session(session_id)
    gate = _require_project_context(session, session.cwd.resolve())
    if gate:
        return gate
    async with state_lock:
        state = processes.get(process_id)
    if state is None or state.session_id != session.session_id:
        return f"Error: process_id is unknown or not owned by session {session.session_id}: {process_id}"
    return _format_process(state, max_lines)


@_hidden_tool()
async def write_process(process_id: str, input_text: str) -> str:
    """Write text to a background process stdin."""
    async with state_lock:
        state = processes.get(process_id)
    if state is None:
        return f"Error: Unknown process_id: {process_id}"
    if state.process.returncode is not None:
        return f"Error: Process is not running. Status: {_process_status(state)}"
    if state.process.stdin is None:
        return "Error: Process stdin is not available."
    state.process.stdin.write(input_text.encode("utf-8"))
    await state.process.stdin.drain()
    return f"Wrote {len(input_text)} chars to {process_id}."

@mcp.tool()
async def stop_process(process_id: str, session_id: str = "default") -> str:
    """Stop a background process owned by this bootstrapped thread."""
    session = await _get_session(session_id)
    gate = _require_project_context(session, session.cwd.resolve())
    if gate:
        return gate
    async with state_lock:
        state = processes.get(process_id)
    if state is None or state.session_id != session.session_id:
        return f"Error: process_id is unknown or not owned by session {session.session_id}: {process_id}"
    await _stop_workload(state.process, state.unit_name)
    return _format_process(state, max_lines=80)


@_hidden_tool()
async def list_processes(session_id: str | None = None) -> str:
    """List background processes, optionally filtered by session_id."""
    normalized = _normalize_session_id(session_id) if session_id else None
    async with state_lock:
        selected = [p for p in processes.values() if normalized is None or p.session_id == normalized]
    if not selected:
        return "No processes."
    return "\n".join(f"{p.process_id}\t{_process_status(p)}\tpid={p.process.pid}\tsession={p.session_id}\tcwd={p.cwd}\tcmd={p.command}" for p in sorted(selected, key=lambda item: item.started_at))


def _public_tool_manifest() -> list[dict[str, str]]:
    order = tuple(globals().get("PUBLIC_TOOL_ORDER", tuple(mcp._tool_manager._tools.keys())))
    rows: list[dict[str, str]] = []
    for name in order:
        function = globals().get(name)
        description = ""
        if function is not None:
            description = " ".join((function.__doc__ or "").strip().split())
        if not description:
            tool = mcp._tool_manager._tools.get(name)
            description = " ".join((getattr(tool, "description", "") or "").strip().split())
        rows.append({"name": name, "description": description})
    return rows


def _configured_mcp_manifest(root: Path) -> list[dict[str, str]]:
    return [
        _public_mcp_summary(name, config)
        for name, config in sorted(_discover_mcp_servers(root).items())
    ]


def _build_thread_context_document(
    thread_id: str,
    run_cwd: Path,
) -> tuple[str, Path, list[tuple[Path, str]], str]:
    root, rows, fingerprint = _agents_snapshot(run_cwd)
    skills = _discover_skills(root)
    tools = _public_tool_manifest()
    nested_servers = _configured_mcp_manifest(root)
    goal = GPT_STORE.get_goal(thread_id, mark_seen=True)

    parts = [
        "[Terminal GPT Thread Bootstrap]",
        "MANDATORY: Follow every instruction below strictly for the lifetime of this model thread.",
        "MANDATORY: Reuse this thread ID as session_id in every later Terminal GPT tool call.",
        "MANDATORY: If context is compacted, forgotten, changed, or uncertain, call get_thread_context before continuing.",
        "",
        f"Thread ID: {thread_id}",
        f"Working Directory: {run_cwd}",
        f"Project Root: {root}",
        f"GPT Home: {_gpt_home()}",
        f"Usage Database: {_thread_db_path()}",
        f"Context Fingerprint: {fingerprint}",
        f"Applicable GPT Instruction Files: {len(rows)}",
        "",
        "## Mandatory GPT instructions",
    ]
    if rows:
        for path, content in rows:
            parts.extend([f"===== {path} =====", content.rstrip(), ""])
    else:
        parts.extend([
            "No GPT AGENTS file was found. This should normally be repaired by bootstrap_thread.",
            "",
        ])

    parts.extend([f"## Available skills ({len(skills)})"])
    if skills:
        for skill in skills:
            description = skill["description"] or "No description provided. Read the skill before use."
            parts.append(
                f"- {skill['name']} [{skill['source']}]: {description} (path: {skill['path']})"
            )
    else:
        parts.append("- No SKILL.md files discovered.")

    parts.extend(["", f"## Public Terminal GPT tools ({len(tools)})"])
    for tool in tools:
        parts.append(f"- {tool['name']}: {tool['description'] or 'No description provided.'}")

    parts.extend(["", f"## Configured nested MCP servers ({len(nested_servers)})"])
    if nested_servers:
        for server in nested_servers:
            parts.append(
                f"- {server['name']}: transport={server['transport']}, endpoint={server['endpoint']}. "
                "Use local_mcp(action='tools', server=...) before invoking nested tools."
            )
    else:
        parts.append("- No nested MCP servers discovered.")

    if goal is not None and goal["status"] in {"active", "technical_error"}:
        parts.extend(["", "## Active thread goal", _format_goal_context(goal)])

    parts.extend([
        "",
        "## Context recovery and accounting",
        "- bootstrap_thread: initialize a genuinely new model thread.",
        "- get_thread_context: reload the complete context after compaction or uncertainty.",
        "- context_manifest: inspect fingerprints, files, skills, tools, and nested MCP names without loading full instruction bodies.",
        "- refresh_startup_context: rebuild MCP initialization instructions for future client initializations.",
        "- record_token_usage: store exact provider-reported input/output/cached-input usage.",
        "- get_token_usage: retrieve exact and estimated totals separately.",
        "",
        "Token-accounting limitation: this MCP cannot independently observe the host model's complete prompt or response usage. "
        "Exact model totals require the host/runtime to call record_token_usage with provider-reported values. "
        "Any bootstrap token count stored automatically is explicitly marked as a server estimate.",
    ])
    return "\n".join(parts).rstrip(), root, rows, fingerprint


async def _load_thread_context(
    thread_id: str,
    cwd: str | None,
    max_chars: int,
    event_type: str,
) -> str:
    _ensure_gpt_layout()
    normalized = _normalize_session_id(thread_id)
    if normalized == "default":
        return (
            "Error: thread_id must be a unique, stable non-default identifier. "
            "Generate one for this model thread and reuse it as session_id on every later call."
        )
    session = await _get_session(normalized)
    run_cwd = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd.resolve()
    full, root, rows, fingerprint = _build_thread_context_document(normalized, run_cwd)
    if max_chars > 0 and len(full) > max_chars:
        return full[:max_chars] + (
            f"\n\n[Context truncated to {max_chars} of {len(full)} chars. Context gate remains unsatisfied. "
            "Call get_thread_context again with a larger max_chars.]"
        )

    session.cwd = run_cwd
    session.context_fingerprints[_context_key(run_cwd)] = fingerprint
    session.bootstrapped_at = time.time()
    session.updated_at = time.time()
    _upsert_thread_record(normalized, run_cwd, fingerprint, loaded=True)
    estimated_tokens = _estimate_tokens(full)
    _record_usage_event(
        normalized,
        event_type=event_type,
        source="server_estimate",
        input_tokens=estimated_tokens,
        is_exact=False,
        request_id=f"{event_type}-{_now_ms()}-{uuid.uuid4().hex[:8]}",
        metadata={
            "chars": len(full),
            "estimation_method": "utf8_bytes_div_4",
            "project_root": str(root),
            "instruction_files": [str(path) for path, _ in rows],
            "context_fingerprint": fingerprint,
        },
    )
    return full + f"\n\nContext Gate: satisfied\nEstimated Context Input Tokens: {estimated_tokens}"


@mcp.tool()
async def bootstrap_thread(
    thread_id: str,
    cwd: str | None = None,
    max_chars: int = DEFAULT_BOOTSTRAP_MAX_CHARS,
) -> str:
    """Initialize a new model thread with .GPT instructions, all discovered skills, public tools, nested MCP names, and a persistent context fingerprint."""
    return await _load_thread_context(thread_id, cwd, max_chars, "thread_bootstrap")


@mcp.tool()
async def get_thread_context(
    thread_id: str,
    cwd: str | None = None,
    max_chars: int = DEFAULT_BOOTSTRAP_MAX_CHARS,
) -> str:
    """Reload the complete GPT thread context after compaction, instruction changes, or uncertainty."""
    return await _load_thread_context(thread_id, cwd, max_chars, "thread_context_reload")


@mcp.tool()
async def context_manifest(
    thread_id: str = "",
    cwd: str | None = None,
) -> str:
    """Return a compact manifest of GPT instruction files, skills, public tools, nested MCP servers, fingerprints, and storage paths."""
    _ensure_gpt_layout()
    normalized = _normalize_session_id(thread_id) if thread_id else ""
    if cwd:
        run_cwd = _resolve_cwd(cwd, WORKSPACE_DIR)
    elif normalized and normalized in sessions:
        run_cwd = sessions[normalized].cwd.resolve()
    else:
        run_cwd = WORKSPACE_DIR.resolve()
    root, rows, fingerprint = _agents_snapshot(run_cwd)
    skills = _discover_skills(root)
    payload = {
        "thread_id": normalized or None,
        "working_directory": str(run_cwd),
        "project_root": str(root),
        "gpt_home": str(_gpt_home()),
        "usage_database": str(_thread_db_path()),
        "context_fingerprint": fingerprint,
        "thread_goal": GPT_STORE.get_goal(normalized, mark_seen=True) if normalized else None,
        "instruction_files": [str(path) for path, _ in rows],
        "skills": [
            {
                "name": skill["name"],
                "source": skill["source"],
                "description": skill["description"],
                "path": skill["path"],
            }
            for skill in skills
        ],
        "public_tools": _public_tool_manifest(),
        "nested_mcp_servers": _configured_mcp_manifest(root),
        "recovery_tools": [
            "bootstrap_thread",
            "get_thread_context",
            "context_manifest",
            "refresh_startup_context",
            "record_token_usage",
            "get_token_usage",
        ],
    }
    return json.dumps(payload, indent=2)


@mcp.tool()
async def refresh_startup_context() -> str:
    """Rebuild MCP initialization instructions from the current ~/.GPT/AGENTS.md, skill manifest, tool manifest, and nested MCP list."""
    instructions = _set_startup_instructions()
    return json.dumps(
        {
            "status": "refreshed",
            "characters": len(instructions),
            "estimated_tokens": _estimate_tokens(instructions),
            "applies_to": "future MCP client initializations; existing threads should call get_thread_context",
            "gpt_agents": str(_gpt_agents_path()),
        },
        indent=2,
    )


@mcp.tool()
async def record_token_usage(
    thread_id: str,
    input_tokens: int,
    output_tokens: int,
    cached_input_tokens: int = 0,
    model: str = "",
    request_id: str = "",
    metadata: dict[str, Any] | None = None,
) -> str:
    """Record exact model token usage reported by the host/provider for one thread response."""
    normalized = _normalize_session_id(thread_id)
    if normalized == "default":
        return "Error: thread_id must be a unique non-default identifier."
    try:
        event_id = _record_usage_event(
            normalized,
            event_type="model_usage",
            source="host_reported",
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cached_input_tokens=cached_input_tokens,
            is_exact=True,
            model=model,
            request_id=request_id,
            metadata=metadata,
        )
    except (TypeError, ValueError, sqlite3.Error) as exc:
        return f"Error recording token usage: {exc}"
    summary = _usage_summary(normalized, limit=10)
    return json.dumps({"event_id": event_id, **summary}, indent=2)


@mcp.tool()
async def get_token_usage(
    thread_id: str = "",
    limit: int = 100,
) -> str:
    """Return per-thread or global exact and estimated token-usage totals with recent events."""
    normalized = _normalize_session_id(thread_id) if thread_id else None
    return json.dumps(_usage_summary(normalized, limit=limit), indent=2)


@mcp.tool()
async def thread_goal(
    action: str = "get",
    session_id: str = "default",
    objective: str = "",
    finish_conditions: list[str] | None = None,
    evidence: list[str] | None = None,
    reason: str = "",
    cwd: str | None = None,
) -> str:
    """Set, inspect, complete, pause for a technical error, resume, or clear one persistent goal for this thread."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    action = action.strip().lower().replace("-", "_")
    try:
        if action in {"set", "create"}:
            goal = GPT_STORE.set_goal(session.session_id, objective, finish_conditions)
        elif action in {"get", "show"}:
            goal = GPT_STORE.get_goal(session.session_id, mark_seen=True)
            return json.dumps({"goal": goal}, indent=2)
        elif action == "complete":
            goal = GPT_STORE.complete_goal(session.session_id, evidence)
        elif action in {"technical_error", "block"}:
            goal = GPT_STORE.mark_goal_technical_error(session.session_id, reason)
        elif action == "resume":
            goal = GPT_STORE.resume_goal(session.session_id)
        elif action == "clear":
            cleared = GPT_STORE.clear_goal(session.session_id)
            return json.dumps({"cleared": cleared, "thread_id": session.session_id}, indent=2)
        else:
            return (
                "Error: action must be set, get, complete, technical_error, resume, or clear."
            )
    except (TypeError, ValueError, sqlite3.Error) as exc:
        return f"Error updating thread goal: {exc}"
    return json.dumps({"goal": goal}, indent=2)


@mcp.tool()
async def project_context(
    session_id: str = "default",
    cwd: str | None = None,
    max_chars: int = DEFAULT_BOOTSTRAP_MAX_CHARS,
) -> str:
    """Compatibility alias that loads the complete GPT thread context using session_id as the stable thread identity."""
    return await _load_thread_context(session_id, cwd, max_chars, "project_context_reload")


@_hidden_tool()
async def agent_claim_task(
    title: str,
    agent_id: str | None = None,
    project_root: str | None = None,
    session_id: str = "default",
    task_id: str | None = None,
    details: str = "",
) -> str:
    """Claim a task for one parallel agent and write that agent's TASK.md."""
    session = await _get_session(session_id)
    root = _project_root(session, project_root)
    aid = _agent_id(agent_id, session.session_id)
    paths = _agent_paths(root, aid)
    tid = _slug(task_id or f"task-{_now_ms()}-{uuid.uuid4().hex[:6]}", "task")
    async with agent_state_lock:
        with _file_lock(paths["lock"]):
            _ensure_agent_files(paths, aid)
            queue = _read_text_cap(paths["queue"], 20000)
            row = f"| {tid} | {aid} | in_progress | {title.replace('|','/')} |"
            if tid not in queue:
                _atomic_write(paths["queue"], queue.rstrip() + "\n" + row + "\n")
            task_doc = "\n".join([
                f"# Task for {aid}",
                "",
                "Status: in_progress",
                f"Task ID: {tid}",
                "",
                "Active Task:",
                f"- {title}",
                "",
                "Details:",
                details or "- None",
                "",
                "Checklist:",
                "- [ ] Work the task",
                "- [ ] Validate changes",
                "- [ ] Update HANDOFF.md",
                "",
            ])
            _atomic_write(paths["task"], task_doc)
            state = _load_agent_state(paths, aid)
            state["active_task_id"] = tid
            _save_agent_state(paths, state)
    return f"Claimed task {tid} for {aid}.\nTask File: {paths['task']}\nQueue File: {paths['queue']}"

@_hidden_tool()
async def agent_update_task(
    status: str,
    summary: str,
    agent_id: str | None = None,
    project_root: str | None = None,
    session_id: str = "default",
    checklist: str = "",
) -> str:
    """Rewrite one agent's TASK.md with current status and summary."""
    allowed = {"idle", "in_progress", "blocked", "done", "cancelled"}
    if status not in allowed:
        return f"Error: status must be one of {sorted(allowed)}"
    session = await _get_session(session_id)
    root = _project_root(session, project_root)
    aid = _agent_id(agent_id, session.session_id)
    paths = _agent_paths(root, aid)
    async with agent_state_lock:
        with _file_lock(paths["lock"]):
            _ensure_agent_files(paths, aid)
            state = _load_agent_state(paths, aid)
            tid = state.get("active_task_id") or "none"
            body = "\n".join([
                f"# Task for {aid}",
                "",
                f"Status: {status}",
                f"Task ID: {tid}",
                f"Updated: {_utc_now()}",
                "",
                "Summary:",
                summary,
                "",
                "Checklist:",
                checklist or "- [ ] Next step not set",
                "",
            ])
            _atomic_write(paths["task"], body)
            state["last_task_status"] = status
            _save_agent_state(paths, state)
    return f"Updated task for {aid}.\nTask File: {paths['task']}"

@_hidden_tool()
async def agent_handoff(
    summary: str,
    next_action: str = "",
    agent_id: str | None = None,
    project_root: str | None = None,
    session_id: str = "default",
) -> str:
    """Write one agent's concise HANDOFF.md for reconnect/compaction/another agent."""
    session = await _get_session(session_id)
    root = _project_root(session, project_root)
    aid = _agent_id(agent_id, session.session_id)
    paths = _agent_paths(root, aid)
    async with agent_state_lock:
        with _file_lock(paths["lock"]):
            _ensure_agent_files(paths, aid)
            task_text = _read_text_cap(paths["task"], 3000)
            state = _load_agent_state(paths, aid)
            handoff = "\n".join([
                f"# Handoff for {aid}",
                "",
                f"Updated: {_utc_now()}",
                f"Task ID: {state.get('active_task_id') or 'none'}",
                "",
                "Summary:",
                summary,
                "",
                "Next Action:",
                next_action or "- Not set",
                "",
                "Current TASK.md Snapshot:",
                "```",
                task_text,
                "```",
                "",
            ])
            _atomic_write(paths["handoff"], handoff)
            _save_agent_state(paths, state)
    return f"Wrote handoff for {aid}.\nHandoff File: {paths['handoff']}"

@_hidden_tool()
async def agent_files(agent_id: str | None = None, project_root: str | None = None, session_id: str = "default") -> str:
    """Show the minimal parallel-agent context file set for this project/agent."""
    session = await _get_session(session_id)
    root = _project_root(session, project_root)
    aid = _agent_id(agent_id, session.session_id)
    paths = _agent_paths(root, aid)
    _ensure_agent_files(paths, aid)
    return "\n".join([
        f"Project Root: {root}",
        f"Agent ID: {aid}",
        f"Repo Instructions: {paths['agents_md']}",
        f"Project Context: {paths['project']}",
        f"Shared Queue: {paths['queue']}",
        f"Agent Task: {paths['task']}",
        f"Agent Handoff: {paths['handoff']}",
        f"Agent State: {paths['state']}",
    ])

@_hidden_tool()
async def terminal_backend_status() -> str:
    """Show whether reconnect-safe terminal backends like tmux are available."""
    available = _tmux_available()
    parts = [
        f"tmux_available: {available}",
        f"tmux_path: {shutil.which('tmux') or ''}",
        "recommended_for_reconnect: tmux",
        "notes: start_process is good for same-server-process background tasks; tmux terminals are better for network reconnects and MCP restarts.",
    ]
    return "\n".join(parts)

@_hidden_tool()
async def start_terminal(
    name: str,
    session_id: str = "default",
    cwd: str | None = None,
    command: str | None = None,
    recreate: bool = False,
) -> str:
    """Start a persistent tmux terminal session. It survives client/network reconnects and usually survives MCP server restarts."""
    if not _tmux_available():
        return _tmux_error()
    session = await _get_session(session_id)
    terminal = _tmux_name(name)
    run_cwd = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd
    existing = _run_tmux(["has-session", "-t", terminal])
    if existing.returncode == 0:
        if not recreate:
            return f"Terminal already exists: {terminal}\nUse recreate=true to replace it."
        killed = _run_tmux(["kill-session", "-t", terminal])
        if killed.returncode != 0:
            return f"Error killing existing terminal {terminal}:\n{killed.stderr.rstrip()}"
    args = ["new-session", "-d", "-s", terminal, "-c", str(run_cwd)]
    if command:
        args.append(command)
    result = _run_tmux(args)
    if result.returncode != 0:
        return "\n".join([f"Error: failed to start terminal {terminal}", result.stderr.rstrip()])
    return "\n".join([
        f"Terminal: {terminal}",
        f"Session ID: {session.session_id}",
        f"Working Directory: {run_cwd}",
        f"Command: {command or SHELL}",
        "Status: running",
    ])

@_hidden_tool()
async def list_terminals() -> str:
    """List persistent tmux terminal sessions."""
    if not _tmux_available():
        return _tmux_error()
    result = _run_tmux(["list-sessions", "-F", "#{session_name}\tcreated=#{session_created_string}\twindows=#{session_windows}\tattached=#{session_attached}"])
    if result.returncode != 0:
        if "no server running" in result.stderr.lower():
            return "No persistent terminals."
        return f"Error listing terminals:\n{result.stderr.rstrip()}"
    return result.stdout.rstrip() or "No persistent terminals."

@_hidden_tool()
async def read_terminal(name: str, lines: int = 200) -> str:
    """Read the visible scrollback of a persistent tmux terminal."""
    if not _tmux_available():
        return _tmux_error()
    terminal = _tmux_name(name)
    start = f"-{max(1, lines)}"
    result = _run_tmux(["capture-pane", "-p", "-S", start, "-t", terminal])
    if result.returncode != 0:
        return f"Error reading terminal {terminal}:\n{result.stderr.rstrip()}"
    return f"Terminal: {terminal}\n\n{result.stdout.rstrip()}"

@_hidden_tool()
async def send_terminal(name: str, text: str, enter: bool = True) -> str:
    """Send literal text to a persistent tmux terminal, optionally pressing Enter."""
    if not _tmux_available():
        return _tmux_error()
    terminal = _tmux_name(name)
    literal = _run_tmux(["send-keys", "-t", terminal, "-l", text])
    if literal.returncode != 0:
        return f"Error sending text to terminal {terminal}:\n{literal.stderr.rstrip()}"
    if enter:
        pressed = _run_tmux(["send-keys", "-t", terminal, "Enter"])
        if pressed.returncode != 0:
            return f"Error pressing Enter in terminal {terminal}:\n{pressed.stderr.rstrip()}"
    return f"Sent {len(text)} chars to terminal {terminal}. enter={enter}"

@_hidden_tool()
async def stop_terminal(name: str) -> str:
    """Stop a persistent tmux terminal session."""
    if not _tmux_available():
        return _tmux_error()
    terminal = _tmux_name(name)
    result = _run_tmux(["kill-session", "-t", terminal])
    if result.returncode != 0:
        return f"Error stopping terminal {terminal}:\n{result.stderr.rstrip()}"
    return f"Stopped terminal: {terminal}"

def _command_display(args: list[str]) -> str:
    return " ".join(shlex.quote(arg) for arg in args)

def _agent_cli_available(binary: str) -> str | None:
    return shutil.which(binary)

def _normalize_str_list(values: list[str] | None) -> list[str]:
    return [str(value) for value in (values or []) if str(value)]

def _codex_yolo_args(
    prompt: str | None,
    cwd: Path,
    model: str | None = None,
    add_dirs: list[str] | None = None,
    extra_args: list[str] | None = None,
) -> list[str]:
    # Codex 0.144+ expects approval/sandbox/model/cwd options before the exec subcommand.
    args = ["codex", "--ask-for-approval", "never", "--sandbox", "danger-full-access", "--cd", str(cwd)]
    if model:
        args.extend(["--model", model])
    for add_dir in _normalize_str_list(add_dirs):
        args.extend(["--add-dir", add_dir])
    args.extend(_normalize_str_list(extra_args))
    args.extend(["exec", "--skip-git-repo-check"])
    if prompt is not None:
        args.append("-")
    return args


def _agy_yolo_args(
    prompt: str | None,
    model: str | None = None,
    add_dirs: list[str] | None = None,
    extra_args: list[str] | None = None,
) -> list[str]:
    args = ["agy", "--dangerously-skip-permissions", "--print"]
    if model:
        args.extend(["--model", model])
    for add_dir in _normalize_str_list(add_dirs):
        args.extend(["--add-dir", add_dir])
    args.extend(_normalize_str_list(extra_args))
    if prompt is not None:
        args.append(prompt)
    return args

async def _run_agent_cli(
    args: list[str],
    prompt_stdin: str | None,
    run_cwd: Path,
    timeout: int | None,
    session: SessionState,
    env: dict[str, str] | None,
    max_output_chars: int,
) -> str:
    request_id = f"agent-{_now_ms()}-{uuid.uuid4().hex[:8]}"
    started = time.time()
    process, unit_name = await _spawn_workload(
        args, shell=False, session=session, request_id=request_id, cwd=run_cwd, env=env,
        stdin=asyncio.subprocess.PIPE if prompt_stdin is not None else None,
    )
    if prompt_stdin is not None and process.stdin is not None:
        process.stdin.write(prompt_stdin.encode("utf-8"))
        await process.stdin.drain()
        process.stdin.close()
    stdout_capture, stderr_capture, timed_out = await _collect_bounded_output(process, request_id, timeout, unit_name)
    if timed_out:
        return "\n\n".join([
            f"Request ID: {request_id}",
            f"Command: {_command_display(args)}",
            f"Error: Agent command timed out after {timeout} seconds.",
            f"Duration: {time.time() - started:.2f}s",
            f"Working Directory: {run_cwd}",
        ])
    stdout, stdout_log = stdout_capture.text(max_output_chars)
    stderr, stderr_log = stderr_capture.text(max_output_chars)
    if stdout_log:
        stdout = stdout.rstrip()
        extra_stdout_log = None
    else:
        stdout, extra_stdout_log = _truncate_output("STDOUT", stdout.rstrip(), request_id, max_output_chars)
    if stderr_log:
        stderr = stderr.rstrip()
        extra_stderr_log = None
    else:
        stderr, extra_stderr_log = _truncate_output("STDERR", stderr.rstrip(), request_id, max_output_chars)
    response = [
        f"Request ID: {request_id}",
        f"Command: {_command_display(args)}",
        f"Exit Code: {process.returncode}",
        f"Duration: {time.time() - started:.2f}s",
        f"Working Directory: {run_cwd}",
    ]
    if stdout.strip():
        response.append(f"STDOUT:\n{stdout}")
    if stderr.strip():
        response.append(f"STDERR:\n{stderr}")
    logs = [path for path in [stdout_log, stderr_log, extra_stdout_log, extra_stderr_log] if path]
    if logs:
        response.append("Full Output Logs:\n" + "\n".join(logs))
    return "\n\n".join(response)

async def _start_agent_cli(
    args: list[str],
    prompt_stdin: str | None,
    run_cwd: Path,
    session: SessionState,
    env: dict[str, str] | None,
) -> str:
    process_id = f"proc-{_now_ms()}-{uuid.uuid4().hex[:8]}"
    process, unit_name = await _spawn_workload(
        args, shell=False, session=session, request_id=process_id, cwd=run_cwd, env=env,
        stdin=asyncio.subprocess.PIPE if prompt_stdin is not None else None,
    )
    state = ProcessState(process_id, session.session_id, _command_display(args), run_cwd, process, unit_name=unit_name)
    async with state_lock:
        processes[process_id] = state
    asyncio.create_task(_drain_stream(process.stdout, state.stdout_lines, "stdout_closed", state))
    asyncio.create_task(_drain_stream(process.stderr, state.stderr_lines, "stderr_closed", state))
    if prompt_stdin is not None and process.stdin is not None:
        process.stdin.write(prompt_stdin.encode("utf-8"))
        await process.stdin.drain()
        process.stdin.close()
    session.updated_at = time.time()
    return "\n".join([
        f"Process ID: {process_id}",
        f"Session ID: {session.session_id}",
        f"PID: {process.pid}",
        f"Workload Unit: {unit_name or 'process-group'}",
        f"Command: {_command_display(args)}",
        f"Working Directory: {run_cwd}",
        "Status: running",
    ])

@mcp.tool()
async def run_codex_yolo(
    instruction: str,
    session_id: str = "default",
    cwd: str | None = None,
    model: str | None = None,
    add_dirs: list[str] | None = None,
    extra_args: list[str] | None = None,
    timeout: int = 1800,
    max_output_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
    env: dict[str, str] | None = None,
) -> str:
    """Run Codex non-interactively with approval prompts and sandbox disabled. Uses codex exec and returns when the job exits."""
    if _agent_cli_available("codex") is None:
        return "Error: codex CLI is not available in PATH."
    session = await _get_session(session_id)
    run_cwd = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    args = _codex_yolo_args(instruction, run_cwd, model, add_dirs, extra_args)
    return await _run_agent_cli(args, instruction, run_cwd, timeout, session, env, max_output_chars)

@mcp.tool()
async def start_codex_yolo(
    instruction: str,
    session_id: str = "default",
    cwd: str | None = None,
    model: str | None = None,
    add_dirs: list[str] | None = None,
    extra_args: list[str] | None = None,
    env: dict[str, str] | None = None,
) -> str:
    """Start a background Codex job with approval prompts and sandbox disabled. Poll with read_process and stop with stop_process."""
    if _agent_cli_available("codex") is None:
        return "Error: codex CLI is not available in PATH."
    session = await _get_session(session_id)
    run_cwd = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    args = _codex_yolo_args(instruction, run_cwd, model, add_dirs, extra_args)
    return await _start_agent_cli(args, instruction, run_cwd, session, env)

@mcp.tool()
async def run_agy_yolo(
    instruction: str,
    session_id: str = "default",
    cwd: str | None = None,
    model: str | None = None,
    add_dirs: list[str] | None = None,
    extra_args: list[str] | None = None,
    max_output_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
    env: dict[str, str] | None = None,
) -> str:
    """Run Antigravity/agy non-interactively with permission prompts disabled. Uses agy --print and waits for the CLI/model to finish without an MCP-side timeout."""
    if _agent_cli_available("agy") is None:
        return "Error: agy CLI is not available in PATH."
    session = await _get_session(session_id)
    run_cwd = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    args = _agy_yolo_args(instruction, model, add_dirs, extra_args)
    return await _run_agent_cli(args, None, run_cwd, None, session, env, max_output_chars)

@mcp.tool()
async def start_agy_yolo(
    instruction: str,
    session_id: str = "default",
    cwd: str | None = None,
    model: str | None = None,
    add_dirs: list[str] | None = None,
    extra_args: list[str] | None = None,
    env: dict[str, str] | None = None,
) -> str:
    """Start a background Antigravity/agy job with permission prompts disabled. Poll with read_process and stop with stop_process."""
    if _agent_cli_available("agy") is None:
        return "Error: agy CLI is not available in PATH."
    session = await _get_session(session_id)
    run_cwd = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    args = _agy_yolo_args(instruction, model, add_dirs, extra_args)
    return await _start_agent_cli(args, None, run_cwd, session, env)

@mcp.tool()
async def stat_path(path: str, session_id: str = "default", cwd: str | None = None) -> str:
    """Return metadata for a file or directory without using shell commands."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    target = _resolve_path(path, session, cwd)
    if not target.exists():
        return f"Path: {target}\nExists: false"
    stat = target.stat()
    return "\n".join([
        f"Path: {target}",
        "Exists: true",
        f"Type: {'directory' if target.is_dir() else 'file' if target.is_file() else 'other'}",
        f"Size: {stat.st_size}",
        f"Mode: {oct(stat.st_mode)}",
        f"Modified: {_format_timestamp(stat.st_mtime)}",
    ])

@mcp.tool()
async def list_dir(
    path: str = ".",
    session_id: str = "default",
    cwd: str | None = None,
    recursive: bool = False,
    max_entries: int = 500,
) -> str:
    """List directory entries without using shell commands."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    root = _resolve_path(path, session, cwd)
    if not root.exists():
        return f"Error: path does not exist: {root}"
    if not root.is_dir():
        return f"Error: path is not a directory: {root}"
    entries: list[str] = []
    count = 0
    iterator = root.rglob("*") if recursive else root.iterdir()
    for item in sorted(iterator, key=lambda p: str(p)):
        count += 1
        if len(entries) >= max_entries:
            continue
        rel = item.relative_to(root) if item != root else item.name
        suffix = "/" if item.is_dir() else ""
        entries.append(f"{rel}{suffix}")
    header = [f"Directory: {root}", f"Entries: {count}"]
    if count > max_entries:
        header.append(f"Returned: first {max_entries} entries")
    return "\n".join(header + [""] + entries)

@mcp.tool()
async def read_file(
    path: str,
    session_id: str = "default",
    cwd: str | None = None,
    start_line: int = 1,
    max_lines: int = 400,
    max_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
    line_numbers: bool = True,
) -> str:
    """Read a text file directly, with optional line range and truncation."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    target = _resolve_path(path, session, cwd)
    if not target.exists():
        return f"Error: file does not exist: {target}"
    if not target.is_file():
        return f"Error: path is not a file: {target}"
    text = target.read_text(encoding="utf-8", errors="replace")
    lines, total_lines, actual_start = _line_slice(text, start_line, max_lines)
    body = _format_file_lines(lines, actual_start) if line_numbers else "\n".join(lines)
    truncated_by_chars = False
    if max_chars > 0 and len(body) > max_chars:
        body = body[:max_chars] + f"\n[truncated to {max_chars} chars]"
        truncated_by_chars = True
    header = [
        f"Path: {target}",
        f"Total Lines: {total_lines}",
        f"Returned Lines: {actual_start}-{actual_start + len(lines) - 1 if lines else actual_start - 1}",
    ]
    if truncated_by_chars:
        header.append(f"Char Limit: {max_chars}")
    return "\n".join(header + ["", body])


def _watch_image_error(message: str) -> mcp_types.CallToolResult:
    return mcp_types.CallToolResult(
        content=[mcp_types.TextContent(type="text", text=message)],
        isError=True,
    )


def _gif_frame_count(data: bytes) -> int | None:
    """Return GIF image-frame count, or None when the file structure is invalid."""
    if len(data) < 13 or data[:6] not in {b"GIF87a", b"GIF89a"}:
        return None
    offset = 13
    packed = data[10]
    if packed & 0x80:
        offset += 3 * (2 ** ((packed & 0x07) + 1))
    frames = 0

    def skip_sub_blocks(position: int) -> int | None:
        while position < len(data):
            size = data[position]
            position += 1
            if size == 0:
                return position
            position += size
            if position > len(data):
                return None
        return None

    while offset < len(data):
        marker = data[offset]
        if marker == 0x3B:  # trailer
            return frames
        if marker == 0x21:  # extension block
            if offset + 2 > len(data):
                return None
            offset = skip_sub_blocks(offset + 2)
            if offset is None:
                return None
            continue
        if marker == 0x2C:  # image descriptor
            if offset + 10 > len(data):
                return None
            descriptor_packed = data[offset + 9]
            offset += 10
            if descriptor_packed & 0x80:
                offset += 3 * (2 ** ((descriptor_packed & 0x07) + 1))
            if offset >= len(data):
                return None
            offset += 1  # LZW minimum code size
            offset = skip_sub_blocks(offset)
            if offset is None:
                return None
            frames += 1
            continue
        return None
    return None


def _watch_image_mime(data: bytes) -> tuple[str | None, str | None]:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", None
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", None
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp", None
    if data[:6] in {b"GIF87a", b"GIF89a"}:
        frames = _gif_frame_count(data)
        if frames is None:
            return None, "Error: malformed GIF image."
        if frames != 1:
            return None, f"Error: animated GIFs are not supported; detected {frames} image frames."
        return "image/gif", None
    return None, "Error: unsupported image format. Supported formats: PNG, JPEG, WEBP, and non-animated GIF."


@mcp.tool(
    annotations=mcp_types.ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    )
)
async def watch_image(
    path: str,
    session_id: str = "default",
    cwd: str | None = None,
) -> mcp_types.CallToolResult:
    """Return a local image as native MCP ImageContent so the model can inspect it visually."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    gate = _require_project_context(session, run_cwd)
    if gate:
        return _watch_image_error(gate)

    target = _resolve_path(path, session, cwd)
    if not target.exists():
        return _watch_image_error(f"Error: image file does not exist: {target}")
    if not target.is_file():
        return _watch_image_error(f"Error: image path is not a regular file: {target}")

    max_bytes = max(1, WATCH_IMAGE_MAX_BYTES)
    try:
        with target.open("rb") as handle:
            data = handle.read(max_bytes + 1)
    except OSError as exc:
        return _watch_image_error(f"Error reading image {target}: {type(exc).__name__}: {exc}")

    if not data:
        return _watch_image_error(f"Error: image file is empty: {target}")
    if len(data) > max_bytes:
        return _watch_image_error(
            f"Error: image exceeds MCP_WATCH_IMAGE_MAX_BYTES ({max_bytes} bytes): {target}"
        )

    mime_type, error = _watch_image_mime(data)
    if error or mime_type is None:
        return _watch_image_error(f"{error or 'Error: unsupported image format.'}\nPath: {target}")

    encoded = base64.b64encode(data).decode("ascii")
    return mcp_types.CallToolResult(
        content=[
            mcp_types.ImageContent(type="image", data=encoded, mimeType=mime_type),
            mcp_types.TextContent(
                type="text",
                text=(
                    f"Image path: {target}\n"
                    f"MIME type: {mime_type}\n"
                    f"Bytes: {len(data)}\n"
                    "Payload: original file bytes"
                ),
            ),
        ],
        isError=False,
    )

@mcp.tool()
async def write_file(
    path: str,
    content: str,
    session_id: str = "default",
    cwd: str | None = None,
    create_dirs: bool = True,
    mode: str = "overwrite",
) -> str:
    """Write text to a file directly. This avoids giant shell heredocs for code generation."""
    session = await _get_session(session_id)
    target = _resolve_path(path, session, cwd)
    gate = _require_project_context(session, _operation_cwd(session, cwd))
    if gate:
        return gate
    if create_dirs:
        target.parent.mkdir(parents=True, exist_ok=True)
    if mode not in {"overwrite", "append", "error_if_exists"}:
        return "Error: mode must be overwrite, append, or error_if_exists."
    if mode == "error_if_exists" and target.exists():
        return f"Error: file already exists: {target}"
    if mode == "append":
        with target.open("a", encoding="utf-8", errors="replace") as handle:
            handle.write(content)
    else:
        target.write_text(content, encoding="utf-8", errors="replace")
    return "\n".join([f"Path: {target}", f"Bytes Written: {len(content.encode('utf-8'))}", f"Mode: {mode}"])

@_hidden_tool()
async def append_file(path: str, content: str, session_id: str = "default", cwd: str | None = None) -> str:
    """Append text to a file directly."""
    return await write_file(path=path, content=content, session_id=session_id, cwd=cwd, create_dirs=True, mode="append")

@mcp.tool()
async def replace_in_file(
    path: str,
    old: str,
    new: str,
    session_id: str = "default",
    cwd: str | None = None,
    count: int = 0,
) -> str:
    """Replace exact text inside a file without shell commands."""
    session = await _get_session(session_id)
    target = _resolve_path(path, session, cwd)
    gate = _require_project_context(session, _operation_cwd(session, cwd))
    if gate:
        return gate
    if not target.exists() or not target.is_file():
        return f"Error: file does not exist: {target}"
    text = target.read_text(encoding="utf-8", errors="replace")
    occurrences = text.count(old)
    if occurrences == 0:
        return f"Error: old text not found in {target}"
    replace_count = count if count > 0 else occurrences
    updated = text.replace(old, new, replace_count)
    target.write_text(updated, encoding="utf-8", errors="replace")
    return "\n".join([f"Path: {target}", f"Occurrences Found: {occurrences}", f"Occurrences Replaced: {min(replace_count, occurrences)}"])

@mcp.tool()
async def apply_patch(
    patch: str,
    session_id: str = "default",
    cwd: str | None = None,
    check_only: bool = False,
) -> str:
    """Apply a unified diff using git apply. Passing the patch as structured tool input avoids shell heredoc commands."""
    session = await _get_session(session_id)
    run_cwd = _resolve_cwd(cwd, session.cwd) if cwd else session.cwd
    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate
    args = ["git", "apply", "--whitespace=nowarn"]
    if check_only:
        args.append("--check")
    started = time.time()
    def _run() -> subprocess.CompletedProcess[str]:
        return subprocess.run(args, input=patch, text=True, capture_output=True, cwd=str(run_cwd), timeout=60)
    try:
        result = await asyncio.to_thread(_run)
    except subprocess.TimeoutExpired:
        return f"Error: git apply timed out.\nWorking Directory: {run_cwd}"
    parts = [
        f"Command: {' '.join(args)}",
        f"Working Directory: {run_cwd}",
        f"Exit Code: {result.returncode}",
        f"Duration: {time.time() - started:.2f}s",
    ]
    if result.stdout.strip():
        parts.append("STDOUT:\n" + result.stdout.rstrip())
    if result.stderr.strip():
        parts.append("STDERR:\n" + result.stderr.rstrip())
    return "\n\n".join(parts)

@mcp.tool()
async def make_dir(path: str, session_id: str = "default", cwd: str | None = None) -> str:
    """Create a directory and parents without shell commands."""
    session = await _get_session(session_id)
    target = _resolve_path(path, session, cwd)
    gate = _require_project_context(session, _operation_cwd(session, cwd))
    if gate:
        return gate
    target.mkdir(parents=True, exist_ok=True)
    return f"Directory created: {target}"

@mcp.tool()
async def copy_path(src: str, dst: str, session_id: str = "default", cwd: str | None = None, overwrite: bool = False) -> str:
    """Copy a file or directory without shell commands."""
    session = await _get_session(session_id)
    source = _resolve_path(src, session, cwd)
    target = _resolve_path(dst, session, cwd)
    gate = _require_project_context(session, _operation_cwd(session, cwd))
    if gate:
        return gate
    if not source.exists():
        return f"Error: source does not exist: {source}"
    if target.exists() and not overwrite:
        return f"Error: destination exists: {target}"
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        if target.exists() and overwrite:
            shutil.rmtree(target)
        shutil.copytree(source, target)
    else:
        shutil.copy2(source, target)
    return f"Copied: {source} -> {target}"

@mcp.tool()
async def move_path(src: str, dst: str, session_id: str = "default", cwd: str | None = None, overwrite: bool = False) -> str:
    """Move or rename a file/directory without shell commands."""
    session = await _get_session(session_id)
    source = _resolve_path(src, session, cwd)
    target = _resolve_path(dst, session, cwd)
    gate = _require_project_context(session, _operation_cwd(session, cwd))
    if gate:
        return gate
    if not source.exists():
        return f"Error: source does not exist: {source}"
    if target.exists():
        if not overwrite:
            return f"Error: destination exists: {target}"
        if target.is_dir():
            shutil.rmtree(target)
        else:
            target.unlink()
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))
    return f"Moved: {source} -> {target}"


def _build_startup_instructions() -> str:
    """Build the context sent in the MCP initialize result for each new client session."""
    _ensure_gpt_layout()
    run_cwd = WORKSPACE_DIR.resolve()
    root = _find_project_root(run_cwd)
    rows, fingerprint = GPT_STORE.global_agents_snapshot()
    skills = _discover_skills(root)
    tools = _public_tool_manifest()
    nested_servers = _configured_mcp_manifest(root)
    parts = [
        "You are connected to the isolated Terminal GPT Experimental MCP.",
        "STRICT REQUIREMENT: At the beginning of each genuinely new model thread, call bootstrap_thread with a unique stable thread_id before substantive terminal work.",
        "Reuse that exact thread_id as session_id on every later tool call. The shared session_id='default' is rejected for gated work.",
        "If context is compacted, forgotten, changed, or uncertain, call get_thread_context before continuing.",
        "When the user writes /goal <objective>, call thread_goal(action='set') with explicit finish conditions and continue until each condition has evidence and the goal is completed, unless a real technical error blocks further work.",
        "The following global .GPT instructions are mandatory and are separate from Codex's ~/.codex/AGENTS.md.",
        "Project-specific .GPT instructions are loaded after bootstrap_thread receives the target cwd.",
        "",
        f"Startup Context Fingerprint: {fingerprint}",
        f"GPT Home: {_gpt_home()}",
        f"Usage Database: {_thread_db_path()}",
        "",
        "## Mandatory .GPT instructions",
    ]
    for path, content in rows:
        parts.extend([f"===== {path} =====", content.rstrip(), ""])
    if not rows:
        parts.extend(["No .GPT instruction file was discovered.", ""])
    parts.append(f"## Available skills ({len(skills)})")
    for skill in skills:
        parts.append(f"- {skill['name']}")
    if not skills:
        parts.append("- No skills discovered.")
    parts.extend(["", f"## Public Terminal GPT tools ({len(tools)})"])
    for tool in tools:
        parts.append(f"- {tool['name']}")
    parts.extend(["", f"## Configured nested MCP servers ({len(nested_servers)})"])
    for server in nested_servers:
        parts.append(f"- {server['name']}")
    if not nested_servers:
        parts.append("- No nested MCP servers discovered.")
    parts.extend([
        "",
        "Context recovery tools: bootstrap_thread, get_thread_context, context_manifest, refresh_startup_context.",
        "Usage tools: record_token_usage and get_token_usage.",
        "Exact model token usage is unavailable to MCP unless the host/provider reports it through record_token_usage; automatic context counts are marked as estimates.",
    ])
    return "\n".join(parts).rstrip()


def _set_startup_instructions() -> str:
    instructions = _build_startup_instructions()
    mcp._mcp_server.instructions = instructions
    return instructions


def _install_dynamic_initialization() -> None:
    """Refresh global startup context immediately before every MCP initialize response."""
    server = mcp._mcp_server
    original = server.create_initialization_options

    def create_initialization_options(
        notification_options: Any = None,
        experimental_capabilities: dict[str, dict[str, Any]] | None = None,
    ) -> Any:
        _set_startup_instructions()
        return original(notification_options, experimental_capabilities)

    server.create_initialization_options = create_initialization_options  # type: ignore[method-assign]


# Keep discovery deterministic and intentionally small. The recovery/bootstrap
# tools are deliberately first so they are visible before any gated work.
PUBLIC_TOOL_ORDER = (
    "bootstrap_thread", "get_thread_context", "context_manifest", "refresh_startup_context",
    "thread_goal", "record_token_usage", "get_token_usage", "project_context", "local_skills", "local_mcp",
    "node_repl_js", "node_repl_js_reset",
    "run_command", "start_process", "poll_process", "stop_process", "set_session_env",
    "read_file", "watch_image", "write_file", "replace_in_file", "apply_patch", "list_dir", "stat_path",
    "make_dir", "copy_path", "move_path", "run_codex_yolo", "start_codex_yolo",
    "run_agy_yolo", "start_agy_yolo",
)
mcp._tool_manager._tools = {name: mcp._tool_manager._tools[name] for name in PUBLIC_TOOL_ORDER}
_set_startup_instructions()
_install_dynamic_initialization()


def main() -> None:
    parser = argparse.ArgumentParser(description="Terminal MCP Server")
    parser.add_argument("--transport", choices=["stdio", "sse", "streamable-http"], default="stdio", help="Transport mode")
    parser.add_argument("--host", default="127.0.0.1", help="Host for SSE/HTTP server")
    parser.add_argument("--port", type=int, default=8011, help="Port for SSE/HTTP server")
    parser.add_argument("--log-level", default=os.environ.get("MCP_UVICORN_LOG_LEVEL", "info"))
    args = parser.parse_args()
    watchdog_config = ChatWatchdogConfig.from_env()
    if args.transport in ["sse", "streamable-http"]:
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware
        app = mcp.sse_app() if args.transport == "sse" else mcp.streamable_http_app()
        chat_watchdog = ChatWatchdog(watchdog_config)
        install_usage_dashboard(app, GPT_STORE, chat_watchdog)
        install_chat_watchdog_lifespan(app, chat_watchdog)
        bearer_token = os.environ.get("MCP_BEARER_TOKEN", "")
        if bearer_token:
            app.add_middleware(BearerAuthMiddleware, token=bearer_token)
        elif args.host not in {"127.0.0.1", "::1", "localhost"}:
            print(
                "WARNING: Terminal MCP is listening beyond loopback without MCP_BEARER_TOKEN.",
                file=sys.stderr,
            )
        cors_origins = os.environ.get("MCP_CORS_ORIGINS", "*")
        allow_origins = [origin.strip() for origin in cors_origins.split(",") if origin.strip()]
        app.add_middleware(CORSMiddleware, allow_origins=allow_origins or ["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
        privacy_proxy_path = os.environ.get("MCP_PRIVACY_PROXY_PATH", "").strip()
        privacy_proxy_upstream = os.environ.get("MCP_PRIVACY_PROXY_UPSTREAM", "").strip()
        if bool(privacy_proxy_path) != bool(privacy_proxy_upstream):
            raise RuntimeError("MCP_PRIVACY_PROXY_PATH and MCP_PRIVACY_PROXY_UPSTREAM must be set together")
        if privacy_proxy_path:
            app.add_middleware(
                PrivacyCloneProxyMiddleware,
                path=privacy_proxy_path,
                upstream=privacy_proxy_upstream,
            )
        print(f"Starting Terminal MCP Server in {args.transport.upper()} mode on {args.host}:{args.port}...", file=sys.stderr)
        uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    else:
        if watchdog_config.enabled:
            print("ChatGPT thread watchdog is disabled for stdio transport; use SSE or streamable-http.", file=sys.stderr)
        mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
