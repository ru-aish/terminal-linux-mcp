from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import time
import tomllib
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

WORKSPACE_DIR = Path(os.environ.get("MCP_WORKSPACE", "~/mcp_workspace")).expanduser().resolve()
WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = Path(os.environ.get("MCP_LOG_DIR", "~/.mcp_terminal_logs")).expanduser().resolve()
LOG_DIR.mkdir(parents=True, exist_ok=True)
SHELL = os.environ.get("MCP_SHELL", "/bin/bash")
DEFAULT_TIMEOUT = int(os.environ.get("MCP_DEFAULT_TIMEOUT", "30"))
DEFAULT_MAX_OUTPUT_CHARS = int(os.environ.get("MCP_MAX_OUTPUT_CHARS", "24000"))
PROCESS_BUFFER_LINES = int(os.environ.get("MCP_PROCESS_BUFFER_LINES", "1000"))
AGENT_DIR_NAME = os.environ.get("MCP_AGENT_DIR_NAME", ".agent")
AGENT_CONTEXT_MAX_CHARS = int(os.environ.get("MCP_AGENT_CONTEXT_MAX_CHARS", "12000"))
MCP_PROXY_IDLE_TIMEOUT = int(os.environ.get("MCP_PROXY_IDLE_TIMEOUT", "1800"))


@contextlib.asynccontextmanager
async def _terminal_lifespan(_: Any):
    try:
        yield {}
    finally:
        await _close_all_mcp_connections()


mcp = FastMCP(
    "Terminal",
    instructions=(
        "You are connected to a native development terminal on the host machine.\n"
        "Use these tools for software development, files, builds, tests, packages, services, and process management.\n"
        "Use a stable session_id for each independent job; commands in one session are serialized while different sessions can run concurrently.\n"
        "Before the first executing or modifying action in a project, and whenever cwd or applicable instructions change, call project_context and follow every returned AGENTS.md instruction.\n"
        "Use local_skills to discover and read project or machine-local skills.\n"
        "Use local_mcp for MCP servers already configured on this machine; inspect a server's tools before calling it.\n"
        "Use run_command for Git, HTTP, databases, networking, services, containers, and package management."
    ),
    lifespan=_terminal_lifespan,
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


@dataclass
class PersistentMcpConnection:
    key: tuple[str, str]
    config_fingerprint: str
    stack: contextlib.AsyncExitStack
    client: Any
    exclusive_resource: str | None = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    active_calls: int = 0
    closed: bool = False


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


def _hidden_tool() -> Any:
    """Keep retired helpers callable internally without publishing MCP tools."""
    return lambda function: function

def _now_ms() -> int:
    return int(time.time() * 1000)

def _normalize_session_id(session_id: str | None) -> str:
    normalized = (session_id or "default").strip()
    return (normalized or "default")[:128]

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
        "agents_md": root / "AGENTS.md",
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
    cwd = cwd.resolve()
    root = _find_project_root(cwd)
    files: list[Path] = []
    global_override = Path.home() / ".codex" / "AGENTS.override.md"
    global_normal = Path.home() / ".codex" / "AGENTS.md"
    if global_override.is_file():
        files.append(global_override)
    elif global_normal.is_file():
        files.append(global_normal)

    chain: list[Path] = [root]
    if cwd != root:
        try:
            relative = cwd.relative_to(root)
            current = root
            for part in relative.parts:
                current = current / part
                chain.append(current)
        except ValueError:
            chain = [cwd]
    for directory in chain:
        override = directory / "AGENTS.override.md"
        normal = directory / "AGENTS.md"
        if override.is_file():
            files.append(override)
        elif normal.is_file():
            files.append(normal)
    return root, files


def _agents_snapshot(cwd: Path) -> tuple[Path, list[tuple[Path, str]], str]:
    root, paths = _applicable_agents_files(cwd)
    rows: list[tuple[Path, str]] = []
    digest = hashlib.sha256()
    digest.update(str(root).encode())
    digest.update(b"\0")
    digest.update(str(cwd.resolve()).encode())
    digest.update(b"\0")
    for item in paths:
        try:
            content = item.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            content = f"[unreadable: {type(exc).__name__}]"
        rows.append((item, content))
        digest.update(str(item.resolve()).encode("utf-8", errors="replace"))
        digest.update(b"\0")
        digest.update(content.encode("utf-8", errors="replace"))
        digest.update(b"\0")
    return root, rows, digest.hexdigest()


def _context_key(cwd: Path) -> str:
    return str(cwd.resolve())


def _context_gate_error(root: Path, cwd: Path, rows: list[tuple[Path, str]]) -> str:
    listed = "\n".join(f"- {path}" for path, _ in rows) or "- none"
    return (
        "Project context has not been loaded, or an applicable AGENTS.md changed.\n"
        "No action was executed.\n\n"
        "Call project_context with this cwd, follow the returned instructions, then retry.\n\n"
        f"Project Root: {root}\nWorking Directory: {cwd}\nApplicable Files:\n{listed}"
    )


def _require_project_context(session: SessionState, cwd: Path) -> str | None:
    cwd = cwd.resolve()
    root, rows, fingerprint = _agents_snapshot(cwd)
    key = _context_key(cwd)
    if not rows:
        session.context_fingerprints[key] = fingerprint
        return None
    if session.context_fingerprints.get(key) != fingerprint:
        return _context_gate_error(root, cwd, rows)
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


async def _open_mcp_connection(
    session_id: str,
    server_name: str,
    config: dict[str, Any],
) -> PersistentMcpConnection:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    from mcp.client.stdio import StdioServerParameters, stdio_client
    from mcp.client.streamable_http import streamablehttp_client

    runtime = _runtime_mcp_config(config)
    stack = contextlib.AsyncExitStack()
    try:
        if runtime.get("url"):
            if runtime["transport"] == "sse":
                streams = await stack.enter_async_context(
                    sse_client(runtime["url"], headers=runtime["headers"])
                )
            else:
                streams = await stack.enter_async_context(
                    streamablehttp_client(runtime["url"], headers=runtime["headers"])
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

        client = await stack.enter_async_context(ClientSession(streams[0], streams[1]))
        await client.initialize()
        return PersistentMcpConnection(
            key=(session_id, server_name),
            config_fingerprint=_mcp_config_fingerprint(config),
            stack=stack,
            client=client,
            exclusive_resource=_mcp_exclusive_resource(config),
        )
    except BaseException:
        await stack.aclose()
        raise


async def _close_mcp_connection(connection: PersistentMcpConnection) -> None:
    if connection.closed:
        return
    connection.closed = True
    with contextlib.suppress(Exception):
        await connection.stack.aclose()


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
        for _, connection in list(mcp_connections.items()):
            if connection.active_calls == 0 and connection.last_used <= cutoff:
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
            if connection is not None and (
                connection.closed or connection.config_fingerprint != fingerprint
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
            async with replaced.lock:
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
        async with connection.lock:
            await _close_mcp_connection(connection)
        await _release_mcp_resource(connection)
    return len(selected)


async def _close_all_mcp_connections() -> None:
    async with mcp_connections_lock:
        selected = list(mcp_connections.values())
        mcp_connections.clear()
        mcp_resource_owners.clear()
    for connection in selected:
        async with connection.lock:
            await _close_mcp_connection(connection)


async def _with_mcp_session(
    session_id: str,
    server_name: str,
    config: dict[str, Any],
    operation: str,
    tool_name: str = "",
    arguments: dict[str, Any] | None = None,
) -> Any:
    last_error: BaseException | None = None
    for attempt in range(2):
        connection = await _acquire_mcp_connection(session_id, server_name, config)
        try:
            async with connection.lock:
                if connection.closed:
                    continue
                try:
                    result = await (
                        connection.client.list_tools()
                        if operation == "tools"
                        else connection.client.call_tool(tool_name, arguments or {})
                    )
                except asyncio.CancelledError:
                    await _discard_mcp_connection(connection)
                    raise
                except BaseException as exc:
                    last_error = exc
                    await _discard_mcp_connection(connection)
                    if attempt == 0:
                        continue
                    raise
                connection.last_used = time.time()
                return result
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
) -> Any:
    """List, inspect, call, or reset configured MCP servers through persistent downstream connections."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    root = _find_project_root(run_cwd)
    servers = _discover_mcp_servers(root)
    action = action.strip().lower().replace("_", "-")
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

    gate = _require_project_context(session, run_cwd)
    if gate:
        return gate

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

    try:
        result = await asyncio.wait_for(
            _with_mcp_session(
                session.session_id,
                server,
                config,
                action,
                tool,
                arguments,
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
            modified = f"{command}\n\nprintf '\\n{marker}:%s\\n' \"$PWD\""
            process = await asyncio.create_subprocess_shell(
                modified,
                cwd=str(run_cwd),
                env=_build_env(session, env),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                executable=SHELL,
                start_new_session=True,
            )
            try:
                stdout_bytes, stderr_bytes = await asyncio.wait_for(process.communicate(), timeout=timeout)
            except asyncio.TimeoutError:
                await _terminate_process_group(process)
                return "\n\n".join([
                    f"Request ID: {request_id}",
                    f"Session ID: {session.session_id}",
                    f"Error: Command timed out after {timeout} seconds.",
                    f"Duration: {time.time() - started:.2f}s",
                    f"Working Directory: {session.cwd}",
                ])
            stdout_raw = stdout_bytes.decode("utf-8", errors="replace") if stdout_bytes else ""
            stderr_raw = stderr_bytes.decode("utf-8", errors="replace") if stderr_bytes else ""
            stdout_clean, new_cwd = _strip_pwd_marker(stdout_raw, marker)
            if new_cwd:
                candidate = Path(new_cwd).expanduser()
                if candidate.is_dir():
                    session.cwd = candidate.resolve()
            session.command_count += 1
            session.updated_at = time.time()
            stdout_clean, stdout_log = _truncate_output("STDOUT", stdout_clean, request_id, max_output_chars)
            stderr_clean, stderr_log = _truncate_output("STDERR", stderr_raw.rstrip(), request_id, max_output_chars)
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
            logs = [path for path in [stdout_log, stderr_log] if path]
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
        process = await asyncio.create_subprocess_shell(
            command,
            cwd=str(run_cwd),
            env=_build_env(session, env),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            executable=SHELL,
            start_new_session=True,
        )
        process_id = f"proc-{_now_ms()}-{uuid.uuid4().hex[:8]}"
        state = ProcessState(process_id, session.session_id, command, run_cwd, process)
        async with state_lock:
            processes[process_id] = state
        asyncio.create_task(_drain_stream(process.stdout, state.stdout_lines, "stdout_closed", state))
        asyncio.create_task(_drain_stream(process.stderr, state.stderr_lines, "stderr_closed", state))
        session.updated_at = time.time()
        return "\n".join([f"Process ID: {process_id}", f"Session ID: {session.session_id}", f"PID: {process.pid}", f"Command: {command}", f"Working Directory: {run_cwd}", "Status: running"])

@mcp.tool()
async def poll_process(process_id: str, max_lines: int = 200) -> str:
    """Read buffered stdout/stderr from a background process."""
    async with state_lock:
        state = processes.get(process_id)
    return f"Error: Unknown process_id: {process_id}" if state is None else _format_process(state, max_lines)

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
async def stop_process(process_id: str) -> str:
    """Stop a background process."""
    async with state_lock:
        state = processes.get(process_id)
    if state is None:
        return f"Error: Unknown process_id: {process_id}"
    await _terminate_process_group(state.process)
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


@mcp.tool()
async def project_context(
    session_id: str = "default",
    cwd: str | None = None,
    max_chars: int = DEFAULT_MAX_OUTPUT_CHARS,
) -> str:
    """Load every applicable AGENTS.md instruction and satisfy the execution gate for this cwd."""
    session = await _get_session(session_id)
    run_cwd = _operation_cwd(session, cwd)
    root, rows, fingerprint = _agents_snapshot(run_cwd)
    parts = [
        f"Session ID: {session.session_id}",
        f"Working Directory: {run_cwd}",
        f"Project Root: {root}",
        f"Context Fingerprint: {fingerprint}",
        f"Applicable Files: {len(rows)}",
        "",
    ]
    if rows:
        for path, content in rows:
            parts.extend([f"===== {path} =====", content.rstrip(), ""])
    else:
        parts.append("No applicable AGENTS.md or AGENTS.override.md files were found.")
    full = "\n".join(parts).rstrip()
    if max_chars > 0 and len(full) > max_chars:
        return full[:max_chars] + (
            f"\n\n[Context truncated to {max_chars} of {len(full)} chars. Context gate remains unsatisfied. "
            "Call project_context again with a larger max_chars.]"
        )
    session.context_fingerprints[_context_key(run_cwd)] = fingerprint
    session.updated_at = time.time()
    return full + "\n\nContext Gate: satisfied"


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
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(run_cwd),
        env=_build_env(session, env),
        stdin=asyncio.subprocess.PIPE if prompt_stdin is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        communicate = process.communicate(prompt_stdin.encode("utf-8") if prompt_stdin is not None else None)
        if timeout and timeout > 0:
            stdout_bytes, stderr_bytes = await asyncio.wait_for(communicate, timeout=timeout)
        else:
            stdout_bytes, stderr_bytes = await communicate
    except asyncio.TimeoutError:
        await _terminate_process_group(process)
        return "\n\n".join([
            f"Request ID: {request_id}",
            f"Command: {_command_display(args)}",
            f"Error: Agent command timed out after {timeout} seconds.",
            f"Duration: {time.time() - started:.2f}s",
            f"Working Directory: {run_cwd}",
        ])
    stdout = stdout_bytes.decode("utf-8", errors="replace").rstrip() if stdout_bytes else ""
    stderr = stderr_bytes.decode("utf-8", errors="replace").rstrip() if stderr_bytes else ""
    stdout, stdout_log = _truncate_output("STDOUT", stdout, request_id, max_output_chars)
    stderr, stderr_log = _truncate_output("STDERR", stderr, request_id, max_output_chars)
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
    logs = [path for path in [stdout_log, stderr_log] if path]
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
    process = await asyncio.create_subprocess_exec(
        *args,
        cwd=str(run_cwd),
        env=_build_env(session, env),
        stdin=asyncio.subprocess.PIPE if prompt_stdin is not None else None,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    process_id = f"proc-{_now_ms()}-{uuid.uuid4().hex[:8]}"
    state = ProcessState(process_id, session.session_id, _command_display(args), run_cwd, process)
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


# Keep discovery deterministic and intentionally small.  The server used to
# expose implementation-era helpers; only this contract is public now.
PUBLIC_TOOL_ORDER = (
    "project_context", "local_skills", "local_mcp", "run_command", "start_process",
    "poll_process", "stop_process", "set_session_env", "read_file", "write_file",
    "replace_in_file", "apply_patch", "list_dir", "stat_path", "make_dir", "copy_path",
    "move_path", "run_codex_yolo", "start_codex_yolo", "run_agy_yolo", "start_agy_yolo",
)
mcp._tool_manager._tools = {name: mcp._tool_manager._tools[name] for name in PUBLIC_TOOL_ORDER}

def main() -> None:
    parser = argparse.ArgumentParser(description="Terminal MCP Server")
    parser.add_argument("--transport", choices=["stdio", "sse", "streamable-http"], default="stdio", help="Transport mode")
    parser.add_argument("--host", default="127.0.0.1", help="Host for SSE/HTTP server")
    parser.add_argument("--port", type=int, default=8000, help="Port for SSE/HTTP server")
    parser.add_argument("--log-level", default=os.environ.get("MCP_UVICORN_LOG_LEVEL", "info"))
    args = parser.parse_args()
    if args.transport in ["sse", "streamable-http"]:
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware
        app = mcp.sse_app() if args.transport == "sse" else mcp.streamable_http_app()
        cors_origins = os.environ.get("MCP_CORS_ORIGINS", "*")
        allow_origins = [origin.strip() for origin in cors_origins.split(",") if origin.strip()]
        app.add_middleware(CORSMiddleware, allow_origins=allow_origins or ["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
        print(f"Starting Terminal MCP Server in {args.transport.upper()} mode on {args.host}:{args.port}...", file=sys.stderr)
        uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)
    else:
        mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
