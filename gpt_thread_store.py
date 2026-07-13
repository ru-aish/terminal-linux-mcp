from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable

import tiktoken


DEFAULT_GPT_AGENTS = """# GPT Agent Instructions

These instructions apply to the experimental Terminal GPT MCP and are separate
from Codex's `~/.codex/AGENTS.md`.

## Mandatory operating rules

1. Treat every instruction in this file and every applicable project `.GPT/AGENTS.md` as mandatory.
2. Start each new model thread by calling `bootstrap_thread` with a unique, stable `thread_id`.
3. Reuse that same value as `session_id` for all later terminal tools in the thread.
4. If context is lost, compacted, changed, or uncertain, call `get_thread_context` before continuing.
5. Read relevant listed skills before performing specialized work.
6. Inspect nested MCP tool schemas before invoking them.
7. Do not expose secrets, credentials, private tokens, or unrelated private data.
8. For code changes, implement thoroughly, test meaningful success and failure paths,
   self-review the final diff, and report limitations honestly.
"""


class GPTThreadStore:
    """Persistent GPT instruction, thread, and usage state for the experimental MCP."""

    def __init__(
        self,
        workspace_provider: Callable[[], Path],
        *,
        home_env: str = "MCP_GPT_HOME",
        db_name: str = "thread_usage.db",
        default_agents: str = DEFAULT_GPT_AGENTS,
    ) -> None:
        self._workspace_provider = workspace_provider
        self.home_env = home_env
        self.db_name = db_name
        self.default_agents = default_agents

    def home(self) -> Path:
        configured = os.environ.get(self.home_env) or os.environ.get("GPT_HOME") or "~/.GPT"
        # Resolve the default against HOME explicitly. Both pathlib and
        # os.path.expanduser may cache the account home directory, which breaks
        # isolated host/test environments that set HOME after import.
        if configured == "~":
            configured = os.environ.get("HOME", str(Path.home()))
        elif configured.startswith("~/"):
            configured = str(Path(os.environ.get("HOME", str(Path.home()))) / configured[2:])
        return Path(configured).expanduser().resolve()

    def agents_path(self) -> Path:
        return self.home() / "AGENTS.md"

    def db_path(self) -> Path:
        return self.home() / self.db_name

    def ensure_layout(self) -> Path:
        home = self.home()
        home.mkdir(parents=True, exist_ok=True)
        agents = self.agents_path()
        if not agents.exists():
            agents.write_text(self.default_agents, encoding="utf-8")
        self._init_db()
        return home

    _tokenizer: tiktoken.Encoding | None = None

    @classmethod
    def tokenizer(cls) -> tiktoken.Encoding:
        """Return the GPT-5-family tokenizer once per process."""
        if cls._tokenizer is None:
            cls._tokenizer = tiktoken.get_encoding("o200k_base")
        return cls._tokenizer

    @classmethod
    def estimate_tokens(cls, text: str) -> int:
        """Count text with o200k_base; this is still an estimate of host usage.

        The tokenizer precisely counts the text passed to it, but an MCP cannot
        see the host's complete prompt envelope, hidden context, or image-token
        accounting. Callers must therefore label these values as proxy estimates.
        """
        if not text:
            return 0
        return len(cls.tokenizer().encode(text, disallowed_special=()))

    @staticmethod
    def _now() -> str:
        return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def _connect(self) -> sqlite3.Connection:
        path = self.db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _init_db(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS threads (
                    thread_id TEXT PRIMARY KEY,
                    cwd TEXT NOT NULL,
                    context_fingerprint TEXT NOT NULL DEFAULT '',
                    context_loaded_at TEXT,
                    bootstrap_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS usage_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source TEXT NOT NULL,
                    model TEXT NOT NULL DEFAULT '',
                    request_id TEXT NOT NULL DEFAULT '',
                    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(input_tokens >= 0),
                    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK(output_tokens >= 0),
                    cached_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK(cached_input_tokens >= 0),
                    is_exact INTEGER NOT NULL DEFAULT 0 CHECK(is_exact IN (0, 1)),
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(thread_id) REFERENCES threads(thread_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_usage_events_thread_created
                    ON usage_events(thread_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_usage_events_request
                    ON usage_events(request_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_usage_events_request_unique
                    ON usage_events(thread_id, source, request_id)
                    WHERE request_id <> '';
                """
            )

    def upsert_thread(
        self,
        thread_id: str,
        cwd: Path,
        fingerprint: str = "",
        *,
        loaded: bool = False,
    ) -> None:
        self._init_db()
        now = self._now()
        with self._connect() as connection:
            existing = connection.execute(
                "SELECT bootstrap_count, created_at FROM threads WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            bootstrap_count = int(existing["bootstrap_count"]) if existing else 0
            created_at = str(existing["created_at"]) if existing else now
            if loaded:
                bootstrap_count += 1
            connection.execute(
                """
                INSERT INTO threads (
                    thread_id, cwd, context_fingerprint, context_loaded_at,
                    bootstrap_count, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(thread_id) DO UPDATE SET
                    cwd = excluded.cwd,
                    context_fingerprint = excluded.context_fingerprint,
                    context_loaded_at = excluded.context_loaded_at,
                    bootstrap_count = excluded.bootstrap_count,
                    updated_at = excluded.updated_at
                """,
                (
                    thread_id,
                    str(cwd),
                    fingerprint,
                    now if loaded else None,
                    bootstrap_count,
                    created_at,
                    now,
                ),
            )

    def record_usage(
        self,
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
        counts = (input_tokens, output_tokens, cached_input_tokens)
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in counts):
            raise ValueError("token counts must be non-negative integers")
        metadata_json = json.dumps(metadata or {}, sort_keys=True)
        if len(metadata_json.encode("utf-8")) > 65536:
            raise ValueError("metadata must be no larger than 64 KiB")

        self._init_db()
        now = self._now()
        workspace = self._workspace_provider().resolve()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO threads (
                    thread_id, cwd, context_fingerprint, context_loaded_at,
                    bootstrap_count, created_at, updated_at
                ) VALUES (?, ?, '', NULL, 0, ?, ?)
                ON CONFLICT(thread_id) DO UPDATE SET updated_at = excluded.updated_at
                """,
                (thread_id, str(workspace), now, now),
            )
            if request_id:
                existing = connection.execute(
                    "SELECT id FROM usage_events WHERE thread_id = ? AND source = ? AND request_id = ?",
                    (thread_id, source, request_id),
                ).fetchone()
                if existing:
                    return int(existing["id"])
            cursor = connection.execute(
                """
                INSERT INTO usage_events (
                    thread_id, event_type, source, model, request_id,
                    input_tokens, output_tokens, cached_input_tokens,
                    is_exact, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_id,
                    event_type,
                    source,
                    model,
                    request_id,
                    input_tokens,
                    output_tokens,
                    cached_input_tokens,
                    1 if is_exact else 0,
                    metadata_json,
                    now,
                ),
            )
            return int(cursor.lastrowid)

    def usage_summary(self, thread_id: str | None = None, limit: int = 100) -> dict[str, Any]:
        self._init_db()
        where = "WHERE thread_id = ?" if thread_id else ""
        params: tuple[Any, ...] = (thread_id,) if thread_id else ()
        event_limit = max(1, min(limit, 1000))
        with self._connect() as connection:
            totals = connection.execute(
                f"""
                SELECT
                    COUNT(*) AS events,
                    COALESCE(SUM(input_tokens), 0) AS input_tokens,
                    COALESCE(SUM(output_tokens), 0) AS output_tokens,
                    COALESCE(SUM(cached_input_tokens), 0) AS cached_input_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 1 THEN input_tokens ELSE 0 END), 0)
                        AS exact_input_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 1 THEN output_tokens ELSE 0 END), 0)
                        AS exact_output_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 1 THEN cached_input_tokens ELSE 0 END), 0)
                        AS exact_cached_input_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 0 THEN input_tokens ELSE 0 END), 0)
                        AS estimated_input_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 0 THEN output_tokens ELSE 0 END), 0)
                        AS estimated_output_tokens
                FROM usage_events {where}
                """,
                params,
            ).fetchone()
            recent = connection.execute(
                f"""
                SELECT id, thread_id, event_type, source, model, request_id,
                       input_tokens, output_tokens, cached_input_tokens,
                       is_exact, metadata_json, created_at
                FROM usage_events {where}
                ORDER BY id DESC LIMIT ?
                """,
                (*params, event_limit),
            ).fetchall()
            by_thread = connection.execute(
                """
                SELECT
                    t.thread_id, t.cwd, t.context_fingerprint, t.context_loaded_at,
                    t.bootstrap_count, t.created_at, t.updated_at,
                    COUNT(u.id) AS events,
                    COALESCE(SUM(CASE WHEN u.is_exact = 1 THEN u.input_tokens ELSE 0 END), 0)
                        AS exact_input_tokens,
                    COALESCE(SUM(CASE WHEN u.is_exact = 1 THEN u.output_tokens ELSE 0 END), 0)
                        AS exact_output_tokens,
                    COALESCE(SUM(CASE WHEN u.is_exact = 1 THEN u.cached_input_tokens ELSE 0 END), 0)
                        AS exact_cached_input_tokens,
                    COALESCE(SUM(CASE WHEN u.is_exact = 0 THEN u.input_tokens ELSE 0 END), 0)
                        AS estimated_input_tokens,
                    COALESCE(SUM(CASE WHEN u.is_exact = 0 THEN u.output_tokens ELSE 0 END), 0)
                        AS estimated_output_tokens
                FROM threads t
                LEFT JOIN usage_events u ON u.thread_id = t.thread_id
                WHERE (? IS NULL OR t.thread_id = ?)
                GROUP BY t.thread_id
                ORDER BY t.updated_at DESC
                """,
                (thread_id, thread_id),
            ).fetchall()
        return {
            "thread_id": thread_id,
            "database": str(self.db_path()),
            "totals": dict(totals),
            "by_thread": [dict(row) for row in by_thread],
            "recent_events": [
                {
                    **{key: row[key] for key in row.keys() if key != "metadata_json"},
                    "is_exact": bool(row["is_exact"]),
                    "metadata": json.loads(row["metadata_json"] or "{}"),
                }
                for row in recent
            ],
            "note": (
                "Exact model usage exists only when the host reports provider usage. "
                "Proxy estimates use the o200k_base tokenizer for MCP tool-call and tool-result text only; "
                "they exclude host prompt framing, hidden context, and non-text modality token costs. "
                "Exact model usage exists only when the host reports provider usage."
            ),
        }

    def global_agents_snapshot(self) -> tuple[list[tuple[Path, str]], str]:
        files: list[Path] = []
        override = self.home() / "AGENTS.override.md"
        normal = self.agents_path()
        if override.is_file():
            files.append(override)
        elif normal.is_file():
            files.append(normal)

        rows: list[tuple[Path, str]] = []
        digest = hashlib.sha256()
        for item in files:
            try:
                content = item.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                content = f"[unreadable: {type(exc).__name__}]"
            rows.append((item, content))
            digest.update(str(item.resolve()).encode("utf-8", errors="replace"))
            digest.update(b"\0")
            digest.update(content.encode("utf-8", errors="replace"))
            digest.update(b"\0")
        return rows, digest.hexdigest()

    def applicable_agents_files(
        self,
        cwd: Path,
        find_project_root: Callable[[Path], Path],
    ) -> tuple[Path, list[Path]]:
        """Return GPT-specific instructions without reading Codex's instruction tree."""
        cwd = cwd.resolve()
        root = find_project_root(cwd)
        files: list[Path] = []
        global_override = self.home() / "AGENTS.override.md"
        global_normal = self.agents_path()
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
            instruction_dir = directory / ".GPT"
            override = instruction_dir / "AGENTS.override.md"
            normal = instruction_dir / "AGENTS.md"
            if override.is_file():
                files.append(override)
            elif normal.is_file():
                files.append(normal)
        # The global store can itself sit at the discovered project root (for
        # example, when work is rooted at a user's home directory).  In that
        # case it must be injected once, not once as global and once as a
        # project-level .GPT directory.
        unique_files: list[Path] = []
        seen: set[Path] = set()
        for item in files:
            resolved = item.resolve()
            if resolved not in seen:
                seen.add(resolved)
                unique_files.append(item)
        return root, unique_files

    def agents_snapshot(
        self,
        cwd: Path,
        find_project_root: Callable[[Path], Path],
    ) -> tuple[Path, list[tuple[Path, str]], str]:
        root, paths = self.applicable_agents_files(cwd, find_project_root)
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
