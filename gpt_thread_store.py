from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from math import ceil, floor
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

## Goal strategy

When the user writes `/goal <objective>`, call `thread_goal(action="set")` with explicit finish conditions and keep working until every condition is evidenced and the goal is completed, unless a real technical error prevents further progress.
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

                CREATE TABLE IF NOT EXISTS thread_goals (
                    thread_id TEXT PRIMARY KEY,
                    objective TEXT NOT NULL,
                    finish_conditions_json TEXT NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('active', 'completed', 'technical_error')),
                    completion_evidence_json TEXT NOT NULL DEFAULT '[]',
                    technical_error_reason TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    completed_at TEXT,
                    technical_error_at TEXT,
                    FOREIGN KEY(thread_id) REFERENCES threads(thread_id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_thread_goals_status_seen
                    ON thread_goals(status, last_seen_at);
                """
            )

    @staticmethod
    def _clean_goal_text(value: str, *, field: str, max_chars: int) -> str:
        if not isinstance(value, str):
            raise ValueError(f"{field} must be a string")
        cleaned = " ".join(value.strip().split())
        if not cleaned:
            raise ValueError(f"{field} must not be empty")
        if len(cleaned) > max_chars:
            raise ValueError(f"{field} must be at most {max_chars} characters")
        return cleaned

    @classmethod
    def _clean_goal_list(
        cls,
        values: list[str] | None,
        *,
        field: str,
        max_items: int,
        max_item_chars: int,
    ) -> list[str]:
        if not isinstance(values, list) or not values:
            raise ValueError(f"{field} must be a non-empty list")
        if len(values) > max_items:
            raise ValueError(f"{field} must contain at most {max_items} items")
        return [
            cls._clean_goal_text(value, field=f"{field}[{index}]", max_chars=max_item_chars)
            for index, value in enumerate(values)
        ]

    @staticmethod
    def _goal_row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        return {
            "thread_id": row["thread_id"],
            "objective": row["objective"],
            "finish_conditions": json.loads(row["finish_conditions_json"] or "[]"),
            "status": row["status"],
            "completion_evidence": json.loads(row["completion_evidence_json"] or "[]"),
            "technical_error_reason": row["technical_error_reason"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_seen_at": row["last_seen_at"],
            "completed_at": row["completed_at"],
            "technical_error_at": row["technical_error_at"],
        }

    def set_goal(
        self,
        thread_id: str,
        objective: str,
        finish_conditions: list[str] | None,
    ) -> dict[str, Any]:
        objective = self._clean_goal_text(objective, field="objective", max_chars=8000)
        conditions = self._clean_goal_list(
            finish_conditions,
            field="finish_conditions",
            max_items=50,
            max_item_chars=2000,
        )
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
            connection.execute(
                """
                INSERT INTO thread_goals (
                    thread_id, objective, finish_conditions_json, status,
                    completion_evidence_json, technical_error_reason,
                    created_at, updated_at, last_seen_at, completed_at, technical_error_at
                ) VALUES (?, ?, ?, 'active', '[]', '', ?, ?, ?, NULL, NULL)
                ON CONFLICT(thread_id) DO UPDATE SET
                    objective = excluded.objective,
                    finish_conditions_json = excluded.finish_conditions_json,
                    status = 'active',
                    completion_evidence_json = '[]',
                    technical_error_reason = '',
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    last_seen_at = excluded.last_seen_at,
                    completed_at = NULL,
                    technical_error_at = NULL
                """,
                (thread_id, objective, json.dumps(conditions), now, now, now),
            )
            row = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        goal = self._goal_row(row)
        assert goal is not None
        return goal

    def get_goal(self, thread_id: str, *, mark_seen: bool = False) -> dict[str, Any] | None:
        self._init_db()
        now = self._now()
        with self._connect() as connection:
            if mark_seen:
                connection.execute(
                    "UPDATE thread_goals SET last_seen_at = ?, updated_at = ? WHERE thread_id = ?",
                    (now, now, thread_id),
                )
            row = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        return self._goal_row(row)

    def complete_goal(self, thread_id: str, evidence: list[str] | None) -> dict[str, Any]:
        self._init_db()
        now = self._now()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            goal = self._goal_row(row)
            if goal is None:
                raise ValueError("no goal exists for this thread")
            if goal["status"] != "active":
                raise ValueError(f"goal status must be active, not {goal['status']}")
            cleaned_evidence = self._clean_goal_list(
                evidence,
                field="evidence",
                max_items=50,
                max_item_chars=4000,
            )
            conditions = goal["finish_conditions"]
            if len(cleaned_evidence) != len(conditions):
                raise ValueError(
                    "completion requires exactly one evidence entry for each finish condition "
                    f"({len(conditions)} required, {len(cleaned_evidence)} provided)"
                )
            connection.execute(
                """
                UPDATE thread_goals
                SET status = 'completed', completion_evidence_json = ?,
                    technical_error_reason = '', updated_at = ?, last_seen_at = ?,
                    completed_at = ?, technical_error_at = NULL
                WHERE thread_id = ?
                """,
                (json.dumps(cleaned_evidence), now, now, now, thread_id),
            )
            updated = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        completed = self._goal_row(updated)
        assert completed is not None
        return completed

    def mark_goal_technical_error(self, thread_id: str, reason: str) -> dict[str, Any]:
        reason = self._clean_goal_text(reason, field="reason", max_chars=4000)
        self._init_db()
        now = self._now()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            if row is None:
                raise ValueError("no goal exists for this thread")
            if row["status"] != "active":
                raise ValueError(f"goal status must be active, not {row['status']}")
            connection.execute(
                """
                UPDATE thread_goals
                SET status = 'technical_error', technical_error_reason = ?,
                    updated_at = ?, last_seen_at = ?, technical_error_at = ?
                WHERE thread_id = ?
                """,
                (reason, now, now, now, thread_id),
            )
            updated = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        goal = self._goal_row(updated)
        assert goal is not None
        return goal

    def resume_goal(self, thread_id: str) -> dict[str, Any]:
        self._init_db()
        now = self._now()
        with self._connect() as connection:
            row = connection.execute(
                "SELECT status FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            if row is None:
                raise ValueError("no goal exists for this thread")
            if row["status"] != "technical_error":
                raise ValueError(f"only a technical_error goal can be resumed, not {row['status']}")
            connection.execute(
                """
                UPDATE thread_goals
                SET status = 'active', technical_error_reason = '',
                    updated_at = ?, last_seen_at = ?, technical_error_at = NULL
                WHERE thread_id = ?
                """,
                (now, now, thread_id),
            )
            updated = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        goal = self._goal_row(updated)
        assert goal is not None
        return goal

    def clear_goal(self, thread_id: str) -> bool:
        self._init_db()
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            )
        return cursor.rowcount > 0

    def claim_goal_reminder(
        self,
        thread_id: str,
        *,
        interval_seconds: int,
    ) -> dict[str, Any] | None:
        """Atomically claim one due reminder for an active thread goal."""
        self._init_db()
        interval = max(1, interval_seconds)
        now = datetime.now(timezone.utc)
        with self._connect() as connection:
            candidate = connection.execute(
                "SELECT last_seen_at FROM thread_goals WHERE thread_id = ? AND status = 'active'",
                (thread_id,),
            ).fetchone()
        if candidate is None:
            return None
        if (now - self._parse_utc(candidate["last_seen_at"])).total_seconds() < interval:
            return None

        now_text = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ? AND status = 'active'",
                (thread_id,),
            ).fetchone()
            if row is None:
                return None
            last_seen = self._parse_utc(row["last_seen_at"])
            if (now - last_seen).total_seconds() < interval:
                return None
            connection.execute(
                "UPDATE thread_goals SET last_seen_at = ?, updated_at = ? WHERE thread_id = ?",
                (now_text, now_text, thread_id),
            )
            updated = connection.execute(
                "SELECT * FROM thread_goals WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        return self._goal_row(updated)

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

    @staticmethod
    def _parse_utc(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    @staticmethod
    def _dashboard_bucket_seconds(hours: int) -> int:
        if hours <= 1:
            return 5 * 60
        if hours <= 6:
            return 15 * 60
        if hours <= 24:
            return 60 * 60
        return 6 * 60 * 60

    def dashboard_version(self) -> str:
        """Return a cheap append-only version marker for live dashboard clients."""
        self._init_db()
        with self._connect() as connection:
            last_event_id = int(
                connection.execute(
                    "SELECT COALESCE(MAX(id), 0) FROM usage_events"
                ).fetchone()[0]
            )
        return str(last_event_id)

    def dashboard_snapshot(self, hours: int = 24, event_limit: int = 40) -> dict[str, Any]:
        """Return a bounded, dashboard-oriented view of live usage accounting.

        Exact provider usage and proxy estimates intentionally remain separate:
        they can describe overlapping portions of the same model turn and must
        not be presented as a single billable-token total.
        """

        self._init_db()
        window_hours = max(1, min(int(hours), 24 * 7))
        recent_limit = max(1, min(int(event_limit), 100))
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=window_hours)
        active_cutoff = now - timedelta(minutes=10)
        cutoff_text = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
        active_cutoff_text = active_cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")

        with self._connect() as connection:
            totals = connection.execute(
                """
                SELECT
                    COUNT(*) AS events,
                    COALESCE(SUM(CASE WHEN event_type = 'mcp_tool_loop' THEN 1 ELSE 0 END), 0)
                        AS tool_calls,
                    COALESCE(SUM(CASE WHEN is_exact = 1 THEN input_tokens ELSE 0 END), 0)
                        AS exact_input_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 1 THEN output_tokens ELSE 0 END), 0)
                        AS exact_output_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 1 THEN cached_input_tokens ELSE 0 END), 0)
                        AS exact_cached_input_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 0 THEN input_tokens ELSE 0 END), 0)
                        AS estimated_input_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 0 THEN output_tokens ELSE 0 END), 0)
                        AS estimated_output_tokens,
                    COALESCE(SUM(CASE WHEN is_exact = 1 THEN 1 ELSE 0 END), 0)
                        AS exact_events,
                    COALESCE(SUM(CASE WHEN is_exact = 0 THEN 1 ELSE 0 END), 0)
                        AS estimated_events,
                    COALESCE(MAX(id), 0) AS last_event_id,
                    MAX(created_at) AS last_event_at
                FROM usage_events
                """
            ).fetchone()
            thread_total = int(
                connection.execute("SELECT COUNT(*) FROM threads").fetchone()[0]
            )
            active_threads = int(
                connection.execute(
                    "SELECT COUNT(DISTINCT thread_id) FROM usage_events WHERE created_at >= ?",
                    (active_cutoff_text,),
                ).fetchone()[0]
            )
            window_rows = list(reversed(connection.execute(
                """
                SELECT id, thread_id, event_type, source, model, request_id,
                       input_tokens, output_tokens, cached_input_tokens,
                       is_exact, metadata_json, created_at
                FROM usage_events
                WHERE created_at >= ?
                ORDER BY id DESC
                LIMIT 50000
                """,
                (cutoff_text,),
            ).fetchall()))
            thread_rows = connection.execute(
                """
                SELECT
                    t.thread_id,
                    t.cwd,
                    t.bootstrap_count,
                    t.updated_at,
                    COUNT(u.id) AS events,
                    COALESCE(SUM(CASE WHEN u.event_type = 'mcp_tool_loop' THEN 1 ELSE 0 END), 0)
                        AS tool_calls,
                    COALESCE(SUM(CASE WHEN u.is_exact = 1 THEN u.input_tokens ELSE 0 END), 0)
                        AS exact_input_tokens,
                    COALESCE(SUM(CASE WHEN u.is_exact = 1 THEN u.output_tokens ELSE 0 END), 0)
                        AS exact_output_tokens,
                    COALESCE(SUM(CASE WHEN u.is_exact = 0 THEN u.input_tokens ELSE 0 END), 0)
                        AS estimated_input_tokens,
                    COALESCE(SUM(CASE WHEN u.is_exact = 0 THEN u.output_tokens ELSE 0 END), 0)
                        AS estimated_output_tokens,
                    MAX(u.created_at) AS last_event_at
                FROM threads t
                LEFT JOIN usage_events u
                    ON u.thread_id = t.thread_id AND u.created_at >= ?
                GROUP BY t.thread_id
                ORDER BY COALESCE(MAX(u.created_at), t.updated_at) DESC
                LIMIT 30
                """,
                (cutoff_text,),
            ).fetchall()
            recent_rows = connection.execute(
                """
                SELECT id, thread_id, event_type, source, model, request_id,
                       input_tokens, output_tokens, cached_input_tokens,
                       is_exact, metadata_json, created_at
                FROM usage_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (recent_limit,),
            ).fetchall()

        bucket_seconds = self._dashboard_bucket_seconds(window_hours)
        start_epoch = floor(cutoff.timestamp() / bucket_seconds) * bucket_seconds
        bucket_count = max(
            1,
            min(120, ceil((now.timestamp() - start_epoch) / bucket_seconds) + 1),
        )
        series = [
            {
                "at": datetime.fromtimestamp(
                    start_epoch + index * bucket_seconds, tz=timezone.utc
                ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "exact_tokens": 0,
                "estimated_tokens": 0,
                "tool_calls": 0,
                "events": 0,
            }
            for index in range(bucket_count)
        ]

        tools: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "name": "unknown",
                "calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "last_seen": None,
            }
        )
        source_breakdown: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"events": 0, "input_tokens": 0, "output_tokens": 0}
        )
        window_exact_tokens = 0
        window_estimated_tokens = 0
        window_tool_calls = 0
        last_five_minutes = now - timedelta(minutes=5)
        calls_last_five_minutes = 0

        for row in window_rows:
            created_at = self._parse_utc(str(row["created_at"]))
            index = int((created_at.timestamp() - start_epoch) // bucket_seconds)
            amount = int(row["input_tokens"]) + int(row["output_tokens"])
            if 0 <= index < len(series):
                series[index]["events"] += 1
                if bool(row["is_exact"]):
                    series[index]["exact_tokens"] += amount
                else:
                    series[index]["estimated_tokens"] += amount
                if row["event_type"] == "mcp_tool_loop":
                    series[index]["tool_calls"] += 1

            if bool(row["is_exact"]):
                window_exact_tokens += amount
            else:
                window_estimated_tokens += amount

            source = str(row["source"] or "unknown")
            source_breakdown[source]["events"] += 1
            source_breakdown[source]["input_tokens"] += int(row["input_tokens"])
            source_breakdown[source]["output_tokens"] += int(row["output_tokens"])

            if row["event_type"] != "mcp_tool_loop":
                continue
            window_tool_calls += 1
            if created_at >= last_five_minutes:
                calls_last_five_minutes += 1
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError):
                metadata = {}
            tool_name = str(metadata.get("tool_name") or "unknown")[:120]
            tool = tools[tool_name]
            tool["name"] = tool_name
            tool["calls"] += 1
            tool["input_tokens"] += int(row["input_tokens"])
            tool["output_tokens"] += int(row["output_tokens"])
            tool["last_seen"] = row["created_at"]

        tool_rows = sorted(
            tools.values(),
            key=lambda item: (int(item["calls"]), str(item["last_seen"] or "")),
            reverse=True,
        )[:14]

        recent_events: list[dict[str, Any]] = []
        for row in recent_rows:
            try:
                metadata = json.loads(row["metadata_json"] or "{}")
            except (TypeError, ValueError):
                metadata = {}
            recent_events.append(
                {
                    "id": int(row["id"]),
                    "thread_id": row["thread_id"],
                    "event_type": row["event_type"],
                    "source": row["source"],
                    "model": row["model"],
                    "request_id": row["request_id"],
                    "input_tokens": int(row["input_tokens"]),
                    "output_tokens": int(row["output_tokens"]),
                    "cached_input_tokens": int(row["cached_input_tokens"]),
                    "is_exact": bool(row["is_exact"]),
                    "tool_name": metadata.get("tool_name"),
                    "created_at": row["created_at"],
                }
            )

        thread_data = []
        for row in thread_rows:
            thread_data.append(
                {
                    **dict(row),
                    "exact_tokens": int(row["exact_input_tokens"])
                    + int(row["exact_output_tokens"]),
                    "estimated_tokens": int(row["estimated_input_tokens"])
                    + int(row["estimated_output_tokens"]),
                }
            )

        db_path = self.db_path()
        database_bytes = sum(
            candidate.stat().st_size
            for candidate in (
                db_path,
                Path(f"{db_path}-wal"),
                Path(f"{db_path}-shm"),
            )
            if candidate.exists()
        )
        totals_dict = dict(totals)
        exact_tokens = int(totals_dict["exact_input_tokens"]) + int(
            totals_dict["exact_output_tokens"]
        )
        estimated_tokens = int(totals_dict["estimated_input_tokens"]) + int(
            totals_dict["estimated_output_tokens"]
        )
        last_event_at = totals_dict.get("last_event_at")
        ingest_lag_seconds = None
        if last_event_at:
            ingest_lag_seconds = max(
                0.0, (now - self._parse_utc(str(last_event_at))).total_seconds()
            )

        return {
            "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "version": str(int(totals_dict["last_event_id"])),
            "database": str(db_path),
            "database_bytes": int(database_bytes),
            "window": {
                "hours": window_hours,
                "bucket_seconds": bucket_seconds,
                "events": len(window_rows),
                "tool_calls": window_tool_calls,
                "exact_tokens": window_exact_tokens,
                "estimated_tokens": window_estimated_tokens,
                "calls_per_minute_5m": round(calls_last_five_minutes / 5, 2),
            },
            "totals": {
                **totals_dict,
                "threads": thread_total,
                "active_threads": active_threads,
                "exact_tokens": exact_tokens,
                "estimated_tokens": estimated_tokens,
                "ingest_lag_seconds": ingest_lag_seconds,
            },
            "series": series,
            "tools": tool_rows,
            "threads": thread_data,
            "recent_events": recent_events,
            "sources": [
                {"name": name, **values}
                for name, values in sorted(
                    source_breakdown.items(),
                    key=lambda item: item[1]["events"],
                    reverse=True,
                )
            ],
            "accounting_note": (
                "Exact provider tokens and proxy-estimated MCP text overlap conceptually and are "
                "shown separately. Cached input is a subset of exact input, not an additional total."
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
