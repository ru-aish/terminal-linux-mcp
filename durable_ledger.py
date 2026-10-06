"""The durable SQLite ledger used by the three-building-block workflow.

This module owns the database connection, schema, migrations, transactions and
ledger queries.  The old ``AgentRepository`` name is re-exported by the
orchestrator for source compatibility; it is an alias, not a second store.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Iterator


COMMAND_STATES = frozenset(
    {
        "queued",
        "waiting_after_cancel",
        "cancel_in_flight",
        "delivery_in_flight",
        "delivery_uncertain",
        "delivered",
        "acknowledged",
        "cancelled",
        "superseded",
    }
)
COMMAND_PURPOSES = frozenset(
    {
        "instruction",
        "answer",
        "question",
        "progress",
        "completion",
        "wakeup",
        "after_completion",
    }
)
AUTOMATION_KINDS = frozenset({"wakeup", "after_completion"})
AUTOMATION_STATES = frozenset(
    {
        "scheduled",
        "waiting",
        "queued",
        "delivered",
        "cancelled",
        "failed",
    }
)
TASK_STATES = frozenset(
    {
        "creating_thread",
        "creation_in_flight",
        "creation_uncertain",
        "running",
        "waiting_assistant",
        "continuation_in_flight",
        "continuation_uncertain",
        "waiting_after_continue",
        "completed",
        "failed",
        "cancelled",
        "unknown",
        "waiting_for_parent",
    }
)


def _row(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


class _ClosingConnection(sqlite3.Connection):
    """SQLite connection whose transaction context also owns its lifetime."""

    def __exit__(self, *args: Any) -> bool | None:
        try:
            return super().__exit__(*args)
        finally:
            self.close()


class DurableLedger:
    """Transactional repository for actors, tasks, commands and cursors."""

    SCHEMA_VERSION = 12

    def __init__(self, path: Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(
            self.path,
            timeout=30,
            factory=_ClosingConnection,
        )
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA busy_timeout=30000")
        return db

    def _migrate(self) -> None:
        with self.connect() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS orchestrations(
                    orchestration_id TEXT PRIMARY KEY, root_agent_id TEXT,
                    notification_policy TEXT NOT NULL, created_at REAL NOT NULL, closed_at REAL
                );
                CREATE TABLE IF NOT EXISTS agents(
                    agent_id TEXT PRIMARY KEY,
                    orchestration_id TEXT NOT NULL REFERENCES orchestrations(orchestration_id) ON DELETE CASCADE,
                    parent_agent_id TEXT REFERENCES agents(agent_id), root_agent_id TEXT NOT NULL,
                    chat_id TEXT UNIQUE, terminal_thread_id TEXT UNIQUE, project_id TEXT, working_directory TEXT,
                    title TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
                    notification_policy TEXT NOT NULL, context_cursor TEXT, current_node TEXT,
                    last_progress_at REAL, progress_signature TEXT, last_error TEXT NOT NULL DEFAULT '',
                    creation_request_id TEXT, creation_user_message_id TEXT,
                    gateway_agent_id TEXT, gateway_control_operation_id TEXT,
                    spawn_idempotency_key TEXT,
                    created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS agents_parent_spawn_key
                    ON agents(parent_agent_id, spawn_idempotency_key)
                    WHERE spawn_idempotency_key IS NOT NULL;
                CREATE TABLE IF NOT EXISTS tasks(
                    task_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL UNIQUE REFERENCES agents(agent_id) ON DELETE CASCADE,
                    prompt TEXT NOT NULL, completion_marker TEXT NOT NULL DEFAULT '', status TEXT NOT NULL,
                    continue_attempts INTEGER NOT NULL DEFAULT 0, last_continue_node TEXT,
                    last_continue_at REAL, gateway_operation_id TEXT,
                    next_check_at REAL NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, completed_at REAL
                );
                CREATE TABLE IF NOT EXISTS commands(
                    command_id TEXT PRIMARY KEY, from_agent_id TEXT REFERENCES agents(agent_id),
                    to_agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
                    sequence_no INTEGER NOT NULL, message TEXT NOT NULL, interrupt_policy TEXT NOT NULL,
                    status TEXT NOT NULL, idempotency_key TEXT, ack_event_seq INTEGER,
                    request_id TEXT, user_message_id TEXT, parent_message_id TEXT,
                    created_at REAL NOT NULL, delivered_at REAL, last_error TEXT NOT NULL DEFAULT '',
                    purpose TEXT NOT NULL DEFAULT 'instruction',
                    gateway_operation_id TEXT,
                    next_attempt_at REAL NOT NULL DEFAULT 0
                );
                CREATE UNIQUE INDEX IF NOT EXISTS commands_sender_key
                    ON commands(from_agent_id, to_agent_id, idempotency_key)
                    WHERE idempotency_key IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS commands_target_sequence ON commands(to_agent_id, sequence_no);
                CREATE TABLE IF NOT EXISTS events(
                    event_seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE,
                    agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
                    task_id TEXT REFERENCES tasks(task_id) ON DELETE SET NULL, kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL, source_cursor TEXT, created_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS events_agent_source
                    ON events(agent_id, source_cursor, kind) WHERE source_cursor IS NOT NULL;
                CREATE TABLE IF NOT EXISTS subscriptions(
                    parent_agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
                    child_agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
                    notification_policy TEXT NOT NULL, last_acked_event_seq INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL, PRIMARY KEY(parent_agent_id, child_agent_id)
                );
                CREATE TABLE IF NOT EXISTS agent_automations(
                    automation_id TEXT PRIMARY KEY,
                    orchestration_id TEXT NOT NULL REFERENCES orchestrations(orchestration_id) ON DELETE CASCADE,
                    kind TEXT NOT NULL,
                    source_agent_id TEXT REFERENCES agents(agent_id) ON DELETE CASCADE,
                    target_agent_id TEXT NOT NULL REFERENCES agents(agent_id) ON DELETE CASCADE,
                    message TEXT NOT NULL, completion_marker TEXT, due_at REAL,
                    status TEXT NOT NULL, command_id TEXT REFERENCES commands(command_id) ON DELETE SET NULL,
                    idempotency_key TEXT, triggered_at REAL, completed_at REAL,
                    last_error TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL, updated_at REAL NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS agent_automations_target_key
                    ON agent_automations(target_agent_id, kind, idempotency_key)
                    WHERE idempotency_key IS NOT NULL;
                CREATE INDEX IF NOT EXISTS agent_automations_due
                    ON agent_automations(status, due_at);
                CREATE INDEX IF NOT EXISTS agent_automations_source
                    ON agent_automations(source_agent_id, status);
                CREATE TABLE IF NOT EXISTS watchdog_tasks(
                    conversation_id TEXT PRIMARY KEY,
                    state_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            for table, column, definition in (
                ("tasks", "continue_attempts", "INTEGER NOT NULL DEFAULT 0"),
                ("tasks", "last_continue_node", "TEXT"),
                ("tasks", "last_continue_at", "REAL"),
                ("tasks", "next_check_at", "REAL NOT NULL DEFAULT 0"),
                ("commands", "ack_event_seq", "INTEGER"),
                ("commands", "request_id", "TEXT"),
                ("commands", "user_message_id", "TEXT"),
                ("commands", "parent_message_id", "TEXT"),
                ("commands", "purpose", "TEXT NOT NULL DEFAULT 'instruction'"),
                ("commands", "next_attempt_at", "REAL NOT NULL DEFAULT 0"),
                ("agents", "last_progress_at", "REAL"),
                ("agents", "terminal_thread_id", "TEXT"),
                ("agents", "working_directory", "TEXT"),
                ("agents", "progress_signature", "TEXT"),
                ("agents", "creation_request_id", "TEXT"),
                ("agents", "creation_user_message_id", "TEXT"),
                ("agents", "gateway_agent_id", "TEXT"),
                ("agents", "gateway_control_operation_id", "TEXT"),
                ("tasks", "gateway_operation_id", "TEXT"),
                ("commands", "gateway_operation_id", "TEXT"),
            ):
                self._ensure_column(db, table, column, definition)
            db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS agents_gateway_agent_id "
                "ON agents(gateway_agent_id) WHERE gateway_agent_id IS NOT NULL"
            )
            db.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS agents_terminal_thread_id "
                "ON agents(terminal_thread_id) WHERE terminal_thread_id IS NOT NULL"
            )
            # Rows created before schema v6 had no purpose column. Notification
            # commands are identifiable by their event cursor and must not be
            # mistaken for parent instructions after migration.
            db.execute(
                "UPDATE commands SET purpose=CASE "
                'WHEN message LIKE \'%"kind":"completed"%\' '
                "OR message LIKE '%\"kind\":\"completion\"%' THEN 'completion' "
                "ELSE 'progress' END "
                "WHERE ack_event_seq IS NOT NULL AND purpose='instruction'"
            )
            db.execute(
                "INSERT INTO schema_meta(key,value) VALUES('version',?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(self.SCHEMA_VERSION),),
            )

    @staticmethod
    def _ensure_column(
        db: sqlite3.Connection, table: str, column: str, definition: str
    ) -> None:
        columns = {str(row[1]) for row in db.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def agent(self, agent_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            return _row(
                db.execute(
                    "SELECT * FROM agents WHERE agent_id=?", (agent_id,)
                ).fetchone()
            )

    def agent_by_chat(self, chat_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            return _row(
                db.execute(
                    "SELECT * FROM agents WHERE chat_id=?", (chat_id,)
                ).fetchone()
            )

    def task_for_agent(self, agent_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            return _row(
                db.execute(
                    "SELECT * FROM tasks WHERE agent_id=?", (agent_id,)
                ).fetchone()
            )

    def children(self, parent_agent_id: str) -> list[dict[str, Any]]:
        with self.connect() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM agents WHERE parent_agent_id=? ORDER BY created_at,agent_id",
                    (parent_agent_id,),
                )
            ]

    def active_agents(
        self,
        *,
        now: float | None = None,
        include_future: bool = False,
    ) -> list[dict[str, Any]]:
        current = time.time() if now is None else float(now)
        with self.connect() as db:
            query = (
                "SELECT a.* FROM agents a JOIN tasks t ON t.agent_id=a.agent_id "
                "WHERE a.chat_id IS NOT NULL "
            )
            params: tuple[Any, ...] = ()
            if include_future:
                query += "AND t.status NOT IN ('completed','failed','cancelled') "
            else:
                query += (
                    "AND t.status NOT IN "
                    "('completed','failed','cancelled','waiting_for_parent') "
                    "AND COALESCE(t.next_check_at,0)<=? "
                )
                params = (current,)
            query += "ORDER BY a.created_at,a.agent_id"
            return [dict(row) for row in db.execute(query, params)]

    def queued_commands(
        self,
        *,
        include_future: bool = False,
    ) -> list[dict[str, Any]]:
        with self.connect() as db:
            query = (
                "SELECT c.* FROM commands c "
                "WHERE c.status IN ('queued','waiting_after_cancel') "
            )
            params: tuple[Any, ...] = ()
            if not include_future:
                query += "AND COALESCE(c.next_attempt_at,0)<=? "
                params = (time.time(),)
            query += (
                "AND NOT EXISTS ("
                "  SELECT 1 FROM commands prior "
                "  WHERE prior.to_agent_id=c.to_agent_id "
                "  AND prior.sequence_no<c.sequence_no "
                "  AND prior.status NOT IN "
                "      ('delivered','acknowledged','cancelled','superseded')"
                ") "
                "ORDER BY c.created_at,c.sequence_no,c.command_id"
            )
            return [dict(row) for row in db.execute(query, params)]

    def uncertain_commands(
        self,
        *,
        include_future: bool = False,
    ) -> list[dict[str, Any]]:
        with self.connect() as db:
            query = "SELECT * FROM commands WHERE status='delivery_uncertain' "
            params: tuple[Any, ...] = ()
            if not include_future:
                query += "AND COALESCE(next_attempt_at,0)<=? "
                params = (time.time(),)
            query += "ORDER BY created_at,sequence_no,command_id"
            return [dict(row) for row in db.execute(query, params)]

    def automation(self, automation_id: str) -> dict[str, Any] | None:
        with self.connect() as db:
            return _row(
                db.execute(
                    "SELECT * FROM agent_automations WHERE automation_id=?",
                    (automation_id,),
                ).fetchone()
            )

    def automations(
        self,
        *,
        agent_id: str | None = None,
        include_terminal: bool = True,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM agent_automations"
        clauses: list[str] = []
        params: list[Any] = []
        if agent_id:
            clauses.append("(source_agent_id=? OR target_agent_id=?)")
            params.extend((agent_id, agent_id))
        if not include_terminal:
            clauses.append("status NOT IN ('delivered','cancelled','failed')")
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY CASE WHEN due_at IS NULL THEN 1 ELSE 0 END,due_at,created_at,automation_id"
        with self.connect() as db:
            return [dict(row) for row in db.execute(query, tuple(params))]

    def next_automation_due_at(self) -> float | None:
        with self.connect() as db:
            row = db.execute(
                "SELECT MIN(due_at) FROM ("
                "  SELECT due_at FROM agent_automations "
                "  WHERE kind='wakeup' AND status='scheduled' AND due_at IS NOT NULL "
                "  UNION ALL "
                "  SELECT CASE WHEN COALESCE(c.next_attempt_at,0)<=0 "
                "              THEN a.triggered_at ELSE c.next_attempt_at END "
                "  FROM agent_automations a JOIN commands c ON c.command_id=a.command_id "
                "  WHERE a.status='queued' AND c.status IN ('queued','waiting_after_cancel')"
                ")"
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return float(row[0])

    def next_due_at(self, *, now: float | None = None) -> float | None:
        current = time.time() if now is None else float(now)
        with self.connect() as db:
            row = db.execute(
                "SELECT MIN(due_at) FROM ("
                "  SELECT CASE WHEN COALESCE(t.next_check_at,0)<=0 THEN ? "
                "              ELSE t.next_check_at END AS due_at "
                "  FROM tasks t JOIN agents a ON a.agent_id=t.agent_id "
                "  WHERE t.status NOT IN ('completed','failed','cancelled','waiting_for_parent') "
                "  AND (a.chat_id IS NOT NULL OR a.status='creating_thread') "
                "  UNION ALL "
                "  SELECT CASE WHEN COALESCE(c.next_attempt_at,0)<=0 THEN ? "
                "              ELSE c.next_attempt_at END "
                "  FROM commands c "
                "  WHERE c.status IN ('queued','waiting_after_cancel','delivery_uncertain') "
                "  AND NOT EXISTS ("
                "    SELECT 1 FROM commands prior "
                "    WHERE prior.to_agent_id=c.to_agent_id "
                "    AND prior.sequence_no<c.sequence_no "
                "    AND prior.status NOT IN "
                "      ('delivered','acknowledged','cancelled','superseded')"
                "  )"
                ")",
                (current, current),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return float(row[0])

    def active_task_conversation_ids(self) -> set[str]:
        with self.connect() as db:
            return {
                str(row[0])
                for row in db.execute(
                    "SELECT a.chat_id FROM agents a JOIN tasks t ON t.agent_id=a.agent_id "
                    "WHERE a.chat_id IS NOT NULL "
                    "AND t.status NOT IN ('completed','failed','cancelled')"
                )
            }

    def events_after(self, agent_id: str, after_seq: int = 0) -> list[dict[str, Any]]:
        with self.connect() as db:
            result = []
            for row in db.execute(
                "SELECT * FROM events WHERE agent_id=? AND event_seq>? ORDER BY event_seq",
                (agent_id, after_seq),
            ):
                item = dict(row)
                item["payload"] = json.loads(item.pop("payload_json"))
                result.append(item)
            return result

    def watchdog_state(self, conversation_id: str) -> dict[str, Any]:
        with self.connect() as db:
            row = db.execute(
                "SELECT state_json FROM watchdog_tasks WHERE conversation_id=?",
                (conversation_id,),
            ).fetchone()
        if row is None:
            return {}
        payload = json.loads(str(row["state_json"]))
        return dict(payload) if isinstance(payload, dict) else {}

    def watchdog_states(self) -> dict[str, dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute(
                "SELECT conversation_id,state_json FROM watchdog_tasks "
                "ORDER BY conversation_id"
            ).fetchall()
        result: dict[str, dict[str, Any]] = {}
        for row in rows:
            payload = json.loads(str(row["state_json"]))
            if isinstance(payload, dict):
                result[str(row["conversation_id"])] = dict(payload)
        return result

    def put_watchdog_state(
        self,
        conversation_id: str,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        normalized = dict(state)
        encoded = json.dumps(
            normalized,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self.transaction() as db:
            db.execute(
                "INSERT INTO watchdog_tasks(conversation_id,state_json,updated_at) "
                "VALUES(?,?,?) ON CONFLICT(conversation_id) DO UPDATE SET "
                "state_json=excluded.state_json,updated_at=excluded.updated_at",
                (conversation_id, encoded, time.time()),
            )
        return normalized


__all__ = [
    "AUTOMATION_KINDS",
    "AUTOMATION_STATES",
    "COMMAND_PURPOSES",
    "COMMAND_STATES",
    "TASK_STATES",
    "DurableLedger",
]
