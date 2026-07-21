from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence

from .models import (
    AgentRecord,
    AgentState,
    CircuitRecord,
    CircuitState,
    Lane,
    OperationRecord,
    OperationState,
    OperationType,
)

_SCHEMA_VERSION = 2
_UNSET = object()


class SQLiteLedger:
    """Project-owned durable ledger.

    Each transaction opens its own connection. This is intentionally a little
    more expensive than sharing a connection, but it is robust across threads,
    processes, CLI invocations, and restarts.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = str(Path(path).expanduser().resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    @contextmanager
    def transaction(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        with self.transaction(immediate=True) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS agents (
                    id TEXT PRIMARY KEY,
                    project_id TEXT NOT NULL,
                    conversation_id TEXT UNIQUE,
                    title TEXT NOT NULL,
                    completion_marker TEXT NOT NULL,
                    state TEXT NOT NULL,
                    snapshot_hash TEXT,
                    unchanged_count INTEGER NOT NULL DEFAULT 0,
                    last_inspected_at REAL,
                    next_inspection_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    completed_at REAL,
                    deleted_at REAL,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS operations (
                    id TEXT PRIMARY KEY,
                    agent_id TEXT REFERENCES agents(id) ON DELETE CASCADE,
                    type TEXT NOT NULL,
                    lane TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    coalesce_key TEXT,
                    payload_json TEXT NOT NULL,
                    due_at REAL NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    claim_token TEXT,
                    claim_expires_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    last_error TEXT,
                    result_json TEXT
                );

                DROP INDEX IF EXISTS operations_active_coalesce;
                CREATE UNIQUE INDEX operations_active_coalesce
                ON operations(coalesce_key)
                WHERE coalesce_key IS NOT NULL
                  AND state='PENDING';

                CREATE INDEX IF NOT EXISTS operations_due
                ON operations(state, due_at, priority, created_at);

                CREATE INDEX IF NOT EXISTS operations_agent
                ON operations(agent_id, state, type);

                CREATE TABLE IF NOT EXISTS request_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_id TEXT REFERENCES operations(id) ON DELETE SET NULL,
                    lane TEXT NOT NULL,
                    circuit_scope TEXT NOT NULL,
                    started_at REAL NOT NULL,
                    finished_at REAL,
                    duration REAL,
                    outcome TEXT NOT NULL,
                    status_code INTEGER,
                    error_class TEXT,
                    is_rate_limit INTEGER NOT NULL DEFAULT 0,
                    details_json TEXT NOT NULL DEFAULT '{}'
                );

                CREATE INDEX IF NOT EXISTS request_events_started
                ON request_events(started_at);

                CREATE INDEX IF NOT EXISTS request_events_lane_started
                ON request_events(lane, started_at);

                CREATE TABLE IF NOT EXISTS circuit_breakers (
                    scope TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    opened_at REAL,
                    retry_at REAL,
                    probe_failures INTEGER NOT NULL DEFAULT 0,
                    half_open_successes INTEGER NOT NULL DEFAULT 0,
                    last_success_at REAL,
                    paced_started_at REAL,
                    last_attempt_at REAL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS cache (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    stored_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS scheduler_state (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT INTO schema_meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(_SCHEMA_VERSION),),
            )

    # ------------------------------------------------------------------
    # Agents and operations
    # ------------------------------------------------------------------
    def register_existing_agent(
        self,
        *,
        project_id: str,
        conversation_id: str,
        title: str,
        completion_marker: str,
        state: AgentState,
        now: float,
        agent_id: Optional[str] = None,
    ) -> str:
        """Register an already-existing conversation without a provider request."""

        project_id = project_id.strip()
        conversation_id = conversation_id.strip()
        if not conversation_id:
            raise ValueError("conversation_id is required")
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT id FROM agents WHERE conversation_id=?",
                (conversation_id,),
            ).fetchone()
            if existing:
                return str(existing["id"])
            selected_id = agent_id or f"agent_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO agents(
                    id, project_id, conversation_id, title, completion_marker,
                    state, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    selected_id,
                    project_id,
                    conversation_id,
                    title,
                    completion_marker,
                    state.value,
                    now,
                    now,
                ),
            )
            return selected_id

    def reopen_agent(self, agent_id: str, *, now: float) -> None:
        """Make a terminal conversation eligible for a new continuation."""

        with self.transaction(immediate=True) as connection:
            row = connection.execute(
                "SELECT deleted_at FROM agents WHERE id=?",
                (agent_id,),
            ).fetchone()
            if row is None:
                raise ValueError("agent was not found")
            if row["deleted_at"] is not None:
                raise ValueError("deleted agents cannot be reopened")
            connection.execute(
                """
                UPDATE agents
                SET state=?, completed_at=NULL, last_error=NULL, updated_at=?
                WHERE id=?
                """,
                (AgentState.UNKNOWN.value, now, agent_id),
            )

    def create_agent_with_operation(
        self,
        *,
        project_id: str,
        title: str,
        completion_marker: str,
        payload: Mapping[str, Any],
        idempotency_key: str,
        due_at: float,
        priority: int,
        max_attempts: int,
    ) -> tuple[str, str]:
        with self.transaction(immediate=True) as connection:
            existing = connection.execute(
                "SELECT id, agent_id, type FROM operations WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if existing:
                if str(existing["type"]) != OperationType.CREATE.value:
                    raise ValueError(
                        "idempotency key already belongs to a different operation type"
                    )
                return str(existing["agent_id"]), str(existing["id"])

            agent_id = f"agent_{uuid.uuid4().hex}"
            operation_id = f"op_{uuid.uuid4().hex}"
            connection.execute(
                """
                INSERT INTO agents(
                    id, project_id, conversation_id, title, completion_marker,
                    state, created_at, updated_at
                ) VALUES(?, ?, NULL, ?, ?, ?, ?, ?)
                """,
                (
                    agent_id,
                    project_id,
                    title,
                    completion_marker,
                    AgentState.CREATING.value,
                    due_at,
                    due_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO operations(
                    id, agent_id, type, lane, priority, state,
                    idempotency_key, coalesce_key, payload_json, due_at,
                    attempts, max_attempts, created_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, 0, ?, ?, ?)
                """,
                (
                    operation_id,
                    agent_id,
                    OperationType.CREATE.value,
                    Lane.HEAVY.value,
                    priority,
                    OperationState.PENDING.value,
                    idempotency_key,
                    _json(payload),
                    due_at,
                    max_attempts,
                    due_at,
                    due_at,
                ),
            )
            return agent_id, operation_id

    def enqueue_operation(
        self,
        *,
        operation_type: OperationType,
        lane: Lane,
        priority: int,
        idempotency_key: str,
        payload: Mapping[str, Any],
        due_at: float,
        max_attempts: int,
        agent_id: Optional[str] = None,
        coalesce_key: Optional[str] = None,
    ) -> str:
        with self.transaction(immediate=True) as connection:
            return self.enqueue_operation_tx(
                connection,
                operation_type=operation_type,
                lane=lane,
                priority=priority,
                idempotency_key=idempotency_key,
                payload=payload,
                due_at=due_at,
                max_attempts=max_attempts,
                agent_id=agent_id,
                coalesce_key=coalesce_key,
            )

    def enqueue_operation_tx(
        self,
        connection: sqlite3.Connection,
        *,
        operation_type: OperationType,
        lane: Lane,
        priority: int,
        idempotency_key: str,
        payload: Mapping[str, Any],
        due_at: float,
        max_attempts: int,
        agent_id: Optional[str] = None,
        coalesce_key: Optional[str] = None,
    ) -> str:
        existing = connection.execute(
            "SELECT id, type, agent_id FROM operations WHERE idempotency_key=?",
            (idempotency_key,),
        ).fetchone()
        if existing:
            if str(existing["type"]) != operation_type.value:
                raise ValueError(
                    "idempotency key already belongs to a different operation type"
                )
            existing_agent = existing["agent_id"]
            if agent_id is not None and existing_agent != agent_id:
                raise ValueError("idempotency key already belongs to a different agent")
            return str(existing["id"])

        if coalesce_key:
            existing = connection.execute(
                """
                SELECT id, priority, due_at FROM operations
                WHERE coalesce_key=? AND state='PENDING'
                """,
                (coalesce_key,),
            ).fetchone()
            if existing:
                if priority < int(existing["priority"]):
                    connection.execute(
                        """
                        UPDATE operations
                        SET type=?, lane=?, priority=?, due_at=MIN(due_at, ?),
                            payload_json=?, updated_at=?
                        WHERE id=? AND state='PENDING'
                        """,
                        (
                            operation_type.value,
                            lane.value,
                            priority,
                            due_at,
                            _json(payload),
                            due_at,
                            existing["id"],
                        ),
                    )
                else:
                    connection.execute(
                        """
                        UPDATE operations
                        SET due_at=MIN(due_at, ?), updated_at=?
                        WHERE id=? AND state='PENDING'
                        """,
                        (due_at, due_at, existing["id"]),
                    )
                return str(existing["id"])

        operation_id = f"op_{uuid.uuid4().hex}"
        connection.execute(
            """
            INSERT INTO operations(
                id, agent_id, type, lane, priority, state,
                idempotency_key, coalesce_key, payload_json, due_at,
                attempts, max_attempts, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            """,
            (
                operation_id,
                agent_id,
                operation_type.value,
                lane.value,
                priority,
                OperationState.PENDING.value,
                idempotency_key,
                coalesce_key,
                _json(payload),
                due_at,
                max_attempts,
                due_at,
                due_at,
            ),
        )
        return operation_id

    def get_agent(self, agent_id: str) -> Optional[AgentRecord]:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM agents WHERE id=?", (agent_id,)
            ).fetchone()
            return _agent(row) if row else None

    def get_agent_tx(
        self, connection: sqlite3.Connection, agent_id: str
    ) -> Optional[AgentRecord]:
        row = connection.execute(
            "SELECT * FROM agents WHERE id=?", (agent_id,)
        ).fetchone()
        return _agent(row) if row else None

    def get_operation(self, operation_id: str) -> Optional[OperationRecord]:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT * FROM operations WHERE id=?", (operation_id,)
            ).fetchone()
            return _operation(row) if row else None

    def get_operation_tx(
        self, connection: sqlite3.Connection, operation_id: str
    ) -> Optional[OperationRecord]:
        row = connection.execute(
            "SELECT * FROM operations WHERE id=?", (operation_id,)
        ).fetchone()
        return _operation(row) if row else None

    def list_agents(self) -> list[AgentRecord]:
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM agents ORDER BY created_at, id"
            ).fetchall()
            return [_agent(row) for row in rows]

    def list_operations(
        self, *, state: Optional[OperationState] = None
    ) -> list[OperationRecord]:
        with self.transaction() as connection:
            if state is None:
                rows = connection.execute(
                    "SELECT * FROM operations ORDER BY created_at, id"
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT * FROM operations WHERE state=? ORDER BY created_at, id",
                    (state.value,),
                ).fetchall()
            return [_operation(row) for row in rows]

    def first_conversation_id_tx(self, connection: sqlite3.Connection) -> Optional[str]:
        row = connection.execute(
            """
            SELECT conversation_id FROM agents
            WHERE conversation_id IS NOT NULL AND deleted_at IS NULL
            ORDER BY updated_at DESC LIMIT 1
            """
        ).fetchone()
        return str(row["conversation_id"]) if row else None

    def capacity_agent_count(self, connection: sqlite3.Connection) -> int:
        row = connection.execute(
            """
            SELECT COUNT(DISTINCT a.id)
            FROM agents AS a
            WHERE a.deleted_at IS NULL
              AND (
                (a.conversation_id IS NOT NULL
                 AND a.state IN ('CREATING','RUNNING','UNKNOWN'))
                OR EXISTS (
                    SELECT 1 FROM operations AS o
                    WHERE o.agent_id=a.id AND o.state='CLAIMED'
                )
              )
            """
        ).fetchone()
        return int(row[0])

    def active_agent_count(
        self, connection: Optional[sqlite3.Connection] = None
    ) -> int:
        query = (
            "SELECT COUNT(*) FROM agents WHERE conversation_id IS NOT NULL "
            "AND deleted_at IS NULL AND state IN ('CREATING','RUNNING','UNKNOWN')"
        )
        if connection is not None:
            return int(connection.execute(query).fetchone()[0])
        with self.transaction() as own:
            return int(own.execute(query).fetchone()[0])

    def due_operations(
        self, connection: sqlite3.Connection, now: float, *, limit: int = 200
    ) -> list[OperationRecord]:
        rows = connection.execute(
            """
            SELECT * FROM operations
            WHERE state='PENDING' AND due_at<=?
            ORDER BY priority ASC, due_at ASC, created_at ASC, rowid ASC
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()
        return [_operation(row) for row in rows]

    def recover_expired_claims(self, connection: sqlite3.Connection, now: float) -> int:
        expired = connection.execute(
            """
            SELECT id FROM operations
            WHERE state='CLAIMED' AND claim_expires_at IS NOT NULL
              AND claim_expires_at<=?
            """,
            (now,),
        ).fetchall()
        if not expired:
            return 0
        operation_ids = [str(row["id"]) for row in expired]
        placeholders = ",".join("?" for _ in operation_ids)
        connection.execute(
            f"""
            UPDATE operations
            SET state='PENDING', claim_token=NULL, claim_expires_at=NULL,
                due_at=?, attempts=attempts+1, updated_at=?,
                last_error='claim lease expired before finalization'
            WHERE id IN ({placeholders})
            """,
            (now, now, *operation_ids),
        )
        connection.execute(
            f"""
            UPDATE request_events
            SET outcome='ABANDONED', finished_at=?, duration=?-started_at,
                error_class='ClaimLeaseExpired'
            WHERE outcome='RESERVED' AND operation_id IN ({placeholders})
            """,
            (now, now, *operation_ids),
        )
        return len(operation_ids)

    def claim_operation(
        self,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        claim_token: str,
        claim_expires_at: float,
        now: float,
    ) -> bool:
        cursor = connection.execute(
            """
            UPDATE operations
            SET state='CLAIMED', claim_token=?, claim_expires_at=?, updated_at=?
            WHERE id=? AND state='PENDING'
            """,
            (claim_token, claim_expires_at, now, operation_id),
        )
        return cursor.rowcount == 1

    def reserve_request_event(
        self,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        lane: Lane,
        circuit_scope: str,
        now: float,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO request_events(
                operation_id, lane, circuit_scope, started_at, outcome
            ) VALUES(?, ?, ?, ?, 'RESERVED')
            """,
            (operation_id, lane.value, circuit_scope, now),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("SQLite did not return a request event row id")
        return int(cursor.lastrowid)

    def finalize_request_event(
        self,
        connection: sqlite3.Connection,
        *,
        event_id: int,
        finished_at: float,
        outcome: str,
        status_code: Optional[int] = None,
        error_class: Optional[str] = None,
        is_rate_limit: bool = False,
        details: Optional[Mapping[str, Any]] = None,
    ) -> None:
        connection.execute(
            """
            UPDATE request_events
            SET finished_at=?, duration=?-started_at, outcome=?, status_code=?,
                error_class=?, is_rate_limit=?, details_json=?
            WHERE id=?
            """,
            (
                finished_at,
                finished_at,
                outcome,
                status_code,
                error_class,
                int(is_rate_limit),
                _json(details or {}),
                event_id,
            ),
        )

    def succeed_operation(
        self,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        now: float,
        result: Optional[Mapping[str, Any]] = None,
    ) -> None:
        connection.execute(
            """
            UPDATE operations
            SET state='SUCCEEDED', result_json=?, claim_token=NULL,
                claim_expires_at=NULL, updated_at=?, last_error=NULL
            WHERE id=?
            """,
            (_json(result or {}), now, operation_id),
        )

    def requeue_operation(
        self,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        due_at: float,
        now: float,
        error: str,
        increment_attempts: bool = True,
        payload: Optional[Mapping[str, Any]] = None,
    ) -> None:
        payload_clause = ", payload_json=?" if payload is not None else ""
        values: list[Any] = [
            OperationState.PENDING.value,
            due_at,
            now,
            error,
            int(increment_attempts),
        ]
        if payload is not None:
            values.append(_json(payload))
        values.append(operation_id)
        connection.execute(
            f"""
            UPDATE operations
            SET state=?, due_at=?, updated_at=?, last_error=?,
                attempts=attempts+?, claim_token=NULL, claim_expires_at=NULL
                {payload_clause}
            WHERE id=?
            """,
            tuple(values),
        )

    def fail_operation(
        self,
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        now: float,
        error: str,
    ) -> None:
        connection.execute(
            """
            UPDATE operations
            SET state='FAILED', updated_at=?, last_error=?, claim_token=NULL,
                claim_expires_at=NULL, attempts=attempts+1
            WHERE id=?
            """,
            (now, error, operation_id),
        )

    def cancel_pending_operations_for_agent(
        self,
        connection: sqlite3.Connection,
        *,
        agent_id: str,
        now: float,
        except_types: Sequence[OperationType] = (),
    ) -> None:
        values: list[Any] = [now, agent_id]
        exclusion = ""
        if except_types:
            placeholders = ",".join("?" for _ in except_types)
            exclusion = f" AND type NOT IN ({placeholders})"
            values.extend(item.value for item in except_types)
        connection.execute(
            f"""
            UPDATE operations SET state='CANCELLED', updated_at=?,
                claim_token=NULL, claim_expires_at=NULL
            WHERE agent_id=? AND state='PENDING' {exclusion}
            """,
            tuple(values),
        )

    def update_agent(
        self,
        connection: sqlite3.Connection,
        *,
        agent_id: str,
        now: float,
        conversation_id: Any = _UNSET,
        state: Any = _UNSET,
        snapshot_hash: Any = _UNSET,
        unchanged_count: Any = _UNSET,
        last_inspected_at: Any = _UNSET,
        next_inspection_at: Any = _UNSET,
        completed_at: Any = _UNSET,
        deleted_at: Any = _UNSET,
        last_error: Any = _UNSET,
    ) -> None:
        assignments = ["updated_at=?"]
        values: list[Any] = [now]
        fields = {
            "conversation_id": conversation_id,
            "state": state.value if isinstance(state, AgentState) else state,
            "snapshot_hash": snapshot_hash,
            "unchanged_count": unchanged_count,
            "last_inspected_at": last_inspected_at,
            "next_inspection_at": next_inspection_at,
            "completed_at": completed_at,
            "deleted_at": deleted_at,
            "last_error": last_error,
        }
        for name, value in fields.items():
            if value is _UNSET:
                continue
            assignments.append(f"{name}=?")
            values.append(value)
        values.append(agent_id)
        connection.execute(
            f"UPDATE agents SET {', '.join(assignments)} WHERE id=?",
            tuple(values),
        )

    # ------------------------------------------------------------------
    # Rate accounting
    # ------------------------------------------------------------------
    def active_request_lock_until(
        self, connection: sqlite3.Connection, *, now: float
    ) -> Optional[float]:
        row = connection.execute(
            """
            SELECT MAX(o.claim_expires_at) AS value
            FROM request_events AS r
            JOIN operations AS o ON o.id=r.operation_id
            WHERE r.outcome='RESERVED'
              AND o.state='CLAIMED'
              AND o.claim_expires_at>?
            """,
            (now,),
        ).fetchone()
        return float(row["value"]) if row and row["value"] is not None else None

    def last_request_time(
        self, connection: sqlite3.Connection, *, lane: Optional[Lane]
    ) -> Optional[float]:
        if lane is None:
            row = connection.execute(
                "SELECT MAX(started_at) AS value FROM request_events"
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT MAX(started_at) AS value FROM request_events WHERE lane=?",
                (lane.value,),
            ).fetchone()
        return float(row["value"]) if row and row["value"] is not None else None

    def request_times_since(
        self,
        connection: sqlite3.Connection,
        *,
        since: float,
        lane: Optional[Lane],
    ) -> list[float]:
        if lane is None:
            rows = connection.execute(
                "SELECT started_at FROM request_events WHERE started_at>? ORDER BY started_at",
                (since,),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT started_at FROM request_events
                WHERE lane=? AND started_at>? ORDER BY started_at
                """,
                (lane.value, since),
            ).fetchall()
        return [float(row["started_at"]) for row in rows]

    def recent_request_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            rows = connection.execute(
                """
                SELECT id, operation_id, lane, circuit_scope, started_at,
                       finished_at, duration, outcome, status_code, error_class,
                       is_rate_limit, details_json
                FROM request_events
                ORDER BY started_at DESC, id DESC LIMIT ?
                """,
                (max(1, min(int(limit), 500)),),
            ).fetchall()
            events: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                item["details"] = dict(_loads(item.pop("details_json", "{}")))
                events.append(item)
            return events

    def request_stats(self, *, since: Optional[float] = None) -> list[dict[str, Any]]:
        with self.transaction() as connection:
            where = "WHERE started_at>=?" if since is not None else ""
            values: tuple[Any, ...] = (since,) if since is not None else ()
            rows = connection.execute(
                f"""
                SELECT lane, outcome, COUNT(*) AS count,
                       AVG(duration) AS average_duration,
                       SUM(is_rate_limit) AS rate_limits
                FROM request_events {where}
                GROUP BY lane, outcome ORDER BY lane, outcome
                """,
                values,
            ).fetchall()
            return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Circuit breakers
    # ------------------------------------------------------------------
    def get_circuit(
        self,
        connection: sqlite3.Connection,
        *,
        scope: str,
        now: float,
    ) -> CircuitRecord:
        row = connection.execute(
            "SELECT * FROM circuit_breakers WHERE scope=?", (scope,)
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO circuit_breakers(
                    scope, state, probe_failures, half_open_successes, updated_at
                ) VALUES(?, 'CLOSED', 0, 0, ?)
                """,
                (scope, now),
            )
            row = connection.execute(
                "SELECT * FROM circuit_breakers WHERE scope=?", (scope,)
            ).fetchone()
        return _circuit(row)

    def save_circuit(
        self, connection: sqlite3.Connection, record: CircuitRecord
    ) -> None:
        connection.execute(
            """
            INSERT INTO circuit_breakers(
                scope, state, opened_at, retry_at, probe_failures,
                half_open_successes, last_success_at, paced_started_at,
                last_attempt_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(scope) DO UPDATE SET
                state=excluded.state,
                opened_at=excluded.opened_at,
                retry_at=excluded.retry_at,
                probe_failures=excluded.probe_failures,
                half_open_successes=excluded.half_open_successes,
                last_success_at=excluded.last_success_at,
                paced_started_at=excluded.paced_started_at,
                last_attempt_at=excluded.last_attempt_at,
                updated_at=excluded.updated_at
            """,
            (
                record.scope,
                record.state.value,
                record.opened_at,
                record.retry_at,
                record.probe_failures,
                record.half_open_successes,
                record.last_success_at,
                record.paced_started_at,
                record.last_attempt_at,
                record.updated_at,
            ),
        )

    def list_circuits(self) -> list[CircuitRecord]:
        with self.transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM circuit_breakers ORDER BY scope"
            ).fetchall()
            return [_circuit(row) for row in rows]

    # ------------------------------------------------------------------
    # Cache and scheduler state
    # ------------------------------------------------------------------
    def cache_get(self, key: str, now: float) -> Optional[Any]:
        with self.transaction() as connection:
            return self.cache_get_tx(connection, key, now)

    def cache_get_tx(
        self, connection: sqlite3.Connection, key: str, now: float
    ) -> Optional[Any]:
        row = connection.execute(
            "SELECT value_json FROM cache WHERE key=? AND expires_at>?",
            (key, now),
        ).fetchone()
        return json.loads(row["value_json"]) if row else None

    def cache_put(self, key: str, value: Any, *, now: float, ttl: float) -> None:
        with self.transaction(immediate=True) as connection:
            self.cache_put_tx(connection, key, value, now=now, ttl=ttl)

    def cache_put_tx(
        self,
        connection: sqlite3.Connection,
        key: str,
        value: Any,
        *,
        now: float,
        ttl: float,
    ) -> None:
        connection.execute(
            """
            INSERT INTO cache(key, value_json, stored_at, expires_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_json=excluded.value_json,
                stored_at=excluded.stored_at,
                expires_at=excluded.expires_at
            """,
            (key, _json(value), now, now + ttl),
        )

    def state_get(self, key: str) -> Optional[Any]:
        with self.transaction() as connection:
            row = connection.execute(
                "SELECT value_json FROM scheduler_state WHERE key=?", (key,)
            ).fetchone()
            return json.loads(row["value_json"]) if row else None

    def state_put(self, key: str, value: Any, *, now: float) -> None:
        with self.transaction(immediate=True) as connection:
            connection.execute(
                """
                INSERT INTO scheduler_state(key, value_json, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json=excluded.value_json,
                    updated_at=excluded.updated_at
                """,
                (key, _json(value), now),
            )

    def integrity_check(self) -> tuple[str, int]:
        with self.transaction() as connection:
            integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
            foreign_keys = len(
                connection.execute("PRAGMA foreign_key_check").fetchall()
            )
            return integrity, foreign_keys


# Sentinel must be defined after the class body on Python 3.11. It is rebound
# to the default object used by update_agent through function defaults above.
# The walrus assignment is intentionally avoided for cross-version parsing.


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _loads(value: Optional[str]) -> Mapping[str, Any]:
    if not value:
        return {}
    loaded = json.loads(value)
    return loaded if isinstance(loaded, Mapping) else {"value": loaded}


def _agent(row: sqlite3.Row) -> AgentRecord:
    return AgentRecord(
        id=str(row["id"]),
        project_id=str(row["project_id"]),
        conversation_id=row["conversation_id"],
        title=str(row["title"]),
        completion_marker=str(row["completion_marker"]),
        state=AgentState(str(row["state"])),
        snapshot_hash=row["snapshot_hash"],
        unchanged_count=int(row["unchanged_count"]),
        last_inspected_at=row["last_inspected_at"],
        next_inspection_at=row["next_inspection_at"],
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
        completed_at=row["completed_at"],
        deleted_at=row["deleted_at"],
        last_error=row["last_error"],
    )


def _operation(row: sqlite3.Row) -> OperationRecord:
    return OperationRecord(
        id=str(row["id"]),
        agent_id=row["agent_id"],
        type=OperationType(str(row["type"])),
        lane=Lane(str(row["lane"])),
        priority=int(row["priority"]),
        state=OperationState(str(row["state"])),
        idempotency_key=str(row["idempotency_key"]),
        coalesce_key=row["coalesce_key"],
        payload=_loads(row["payload_json"]),
        due_at=float(row["due_at"]),
        attempts=int(row["attempts"]),
        max_attempts=int(row["max_attempts"]),
        claim_token=row["claim_token"],
        claim_expires_at=row["claim_expires_at"],
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
        last_error=row["last_error"],
        result=_loads(row["result_json"]) if row["result_json"] else None,
    )


def _circuit(row: sqlite3.Row) -> CircuitRecord:
    return CircuitRecord(
        scope=str(row["scope"]),
        state=CircuitState(str(row["state"])),
        opened_at=row["opened_at"],
        retry_at=row["retry_at"],
        probe_failures=int(row["probe_failures"]),
        half_open_successes=int(row["half_open_successes"]),
        last_success_at=row["last_success_at"],
        paced_started_at=row["paced_started_at"],
        last_attempt_at=row["last_attempt_at"],
        updated_at=float(row["updated_at"]),
    )
