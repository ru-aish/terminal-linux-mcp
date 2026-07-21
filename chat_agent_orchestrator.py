from __future__ import annotations
import asyncio
import contextlib
import hashlib
import json
import secrets
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, AsyncContextManager, AsyncIterator, Callable, Protocol

from conversation_gateway import ConversationGateway, DeliveryResult, DeliveryState
from durable_ledger import COMMAND_PURPOSES, DurableLedger
from state_reducer import ActionType, ReducedState, reduce_snapshot


PROJECT_POLICIES = frozenset({"explicit", "inherit_parent", "none"})
NOTIFICATION_POLICIES = frozenset({"notify_only", "auto_resume"})
INTERRUPT_POLICIES = frozenset({"queue", "interrupt"})
DEFAULT_AGENT_COMPLETION_MARKER = "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"
DEFAULT_AGENT_CONTINUE_MESSAGE = (
    "Continue working on the assigned task. Do not stop at an intermediate result. "
    "When every requested step is complete, output the required completion marker "
    "as an exact standalone line."
)


def _now() -> float:
    return time.time()


def _id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(12)}"


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _safe_error(error: BaseException) -> str:
    text = " ".join(str(error or type(error).__name__).split())
    for needle in ("authorization", "cookie", "bearer", "token"):
        lowered = text.casefold()
        index = lowered.find(needle)
        if index >= 0:
            text = text[:index] + f"{needle}=[redacted]"
    return text[:500]


def _parse_wake_at(value: str | float | int) -> float:
    if isinstance(value, (int, float)):
        parsed = float(value)
    else:
        text = str(value or "").strip()
        if not text:
            raise ValueError("wake_at is required")
        try:
            parsed = float(text)
        except ValueError:
            try:
                moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(
                    "wake_at must be a Unix timestamp or ISO-8601 time with timezone"
                ) from exc
            if moment.tzinfo is None:
                raise ValueError("wake_at ISO-8601 values must include a timezone")
            parsed = moment.timestamp()
    if not 0 < parsed < 32_503_680_000:
        raise ValueError("wake_at is outside the supported timestamp range")
    return parsed


class AgentRuntime(Protocol):
    async def list_projects(
        self, *, limit: int = 20, cursor: str | None = None, owned_only: bool = True
    ) -> dict[str, Any]: ...

    async def get_project(self, project_id: str) -> dict[str, Any]: ...

    async def list_project_threads(
        self,
        project_id: str,
        *,
        limit: int = 20,
        cursor: str | None = None,
        owned_only: bool = True,
    ) -> dict[str, Any]: ...

    async def create_thread(
        self, prompt: str, *, project_id: str | None = None, title: str | None = None
    ) -> dict[str, Any]: ...

    async def get_thread(self, conversation_id: str) -> dict[str, Any]: ...

    async def continue_thread(
        self,
        conversation_id: str,
        message: str,
        *,
        expected_current_node: str,
        wait_for_completion: bool = True,
        user_message_id: str | None = None,
    ) -> dict[str, Any]: ...

    async def thread_context(
        self,
        conversation_id: str,
        *,
        since_cursor: str | None = None,
        max_events: int = 60,
        max_chars: int = 12000,
    ) -> dict[str, Any]: ...

    async def thread_tail(
        self, conversation_id: str, *, lines: int = 60, max_chars: int = 12000
    ) -> dict[str, Any]: ...

    async def cancel(self, conversation_id: str) -> dict[str, Any]: ...


RuntimeFactory = Callable[[], AsyncContextManager[AgentRuntime]]


@dataclass(frozen=True)
class CoordinatorConfig:
    database_path: Path
    context_max_events: int = 60
    context_max_chars: int = 12000
    tail_lines: int = 60
    tail_max_chars: int = 12000
    max_continue_attempts: int = 20
    stale_after_seconds: float = 600.0
    initial_check_seconds: float = 60.0
    progress_check_seconds: float = 180.0
    idle_check_seconds: float = 60.0
    heartbeat_seconds: float = 600.0


# Compatibility name retained for existing integrations and tests.
AgentRepository = DurableLedger

class ChatAgentCoordinator:
    def __init__(self, config: CoordinatorConfig, runtime_factory: RuntimeFactory):
        self.config = config
        self.repository = AgentRepository(config.database_path)
        self.runtime_factory = runtime_factory
        self._lock = asyncio.Lock()
        self._reconcile_inflight_operations()

    def _reconcile_inflight_operations(self) -> None:
        now = _now()
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='delivery_uncertain',last_error=?,next_attempt_at=? "
                "WHERE status='delivery_in_flight'",
                (
                    "Terminal MCP stopped during an external ChatGPT operation",
                    now + self.config.heartbeat_seconds,
                ),
            )
            db.execute(
                "UPDATE commands SET status='waiting_after_cancel',last_error=?,next_attempt_at=? "
                "WHERE status='cancel_in_flight'",
                (
                    "Terminal MCP stopped while cancellation was in flight; "
                    "waiting for a verified terminal target before message delivery",
                    now + self.config.progress_check_seconds,
                ),
            )

            continuation_rows = [
                dict(row)
                for row in db.execute(
                    "SELECT t.task_id,t.agent_id,t.last_continue_node "
                    "FROM tasks t WHERE t.status='continuation_in_flight'"
                )
            ]
            for row in continuation_rows:
                db.execute(
                    "UPDATE tasks SET status='continuation_uncertain',next_check_at=? WHERE task_id=?",
                    (
                        now + self.config.heartbeat_seconds,
                        row["task_id"],
                    ),
                )
                db.execute(
                    "UPDATE agents SET status='error',last_error=?,updated_at=? WHERE agent_id=?",
                    (
                        "Terminal MCP stopped during automatic continuation",
                        now,
                        row["agent_id"],
                    ),
                )
                self._insert_event(
                    db,
                    row["agent_id"],
                    row["task_id"],
                    "error",
                    {
                        "reason": "Terminal MCP stopped during automatic continuation",
                        "operation": "automatic_continuation",
                    },
                    source_cursor=(
                        f"restart-continuation:{row['task_id']}:"
                        f"{row.get('last_continue_node') or 'unknown'}"
                    ),
                )

            cancel_rows = [
                dict(row)
                for row in db.execute(
                    "SELECT a.agent_id,t.task_id FROM agents a "
                    "LEFT JOIN tasks t ON t.agent_id=a.agent_id "
                    "WHERE a.status='cancel_in_flight' "
                    "OR t.status='cancel_in_flight'"
                )
            ]
            for row in cancel_rows:
                reason = "Terminal MCP stopped while cancelling the agent"
                db.execute(
                    "UPDATE agents SET status='cancelled',last_error=?,updated_at=? WHERE agent_id=?",
                    (reason, now, row["agent_id"]),
                )
                if row.get("task_id"):
                    db.execute(
                        "UPDATE tasks SET status='cancelled',completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                        (now, row["task_id"]),
                    )
                db.execute(
                    "UPDATE commands SET status='cancelled',last_error=? "
                    "WHERE to_agent_id=? AND status IN ('queued','waiting_after_cancel','delivery_in_flight','cancel_in_flight')",
                    (reason, row["agent_id"]),
                )
                self._insert_event(
                    db,
                    row["agent_id"],
                    row.get("task_id"),
                    "cancelled",
                    {"reason": reason, "interrupt_outcome": "uncertain"},
                    source_cursor=f"restart-cancel:{row['agent_id']}",
                )

            creation_rows = [
                dict(row)
                for row in db.execute(
                    "SELECT a.agent_id,a.chat_id,t.task_id FROM agents a "
                    "JOIN tasks t ON t.agent_id=a.agent_id "
                    "WHERE a.status='creation_in_flight' "
                    "OR t.status='creation_in_flight'"
                )
            ]
            for row in creation_rows:
                reason = "Terminal MCP stopped while creating the ChatGPT child thread"
                if row.get("chat_id"):
                    db.execute(
                        "UPDATE agents SET status='creation_uncertain',last_error=?,updated_at=? "
                        "WHERE agent_id=?",
                        (reason, now, row["agent_id"]),
                    )
                    db.execute(
                        "UPDATE tasks SET status='creation_uncertain',completed_at=NULL,next_check_at=? "
                        "WHERE task_id=?",
                        (now, row["task_id"]),
                    )
                    self._insert_event(
                        db,
                        row["agent_id"],
                        row["task_id"],
                        "blocked",
                        {
                            "reason": reason,
                            "operation": "thread_creation",
                            "uncertain": True,
                        },
                        source_cursor=f"restart-creation-uncertain:{row['task_id']}",
                    )
                    continue
                db.execute(
                    "UPDATE agents SET status='failed',last_error=?,updated_at=? WHERE agent_id=?",
                    (reason, now, row["agent_id"]),
                )
                db.execute(
                    "UPDATE tasks SET status='failed',completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                    (now, row["task_id"]),
                )
                self._insert_event(
                    db,
                    row["agent_id"],
                    row["task_id"],
                    "failed",
                    {"reason": reason, "operation": "thread_creation"},
                    source_cursor=f"restart-creation:{row['task_id']}",
                )

    def next_due_at(self) -> float | None:
        value = self.repository.next_due_at()
        return float(value) if value is not None else None

    def has_pending_work(self) -> bool:
        due_at = self.next_due_at()
        return due_at is not None and due_at <= _now()

    def prepare_local_work(self) -> None:
        self._queue_parent_notifications()

    def _task_next_check_at(
        self,
        *,
        status: str,
        progress_changed: bool,
        now: float,
    ) -> float:
        if status in {"completed", "failed", "cancelled", "waiting_for_parent"}:
            return 0.0
        if status in {"idle", "stale"}:
            return now + self.config.idle_check_seconds
        if progress_changed:
            return now + self.config.progress_check_seconds
        return now + self.config.heartbeat_seconds

    def _defer_command(
        self,
        db: sqlite3.Connection,
        command_id: str,
        *,
        seconds: float | None = None,
        reason: str | None = None,
    ) -> None:
        delay = self.config.heartbeat_seconds if seconds is None else max(0.0, seconds)
        if reason is None:
            db.execute(
                "UPDATE commands SET next_attempt_at=? WHERE command_id=?",
                (_now() + delay, command_id),
            )
        else:
            db.execute(
                "UPDATE commands SET next_attempt_at=?,last_error=? WHERE command_id=?",
                (_now() + delay, reason, command_id),
            )

    @staticmethod
    def _normalize_working_directory(value: str | None) -> str | None:
        if not value:
            return None
        path = Path(value).expanduser()
        if not path.is_absolute():
            raise ValueError("working_directory must be an absolute path")
        path = path.resolve()
        if not path.exists():
            raise ValueError(f"working_directory does not exist: {path}")
        if not path.is_dir():
            raise ValueError(f"working_directory is not a directory: {path}")
        return str(path)

    @staticmethod
    def _progress_signature(snapshot: dict[str, Any]) -> str:
        turns = snapshot.get("turns")
        normalized_turns = turns if isinstance(turns, list) else []
        latest = normalized_turns[-1] if normalized_turns else {}
        payload = {
            "current_node": str(snapshot.get("current_node") or ""),
            "visible_current_node": str(
                snapshot.get("visible_current_node") or snapshot.get("current_node") or ""
            ),
            "turn_count": len(normalized_turns),
            "running": bool(snapshot.get("running")),
            "active_stream": bool(snapshot.get("active_stream")),
            "latest": {
                "key": str(latest.get("key") or latest.get("node_id") or ""),
                "role": str(latest.get("role") or ""),
                "status": str(latest.get("status") or ""),
                "end_turn": latest.get("end_turn"),
                "text_hash": hashlib.sha256(
                    str(latest.get("text") or "").encode("utf-8")
                ).hexdigest(),
            },
        }
        return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()

    async def register_parent(
        self,
        chat_id: str,
        *,
        project_id: str | None = None,
        working_directory: str | None = None,
        title: str = "",
        notification_policy: str = "notify_only",
    ) -> dict[str, Any]:
        chat_id = chat_id.strip()
        if not chat_id:
            raise ValueError("chat_id is required")
        if notification_policy not in NOTIFICATION_POLICIES:
            raise ValueError("notification_policy must be notify_only or auto_resume")
        existing = self.repository.agent_by_chat(chat_id)
        if existing:
            requested_working_directory = self._normalize_working_directory(
                working_directory
            )
            stored_working_directory = str(
                existing.get("working_directory") or ""
            ) or None
            if (
                requested_working_directory
                and stored_working_directory
                and requested_working_directory != stored_working_directory
            ):
                raise ValueError(
                    "registered parent already belongs to a different working_directory"
                )
            if requested_working_directory and not stored_working_directory:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE agents SET working_directory=?,updated_at=? WHERE agent_id=?",
                        (requested_working_directory, _now(), existing["agent_id"]),
                    )
                existing = self.repository.agent(existing["agent_id"]) or existing
            return self._public_agent(existing)

        async with self.runtime_factory() as runtime:
            snapshot = await runtime.get_thread(chat_id)
        if not snapshot.get("found") or not snapshot.get("state_verified"):
            raise ValueError(
                str(snapshot.get("reason") or "parent conversation is unavailable or unverified")
            )
        discovered_project = str(snapshot.get("project_id") or "") or None
        requested_project = project_id.strip() if project_id else None
        if requested_project != discovered_project and requested_project is not None:
            raise ValueError("supplied project_id does not match the parent conversation")
        selected_project = discovered_project
        selected_working_directory = self._normalize_working_directory(working_directory)
        selected_title = title.strip() or str(snapshot.get("title") or "")

        now = _now()
        orchestration_id = _id("orch")
        agent_id = _id("agent")
        try:
            with self.repository.transaction() as db:
                db.execute(
                    "INSERT INTO orchestrations(orchestration_id,root_agent_id,notification_policy,created_at) VALUES(?,?,?,?)",
                    (orchestration_id, agent_id, notification_policy, now),
                )
                db.execute(
                    "INSERT INTO agents(agent_id,orchestration_id,parent_agent_id,root_agent_id,chat_id,project_id,working_directory,title,status,notification_policy,current_node,last_progress_at,progress_signature,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        agent_id,
                        orchestration_id,
                        None,
                        agent_id,
                        chat_id,
                        selected_project,
                        selected_working_directory,
                        selected_title,
                        "running" if snapshot.get("running") else "registered",
                        notification_policy,
                        str(snapshot.get("current_node") or ""),
                        now,
                        self._progress_signature(snapshot),
                        now,
                        now,
                    ),
                )
        except sqlite3.IntegrityError:
            concurrent = self.repository.agent_by_chat(chat_id)
            if concurrent:
                return self._public_agent(concurrent)
            raise
        return self._public_agent(self.repository.agent(agent_id) or {})

    async def list_projects(self, *, limit: int = 20, cursor: str | None = None) -> dict[str, Any]:
        async with self.runtime_factory() as runtime:
            return await runtime.list_projects(limit=min(max(limit, 1), 50), cursor=cursor)

    async def get_project(self, project_id: str) -> dict[str, Any]:
        async with self.runtime_factory() as runtime:
            return await runtime.get_project(project_id)

    async def list_project_threads(
        self, project_id: str, *, limit: int = 20, cursor: str | None = None
    ) -> dict[str, Any]:
        async with self.runtime_factory() as runtime:
            return await runtime.list_project_threads(
                project_id, limit=min(max(limit, 1), 50), cursor=cursor
            )

    @staticmethod
    def _build_initial_prompt(
        prompt: str,
        marker: str,
        *,
        agent_id: str,
        task_id: str,
        parent_agent_id: str,
        root_agent_id: str,
        orchestration_id: str,
        project_id: str | None,
        working_directory: str | None,
    ) -> str:
        identity = (
            "[TERMINAL MCP AGENT CONTEXT]\n"
            f"agent_id: {agent_id}\n"
            f"task_id: {task_id}\n"
            f"parent_agent_id: {parent_agent_id}\n"
            f"root_agent_id: {root_agent_id}\n"
            f"orchestration_id: {orchestration_id}\n"
            f"project_id: {project_id or 'none'}\n"
            f"working_directory: {working_directory or 'none'}\n"
            "Use these exact IDs when calling agent_send, agent_spawn, agent_status, "
            "agent_context, or agent_children. Do not invent replacement IDs. "
            "To spawn a grandchild, pass your own agent_id as parent_agent_id. "
            "To message your parent, pass your own agent_id as from_agent_id and "
            "parent_agent_id as to_agent_id. Use purpose='question' only for a "
            "blocking question that requires the direct parent's answer; this pauses "
            "generic continuation. The parent replies with purpose='answer'. Use the "
            "default purpose for non-blocking updates.\n"
            + (
                "Before terminal work, call bootstrap_thread with thread_id equal to "
                f"your agent_id and cwd={working_directory!r}. Reuse agent_id as "
                "session_id in every later Terminal MCP call."
                if working_directory
                else "Before terminal work, obtain a working_directory from your parent."
            )
        )
        completion = ""
        if marker:
            completion = (
                "\n\nCompletion contract: keep working until the entire assigned task is "
                "complete. Only then, output this marker as an exact standalone line:\n"
                f"{marker}"
            )
        return f"{identity}\n\n[ASSIGNED TASK]\n{prompt}{completion}"

    def _resolve_project(
        self,
        parent: dict[str, Any],
        project_policy: str,
        project_id: str | None,
    ) -> str | None:
        if project_policy not in PROJECT_POLICIES:
            raise ValueError("project_policy must be explicit, inherit_parent, or none")
        if project_policy == "none":
            if project_id:
                raise ValueError("project_id is not allowed when project_policy=none")
            return None
        if project_policy == "inherit_parent":
            if project_id:
                raise ValueError("project_id is not allowed when project_policy=inherit_parent")
            return str(parent.get("project_id") or "") or None
        if not project_id:
            raise ValueError("project_id is required when project_policy=explicit")
        return project_id

    def _mark_creation_failed(
        self,
        agent_id: str,
        task_id: str,
        reason: str,
        *,
        uncertain: bool,
    ) -> None:
        message = (
            f"{reason}; creation outcome is uncertain and will not be retried automatically"
            if uncertain
            else reason
        )
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET status='failed',last_error=?,updated_at=? WHERE agent_id=?",
                (message, _now(), agent_id),
            )
            db.execute(
                "UPDATE tasks SET status='failed',completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                (_now(), task_id),
            )
            self._insert_event(
                db,
                agent_id,
                task_id,
                "failed",
                {
                    "reason": message,
                    "operation": "thread_creation",
                    "uncertain": uncertain,
                },
                source_cursor=f"creation-failed:{task_id}",
            )

    def _mark_creation_uncertain(
        self,
        agent_id: str,
        task_id: str,
        created: dict[str, Any],
        reason: str,
    ) -> None:
        chat_id = str(created.get("conversation_id") or "")
        if not chat_id:
            raise ValueError("an uncertain creation requires a conversation_id")
        message = f"{reason}; awaiting canonical creation reconciliation"
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET chat_id=?,status='creation_uncertain',current_node=?,"
                "creation_request_id=?,creation_user_message_id=?,last_error=?,updated_at=? "
                "WHERE agent_id=?",
                (
                    chat_id,
                    str(created.get("current_node") or ""),
                    str(created.get("request_id") or "") or None,
                    str(created.get("user_message_id") or "") or None,
                    message,
                    _now(),
                    agent_id,
                ),
            )
            db.execute(
                "UPDATE tasks SET status='creation_uncertain',completed_at=NULL,next_check_at=? "
                "WHERE task_id=?",
                (_now() + self.config.initial_check_seconds, task_id),
            )
            self._insert_event(
                db,
                agent_id,
                task_id,
                "blocked",
                {
                    "reason": message,
                    "operation": "thread_creation",
                    "uncertain": True,
                    "chat_id": chat_id,
                },
                source_cursor=f"creation-uncertain:{task_id}",
            )

    async def _create_reserved_agent(
        self,
        runtime: AgentRuntime,
        agent_id: str,
    ) -> None:
        agent = self.repository.agent(agent_id)
        task = self.repository.task_for_agent(agent_id)
        if not agent or not task:
            raise ValueError("reserved agent or task was not found")
        if agent.get("chat_id"):
            return
        if agent.get("status") != "creating_thread":
            raise ValueError("reserved agent is not eligible for thread creation")

        project_id = str(agent.get("project_id") or "") or None
        try:
            if project_id:
                project = await runtime.get_project(project_id)
                permissions = (
                    project.get("permissions")
                    if isinstance(project, dict)
                    else None
                )
                if (
                    not project.get("id")
                    or not isinstance(permissions, dict)
                    or not permissions.get("can_write")
                ):
                    raise ValueError(
                        "selected project is unavailable or not writable"
                    )
        except Exception as exc:
            self._mark_creation_failed(
                agent_id,
                task["task_id"],
                _safe_error(exc),
                uncertain=False,
            )
            raise

        effective_prompt = self._build_initial_prompt(
            str(task["prompt"]),
            str(task.get("completion_marker") or ""),
            agent_id=agent_id,
            task_id=str(task["task_id"]),
            parent_agent_id=str(agent["parent_agent_id"]),
            root_agent_id=str(agent["root_agent_id"]),
            orchestration_id=str(agent["orchestration_id"]),
            project_id=project_id,
            working_directory=(
                str(agent.get("working_directory") or "") or None
            ),
        )
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET status='creation_in_flight',updated_at=? WHERE agent_id=?",
                (_now(), agent_id),
            )
            db.execute(
                "UPDATE tasks SET status='creation_in_flight' WHERE task_id=?",
                (task["task_id"],),
            )

        try:
            created = await runtime.create_thread(
                effective_prompt,
                project_id=project_id,
                title=str(agent.get("title") or "") or None,
            )
        except Exception as exc:
            self._mark_creation_failed(
                agent_id,
                task["task_id"],
                _safe_error(exc),
                uncertain=True,
            )
            raise

        chat_id = str(created.get("conversation_id") or "")
        if not chat_id or not created.get("observed", True):
            reason = str(
                created.get("reason")
                or "new conversation was not canonically observed"
            )
            if chat_id and created.get("sent"):
                self._mark_creation_uncertain(
                    agent_id,
                    task["task_id"],
                    created,
                    reason,
                )
                return
            self._mark_creation_failed(
                agent_id,
                task["task_id"],
                reason,
                uncertain=bool(created.get("sent")),
            )
            raise RuntimeError(reason)

        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET chat_id=?,status=?,current_node=?,last_progress_at=?,"
                "progress_signature=?,creation_request_id=?,creation_user_message_id=?,"
                "updated_at=?,last_error='' WHERE agent_id=?",
                (
                    chat_id,
                    "running" if created.get("running") else "idle",
                    str(created.get("current_node") or ""),
                    _now(),
                    self._progress_signature(created),
                    str(created.get("request_id") or "") or None,
                    str(created.get("user_message_id") or "") or None,
                    _now(),
                    agent_id,
                ),
            )
            db.execute(
                "UPDATE tasks SET status='running',next_check_at=? WHERE task_id=?",
                (_now() + self.config.initial_check_seconds, task["task_id"]),
            )
            self._insert_event(
                db,
                agent_id,
                task["task_id"],
                "started",
                {"chat_id": chat_id, "project_id": project_id},
                source_cursor=f"started:{task['task_id']}",
            )

    async def _recover_one_creation(
        self,
        runtime: AgentRuntime,
        *,
        force: bool = False,
    ) -> int | None:
        with self.repository.connect() as db:
            uncertain_query = (
                "SELECT a.*,t.task_id,t.next_check_at FROM agents a "
                "JOIN tasks t ON t.agent_id=a.agent_id "
                "WHERE a.status='creation_uncertain' AND a.chat_id IS NOT NULL "
            )
            uncertain_params: tuple[Any, ...] = ()
            if not force:
                uncertain_query += "AND COALESCE(t.next_check_at,0)<=? "
                uncertain_params = (_now(),)
            uncertain_query += "ORDER BY a.created_at,a.agent_id LIMIT 1"
            uncertain = db.execute(
                uncertain_query, uncertain_params
            ).fetchone()
        if uncertain:
            candidate = dict(uncertain)
            try:
                snapshot = await runtime.get_thread(str(candidate["chat_id"]))
            except Exception as exc:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE agents SET last_error=?,updated_at=? WHERE agent_id=?",
                        (_safe_error(exc), _now(), candidate["agent_id"]),
                    )
                    db.execute(
                        "UPDATE tasks SET next_check_at=? WHERE task_id=?",
                        (
                            _now() + self.config.progress_check_seconds,
                            candidate["task_id"],
                        ),
                    )
                return 0
            expected_user_id = str(
                candidate.get("creation_user_message_id") or ""
            )
            user_observed = not expected_user_id or (
                expected_user_id in {
                    str(value)
                    for value in snapshot.get("all_message_ids", ())
                    if value
                }
                or any(
                    str(turn.get("key") or "") == expected_user_id
                    for turn in snapshot.get("turns", ())
                    if isinstance(turn, dict)
                )
            )
            verified = bool(
                snapshot.get("found")
                and snapshot.get("canonical")
                and snapshot.get("state_verified")
                and user_observed
            )
            if not verified:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE agents SET last_error=?,updated_at=? WHERE agent_id=?",
                        (
                            "created conversation is not canonically verified yet",
                            _now(),
                            candidate["agent_id"],
                        ),
                    )
                    db.execute(
                        "UPDATE tasks SET next_check_at=? WHERE task_id=?",
                        (
                            _now() + self.config.progress_check_seconds,
                            candidate["task_id"],
                        ),
                    )
                return 0
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE agents SET status=?,current_node=?,last_progress_at=?,"
                    "progress_signature=?,last_error='',updated_at=? WHERE agent_id=?",
                    (
                        "running" if snapshot.get("running") else "idle",
                        str(snapshot.get("current_node") or ""),
                        _now(),
                        self._progress_signature(snapshot),
                        _now(),
                        candidate["agent_id"],
                    ),
                )
                db.execute(
                    "UPDATE tasks SET status='running',completed_at=NULL,next_check_at=? "
                    "WHERE task_id=?",
                    (
                        _now() + self.config.initial_check_seconds,
                        candidate["task_id"],
                    ),
                )
                self._insert_event(
                    db,
                    candidate["agent_id"],
                    candidate["task_id"],
                    "started",
                    {
                        "chat_id": candidate["chat_id"],
                        "project_id": candidate.get("project_id"),
                        "recovered": True,
                    },
                    source_cursor=f"started:{candidate['task_id']}",
                )
            return 0

        with self.repository.connect() as db:
            query = (
                "SELECT a.agent_id FROM agents a JOIN tasks t ON t.agent_id=a.agent_id "
                "WHERE a.status='creating_thread' AND a.chat_id IS NULL "
            )
            params: tuple[Any, ...] = ()
            if not force:
                query += "AND COALESCE(t.next_check_at,0)<=? "
                params = (_now(),)
            query += "ORDER BY a.created_at,a.agent_id LIMIT 1"
            row = db.execute(query, params).fetchone()
        if not row:
            return None
        try:
            await self._create_reserved_agent(runtime, str(row["agent_id"]))
        except Exception:
            pass
        return 1

    async def spawn(
        self,
        parent_agent_id: str,
        prompt: str,
        *,
        project_policy: str = "inherit_parent",
        project_id: str | None = None,
        working_directory: str | None = None,
        title: str = "",
        completion_marker: str = DEFAULT_AGENT_COMPLETION_MARKER,
        notification_policy: str = "auto_resume",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        prompt = prompt.strip()
        if not prompt:
            raise ValueError("prompt is required")
        if notification_policy not in NOTIFICATION_POLICIES:
            raise ValueError("notification_policy must be notify_only or auto_resume")
        parent = self.repository.agent(parent_agent_id)
        if not parent:
            raise ValueError("parent agent was not found")
        selected_project = self._resolve_project(parent, project_policy, project_id)
        selected_working_directory = self._normalize_working_directory(
            working_directory or parent.get("working_directory")
        )
        marker = completion_marker.strip() or DEFAULT_AGENT_COMPLETION_MARKER
        key = idempotency_key.strip() if idempotency_key else None
        if key:
            with self.repository.connect() as db:
                existing = db.execute(
                    "SELECT * FROM agents WHERE parent_agent_id=? AND spawn_idempotency_key=?",
                    (parent_agent_id, key),
                ).fetchone()
            if existing:
                return await self.status(dict(existing)["agent_id"])

        now = _now()
        agent_id, task_id = _id("agent"), _id("task")
        try:
            with self.repository.transaction() as db:
                db.execute(
                    "INSERT INTO agents(agent_id,orchestration_id,parent_agent_id,root_agent_id,project_id,working_directory,title,status,notification_policy,spawn_idempotency_key,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        agent_id,
                        parent["orchestration_id"],
                        parent_agent_id,
                        parent["root_agent_id"],
                        selected_project,
                        selected_working_directory,
                        title,
                        "creating_thread",
                        notification_policy,
                        key,
                        now,
                        now,
                    ),
                )
                db.execute(
                    "INSERT INTO tasks(task_id,agent_id,prompt,completion_marker,status,created_at) VALUES(?,?,?,?,?,?)",
                    (task_id, agent_id, prompt, marker, "creating_thread", now),
                )
                db.execute(
                    "INSERT INTO subscriptions(parent_agent_id,child_agent_id,notification_policy,created_at) VALUES(?,?,?,?)",
                    (parent_agent_id, agent_id, notification_policy, now),
                )
        except sqlite3.IntegrityError:
            if key:
                with self.repository.connect() as db:
                    concurrent = db.execute(
                        "SELECT agent_id FROM agents WHERE parent_agent_id=? AND spawn_idempotency_key=?",
                        (parent_agent_id, key),
                    ).fetchone()
                if concurrent:
                    return await self.status(str(concurrent["agent_id"]))
            raise

        async with self._lock:
            async with self.runtime_factory() as runtime:
                await self._create_reserved_agent(runtime, agent_id)
        return await self.status(agent_id)

    async def status(self, agent_id: str) -> dict[str, Any]:
        agent = self.repository.agent(agent_id)
        if not agent:
            raise ValueError("agent was not found")
        task = self.repository.task_for_agent(agent_id)
        with self.repository.connect() as db:
            mailbox = db.execute(
                "SELECT status,COUNT(*) AS count FROM commands WHERE to_agent_id=? GROUP BY status",
                (agent_id,),
            ).fetchall()
        return {
            "agent": self._public_agent(agent),
            "task": task,
            "mailbox": {str(row["status"]): int(row["count"]) for row in mailbox},
        }

    async def context(
        self,
        agent_id: str,
        *,
        since_cursor: str | None = None,
        max_events: int | None = None,
        max_chars: int | None = None,
    ) -> dict[str, Any]:
        agent = self._require_chat_agent(agent_id)
        async with self.runtime_factory() as runtime:
            payload = await runtime.thread_context(
                agent["chat_id"],
                since_cursor=since_cursor,
                max_events=max_events or self.config.context_max_events,
                max_chars=max_chars or self.config.context_max_chars,
            )
        return {"agent_id": agent_id, **payload}

    async def tail(
        self, agent_id: str, *, lines: int | None = None, max_chars: int | None = None
    ) -> dict[str, Any]:
        agent = self._require_chat_agent(agent_id)
        async with self.runtime_factory() as runtime:
            payload = await runtime.thread_tail(
                agent["chat_id"],
                lines=lines or self.config.tail_lines,
                max_chars=max_chars or self.config.tail_max_chars,
            )
        return {"agent_id": agent_id, **payload}

    async def send(
        self,
        from_agent_id: str,
        to_agent_id: str,
        message: str,
        *,
        interrupt_policy: str = "queue",
        idempotency_key: str | None = None,
        purpose: str = "instruction",
    ) -> dict[str, Any]:
        if interrupt_policy not in INTERRUPT_POLICIES:
            raise ValueError("interrupt_policy must be queue or interrupt")
        if purpose not in COMMAND_PURPOSES:
            raise ValueError(
                "purpose must be instruction, answer, question, progress, or completion"
            )
        message = message.strip()
        if not message:
            raise ValueError("message is required")
        sender = self.repository.agent(from_agent_id)
        target = self.repository.agent(to_agent_id)
        if not sender or not target:
            raise ValueError("sender or target agent was not found")
        if sender["orchestration_id"] != target["orchestration_id"]:
            raise ValueError("agents belong to different orchestrations")
        if purpose == "question" and sender.get("parent_agent_id") != to_agent_id:
            raise ValueError("a question must be sent from a child to its direct parent")
        if purpose == "answer" and target.get("parent_agent_id") != from_agent_id:
            raise ValueError("an answer must be sent from a parent to its direct child")
        if purpose == "instruction" and sender.get("parent_agent_id") == to_agent_id:
            # The default downward message is an instruction. The same default
            # used upward is a non-blocking child update, so persist it as
            # progress; final completion may then supersede uncertain progress.
            purpose = "progress"
        key = idempotency_key.strip() if idempotency_key else None
        if key:
            with self.repository.connect() as db:
                existing = db.execute(
                    "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=? AND idempotency_key=?",
                    (from_agent_id, to_agent_id, key),
                ).fetchone()
            if existing:
                return dict(existing)
        terminal_statuses = {"cancelled", "failed", "completed"}
        if str(sender.get("status") or "") in terminal_statuses:
            raise ValueError("sender agent is terminal and cannot send new messages")
        if str(target.get("status") or "") in terminal_statuses:
            raise ValueError("target agent is terminal and cannot receive new messages")
        try:
            with self.repository.transaction() as db:
                if interrupt_policy == "interrupt" and purpose == "instruction":
                    db.execute(
                        "UPDATE commands SET status='superseded',last_error=?,next_attempt_at=0 "
                        "WHERE from_agent_id=? AND to_agent_id=? AND purpose='instruction' "
                        "AND interrupt_policy='queue' AND status IN ('queued','waiting_after_cancel')",
                        (
                            "superseded by direct interrupt steering",
                            from_agent_id,
                            to_agent_id,
                        ),
                    )
                sequence = int(
                    db.execute(
                        "SELECT COALESCE(MAX(sequence_no),0)+1 FROM commands WHERE to_agent_id=?",
                        (to_agent_id,),
                    ).fetchone()[0]
                )
                command_id = _id("cmd")
                db.execute(
                    "INSERT INTO commands(command_id,from_agent_id,to_agent_id,sequence_no,message,interrupt_policy,status,idempotency_key,purpose,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        command_id,
                        from_agent_id,
                        to_agent_id,
                        sequence,
                        message,
                        interrupt_policy,
                        "queued",
                        key,
                        purpose,
                        _now(),
                    ),
                )
                if purpose == "question":
                    db.execute(
                        "UPDATE agents SET status='waiting_for_parent',updated_at=? "
                        "WHERE agent_id=?",
                        (_now(), from_agent_id),
                    )
                    db.execute(
                        "UPDATE tasks SET status='waiting_for_parent',next_check_at=0 WHERE agent_id=?",
                        (from_agent_id,),
                    )
        except sqlite3.IntegrityError:
            if key:
                with self.repository.connect() as db:
                    concurrent = db.execute(
                        "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=? AND idempotency_key=?",
                        (from_agent_id, to_agent_id, key),
                    ).fetchone()
                if concurrent:
                    return dict(concurrent)
            raise
        with self.repository.connect() as db:
            return dict(
                db.execute(
                    "SELECT * FROM commands WHERE command_id=?", (command_id,)
                ).fetchone()
            )

    def _resolve_agent_reference(self, value: str) -> dict[str, Any]:
        reference = str(value or "").strip()
        if not reference:
            raise ValueError("thread_id is required")
        agent = self.repository.agent(reference)
        if agent is None:
            agent = self.repository.agent_by_chat(reference)
        if agent is None:
            raise ValueError("agent/thread was not found")
        return dict(agent)

    @staticmethod
    def _public_automation(row: dict[str, Any]) -> dict[str, Any]:
        return {
            key: row.get(key)
            for key in (
                "automation_id",
                "orchestration_id",
                "kind",
                "source_agent_id",
                "target_agent_id",
                "message",
                "completion_marker",
                "due_at",
                "status",
                "command_id",
                "triggered_at",
                "completed_at",
                "last_error",
                "created_at",
                "updated_at",
            )
        }

    async def schedule_wakeup(
        self,
        thread_id: str,
        wake_at: str | float | int,
        *,
        prompt: str = "",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        target = self._resolve_agent_reference(thread_id)
        if str(target.get("status") or "") in {"failed", "cancelled"}:
            raise ValueError("failed or cancelled agents cannot be scheduled")
        message = prompt.strip() or (
            "Wake up now. Inspect your durable objective, managed subagents, queued "
            "messages, timers, and synchronization state, then continue the work that "
            "is actually due."
        )
        if len(message) > 100_000:
            raise ValueError("prompt is too large")
        due_at = _parse_wake_at(wake_at)
        key = idempotency_key.strip() if idempotency_key else None
        if key:
            with self.repository.connect() as db:
                existing = db.execute(
                    "SELECT * FROM agent_automations WHERE target_agent_id=? "
                    "AND kind='wakeup' AND idempotency_key=?",
                    (target["agent_id"], key),
                ).fetchone()
            if existing:
                return self._public_automation(dict(existing))
        now = _now()
        automation_id = _id("auto")
        with self.repository.transaction() as db:
            if key:
                existing = db.execute(
                    "SELECT * FROM agent_automations WHERE target_agent_id=? "
                    "AND kind='wakeup' AND idempotency_key=?",
                    (target["agent_id"], key),
                ).fetchone()
                if existing:
                    return self._public_automation(dict(existing))
            db.execute(
                "INSERT INTO agent_automations("
                "automation_id,orchestration_id,kind,target_agent_id,message,due_at,"
                "status,idempotency_key,created_at,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?)",
                (
                    automation_id,
                    target["orchestration_id"],
                    "wakeup",
                    target["agent_id"],
                    message,
                    due_at,
                    "scheduled",
                    key,
                    now,
                    now,
                ),
            )
        row = self.repository.automation(automation_id)
        assert row is not None
        return self._public_automation(row)

    async def queue_after_completion(
        self,
        source_thread_id: str,
        target_thread_id: str,
        prompt: str,
        *,
        completion_marker: str = "",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        source = self._resolve_agent_reference(source_thread_id)
        target = self._resolve_agent_reference(target_thread_id)
        if source["orchestration_id"] != target["orchestration_id"]:
            raise ValueError("source and target agents belong to different orchestrations")
        if str(target.get("status") or "") in {"failed", "cancelled"}:
            raise ValueError("failed or cancelled agents cannot receive queued prompts")
        message = prompt.strip()
        if not message:
            raise ValueError("prompt is required")
        if len(message) > 100_000:
            raise ValueError("prompt is too large")
        source_task = self.repository.task_for_agent(str(source["agent_id"]))
        marker = completion_marker.strip() or str(
            (source_task or {}).get("completion_marker")
            or DEFAULT_AGENT_COMPLETION_MARKER
        ).strip()
        if not marker:
            raise ValueError("completion_marker is required for this source thread")
        if "\n" in marker or "\r" in marker:
            raise ValueError("completion_marker must be a single line")
        if len(marker) > 2_000:
            raise ValueError("completion_marker is too large")
        key = idempotency_key.strip() if idempotency_key else None
        if key:
            with self.repository.connect() as db:
                existing = db.execute(
                    "SELECT * FROM agent_automations WHERE target_agent_id=? "
                    "AND kind='after_completion' AND idempotency_key=?",
                    (target["agent_id"], key),
                ).fetchone()
            if existing:
                return self._public_automation(dict(existing))
        now = _now()
        automation_id = _id("auto")
        with self.repository.transaction() as db:
            if key:
                existing = db.execute(
                    "SELECT * FROM agent_automations WHERE target_agent_id=? "
                    "AND kind='after_completion' AND idempotency_key=?",
                    (target["agent_id"], key),
                ).fetchone()
                if existing:
                    return self._public_automation(dict(existing))
            db.execute(
                "INSERT INTO agent_automations("
                "automation_id,orchestration_id,kind,source_agent_id,target_agent_id,"
                "message,completion_marker,status,idempotency_key,created_at,updated_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    automation_id,
                    source["orchestration_id"],
                    "after_completion",
                    source["agent_id"],
                    target["agent_id"],
                    message,
                    marker,
                    "waiting",
                    key,
                    now,
                    now,
                ),
            )
        row = self.repository.automation(automation_id)
        assert row is not None
        return self._public_automation(row)

    async def automations(
        self,
        *,
        thread_id: str = "",
        include_terminal: bool = True,
    ) -> dict[str, Any]:
        agent_id = ""
        if thread_id.strip():
            agent_id = str(self._resolve_agent_reference(thread_id)["agent_id"])
        rows = self.repository.automations(
            agent_id=agent_id or None, include_terminal=include_terminal
        )
        return {
            "agent_id": agent_id or None,
            "automations": [self._public_automation(row) for row in rows],
        }

    async def cancel_automation(self, automation_id: str) -> dict[str, Any]:
        row = self.repository.automation(automation_id.strip())
        if row is None:
            raise ValueError("automation was not found")
        if row["status"] in {"delivered", "cancelled", "failed"}:
            return self._public_automation(row)
        command_id = str(row.get("command_id") or "")
        now = _now()
        with self.repository.transaction() as db:
            if command_id:
                command = db.execute(
                    "SELECT status FROM commands WHERE command_id=?", (command_id,)
                ).fetchone()
                command_status = str(command["status"] or "") if command else ""
                if command_status in {
                    "delivery_in_flight",
                    "delivery_uncertain",
                    "cancel_in_flight",
                }:
                    raise ValueError(
                        "automation delivery is already in flight and cannot be cancelled safely"
                    )
                if command_status in {"queued", "waiting_after_cancel"}:
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=? WHERE command_id=?",
                        ("automation cancelled before delivery", command_id),
                    )
            db.execute(
                "UPDATE agent_automations SET status='cancelled',completed_at=?,"
                "updated_at=?,last_error='' WHERE automation_id=?",
                (now, now, row["automation_id"]),
            )
        updated = self.repository.automation(str(row["automation_id"]))
        assert updated is not None
        return self._public_automation(updated)

    async def sync_once(self, *, force: bool = True) -> dict[str, Any]:
        if self._lock.locked():
            return {"status": "busy", "write_count": 0, "inspected": []}
        async with self._lock:
            inspected: list[dict[str, Any]] = []
            new_events: list[str] = []
            continuation_candidates: list[dict[str, Any]] = []
            async with self.runtime_factory() as runtime:
                self._queue_parent_notifications()
                source_refresh_agent_id = ""
                include_future_queued = False
                uncertain_commands = self.repository.uncertain_commands()
                future_uncertain_commands = (
                    self.repository.uncertain_commands(include_future=True)
                    if force and not uncertain_commands
                    else uncertain_commands
                )
                queued_commands = self.repository.queued_commands()
                if force and not uncertain_commands and not queued_commands:
                    queued_commands = self.repository.queued_commands(
                        include_future=True
                    )
                    include_future_queued = bool(queued_commands)
                if uncertain_commands:
                    head = uncertain_commands[0]
                    source_task = self.repository.task_for_agent(
                        str(head.get("from_agent_id") or "")
                    )
                    if (
                        force
                        and head.get("purpose") in {"progress", "question"}
                        and source_task
                        and source_task.get("status")
                        not in {"completed", "failed", "cancelled"}
                    ):
                        source_refresh_agent_id = str(
                            head.get("from_agent_id") or ""
                        )
                    else:
                        write_count = await self._reconcile_uncertain_commands(runtime)
                        return {
                            "status": "ok",
                            "write_count": write_count,
                            "inspected": inspected,
                            "new_event_ids": new_events,
                        }
                if queued_commands and not source_refresh_agent_id:
                    head = queued_commands[0]
                    source_task = self.repository.task_for_agent(
                        str(head.get("from_agent_id") or "")
                    )
                    if (
                        force
                        and head.get("purpose") in {"progress", "question"}
                        and source_task
                        and source_task.get("status")
                        not in {"completed", "failed", "cancelled"}
                    ):
                        source_refresh_agent_id = str(
                            head.get("from_agent_id") or ""
                        )
                    else:
                        write_count = await self._deliver_one_command(
                            runtime,
                            force=include_future_queued,
                        )
                        return {
                            "status": "ok",
                            "write_count": write_count,
                            "inspected": inspected,
                            "new_event_ids": new_events,
                        }
                creation_write_count = await self._recover_one_creation(
                    runtime, force=force
                )
                if creation_write_count is not None:
                    return {
                        "status": "ok",
                        "write_count": creation_write_count,
                        "inspected": inspected,
                        "new_event_ids": new_events,
                    }
                active_agents = self.repository.active_agents(include_future=force)
                if future_uncertain_commands and not uncertain_commands:
                    cooling_targets = {
                        str(command.get("to_agent_id") or "")
                        for command in future_uncertain_commands
                    }
                    active_agents = [
                        agent
                        for agent in active_agents
                        if agent["agent_id"] not in cooling_targets
                    ]
                if source_refresh_agent_id:
                    active_agents = [
                        agent
                        for agent in active_agents
                        if agent["agent_id"] == source_refresh_agent_id
                    ]
                else:
                    active_agents = active_agents[:1]
                for agent in active_agents:
                    task = self.repository.task_for_agent(agent["agent_id"])
                    try:
                        snapshot = await runtime.get_thread(agent["chat_id"])
                        with self.repository.connect() as db:
                            pending = [dict(row) for row in db.execute(
                                "SELECT * FROM commands WHERE to_agent_id=? AND status IN "
                                "('queued','waiting_after_cancel','delivery_uncertain') ORDER BY sequence_no",
                                (agent["agent_id"],),
                            )]
                        reduction = reduce_snapshot(task or {}, snapshot, pending)
                        status = {
                            ReducedState.RUNNING: "running",
                            ReducedState.AWAITING_ASSISTANT: "waiting_assistant",
                            ReducedState.STOPPED_INCOMPLETE: "idle",
                            ReducedState.WAITING_FOR_PARENT: "waiting_for_parent",
                            ReducedState.COMPLETED: "completed",
                            ReducedState.STALE: "stale",
                            ReducedState.FAILED: "failed",
                            ReducedState.CANCELLED: "cancelled",
                        }.get(reduction.state, "unknown")
                        current_node = str(snapshot.get("current_node") or "")
                        observed_at = _now()
                        progress_signature = self._progress_signature(snapshot)
                        stored_progress_signature = agent.get("progress_signature")
                        stored_progress_at = agent.get("last_progress_at")
                        previous_progress_at = float(
                            observed_at
                            if (
                                stored_progress_at is None
                                or stored_progress_signature is None
                            )
                            else stored_progress_at
                        )
                        progress_at = (
                            observed_at
                            if progress_signature != stored_progress_signature
                            else previous_progress_at
                        )
                        stale = bool(
                            status in {"running", "waiting_assistant", "unknown"}
                            and current_node
                            and observed_at - progress_at
                            >= self.config.stale_after_seconds
                        )
                        display_status = "stale" if stale else status
                        exhausted = False
                        if task and status == "idle":
                            attempts = int(task.get("continue_attempts") or 0)
                            if attempts >= self.config.max_continue_attempts:
                                exhausted = True
                                display_status = "failed"
                            elif str(task.get("last_continue_node") or "") == current_node:
                                display_status = "waiting_after_continue"
                            else:
                                continuation_candidates.append(
                                    {
                                        "agent": agent,
                                        "task": task,
                                        "snapshot": snapshot,
                                        "fairness": float(task.get("last_continue_at") or 0),
                                    }
                                )

                        progress_changed = progress_signature != stored_progress_signature
                        needs_context = progress_changed or display_status in {
                            "completed",
                            "failed",
                            "idle",
                            "stale",
                        }
                        if needs_context:
                            digest = await runtime.thread_context(
                                agent["chat_id"],
                                since_cursor=agent.get("context_cursor"),
                                max_events=self.config.context_max_events,
                                max_chars=self.config.context_max_chars,
                            )
                        else:
                            digest = {
                                "events": [],
                                "next_cursor": agent.get("context_cursor") or "",
                            }
                        next_check_at = self._task_next_check_at(
                            status=display_status,
                            progress_changed=progress_changed,
                            now=observed_at,
                        )
                        next_cursor = str(
                            digest.get("next_cursor")
                            or agent.get("context_cursor")
                            or ""
                        )
                        with self.repository.transaction() as db:
                            if digest.get("events"):
                                event_id = self._insert_event(
                                    db,
                                    agent["agent_id"],
                                    task.get("task_id") if task else None,
                                    "progress",
                                    digest,
                                    source_cursor=next_cursor or None,
                                )
                                if event_id:
                                    new_events.append(event_id)
                            if status == "completed":
                                event_id = self._insert_event(
                                    db,
                                    agent["agent_id"],
                                    task.get("task_id") if task else None,
                                    "completed",
                                    {"chat_id": agent["chat_id"]},
                                    source_cursor=f"completed:{current_node}",
                                )
                                if event_id:
                                    new_events.append(event_id)
                            if stale:
                                event_id = self._insert_event(
                                    db,
                                    agent["agent_id"],
                                    task.get("task_id") if task else None,
                                    "blocked",
                                    {
                                        "reason": "canonical thread made no structural progress",
                                        "status": status,
                                        "current_node": current_node,
                                        "stale_seconds": observed_at - progress_at,
                                    },
                                    source_cursor=f"stale:{current_node}",
                                )
                                if event_id:
                                    new_events.append(event_id)
                            if exhausted:
                                event_id = self._insert_event(
                                    db,
                                    agent["agent_id"],
                                    task.get("task_id") if task else None,
                                    "failed",
                                    {
                                        "reason": "maximum automatic continuation attempts reached",
                                        "attempts": int(task.get("continue_attempts") or 0),
                                    },
                                    source_cursor=f"continuation-exhausted:{current_node}",
                                )
                                if event_id:
                                    new_events.append(event_id)
                            if exhausted:
                                last_error = "maximum automatic continuation attempts reached"
                            elif stale:
                                last_error = "canonical thread made no structural progress"
                            else:
                                last_error = ""
                            db.execute(
                                "UPDATE agents SET status=?,current_node=?,context_cursor=?,last_progress_at=?,progress_signature=?,last_error=?,updated_at=? WHERE agent_id=?",
                                (
                                    display_status,
                                    current_node,
                                    next_cursor,
                                    progress_at,
                                    progress_signature,
                                    last_error,
                                    observed_at,
                                    agent["agent_id"],
                                ),
                            )
                            if task:
                                if status == "completed":
                                    db.execute(
                                        "UPDATE tasks SET status='completed',next_check_at=0,completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                                        (_now(), task["task_id"]),
                                    )
                                elif exhausted:
                                    db.execute(
                                        "UPDATE tasks SET status='failed',next_check_at=0,completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                                        (_now(), task["task_id"]),
                                    )
                                else:
                                    durable_task_status = display_status
                                    if (
                                        str(task.get("status") or "") == "waiting_for_parent"
                                        and display_status in {"running", "idle", "waiting_assistant"}
                                    ):
                                        durable_task_status = "waiting_for_parent"
                                    durable_next_check_at = (
                                        0.0
                                        if durable_task_status == "waiting_for_parent"
                                        else next_check_at
                                    )
                                    db.execute(
                                        "UPDATE tasks SET status=?,next_check_at=? WHERE task_id=?",
                                        (
                                            durable_task_status,
                                            durable_next_check_at,
                                            task["task_id"],
                                        ),
                                    )
                        inspected.append(
                            {"agent_id": agent["agent_id"], "status": display_status}
                        )
                    except Exception as exc:
                        reason = _safe_error(exc)
                        retry_at = _now() + self.config.heartbeat_seconds
                        with self.repository.transaction() as db:
                            db.execute(
                                "UPDATE agents SET status='error',last_error=?,updated_at=? WHERE agent_id=?",
                                (reason, _now(), agent["agent_id"]),
                            )
                            if task:
                                db.execute(
                                    "UPDATE tasks SET next_check_at=? WHERE task_id=?",
                                    (retry_at, task["task_id"]),
                                )
                        inspected.append(
                            {
                                "agent_id": agent["agent_id"],
                                "status": "error",
                                "reason": reason,
                            }
                        )

                self._queue_parent_notifications()
                write_count = await self._reconcile_uncertain_commands(runtime)
                if write_count == 0:
                    write_count = await self._deliver_one_command(
                        runtime,
                        force=include_future_queued,
                    )
                if write_count == 0:
                    write_count = await self._continue_one_task(
                        runtime, continuation_candidates
                    )
            return {
                "status": "ok",
                "write_count": write_count,
                "inspected": inspected,
                "new_event_ids": new_events,
            }

    async def _reconcile_uncertain_commands(
        self,
        runtime: AgentRuntime,
        *,
        force: bool = False,
    ) -> int:
        """Confirm an earlier submission from durable generated-message evidence.

        An uncertain command is never resent merely because the process restarted.
        If the generated user ID is visible on any normalized branch, it is
        delivered and its event cursor advances normally.
        """
        for command in self.repository.uncertain_commands(include_future=force):
            target = self.repository.agent(command["to_agent_id"])
            message_id = str(command.get("user_message_id") or "")
            parent_message_id = str(command.get("parent_message_id") or "")
            if not message_id and not parent_message_id:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=?,next_attempt_at=0 WHERE command_id=?",
                        (
                            "legacy uncertain delivery lacks durable reconciliation evidence",
                            command["command_id"],
                        ),
                    )
                continue
            if not target or not target.get("chat_id"):
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=?,next_attempt_at=0 WHERE command_id=?",
                        ("delivery target is unavailable", command["command_id"]),
                    )
                continue
            try:
                snapshot = await runtime.get_thread(target["chat_id"])
            except Exception as exc:
                with self.repository.transaction() as db:
                    self._defer_command(
                        db,
                        command["command_id"],
                        reason=_safe_error(exc),
                    )
                return 0
            reconciliation = ConversationGateway.reconcile(
                DeliveryResult(
                    state=DeliveryState.SENT_UNCONFIRMED,
                    reason=str(command.get("last_error") or ""),
                    request_id=str(command.get("request_id") or ""),
                    user_message_id=message_id,
                    parent_message_id=str(command.get("parent_message_id") or ""),
                    message=str(command.get("message") or ""),
                ),
                snapshot,
            )
            if not reconciliation.delivered:
                verified_absence = bool(
                    message_id
                    and snapshot.get("found") is True
                    and snapshot.get("canonical") is True
                    and snapshot.get("state_verified") is True
                    and not snapshot.get("running")
                    and not snapshot.get("active_stream")
                )
                with self.repository.transaction() as db:
                    if verified_absence:
                        db.execute(
                            "UPDATE commands SET status='queued',last_error=?,next_attempt_at=0 WHERE command_id=?",
                            (
                                "previous submission was not found in verified conversation state; retrying with the same user message id",
                                command["command_id"],
                            ),
                        )
                    else:
                        self._defer_command(db, command["command_id"])
                return 0
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE commands SET status='delivered',delivered_at=?,last_error='',next_attempt_at=0 WHERE command_id=?",
                    (_now(), command["command_id"]),
                )
                self._activate_target_after_delivery(db, command)
                self._ack_command_cursor(db, command)
            return 1
        return 0

    @staticmethod
    def _activate_target_after_delivery(
        db: sqlite3.Connection, command: dict[str, Any]
    ) -> None:
        if command.get("purpose") not in {"instruction", "answer"}:
            return
        db.execute(
            "UPDATE agents SET status='waiting_assistant',last_error='',updated_at=? "
            "WHERE agent_id=? AND status NOT IN ('failed','cancelled')",
            (_now(), command["to_agent_id"]),
        )
        db.execute(
            "UPDATE tasks SET status='waiting_assistant',completed_at=NULL,next_check_at=0 "
            "WHERE agent_id=? AND status NOT IN ('failed','cancelled')",
            (command["to_agent_id"],),
        )

    @staticmethod
    def _ack_command_cursor(db: sqlite3.Connection, command: dict[str, Any]) -> None:
        if not command.get("ack_event_seq"):
            return
        db.execute(
            "UPDATE subscriptions SET last_acked_event_seq="
            "CASE WHEN last_acked_event_seq<? THEN ? ELSE last_acked_event_seq END "
            "WHERE parent_agent_id=? AND child_agent_id=?",
            (
                int(command["ack_event_seq"]), int(command["ack_event_seq"]),
                command["to_agent_id"], command["from_agent_id"],
            ),
        )

    async def _deliver_one_command(
        self,
        runtime: AgentRuntime,
        *,
        force: bool = False,
    ) -> int:
        for command in self.repository.queued_commands(include_future=force):
            target = self.repository.agent(command["to_agent_id"])
            if not target or not target.get("chat_id"):
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=?,next_attempt_at=0 WHERE command_id=?",
                        ("delivery target is unavailable", command["command_id"]),
                    )
                continue
            target_status = str(target.get("status") or "")
            if target_status in {"cancelled", "failed"} or (
                target_status == "completed"
                and command.get("purpose") not in {"instruction", "answer"}
            ):
                with self.repository.transaction() as db:
                    terminal_reason = "target agent became terminal before delivery"
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=? WHERE command_id=?",
                        (terminal_reason, command["command_id"]),
                    )
                    if command.get("purpose") == "question" and command.get("from_agent_id"):
                        sender = self.repository.agent(str(command["from_agent_id"]))
                        sender_task = self.repository.task_for_agent(
                            str(command["from_agent_id"])
                        )
                        if sender and sender.get("parent_agent_id") == command["to_agent_id"]:
                            blocked_reason = (
                                "blocking question could not reach its terminal parent"
                            )
                            db.execute(
                                "UPDATE agents SET status='failed',last_error=?,updated_at=? "
                                "WHERE agent_id=? AND status NOT IN "
                                "('completed','failed','cancelled')",
                                (
                                    blocked_reason,
                                    _now(),
                                    command["from_agent_id"],
                                ),
                            )
                            if sender_task:
                                db.execute(
                                    "UPDATE tasks SET status='failed',"
                                    "completed_at=COALESCE(completed_at,?) "
                                    "WHERE task_id=? AND status NOT IN "
                                    "('completed','failed','cancelled')",
                                    (_now(), sender_task["task_id"]),
                                )
                                self._insert_event(
                                    db,
                                    str(command["from_agent_id"]),
                                    sender_task["task_id"],
                                    "blocked",
                                    {"reason": blocked_reason},
                                    source_cursor=(
                                        f"question-target-terminal:{command['command_id']}"
                                    ),
                                )
                    if command.get("ack_event_seq"):
                        db.execute(
                            "UPDATE subscriptions SET last_acked_event_seq="
                            "CASE WHEN last_acked_event_seq<? THEN ? ELSE last_acked_event_seq END "
                            "WHERE parent_agent_id=? AND child_agent_id=?",
                            (
                                int(command["ack_event_seq"]),
                                int(command["ack_event_seq"]),
                                command["to_agent_id"],
                                command["from_agent_id"],
                            ),
                        )
                continue
            try:
                snapshot = await runtime.get_thread(target["chat_id"])
            except Exception as exc:
                with self.repository.transaction() as db:
                    self._defer_command(
                        db,
                        command["command_id"],
                        reason=_safe_error(exc),
                    )
                return 0

            write_count = 0
            if snapshot.get("running") or snapshot.get("active_stream"):
                if command["status"] == "waiting_after_cancel":
                    with self.repository.transaction() as db:
                        self._defer_command(
                            db,
                            command["command_id"],
                            seconds=self.config.progress_check_seconds,
                        )
                    return 0
                if command["interrupt_policy"] != "interrupt":
                    with self.repository.transaction() as db:
                        self._defer_command(
                            db,
                            command["command_id"],
                            seconds=self.config.progress_check_seconds,
                        )
                    return 0
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancel_in_flight',last_error='' WHERE command_id=?",
                        (command["command_id"],),
                    )
                try:
                    result = await runtime.cancel(target["chat_id"])
                except Exception as exc:
                    with self.repository.transaction() as db:
                        db.execute(
                            "UPDATE commands SET status='delivery_uncertain',last_error=?,next_attempt_at=? WHERE command_id=?",
                            (
                                _safe_error(exc),
                                _now() + self.config.heartbeat_seconds,
                                command["command_id"],
                            ),
                        )
                    return 1
                write_count = 1
                if not result.get("cancelled"):
                    with self.repository.transaction() as db:
                        db.execute(
                            "UPDATE commands SET status='waiting_after_cancel',last_error=?,next_attempt_at=? WHERE command_id=?",
                            (
                                str(result.get("reason") or "cancel was not accepted"),
                                _now() + self.config.progress_check_seconds,
                                command["command_id"],
                            ),
                        )
                    return write_count
                for attempt in range(10):
                    if attempt:
                        await asyncio.sleep(0.2)
                    try:
                        snapshot = await runtime.get_thread(target["chat_id"])
                    except Exception as exc:
                        with self.repository.transaction() as db:
                            db.execute(
                                "UPDATE commands SET status='waiting_after_cancel',last_error=?,next_attempt_at=? WHERE command_id=?",
                                (
                                    _safe_error(exc),
                                    _now() + min(self.config.progress_check_seconds, 1.0),
                                    command["command_id"],
                                ),
                            )
                        return write_count
                    if not (
                        snapshot.get("running") or snapshot.get("active_stream")
                    ):
                        break
                if snapshot.get("running") or snapshot.get("active_stream"):
                    with self.repository.transaction() as db:
                        db.execute(
                            "UPDATE commands SET status='waiting_after_cancel',last_error=?,next_attempt_at=? WHERE command_id=?",
                            (
                                "cancel accepted; waiting for canonical terminal state",
                                _now() + min(self.config.progress_check_seconds, 1.0),
                                command["command_id"],
                            ),
                        )
                    return write_count

            target_task = self.repository.task_for_agent(target["agent_id"])
            decision = reduce_snapshot(target_task or {}, snapshot, (command,))
            if decision.state is ReducedState.COMPLETED and command.get("purpose") in {
                "progress",
                "question",
            }:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=? WHERE command_id=?",
                        ("target completed before obsolete command delivery", command["command_id"]),
                    )
                continue
            if decision.state not in {ReducedState.STOPPED_INCOMPLETE, ReducedState.COMPLETED}:
                with self.repository.transaction() as db:
                    self._defer_command(
                        db,
                        command["command_id"],
                        seconds=self.config.progress_check_seconds,
                    )
                return write_count
            delivery_message_id = str(
                command.get("user_message_id") or uuid.uuid4()
            )
            parent_message_id = str(snapshot.get("current_node") or "")
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE commands SET status='delivery_in_flight',last_error='',user_message_id=?,parent_message_id=? WHERE command_id=?",
                    (
                        delivery_message_id,
                        parent_message_id,
                        command["command_id"],
                    ),
                )
            try:
                result = await runtime.continue_thread(
                    target["chat_id"],
                    command["message"],
                    expected_current_node=parent_message_id,
                    wait_for_completion=False,
                    user_message_id=delivery_message_id,
                )
            except Exception as exc:
                delivery = ConversationGateway.classify_send_exception(
                    exc,
                    message=str(command.get("message") or ""),
                    submission_started=True,
                    user_message_id=delivery_message_id,
                    parent_message_id=parent_message_id,
                )
                command_status = (
                    "cancelled"
                    if delivery.state is DeliveryState.PERMANENT_FAILURE
                    else "delivery_uncertain"
                )
                next_attempt_at = (
                    0.0
                    if command_status == "cancelled"
                    else _now() + self.config.heartbeat_seconds
                )
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status=?,last_error=?,next_attempt_at=? WHERE command_id=?",
                        (
                            command_status,
                            delivery.reason,
                            next_attempt_at,
                            command["command_id"],
                        ),
                    )
                return write_count + 1

            delivery = ConversationGateway.classify_send_result(
                result,
                message=str(command.get("message") or ""),
                user_message_id=delivery_message_id,
                parent_message_id=parent_message_id,
            )
            command_status = {
                DeliveryState.DELIVERED: "delivered",
                DeliveryState.SENT_UNCONFIRMED: "delivery_uncertain",
                DeliveryState.DEFERRED_RUNNING: "queued",
                DeliveryState.TARGET_CHANGED: "queued",
                DeliveryState.NOT_SENT: "queued",
                DeliveryState.TEMPORARILY_UNREADABLE: "queued",
                DeliveryState.PERMANENT_FAILURE: "cancelled",
            }[delivery.state]
            if command_status in {"delivered", "cancelled"}:
                next_attempt_at = 0.0
            elif command_status == "delivery_uncertain":
                next_attempt_at = _now() + self.config.heartbeat_seconds
            else:
                next_attempt_at = _now() + self.config.progress_check_seconds
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE commands SET status=?,delivered_at=?,last_error=?,request_id=?,user_message_id=?,parent_message_id=?,next_attempt_at=? WHERE command_id=?",
                    (
                        command_status,
                        _now() if command_status == "delivered" else None,
                        delivery.reason,
                        delivery.request_id,
                        delivery.user_message_id or delivery_message_id,
                        delivery.parent_message_id or parent_message_id,
                        next_attempt_at,
                        command["command_id"],
                    ),
                )
                if command_status == "delivered":
                    self._activate_target_after_delivery(db, command)
                if command_status == "delivered" and command.get("ack_event_seq"):
                    db.execute(
                        "UPDATE subscriptions SET last_acked_event_seq="
                        "CASE WHEN last_acked_event_seq<? THEN ? ELSE last_acked_event_seq END "
                        "WHERE parent_agent_id=? AND child_agent_id=?",
                        (
                            int(command["ack_event_seq"]),
                            int(command["ack_event_seq"]),
                            command["to_agent_id"],
                            command["from_agent_id"],
                        ),
                    )
            return write_count + 1
        return 0

    async def _continue_one_task(
        self,
        runtime: AgentRuntime,
        candidates: list[dict[str, Any]],
    ) -> int:
        ordered = sorted(
            candidates,
            key=lambda item: (
                float(item.get("fairness") or 0),
                float(item["agent"].get("created_at") or 0),
                str(item["agent"].get("agent_id") or ""),
            ),
        )
        for candidate in ordered:
            agent = self.repository.agent(candidate["agent"]["agent_id"])
            task = self.repository.task_for_agent(candidate["agent"]["agent_id"])
            if not agent or not task or not agent.get("chat_id"):
                continue
            if int(task.get("continue_attempts") or 0) >= self.config.max_continue_attempts:
                continue
            try:
                confirmation = await runtime.get_thread(agent["chat_id"])
            except Exception as exc:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE agents SET status='error',last_error=?,updated_at=? WHERE agent_id=?",
                        (_safe_error(exc), _now(), agent["agent_id"]),
                    )
                    db.execute(
                        "UPDATE tasks SET next_check_at=? WHERE task_id=?",
                        (
                            _now() + self.config.heartbeat_seconds,
                            task["task_id"],
                        ),
                    )
                continue
            current_node = str(confirmation.get("current_node") or "")
            marker = str(task.get("completion_marker") or "")
            with self.repository.connect() as db:
                pending_for_target = [dict(row) for row in db.execute(
                    "SELECT * FROM commands WHERE to_agent_id=? AND status IN "
                    "('queued','waiting_after_cancel','delivery_uncertain') ORDER BY sequence_no",
                    (agent["agent_id"],),
                )]
            reduction = reduce_snapshot(task, confirmation, pending_for_target)
            if (
                current_node != str(candidate["snapshot"].get("current_node") or "")
                or reduction.state is not ReducedState.STOPPED_INCOMPLETE
                or ActionType.SEND_CONTINUATION not in reduction.actions
                or str(task.get("last_continue_node") or "") == current_node
            ):
                continue
            message = DEFAULT_AGENT_CONTINUE_MESSAGE
            if marker:
                message += f"\n\nRequired completion marker:\n{marker}"
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE tasks SET status='continuation_in_flight',continue_attempts=continue_attempts+1,last_continue_node=?,last_continue_at=?,next_check_at=? WHERE task_id=?",
                    (
                        current_node,
                        _now(),
                        _now() + self.config.heartbeat_seconds,
                        task["task_id"],
                    ),
                )
                db.execute(
                    "UPDATE agents SET status='continuation_in_flight',last_error='',updated_at=? WHERE agent_id=?",
                    (_now(), agent["agent_id"]),
                )
            try:
                result = await runtime.continue_thread(
                    agent["chat_id"],
                    message,
                    expected_current_node=current_node,
                    wait_for_completion=False,
                )
            except Exception as exc:
                reason = _safe_error(exc)
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE tasks SET status='continuation_uncertain',next_check_at=? WHERE task_id=?",
                        (
                            _now() + self.config.heartbeat_seconds,
                            task["task_id"],
                        ),
                    )
                    db.execute(
                        "UPDATE agents SET status='error',last_error=?,updated_at=? WHERE agent_id=?",
                        (reason, _now(), agent["agent_id"]),
                    )
                self._emit_event(
                    agent["agent_id"],
                    task["task_id"],
                    "error",
                    {"reason": reason, "operation": "automatic_continuation"},
                    source_cursor=f"continuation-error:{current_node}",
                )
                return 1

            sent = bool(result.get("sent"))
            observed = bool(result.get("observed"))
            if sent and observed:
                next_status = "running"
            elif sent:
                next_status = "continuation_uncertain"
            else:
                next_status = "idle"
            next_check_at = _now() + (
                self.config.progress_check_seconds
                if sent
                else self.config.idle_check_seconds
            )
            with self.repository.transaction() as db:
                if sent:
                    db.execute(
                        "UPDATE tasks SET status=?,next_check_at=? WHERE task_id=?",
                        (next_status, next_check_at, task["task_id"]),
                    )
                else:
                    db.execute(
                        "UPDATE tasks SET status=?,continue_attempts=CASE WHEN continue_attempts>0 THEN continue_attempts-1 ELSE 0 END,last_continue_node=NULL,last_continue_at=NULL,next_check_at=? WHERE task_id=?",
                        (next_status, next_check_at, task["task_id"]),
                    )
                db.execute(
                    "UPDATE agents SET status=?,last_error=?,updated_at=? WHERE agent_id=?",
                    (
                        next_status,
                        str(result.get("reason") or ""),
                        _now(),
                        agent["agent_id"],
                    ),
                )
            return 1
        return 0

    async def wait(self, agent_id: str, *, after_event_seq: int = 0) -> dict[str, Any]:
        status = await self.status(agent_id)
        return {
            **status,
            "events": self.repository.events_after(agent_id, max(0, int(after_event_seq))),
        }

    async def children(self, parent_agent_id: str, *, recursive: bool = False) -> dict[str, Any]:
        if not self.repository.agent(parent_agent_id):
            raise ValueError("parent agent was not found")

        def build(agent_id: str) -> list[dict[str, Any]]:
            result = []
            for child in self.repository.children(agent_id):
                item = self._public_agent(child)
                if recursive:
                    item["children"] = build(child["agent_id"])
                result.append(item)
            return result

        return {"parent_agent_id": parent_agent_id, "children": build(parent_agent_id)}

    async def subscribe(
        self, parent_agent_id: str, child_agent_id: str, notification_policy: str
    ) -> dict[str, Any]:
        if notification_policy not in NOTIFICATION_POLICIES:
            raise ValueError("notification_policy must be notify_only or auto_resume")
        parent = self.repository.agent(parent_agent_id)
        child = self.repository.agent(child_agent_id)
        if not parent or not child or child.get("parent_agent_id") != parent_agent_id:
            raise ValueError("the child does not belong to the supplied parent")
        with self.repository.transaction() as db:
            db.execute(
                "INSERT INTO subscriptions(parent_agent_id,child_agent_id,notification_policy,created_at) VALUES(?,?,?,?) "
                "ON CONFLICT(parent_agent_id,child_agent_id) DO UPDATE SET notification_policy=excluded.notification_policy",
                (parent_agent_id, child_agent_id, notification_policy, _now()),
            )
            db.execute(
                "UPDATE agents SET notification_policy=?,updated_at=? WHERE agent_id=?",
                (notification_policy, _now(), child_agent_id),
            )
        return {"parent_agent_id": parent_agent_id, "child_agent_id": child_agent_id, "notification_policy": notification_policy}

    async def ack(self, parent_agent_id: str, child_agent_id: str, event_seq: int) -> dict[str, Any]:
        requested = int(event_seq)
        if requested < 0:
            raise ValueError("event_seq must be non-negative")
        with self.repository.transaction() as db:
            row = db.execute(
                "SELECT last_acked_event_seq FROM subscriptions WHERE parent_agent_id=? AND child_agent_id=?",
                (parent_agent_id, child_agent_id),
            ).fetchone()
            if not row:
                raise ValueError("subscription was not found")
            maximum = int(
                db.execute(
                    "SELECT COALESCE(MAX(event_seq),0) FROM events WHERE agent_id=?",
                    (child_agent_id,),
                ).fetchone()[0]
            )
            if requested > maximum:
                raise ValueError("event_seq does not exist for this child agent")
            value = max(int(row[0]), requested)
            db.execute(
                "UPDATE subscriptions SET last_acked_event_seq=? WHERE parent_agent_id=? AND child_agent_id=?",
                (value, parent_agent_id, child_agent_id),
            )
            db.execute(
                "UPDATE commands SET status='acknowledged',last_error='' "
                "WHERE from_agent_id=? AND to_agent_id=? AND ack_event_seq<=? "
                "AND status IN ('queued','waiting_after_cancel','delivery_uncertain')",
                (child_agent_id, parent_agent_id, value),
            )
        return {
            "parent_agent_id": parent_agent_id,
            "child_agent_id": child_agent_id,
            "last_acked_event_seq": value,
        }

    async def cancel(self, agent_id: str, *, interrupt: bool = False) -> dict[str, Any]:
        async with self._lock:
            agent = self.repository.agent(agent_id)
            if not agent:
                raise ValueError("agent was not found")
            if str(agent.get("status") or "") == "cancelled":
                return {
                    "agent_id": agent_id,
                    "cancelled": True,
                    "reason": "agent was already cancelled",
                }
            task = self.repository.task_for_agent(agent_id)
            runtime_result: dict[str, Any] = {
                "cancelled": False,
                "reason": "not interrupted",
            }
            if interrupt and agent.get("chat_id"):
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE agents SET status='cancel_in_flight',updated_at=? WHERE agent_id=?",
                        (_now(), agent_id),
                    )
                    if task:
                        db.execute(
                            "UPDATE tasks SET status='cancel_in_flight' WHERE task_id=?",
                            (task["task_id"],),
                        )
                try:
                    async with self.runtime_factory() as runtime:
                        runtime_result = await runtime.cancel(agent["chat_id"])
                except Exception as exc:
                    runtime_result = {
                        "cancelled": False,
                        "reason": _safe_error(exc),
                    }

            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE agents SET status='cancelled',last_error=?,updated_at=? WHERE agent_id=?",
                    (str(runtime_result.get("reason") or ""), _now(), agent_id),
                )
                if task:
                    db.execute(
                        "UPDATE tasks SET status='cancelled',completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                        (_now(), task["task_id"]),
                    )
                notification_rows = [
                    dict(row)
                    for row in db.execute(
                        "SELECT from_agent_id,MAX(ack_event_seq) AS ack_event_seq "
                        "FROM commands WHERE to_agent_id=? AND ack_event_seq IS NOT NULL "
                        "AND status IN ('queued','waiting_after_cancel','delivery_uncertain','delivery_in_flight','cancel_in_flight') "
                        "GROUP BY from_agent_id",
                        (agent_id,),
                    )
                ]
                for notification in notification_rows:
                    db.execute(
                        "UPDATE subscriptions SET last_acked_event_seq="
                        "CASE WHEN last_acked_event_seq<? THEN ? ELSE last_acked_event_seq END "
                        "WHERE parent_agent_id=? AND child_agent_id=?",
                        (
                            int(notification["ack_event_seq"]),
                            int(notification["ack_event_seq"]),
                            agent_id,
                            notification["from_agent_id"],
                        ),
                    )
                db.execute(
                    "UPDATE commands SET status='cancelled' WHERE to_agent_id=? "
                    "AND status IN ('queued','waiting_after_cancel','delivery_uncertain','delivery_in_flight','cancel_in_flight')",
                    (agent_id,),
                )
                self._insert_event(
                    db,
                    agent_id,
                    task.get("task_id") if task else None,
                    "cancelled",
                    runtime_result,
                    source_cursor=f"cancelled:{agent_id}",
                )
            return {"agent_id": agent_id, **runtime_result}

    @staticmethod
    def _insert_event(
        db: sqlite3.Connection,
        agent_id: str,
        task_id: str | None,
        kind: str,
        payload: dict[str, Any],
        *,
        source_cursor: str | None = None,
    ) -> str | None:
        event_id = _id("evt")
        try:
            db.execute(
                "INSERT INTO events(event_id,agent_id,task_id,kind,payload_json,source_cursor,created_at) VALUES(?,?,?,?,?,?,?)",
                (event_id, agent_id, task_id, kind, _json(payload), source_cursor, _now()),
            )
        except sqlite3.IntegrityError:
            return None
        return event_id

    def _emit_event(
        self,
        agent_id: str,
        task_id: str | None,
        kind: str,
        payload: dict[str, Any],
        *,
        source_cursor: str | None = None,
    ) -> str | None:
        with self.repository.transaction() as db:
            return self._insert_event(
                db,
                agent_id,
                task_id,
                kind,
                payload,
                source_cursor=source_cursor,
            )

    def _queue_parent_notifications(self) -> None:
        with self.repository.connect() as db:
            subscriptions = [dict(row) for row in db.execute("SELECT * FROM subscriptions")]
        for subscription in subscriptions:
            if subscription["notification_policy"] != "auto_resume":
                continue
            with self.repository.connect() as db:
                pending_rows = [
                    dict(row)
                    for row in db.execute(
                        "SELECT purpose FROM commands WHERE from_agent_id=? "
                        "AND to_agent_id=? AND status IN ('queued','waiting_after_cancel')",
                        (
                            subscription["child_agent_id"],
                            subscription["parent_agent_id"],
                        ),
                    )
                ]
            events = [
                event
                for event in self.repository.events_after(
                    subscription["child_agent_id"],
                    int(subscription["last_acked_event_seq"]),
                )
                if event["kind"] != "started"
            ]
            if not events:
                continue

            completion_event = next(
                (
                    event
                    for event in reversed(events)
                    if event["kind"] in {"completed", "completion"}
                ),
                None,
            )
            if pending_rows and (
                completion_event is None
                or any(
                    row.get("purpose") not in {"progress", "question"}
                    for row in pending_rows
                )
            ):
                continue
            event_source = events
            if completion_event is not None:
                completion_index = events.index(completion_event)
                event_source = events[: completion_index + 1]

            selected: list[dict[str, Any]] = []
            iterable = reversed(event_source) if completion_event is not None else iter(event_source)
            for event in iterable:
                compact = {
                    "event_seq": int(event["event_seq"]),
                    "event_id": event["event_id"],
                    "task_id": event.get("task_id"),
                    "kind": event["kind"],
                    "payload": event["payload"],
                }
                candidate = (
                    [compact, *selected]
                    if completion_event is not None
                    else [*selected, compact]
                )
                if selected and len(_json(candidate)) > self.config.context_max_chars:
                    break
                if completion_event is not None:
                    selected.insert(0, compact)
                else:
                    selected.append(compact)
                if len(_json(selected)) >= self.config.context_max_chars:
                    break
            if not selected:
                continue
            if len(_json(selected)) > self.config.context_max_chars:
                only = selected[0]
                only["payload"] = {
                    "truncated": True,
                    "summary": str(only.get("payload") or "")[
                        : max(200, self.config.context_max_chars // 2)
                    ],
                }

            first_seq = int(selected[0]["event_seq"] )
            last_seq = int(selected[-1]["event_seq"] )
            is_completion = completion_event is not None
            message = (
                "[SUBAGENT UPDATE]\n"
                f"agent_id: {subscription['child_agent_id']}\n"
                f"events: {_json(selected)}"
            )
            key = (
                f"events:{subscription['child_agent_id']}:{first_seq}:{last_seq}"
            )
            try:
                self._insert_notification_command(
                    subscription["child_agent_id"],
                    subscription["parent_agent_id"],
                    message,
                    key,
                    ack_event_seq=last_seq,
                    purpose="completion" if is_completion else "progress",
                )
                if is_completion:
                    with self.repository.transaction() as db:
                        db.execute(
                            "UPDATE commands SET status='superseded',last_error=? WHERE "
                            "from_agent_id=? AND to_agent_id=? "
                            "AND purpose IN ('progress','question') "
                            "AND status IN ('queued','waiting_after_cancel','delivery_uncertain') "
                            "AND (ack_event_seq IS NULL OR ack_event_seq<?)",
                            ("superseded by final completion envelope", subscription["child_agent_id"],
                             subscription["parent_agent_id"], last_seq),
                        )
            except sqlite3.IntegrityError:
                pass

    def _insert_notification_command(
        self,
        from_agent_id: str,
        to_agent_id: str,
        message: str,
        idempotency_key: str,
        *,
        ack_event_seq: int,
        purpose: str = "progress",
    ) -> None:
        with self.repository.transaction() as db:
            sequence = int(
                db.execute(
                    "SELECT COALESCE(MAX(sequence_no),0)+1 FROM commands WHERE to_agent_id=?",
                    (to_agent_id,),
                ).fetchone()[0]
            )
            db.execute(
                "INSERT INTO commands(command_id,from_agent_id,to_agent_id,sequence_no,message,interrupt_policy,status,idempotency_key,ack_event_seq,purpose,created_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    _id("cmd"),
                    from_agent_id,
                    to_agent_id,
                    sequence,
                    message,
                    "queue",
                    "queued",
                    idempotency_key,
                    int(ack_event_seq),
                    purpose,
                    _now(),
                ),
            )

    def _require_chat_agent(self, agent_id: str) -> dict[str, Any]:
        agent = self.repository.agent(agent_id)
        if not agent:
            raise ValueError("agent was not found")
        if not agent.get("chat_id"):
            raise ValueError("agent does not have a ChatGPT conversation yet")
        return dict(agent)

    @staticmethod
    def _public_agent(agent: dict[str, Any]) -> dict[str, Any]:
        return {
            key: agent.get(key)
            for key in (
                "agent_id",
                "orchestration_id",
                "parent_agent_id",
                "root_agent_id",
                "chat_id",
                "project_id",
                "working_directory",
                "title",
                "status",
                "notification_policy",
                "context_cursor",
                "current_node",
                "last_progress_at",
                "last_error",
                "created_at",
                "updated_at",
            )
        }


class ChatAgentService:
    """Optional background coordinator loop; activation is controlled by service startup."""

    def __init__(
        self,
        coordinator: ChatAgentCoordinator,
        *,
        enabled: bool = True,
        interval_seconds: float = 600.0,
    ):
        self.coordinator = coordinator
        self.enabled = enabled
        self.interval_seconds = max(30.0, float(interval_seconds))
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._wake = asyncio.Event()
        self.state: dict[str, Any] = {
            "enabled": enabled,
            "running": False,
            "last_sync_at": None,
            "last_result": None,
            "last_error": "",
        }

    async def start(self) -> None:
        if not self.enabled or self._task is not None:
            return
        self._stop.clear()
        self.state["running"] = True
        self._task = asyncio.create_task(self._run(), name="terminal-mcp-chat-agent-service")

    async def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self.state["running"] = False

    def wake(self) -> None:
        self._wake.set()

    async def sync_now(self, *, force: bool = True) -> dict[str, Any]:
        self.coordinator.prepare_local_work()
        due_at = self.coordinator.next_due_at()
        if due_at is None or (not force and due_at > _now()):
            if hasattr(self.coordinator, "gateway"):
                result = {
                    "status": "idle",
                    "write_count": 0,
                    "physical_requests": 0,
                    "selected_operation": None,
                    "operation_type": None,
                    "lane": None,
                    "circuit_state": "CLOSED",
                    "next_eligible_at": due_at,
                    "agent_id": None,
                    "outcome": None,
                    "error": None,
                    "inspected": [],
                }
            else:
                result = {"status": "idle", "write_count": 0, "inspected": []}
        else:
            result = await self.coordinator.sync_once(force=force)
        self.state.update(last_sync_at=_now(), last_result=result, last_error="")
        return result

    def _next_wait_seconds(self) -> float:
        now = _now()
        due_at = self.coordinator.next_due_at()
        regular_wait = (
            self.interval_seconds
            if due_at is None
            else min(self.interval_seconds, max(30.0, due_at - now))
        )
        timer_getter = getattr(self.coordinator, "next_wakeup_at", None)
        timer_due = timer_getter() if callable(timer_getter) else None
        if timer_due is None:
            return regular_wait
        return min(regular_wait, max(1.0, float(timer_due) - now))

    async def _run(self) -> None:
        while not self._stop.is_set():
            self._wake.clear()
            try:
                await self.sync_now(force=False)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.update(last_sync_at=_now(), last_error=_safe_error(exc))
            try:
                await asyncio.wait_for(
                    self._wake.wait(),
                    timeout=self._next_wait_seconds(),
                )
            except asyncio.TimeoutError:
                pass


def install_chat_agent_lifespan(app: Any, service: ChatAgentService) -> None:
    """Compose a Starlette/FastMCP lifespan with the optional agent loop."""

    original = app.router.lifespan_context

    @contextlib.asynccontextmanager
    async def combined(application: Any) -> AsyncIterator[Any]:
        async with original(application) as state:
            await service.start()
            try:
                yield state
            finally:
                await service.stop()

    app.router.lifespan_context = combined
