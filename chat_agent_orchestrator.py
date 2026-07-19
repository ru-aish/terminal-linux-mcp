from __future__ import annotations
import asyncio
import contextlib
import hashlib
import json
import secrets
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncContextManager, Callable, Protocol

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
                "UPDATE commands SET status='delivery_uncertain',last_error=? "
                "WHERE status='delivery_in_flight'",
                ("Terminal MCP stopped during an external ChatGPT operation",),
            )
            db.execute(
                "UPDATE commands SET status='waiting_after_cancel',last_error=? "
                "WHERE status='cancel_in_flight'",
                (
                    "Terminal MCP stopped while cancellation was in flight; "
                    "waiting for a verified terminal target before message delivery",
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
                    "UPDATE tasks SET status='continuation_uncertain' WHERE task_id=?",
                    (row["task_id"],),
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
                    "SELECT a.agent_id,t.task_id FROM agents a "
                    "JOIN tasks t ON t.agent_id=a.agent_id "
                    "WHERE a.status='creation_in_flight' "
                    "OR t.status='creation_in_flight'"
                )
            ]
            for row in creation_rows:
                reason = "Terminal MCP stopped while creating the ChatGPT child thread"
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

    def has_pending_work(self) -> bool:
        with self.repository.connect() as db:
            row = db.execute(
                "SELECT "
                "EXISTS(SELECT 1 FROM tasks WHERE status NOT IN ('completed','failed','cancelled')) "
                "OR EXISTS(SELECT 1 FROM commands WHERE status IN "
                "('queued','waiting_after_cancel','delivery_uncertain')) "
                "OR EXISTS("
                "  SELECT 1 FROM subscriptions s JOIN events e ON e.agent_id=s.child_agent_id "
                "  WHERE s.notification_policy='auto_resume' AND e.kind!='started' "
                "  AND e.event_seq>s.last_acked_event_seq"
                ")"
            ).fetchone()
        return bool(row and row[0])

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
            self._mark_creation_failed(
                agent_id,
                task["task_id"],
                reason,
                uncertain=bool(created.get("sent")),
            )
            raise RuntimeError(reason)

        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET chat_id=?,status=?,current_node=?,last_progress_at=?,progress_signature=?,updated_at=?,last_error='' WHERE agent_id=?",
                (
                    chat_id,
                    "running" if created.get("running") else "idle",
                    str(created.get("current_node") or ""),
                    _now(),
                    self._progress_signature(created),
                    _now(),
                    agent_id,
                ),
            )
            db.execute(
                "UPDATE tasks SET status='running' WHERE task_id=?",
                (task["task_id"],),
            )
            self._insert_event(
                db,
                agent_id,
                task["task_id"],
                "started",
                {"chat_id": chat_id, "project_id": project_id},
                source_cursor=f"started:{task['task_id']}",
            )

    async def _recover_one_creation(self, runtime: AgentRuntime) -> int:
        with self.repository.connect() as db:
            row = db.execute(
                "SELECT agent_id FROM agents WHERE status='creating_thread' "
                "AND chat_id IS NULL ORDER BY created_at,agent_id LIMIT 1"
            ).fetchone()
        if not row:
            return 0
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
        terminal_statuses = {"cancelled", "failed", "completed"}
        if str(sender.get("status") or "") in terminal_statuses:
            raise ValueError("sender agent is terminal and cannot send new messages")
        if str(target.get("status") or "") in terminal_statuses:
            raise ValueError("target agent is terminal and cannot receive new messages")
        key = idempotency_key.strip() if idempotency_key else None
        if key:
            with self.repository.connect() as db:
                existing = db.execute(
                    "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=? AND idempotency_key=?",
                    (from_agent_id, to_agent_id, key),
                ).fetchone()
            if existing:
                return dict(existing)
        try:
            with self.repository.transaction() as db:
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
                        "UPDATE tasks SET status='waiting_for_parent' WHERE agent_id=?",
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

    async def sync_once(self) -> dict[str, Any]:
        if self._lock.locked():
            return {"status": "busy", "write_count": 0, "inspected": []}
        async with self._lock:
            inspected: list[dict[str, Any]] = []
            new_events: list[str] = []
            continuation_candidates: list[dict[str, Any]] = []
            async with self.runtime_factory() as runtime:
                for agent in self.repository.active_agents():
                    try:
                        snapshot = await runtime.get_thread(agent["chat_id"])
                        digest = await runtime.thread_context(
                            agent["chat_id"],
                            since_cursor=agent.get("context_cursor"),
                            max_events=self.config.context_max_events,
                            max_chars=self.config.context_max_chars,
                        )
                        task = self.repository.task_for_agent(agent["agent_id"])
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
                                        "UPDATE tasks SET status='completed',completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                                        (_now(), task["task_id"]),
                                    )
                                elif exhausted:
                                    db.execute(
                                        "UPDATE tasks SET status='failed',completed_at=COALESCE(completed_at,?) WHERE task_id=?",
                                        (_now(), task["task_id"]),
                                    )
                                else:
                                    durable_task_status = display_status
                                    if (
                                        str(task.get("status") or "") == "waiting_for_parent"
                                        and display_status in {"running", "idle", "waiting_assistant"}
                                    ):
                                        durable_task_status = "waiting_for_parent"
                                    db.execute(
                                        "UPDATE tasks SET status=? WHERE task_id=?",
                                        (durable_task_status, task["task_id"]),
                                    )
                        inspected.append(
                            {"agent_id": agent["agent_id"], "status": display_status}
                        )
                    except Exception as exc:
                        reason = _safe_error(exc)
                        with self.repository.transaction() as db:
                            db.execute(
                                "UPDATE agents SET status='error',last_error=?,updated_at=? WHERE agent_id=?",
                                (reason, _now(), agent["agent_id"]),
                            )
                        inspected.append(
                            {
                                "agent_id": agent["agent_id"],
                                "status": "error",
                                "reason": reason,
                            }
                        )

                self._queue_parent_notifications()
                write_count = await self._recover_one_creation(runtime)
                if write_count == 0:
                    write_count = await self._reconcile_uncertain_commands(runtime)
                if write_count == 0:
                    write_count = await self._deliver_one_command(runtime)
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

    async def _reconcile_uncertain_commands(self, runtime: AgentRuntime) -> int:
        """Confirm an earlier submission from durable generated-message evidence.

        An uncertain command is never resent merely because the process restarted.
        If the generated user ID is visible on any normalized branch, it is
        delivered and its event cursor advances normally.
        """
        for command in self.repository.uncertain_commands():
            target = self.repository.agent(command["to_agent_id"])
            message_id = str(command.get("user_message_id") or "")
            if not target or not target.get("chat_id"):
                continue
            try:
                snapshot = await runtime.get_thread(target["chat_id"])
            except Exception as exc:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET last_error=? WHERE command_id=?",
                        (_safe_error(exc), command["command_id"]),
                    )
                continue
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
                continue
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE commands SET status='delivered',delivered_at=?,last_error='' WHERE command_id=?",
                    (_now(), command["command_id"]),
                )
                self._ack_command_cursor(db, command)
            return 1
        return 0

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

    async def _deliver_one_command(self, runtime: AgentRuntime) -> int:
        for command in self.repository.queued_commands():
            target = self.repository.agent(command["to_agent_id"])
            if not target or not target.get("chat_id"):
                continue
            if str(target.get("status") or "") in {
                "cancelled",
                "failed",
                "completed",
            }:
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
                    db.execute(
                        "UPDATE commands SET last_error=? WHERE command_id=?",
                        (_safe_error(exc), command["command_id"]),
                    )
                continue

            if snapshot.get("running") or snapshot.get("active_stream"):
                if command["status"] == "waiting_after_cancel":
                    continue
                if command["interrupt_policy"] != "interrupt":
                    continue
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
                            "UPDATE commands SET status='delivery_uncertain',last_error=? WHERE command_id=?",
                            (_safe_error(exc), command["command_id"]),
                        )
                    return 1
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status=?,last_error=? WHERE command_id=?",
                        (
                            "waiting_after_cancel",
                            str(result.get("reason") or ""),
                            command["command_id"],
                        ),
                    )
                return 1

            target_task = self.repository.task_for_agent(target["agent_id"])
            decision = reduce_snapshot(target_task or {}, snapshot, (command,))
            if decision.state is ReducedState.COMPLETED and command.get("purpose") != "completion":
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=? WHERE command_id=?",
                        ("target completed before obsolete command delivery", command["command_id"]),
                    )
                continue
            if decision.state not in {ReducedState.STOPPED_INCOMPLETE, ReducedState.COMPLETED}:
                continue
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE commands SET status='delivery_in_flight',last_error='' WHERE command_id=?",
                    (command["command_id"],),
                )
            try:
                result = await runtime.continue_thread(
                    target["chat_id"],
                    command["message"],
                    expected_current_node=str(snapshot.get("current_node") or ""),
                    wait_for_completion=False,
                )
            except Exception as exc:
                delivery = ConversationGateway.classify_send_exception(
                    exc,
                    message=str(command.get("message") or ""),
                    submission_started=True,
                )
                command_status = (
                    "cancelled"
                    if delivery.state is DeliveryState.PERMANENT_FAILURE
                    else "delivery_uncertain"
                )
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status=?,last_error=? WHERE command_id=?",
                        (command_status, delivery.reason, command["command_id"]),
                    )
                return 1

            delivery = ConversationGateway.classify_send_result(
                result,
                message=str(command.get("message") or ""),
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
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE commands SET status=?,delivered_at=?,last_error=?,request_id=?,user_message_id=?,parent_message_id=? WHERE command_id=?",
                    (
                        command_status,
                        _now() if command_status == "delivered" else None,
                        str(result.get("reason") or ""),
                        str(result.get("request_id") or ""),
                        str(result.get("user_message_id") or ""),
                        str(result.get("parent_message_id") or ""),
                        command["command_id"],
                    ),
                )
                if command_status == "delivered" and command.get("purpose") == "answer":
                    db.execute(
                        "UPDATE agents SET status='waiting_assistant',updated_at=? "
                        "WHERE agent_id=? AND status NOT IN ('completed','failed','cancelled')",
                        (_now(), command["to_agent_id"]),
                    )
                    db.execute(
                        "UPDATE tasks SET status='waiting_assistant' "
                        "WHERE agent_id=? AND status='waiting_for_parent'",
                        (command["to_agent_id"],),
                    )
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
            return 1
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
                    "UPDATE tasks SET status='continuation_in_flight',continue_attempts=continue_attempts+1,last_continue_node=?,last_continue_at=? WHERE task_id=?",
                    (current_node, _now(), task["task_id"]),
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
                        "UPDATE tasks SET status='continuation_uncertain' WHERE task_id=?",
                        (task["task_id"],),
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
            with self.repository.transaction() as db:
                if sent:
                    db.execute(
                        "UPDATE tasks SET status=? WHERE task_id=?",
                        (next_status, task["task_id"]),
                    )
                else:
                    db.execute(
                        "UPDATE tasks SET status=?,continue_attempts=CASE WHEN continue_attempts>0 THEN continue_attempts-1 ELSE 0 END,last_continue_node=NULL,last_continue_at=NULL WHERE task_id=?",
                        (next_status, task["task_id"]),
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
        return agent

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
        interval_seconds: float = 15.0,
    ):
        self.coordinator = coordinator
        self.enabled = enabled
        self.interval_seconds = max(1.0, float(interval_seconds))
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

    async def sync_now(self) -> dict[str, Any]:
        if not self.coordinator.has_pending_work():
            result = {"status": "idle", "write_count": 0, "inspected": []}
        else:
            result = await self.coordinator.sync_once()
        self.state.update(last_sync_at=_now(), last_result=result, last_error="")
        return result

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.sync_now()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.update(last_sync_at=_now(), last_error=_safe_error(exc))
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=self.interval_seconds)
            except asyncio.TimeoutError:
                pass


def install_chat_agent_lifespan(app: Any, service: ChatAgentService) -> None:
    """Compose a Starlette/FastMCP lifespan with the optional agent loop."""

    original = app.router.lifespan_context

    @contextlib.asynccontextmanager
    async def combined(application: Any):
        async with original(application) as state:
            await service.start()
            try:
                yield state
            finally:
                await service.stop()

    app.router.lifespan_context = combined
