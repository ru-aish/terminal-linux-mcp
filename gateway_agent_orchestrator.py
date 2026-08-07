from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Mapping

from chat_agent_orchestrator import (
    COMMAND_PURPOSES,
    DEFAULT_AGENT_COMPLETION_MARKER,
    DEFAULT_AGENT_CONTINUE_MESSAGE,
    INTERRUPT_POLICIES,
    NOTIFICATION_POLICIES,
    ChatAgentCoordinator,
    CoordinatorConfig,
    RuntimeFactory,
    _id,
    _now,
    _safe_error,
)
from chat_gateway import (
    AgentState,
    ChatGateway,
    OperationState,
    OperationType,
)

_TERMINAL_DOMAIN_STATES = {"completed", "failed", "cancelled"}
_TERMINAL_MESSAGE_STATUSES = {
    "finished_successfully",
    "finished_error",
    "failed",
    "cancelled",
    "canceled",
    "interrupted",
    "incomplete",
}


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class GatewayChatAgentCoordinator(ChatAgentCoordinator):
    """Terminal MCP orchestration backed by the durable single-request gateway.

    The inherited repository remains the source of truth for relationships,
    tasks, mailboxes, subscriptions, and events. ``ChatGateway`` is the source
    of truth for provider operations, request pacing, circuit breakers, and
    canonical conversation lifecycle.
    """

    def __init__(
        self,
        config: CoordinatorConfig,
        runtime_factory: RuntimeFactory,
        gateway: ChatGateway,
        *,
        maximum_active_children: int = 5,
    ) -> None:
        super().__init__(config, runtime_factory)
        self.gateway = gateway
        self.maximum_active_children = max(1, int(maximum_active_children))
        self._adopt_existing_conversations()

    # ------------------------------------------------------------------
    # Durable mapping and local preparation
    # ------------------------------------------------------------------
    def _adopt_existing_conversations(self) -> None:
        with self.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM agents WHERE chat_id IS NOT NULL "
                    "AND gateway_agent_id IS NULL ORDER BY created_at,agent_id"
                )
            ]
        for row in rows:
            gateway_id = str(
                self.gateway.register_existing(
                    project_id=str(row.get("project_id") or ""),
                    conversation_id=str(row["chat_id"]),
                    title=str(row.get("title") or ""),
                    completion_marker=str(
                        (
                            self.repository.task_for_agent(str(row["agent_id"])) or {}
                        ).get("completion_marker")
                        or self.gateway.config.completion_marker
                    ),
                    state=self._gateway_state_from_domain(str(row.get("status") or "")),
                )
            )
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE agents SET gateway_agent_id=?,updated_at=? WHERE agent_id=?",
                    (gateway_id, _now(), row["agent_id"]),
                )
            if (
                row.get("parent_agent_id")
                and str(row.get("status") or "") not in _TERMINAL_DOMAIN_STATES
            ):
                try:
                    self.gateway.enqueue_inspect(
                        agent_id=gateway_id, due_at=self.gateway.clock.now()
                    )
                except ValueError:
                    pass

    @staticmethod
    def _gateway_state_from_domain(status: str) -> AgentState:
        if status == "completed":
            return AgentState.COMPLETED
        if status == "failed":
            return AgentState.FAILED
        if status == "cancelled":
            return AgentState.CANCELLED
        if status in {"running", "waiting_assistant", "continuation_in_flight"}:
            return AgentState.RUNNING
        if status in {"creating_thread", "creation_in_flight", "creation_uncertain"}:
            return AgentState.CREATING
        return AgentState.UNKNOWN

    def _ensure_gateway_mapping(self, agent: Mapping[str, Any]) -> str:
        existing = str(agent.get("gateway_agent_id") or "")
        if existing:
            return existing
        chat_id = str(agent.get("chat_id") or "")
        if not chat_id:
            raise ValueError("agent has no gateway mapping or conversation")
        task = self.repository.task_for_agent(str(agent["agent_id"])) or {}
        gateway_id = str(
            self.gateway.register_existing(
                project_id=str(agent.get("project_id") or ""),
                conversation_id=chat_id,
                title=str(agent.get("title") or ""),
                completion_marker=str(
                    task.get("completion_marker")
                    or self.gateway.config.completion_marker
                ),
                state=self._gateway_state_from_domain(str(agent.get("status") or "")),
            )
        )
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET gateway_agent_id=?,updated_at=? WHERE agent_id=?",
                (gateway_id, _now(), agent["agent_id"]),
            )
        return gateway_id

    def _gateway_operation_due_at(self) -> float | None:
        due = [
            operation.due_at
            for operation in self.gateway.ledger.list_operations(
                state=OperationState.PENDING
            )
        ]
        return min(due) if due else None

    def next_due_at(self) -> float | None:
        now = float(_now())
        with self.repository.connect() as db:
            local = db.execute(
                "SELECT 1 FROM commands WHERE status IN "
                "('queued','waiting_after_cancel','cancel_in_flight','delivery_in_flight') "
                "AND (gateway_operation_id IS NULL OR gateway_operation_id='') LIMIT 1"
            ).fetchone()
            local = (
                local
                or db.execute(
                    "SELECT 1 FROM tasks WHERE status NOT IN ('completed','failed','cancelled','waiting_for_parent') "
                    "AND (gateway_operation_id IS NULL OR gateway_operation_id='') LIMIT 1"
                ).fetchone()
            )
        candidates: list[float] = []
        if local:
            candidates.append(now)
        gateway_due = self._gateway_operation_due_at()
        if gateway_due is not None:
            candidates.append(float(gateway_due))
        automation_due = self.next_wakeup_at()
        if automation_due is not None:
            candidates.append(float(automation_due))
        return min(candidates) if candidates else None

    def next_wakeup_at(self) -> float | None:
        value = self.repository.next_automation_due_at()
        return float(value) if value is not None else None

    def has_pending_work(self) -> bool:
        due = self.next_due_at()
        return due is not None and due <= _now()

    def prepare_local_work(self) -> None:
        self._queue_parent_notifications()
        self._prepare_gateway_work()

    # ------------------------------------------------------------------
    # Parent registration and child creation
    # ------------------------------------------------------------------
    async def register_parent(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        result = dict(await super().register_parent(*args, **kwargs))
        agent = self.repository.agent(str(result["agent_id"]))
        if agent:
            self._ensure_gateway_mapping(agent)
            result = self._public_agent(
                self.repository.agent(str(result["agent_id"])) or agent
            )
        return result

    async def queue_after_completion(
        self,
        source_thread_id: str,
        target_thread_id: str,
        prompt: str,
        *,
        completion_marker: str = "",
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        result = await super().queue_after_completion(
            source_thread_id,
            target_thread_id,
            prompt,
            completion_marker=completion_marker,
            idempotency_key=idempotency_key,
        )
        if str(result.get("status") or "") == "waiting":
            source = self._resolve_agent_reference(source_thread_id)
            self._ensure_completion_source_monitoring(source)
        return dict(result)

    def _ensure_completion_source_monitoring(self, source: Mapping[str, Any]) -> None:
        status = str(source.get("status") or "")
        if status in {"failed", "cancelled"}:
            return
        gateway_id = str(source.get("gateway_agent_id") or "")
        if not gateway_id and source.get("chat_id"):
            gateway_id = self._ensure_gateway_mapping(source)
        if not gateway_id:
            return
        gateway_agent = self.gateway.ledger.get_agent(gateway_id)
        if gateway_agent is None or not gateway_agent.conversation_id:
            return
        if gateway_agent.state in {AgentState.FAILED, AgentState.CANCELLED}:
            return
        if gateway_agent.state is AgentState.COMPLETED:
            self.gateway.ledger.reopen_agent(gateway_id, now=self.gateway.clock.now())
            gateway_agent = self.gateway.ledger.get_agent(gateway_id)
            if gateway_agent is None:
                return
        due_at = max(
            self.gateway.clock.now(),
            float(gateway_agent.next_inspection_at or self.gateway.clock.now()),
        )
        try:
            self.gateway.enqueue_inspect(
                agent_id=gateway_id,
                due_at=due_at,
                completion_sensitive=True,
            )
        except ValueError:
            pass

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
                    "SELECT agent_id FROM agents WHERE parent_agent_id=? "
                    "AND spawn_idempotency_key=?",
                    (parent_agent_id, key),
                ).fetchone()
            if existing:
                return await self.status(str(existing["agent_id"]))

        now = _now()
        agent_id, task_id = _id("agent"), _id("task")
        with self.repository.transaction() as db:
            active = int(
                db.execute(
                    "SELECT COUNT(*) FROM agents WHERE parent_agent_id IS NOT NULL "
                    "AND status NOT IN ('completed','failed','cancelled')"
                ).fetchone()[0]
            )
            if active >= self.maximum_active_children:
                raise ValueError(
                    f"maximum active managed sub-agents reached ({self.maximum_active_children})"
                )
            db.execute(
                "INSERT INTO agents(agent_id,orchestration_id,parent_agent_id,root_agent_id,"
                "project_id,working_directory,title,status,notification_policy,"
                "spawn_idempotency_key,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
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
                "INSERT INTO tasks(task_id,agent_id,prompt,completion_marker,status,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (task_id, agent_id, prompt, marker, "creating_thread", now),
            )
            db.execute(
                "INSERT INTO subscriptions(parent_agent_id,child_agent_id,notification_policy,created_at) "
                "VALUES(?,?,?,?)",
                (parent_agent_id, agent_id, notification_policy, now),
            )

        effective_prompt = self._build_initial_prompt(
            prompt,
            marker,
            agent_id=agent_id,
            task_id=task_id,
            parent_agent_id=parent_agent_id,
            root_agent_id=str(parent["root_agent_id"]),
            orchestration_id=str(parent["orchestration_id"]),
            project_id=selected_project,
            working_directory=selected_working_directory,
        )
        try:
            gateway_agent_id, operation_id = self.gateway.enqueue_create(
                project_id=selected_project or "",
                prompt=effective_prompt,
                title=title,
                completion_marker=marker,
                idempotency_key=(
                    f"domain-spawn:{parent_agent_id}:{key}"
                    if key
                    else f"domain-spawn:{agent_id}"
                ),
            )
        except Exception as exc:
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE agents SET status='failed',last_error=?,updated_at=? WHERE agent_id=?",
                    (_safe_error(exc), _now(), agent_id),
                )
                db.execute(
                    "UPDATE tasks SET status='failed',completed_at=? WHERE task_id=?",
                    (_now(), task_id),
                )
            raise
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET gateway_agent_id=?,creation_request_id=?,updated_at=? "
                "WHERE agent_id=?",
                (gateway_agent_id, operation_id, _now(), agent_id),
            )
        return await self.status(agent_id)

    # ------------------------------------------------------------------
    # One-request synchronization
    # ------------------------------------------------------------------
    async def sync_once(self, *, force: bool = True) -> dict[str, Any]:
        del force
        if self._lock.locked():
            return {
                "status": "busy",
                "write_count": 0,
                "physical_requests": 0,
                "inspected": [],
            }
        async with self._lock:
            self._prepare_gateway_work()
            tick = await self.gateway.tick()
            self._reconcile_gateway_state()
            self._reconcile_gateway_operations()
            self._queue_parent_notifications()
            payload = dict(tick.as_dict())
            payload.update(
                write_count=(
                    tick.physical_requests
                    if tick.operation_type
                    in {
                        OperationType.CREATE.value,
                        OperationType.CONTINUE.value,
                        OperationType.CANCEL.value,
                        OperationType.DELETE.value,
                    }
                    else 0
                ),
                inspected=(
                    [
                        {
                            "agent_id": tick.agent_id,
                            "status": tick.outcome or tick.status,
                        }
                    ]
                    if tick.agent_id
                    and tick.operation_type
                    in {
                        OperationType.INSPECT.value,
                        OperationType.VERIFY_CREATION.value,
                        OperationType.VERIFY_CONTINUE.value,
                        OperationType.RECOVERY_PROBE.value,
                    }
                    else []
                ),
            )
            return payload

    def _prepare_gateway_work(self) -> None:
        self._reconcile_gateway_state()
        self._reconcile_gateway_operations()
        self._queue_parent_notifications()
        self._prepare_automations()
        self._queue_command_operations()
        self._queue_automatic_continuations()

    def _prepare_automations(self) -> None:
        self._reconcile_automation_commands()
        now = _now()
        for automation in self.repository.automations(include_terminal=False):
            kind = str(automation.get("kind") or "")
            status = str(automation.get("status") or "")
            if kind == "wakeup" and status == "scheduled":
                due_at = float(automation.get("due_at") or 0)
                if due_at <= now:
                    self._materialize_automation(automation)
                continue
            if kind != "after_completion" or status != "waiting":
                continue
            source_id = str(automation.get("source_agent_id") or "")
            source = self.repository.agent(source_id)
            if source is None:
                self._fail_automation(
                    automation, "completion source agent was not found"
                )
                continue
            gateway_id = str(source.get("gateway_agent_id") or "")
            snapshot = self.gateway.latest_snapshot(gateway_id) if gateway_id else None
            marker = str(automation.get("completion_marker") or "")
            if self._snapshot_has_completion_marker(snapshot, marker):
                self._materialize_automation(automation)
                continue
            if snapshot is None:
                self._ensure_completion_source_monitoring(source)
                continue
            if str(source.get("status") or "") in {
                "completed",
                "failed",
                "cancelled",
            }:
                self._fail_automation(
                    automation,
                    "completion source became terminal without the exact standalone marker",
                )

    @staticmethod
    def _snapshot_has_completion_marker(
        snapshot: Mapping[str, Any] | None, marker: str
    ) -> bool:
        if not snapshot or snapshot.get("found") is not True or not marker:
            return False
        turns = snapshot.get("turns")
        if not isinstance(turns, list):
            return False
        latest = next(
            (
                turn
                for turn in reversed(turns)
                if isinstance(turn, Mapping) and turn.get("role") == "assistant"
            ),
            None,
        )
        if latest is None:
            return False
        return (
            str(latest.get("status") or "").lower() in _TERMINAL_MESSAGE_STATUSES
            and latest.get("end_turn") is True
            and any(
                line.strip() == marker
                for line in str(latest.get("text") or "").splitlines()
            )
        )

    def _materialize_automation(self, automation: Mapping[str, Any]) -> None:
        automation_id = str(automation["automation_id"])
        target_id = str(automation["target_agent_id"])
        target = self.repository.agent(target_id)
        if target is None:
            self._fail_automation(automation, "automation target agent was not found")
            return
        if str(target.get("status") or "") in {"failed", "cancelled"}:
            self._fail_automation(
                automation, "automation target is failed or cancelled"
            )
            return
        now = _now()
        try:
            with self.repository.transaction() as db:
                current = db.execute(
                    "SELECT * FROM agent_automations WHERE automation_id=?",
                    (automation_id,),
                ).fetchone()
                if current is None or str(current["status"]) not in {
                    "scheduled",
                    "waiting",
                }:
                    return
                sequence = int(
                    db.execute(
                        "SELECT COALESCE(MAX(sequence_no),0)+1 FROM commands "
                        "WHERE to_agent_id=?",
                        (target_id,),
                    ).fetchone()[0]
                )
                command_id = _id("cmd")
                kind = str(automation["kind"])
                db.execute(
                    "INSERT INTO commands("
                    "command_id,from_agent_id,to_agent_id,sequence_no,message,"
                    "interrupt_policy,status,idempotency_key,purpose,created_at"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (
                        command_id,
                        None,
                        target_id,
                        sequence,
                        str(automation["message"]),
                        "queue",
                        "queued",
                        f"automation:{automation_id}",
                        kind,
                        now,
                    ),
                )
                db.execute(
                    "UPDATE agent_automations SET status='queued',command_id=?,"
                    "triggered_at=?,updated_at=?,last_error='' WHERE automation_id=?",
                    (command_id, now, now, automation_id),
                )
                task = db.execute(
                    "SELECT task_id FROM tasks WHERE agent_id=?", (target_id,)
                ).fetchone()
                self._insert_event(
                    db,
                    target_id,
                    str(task["task_id"]) if task else None,
                    "wakeup_due" if kind == "wakeup" else "completion_prompt_queued",
                    {
                        "automation_id": automation_id,
                        "kind": kind,
                        "source_agent_id": automation.get("source_agent_id"),
                        "due_at": automation.get("due_at"),
                    },
                    source_cursor=f"automation:{automation_id}:queued",
                )
        except Exception as exc:
            self._fail_automation(automation, _safe_error(exc))

    def _fail_automation(self, automation: Mapping[str, Any], reason: str) -> None:
        now = _now()
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agent_automations SET status='failed',completed_at=?,"
                "updated_at=?,last_error=? WHERE automation_id=? "
                "AND status NOT IN ('delivered','cancelled','failed')",
                (now, now, reason[:500], automation["automation_id"]),
            )

    def _reconcile_automation_commands(self) -> None:
        with self.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT a.automation_id,a.status AS automation_status,"
                    "c.status AS command_status,c.last_error "
                    "FROM agent_automations a JOIN commands c ON c.command_id=a.command_id "
                    "WHERE a.status='queued'"
                )
            ]
        now = _now()
        for row in rows:
            command_status = str(row.get("command_status") or "")
            if command_status in {"delivered", "acknowledged"}:
                status, error = "delivered", ""
            elif command_status in {"cancelled", "superseded"}:
                status, error = (
                    "failed",
                    str(row.get("last_error") or "delivery cancelled"),
                )
            else:
                continue
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE agent_automations SET status=?,completed_at=?,updated_at=?,"
                    "last_error=? WHERE automation_id=? AND status='queued'",
                    (status, now, now, error[:500], row["automation_id"]),
                )

    def _reopen_automation_target(
        self, target: Mapping[str, Any], gateway_id: str
    ) -> None:
        gateway_agent = self.gateway.ledger.get_agent(gateway_id)
        if gateway_agent is None or gateway_agent.state is not AgentState.COMPLETED:
            return
        self.gateway.ledger.reopen_agent(gateway_id, now=self.gateway.clock.now())
        now = _now()
        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET status='unknown',last_error='',updated_at=? "
                "WHERE agent_id=? AND status='completed'",
                (now, target["agent_id"]),
            )
            db.execute(
                "UPDATE tasks SET status='unknown',completed_at=NULL,next_check_at=0 "
                "WHERE agent_id=? AND status='completed'",
                (target["agent_id"],),
            )

    def _queue_command_operations(self) -> None:
        for command in self.repository.queued_commands(include_future=False):
            if command.get("gateway_operation_id"):
                continue
            target = self.repository.agent(str(command["to_agent_id"]))
            if not target:
                continue
            try:
                gateway_id = self._ensure_gateway_mapping(target)
                gateway_agent = self.gateway.ledger.get_agent(gateway_id)
            except Exception as exc:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET last_error=?,next_attempt_at=? WHERE command_id=?",
                        (_safe_error(exc), _now() + 60, command["command_id"]),
                    )
                continue
            if gateway_agent is None:
                continue
            if str(command.get("purpose") or "") in {"wakeup", "after_completion"}:
                self._reopen_automation_target(target, gateway_id)
                gateway_agent = self.gateway.ledger.get_agent(gateway_id)
                if gateway_agent is None:
                    continue
            if gateway_agent.state in {AgentState.FAILED, AgentState.CANCELLED}:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancelled',last_error=? WHERE command_id=?",
                        ("gateway target is terminal", command["command_id"]),
                    )
                continue
            if (
                command.get("interrupt_policy") == "interrupt"
                and gateway_agent.state is AgentState.RUNNING
            ):
                operation_id = self.gateway.enqueue_cancel(
                    agent_id=gateway_id,
                    idempotency_key=f"command-cancel:{command['command_id']}",
                )
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET status='cancel_in_flight',gateway_operation_id=?,"
                        "last_error='',next_attempt_at=0 WHERE command_id=?",
                        (operation_id, command["command_id"]),
                    )
                continue
            if gateway_agent.state is AgentState.CREATING:
                continue
            if gateway_agent.state is AgentState.RUNNING:
                gateway_now = self.gateway.clock.now()
                due_at = max(
                    gateway_now,
                    float(gateway_agent.next_inspection_at or gateway_now),
                )
                try:
                    operation_id = self.gateway.enqueue_inspect(
                        agent_id=gateway_id,
                        due_at=due_at,
                        completion_sensitive=str(command.get("purpose") or "")
                        in {"completion", "question", "wakeup", "after_completion"},
                    )
                except Exception as exc:
                    with self.repository.transaction() as db:
                        db.execute(
                            "UPDATE commands SET last_error=?,next_attempt_at=? WHERE command_id=?",
                            (_safe_error(exc), _now() + 60, command["command_id"]),
                        )
                    continue
                delay = max(1.0, due_at - gateway_now)
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET last_error=?,next_attempt_at=? WHERE command_id=?",
                        (
                            f"waiting for canonical running target read ({operation_id})",
                            _now() + delay,
                            command["command_id"],
                        ),
                    )
                continue
            if self.gateway.latest_snapshot(gateway_id) is None:
                try:
                    operation_id = self.gateway.enqueue_inspect(
                        agent_id=gateway_id,
                        due_at=self.gateway.clock.now(),
                        completion_sensitive=True,
                    )
                except ValueError:
                    continue
                with self.repository.transaction() as db:
                    delay = (
                        1
                        if str(command.get("purpose") or "")
                        in {"wakeup", "after_completion"}
                        else 30
                    )
                    db.execute(
                        "UPDATE commands SET last_error=?,next_attempt_at=? WHERE command_id=?",
                        (
                            f"waiting for canonical target read ({operation_id})",
                            _now() + delay,
                            command["command_id"],
                        ),
                    )
                continue
            try:
                operation_id = self.gateway.enqueue_continue(
                    agent_id=gateway_id,
                    message=str(command["message"]),
                    idempotency_key=f"command:{command['command_id']}",
                )
            except Exception as exc:
                with self.repository.transaction() as db:
                    db.execute(
                        "UPDATE commands SET last_error=?,next_attempt_at=? WHERE command_id=?",
                        (_safe_error(exc), _now() + 60, command["command_id"]),
                    )
                continue
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE commands SET status='delivery_in_flight',gateway_operation_id=?,"
                    "last_error='',next_attempt_at=0 WHERE command_id=?",
                    (operation_id, command["command_id"]),
                )

    def _queue_automatic_continuations(self) -> None:
        pending_gateway_agents = {
            operation.agent_id
            for state in (OperationState.PENDING, OperationState.CLAIMED)
            for operation in self.gateway.ledger.list_operations(state=state)
            if operation.agent_id
            and operation.type
            in {
                OperationType.CREATE,
                OperationType.VERIFY_CREATION,
                OperationType.CONTINUE,
                OperationType.VERIFY_CONTINUE,
            }
        }
        with self.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT a.*,t.task_id,t.completion_marker,t.continue_attempts,"
                    "t.gateway_operation_id,t.status AS task_status FROM agents a "
                    "JOIN tasks t ON t.agent_id=a.agent_id "
                    "WHERE a.parent_agent_id IS NOT NULL "
                    "AND t.status NOT IN ('completed','failed','cancelled','waiting_for_parent') "
                    "AND (t.gateway_operation_id IS NULL OR t.gateway_operation_id='') "
                    "ORDER BY COALESCE(t.last_continue_at,0),a.created_at,a.agent_id"
                )
            ]
        for row in rows:
            if (
                int(row.get("continue_attempts") or 0)
                >= self.config.max_continue_attempts
            ):
                continue
            gateway_id = str(row.get("gateway_agent_id") or "")
            if not gateway_id:
                continue
            gateway_agent = self.gateway.ledger.get_agent(gateway_id)
            snapshot = self.gateway.latest_snapshot(gateway_id)
            if gateway_id in pending_gateway_agents:
                continue
            if (
                gateway_agent is None
                or gateway_agent.state is not AgentState.UNKNOWN
                or not self._snapshot_stopped_incomplete(
                    snapshot, str(row.get("completion_marker") or "")
                )
            ):
                continue
            with self.repository.connect() as db:
                pending = db.execute(
                    "SELECT 1 FROM commands WHERE to_agent_id=? AND status IN "
                    "('queued','waiting_after_cancel','cancel_in_flight','delivery_in_flight',"
                    "'delivery_uncertain') LIMIT 1",
                    (row["agent_id"],),
                ).fetchone()
            if pending:
                continue
            marker = str(row.get("completion_marker") or "")
            message = DEFAULT_AGENT_CONTINUE_MESSAGE
            if marker:
                message += f"\n\nRequired completion marker:\n{marker}"
            try:
                operation_id = self.gateway.enqueue_continue(
                    agent_id=gateway_id,
                    message=message,
                    idempotency_key=(
                        f"automatic:{row['task_id']}:{int(row.get('continue_attempts') or 0) + 1}"
                    ),
                )
            except Exception:
                continue
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE tasks SET status='continuation_in_flight',"
                    "continue_attempts=continue_attempts+1,gateway_operation_id=?,"
                    "last_continue_at=? WHERE task_id=?",
                    (operation_id, _now(), row["task_id"]),
                )
                db.execute(
                    "UPDATE agents SET status='continuation_in_flight',updated_at=? "
                    "WHERE agent_id=?",
                    (_now(), row["agent_id"]),
                )
            break

    @staticmethod
    def _snapshot_stopped_incomplete(
        snapshot: Mapping[str, Any] | None, marker: str
    ) -> bool:
        if not snapshot or snapshot.get("found") is not True or snapshot.get("running"):
            return False
        turns = snapshot.get("turns")
        if not isinstance(turns, list) or not turns:
            return False
        latest = next(
            (
                turn
                for turn in reversed(turns)
                if isinstance(turn, Mapping) and turn.get("role") == "assistant"
            ),
            None,
        )
        if not latest:
            return False
        status = str(latest.get("status") or "").lower()
        text = str(latest.get("text") or "")
        return (
            status in _TERMINAL_MESSAGE_STATUSES
            and latest.get("end_turn") is True
            and (not marker or marker not in text)
        )

    # ------------------------------------------------------------------
    # Local reconciliation
    # ------------------------------------------------------------------
    def _reconcile_gateway_operations(self) -> None:
        self._reconcile_command_operations()
        self._reconcile_task_operations()
        self._reconcile_control_operations()
        self._reconcile_automation_commands()

    def _reconcile_command_operations(self) -> None:
        with self.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM commands WHERE gateway_operation_id IS NOT NULL "
                    "AND gateway_operation_id<>''"
                )
            ]
        for command in rows:
            operation = self.gateway.ledger.get_operation(
                str(command["gateway_operation_id"])
            )
            if operation is None or operation.state in {
                OperationState.PENDING,
                OperationState.CLAIMED,
            }:
                continue
            with self.repository.transaction() as db:
                if operation.type is OperationType.CANCEL:
                    if operation.state is OperationState.SUCCEEDED:
                        db.execute(
                            "UPDATE commands SET status='queued',interrupt_policy='queue',"
                            "gateway_operation_id=NULL,last_error='',next_attempt_at=0 "
                            "WHERE command_id=?",
                            (command["command_id"],),
                        )
                    else:
                        db.execute(
                            "UPDATE commands SET status='cancelled',gateway_operation_id=NULL,"
                            "last_error=? WHERE command_id=?",
                            (
                                operation.last_error or "interrupt cancellation failed",
                                command["command_id"],
                            ),
                        )
                    continue
                if operation.type is not OperationType.CONTINUE:
                    continue
                if operation.state is OperationState.SUCCEEDED:
                    db.execute(
                        "UPDATE commands SET status='delivered',delivered_at=?,"
                        "gateway_operation_id=NULL,last_error='',next_attempt_at=0 "
                        "WHERE command_id=?",
                        (_now(), command["command_id"]),
                    )
                    self._activate_target_after_delivery(db, command)
                    self._ack_command_cursor(db, command)
                else:
                    db.execute(
                        "UPDATE commands SET status='cancelled',gateway_operation_id=NULL,"
                        "last_error=? WHERE command_id=?",
                        (
                            operation.last_error or "gateway delivery failed",
                            command["command_id"],
                        ),
                    )

    def _reconcile_task_operations(self) -> None:
        with self.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT t.*,a.agent_id FROM tasks t JOIN agents a ON a.agent_id=t.agent_id "
                    "WHERE t.gateway_operation_id IS NOT NULL AND t.gateway_operation_id<>''"
                )
            ]
        for task in rows:
            operation = self.gateway.ledger.get_operation(
                str(task["gateway_operation_id"])
            )
            if operation is None or operation.state in {
                OperationState.PENDING,
                OperationState.CLAIMED,
            }:
                continue
            with self.repository.transaction() as db:
                if operation.state is OperationState.SUCCEEDED:
                    db.execute(
                        "UPDATE tasks SET status='waiting_assistant',gateway_operation_id=NULL "
                        "WHERE task_id=?",
                        (task["task_id"],),
                    )
                    db.execute(
                        "UPDATE agents SET status='waiting_assistant',last_error='',updated_at=? "
                        "WHERE agent_id=?",
                        (_now(), task["agent_id"]),
                    )
                else:
                    error = operation.last_error or "automatic continuation failed"
                    db.execute(
                        "UPDATE tasks SET status='unknown',gateway_operation_id=NULL WHERE task_id=?",
                        (task["task_id"],),
                    )
                    db.execute(
                        "UPDATE agents SET status='unknown',last_error=?,updated_at=? WHERE agent_id=?",
                        (error, _now(), task["agent_id"]),
                    )

    def _reconcile_control_operations(self) -> None:
        with self.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM agents WHERE gateway_control_operation_id IS NOT NULL "
                    "AND gateway_control_operation_id<>''"
                )
            ]
        for agent in rows:
            operation = self.gateway.ledger.get_operation(
                str(agent["gateway_control_operation_id"])
            )
            if operation is None or operation.state in {
                OperationState.PENDING,
                OperationState.CLAIMED,
            }:
                continue
            task = self.repository.task_for_agent(str(agent["agent_id"]))
            with self.repository.transaction() as db:
                if operation.state is OperationState.SUCCEEDED:
                    db.execute(
                        "UPDATE agents SET status='cancelled',gateway_control_operation_id=NULL,"
                        "last_error='',updated_at=? WHERE agent_id=?",
                        (_now(), agent["agent_id"]),
                    )
                    if task:
                        db.execute(
                            "UPDATE tasks SET status='cancelled',completed_at=COALESCE(completed_at,?) "
                            "WHERE task_id=?",
                            (_now(), task["task_id"]),
                        )
                    self._insert_event(
                        db,
                        str(agent["agent_id"]),
                        str(task["task_id"]) if task else None,
                        "cancelled",
                        {"reason": "gateway cancellation completed"},
                        source_cursor=f"gateway-cancelled:{agent['agent_id']}",
                    )
                else:
                    db.execute(
                        "UPDATE agents SET status='error',gateway_control_operation_id=NULL,"
                        "last_error=?,updated_at=? WHERE agent_id=?",
                        (
                            operation.last_error or "gateway cancellation failed",
                            _now(),
                            agent["agent_id"],
                        ),
                    )

    def _reconcile_gateway_state(self) -> None:
        with self.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM agents WHERE gateway_agent_id IS NOT NULL "
                    "AND gateway_agent_id<>''"
                )
            ]
        for domain in rows:
            gateway_agent = self.gateway.ledger.get_agent(
                str(domain["gateway_agent_id"])
            )
            if gateway_agent is None:
                continue
            snapshot = self.gateway.latest_snapshot(gateway_agent.id)
            task = self.repository.task_for_agent(str(domain["agent_id"]))
            current_node = (
                str(snapshot.get("current_node") or "")
                if snapshot
                else str(domain.get("current_node") or "")
            )
            signature = (
                hashlib.sha256(_compact_json(snapshot).encode("utf-8")).hexdigest()
                if snapshot
                else str(domain.get("progress_signature") or "")
            )
            previous_signature = str(domain.get("progress_signature") or "")
            status = self._domain_status_from_gateway(domain, gateway_agent.state)
            now = _now()
            with self.repository.transaction() as db:
                if gateway_agent.conversation_id and not domain.get("chat_id"):
                    self._insert_event(
                        db,
                        str(domain["agent_id"]),
                        str(task["task_id"]) if task else None,
                        "started",
                        {
                            "chat_id": gateway_agent.conversation_id,
                            "project_id": gateway_agent.project_id or None,
                        },
                        source_cursor=f"gateway-started:{gateway_agent.conversation_id}",
                    )
                if snapshot and signature != previous_signature:
                    self._insert_event(
                        db,
                        str(domain["agent_id"]),
                        str(task["task_id"]) if task else None,
                        "progress",
                        self._snapshot_event(snapshot),
                        source_cursor=f"gateway-progress:{current_node}:{signature[:12]}",
                    )
                if status == "completed":
                    self._insert_event(
                        db,
                        str(domain["agent_id"]),
                        str(task["task_id"]) if task else None,
                        "completed",
                        self._snapshot_event(snapshot)
                        if snapshot
                        else {"chat_id": gateway_agent.conversation_id},
                        source_cursor=f"gateway-completed:{current_node or gateway_agent.id}",
                    )
                db.execute(
                    "UPDATE agents SET chat_id=COALESCE(?,chat_id),status=?,current_node=?,"
                    "progress_signature=?,last_progress_at=?,last_error=?,updated_at=? "
                    "WHERE agent_id=?",
                    (
                        gateway_agent.conversation_id,
                        status,
                        current_node,
                        signature or None,
                        now
                        if signature != previous_signature
                        else domain.get("last_progress_at"),
                        gateway_agent.last_error or "",
                        now,
                        domain["agent_id"],
                    ),
                )
                if task and not task.get("gateway_operation_id"):
                    task_status = self._task_status_from_domain(
                        status, str(task.get("status") or "")
                    )
                    db.execute(
                        "UPDATE tasks SET status=?,next_check_at=?,completed_at="
                        "CASE WHEN ? IN ('completed','failed','cancelled') "
                        "THEN COALESCE(completed_at,?) ELSE completed_at END WHERE task_id=?",
                        (
                            task_status,
                            gateway_agent.next_inspection_at or 0,
                            task_status,
                            now,
                            task["task_id"],
                        ),
                    )

    @staticmethod
    def _domain_status_from_gateway(
        domain: Mapping[str, Any], state: AgentState
    ) -> str:
        is_root = not domain.get("parent_agent_id")
        current = str(domain.get("status") or "")
        if state is AgentState.CREATING:
            return "creation_in_flight" if domain.get("chat_id") else "creating_thread"
        if state is AgentState.RUNNING:
            return "running"
        if state is AgentState.UNKNOWN:
            if current == "waiting_for_parent":
                return current
            if current in {"waiting_assistant", "continuation_in_flight"}:
                return "waiting_assistant"
            return "registered" if is_root else "unknown"
        if state is AgentState.COMPLETED:
            return "registered" if is_root else "completed"
        if state is AgentState.FAILED:
            return "failed"
        return "cancelled"

    @staticmethod
    def _task_status_from_domain(status: str, current: str) -> str:
        if current == "waiting_for_parent" and status not in _TERMINAL_DOMAIN_STATES:
            return current
        return {
            "registered": "unknown",
            "creating_thread": "creating_thread",
            "creation_in_flight": "creation_in_flight",
            "running": "running",
            "waiting_assistant": "waiting_assistant",
            "unknown": "unknown",
            "completed": "completed",
            "failed": "failed",
            "cancelled": "cancelled",
        }.get(status, "unknown")

    @staticmethod
    def _snapshot_event(snapshot: Mapping[str, Any] | None) -> dict[str, Any]:
        if not snapshot:
            return {}
        turns = snapshot.get("turns")
        latest = turns[-1] if isinstance(turns, list) and turns else {}
        return {
            "conversation_id": snapshot.get("conversation_id"),
            "running": bool(snapshot.get("running")),
            "current_node": snapshot.get("current_node"),
            "visible_current_node": snapshot.get("visible_current_node"),
            "turn_count": len(turns) if isinstance(turns, list) else 0,
            "latest": latest,
        }

    # ------------------------------------------------------------------
    # Read-only public surfaces
    # ------------------------------------------------------------------
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
        target = self.repository.agent(to_agent_id)
        reopen_completed = bool(
            target
            and str(target.get("status") or "") == "completed"
            and purpose in {"instruction", "answer"}
        )
        if reopen_completed:
            if interrupt_policy not in INTERRUPT_POLICIES:
                raise ValueError("interrupt_policy must be queue or interrupt")
            if purpose not in COMMAND_PURPOSES:
                raise ValueError(
                    "purpose must be instruction, answer, question, progress, or completion"
                )
            if not message.strip():
                raise ValueError("message is required")
            sender = self.repository.agent(from_agent_id)
            if not sender or not target:
                raise ValueError("sender or target agent was not found")
            if sender["orchestration_id"] != target["orchestration_id"]:
                raise ValueError("agents belong to different orchestrations")
            if purpose == "answer" and target.get("parent_agent_id") != from_agent_id:
                raise ValueError(
                    "an answer must be sent from a parent to its direct child"
                )
            if str(sender.get("status") or "") in _TERMINAL_DOMAIN_STATES:
                raise ValueError(
                    "sender agent is terminal and cannot send new messages"
                )
            key = idempotency_key.strip() if idempotency_key else None
            if key:
                with self.repository.connect() as db:
                    existing = db.execute(
                        "SELECT * FROM commands WHERE from_agent_id=? "
                        "AND to_agent_id=? AND idempotency_key=?",
                        (from_agent_id, to_agent_id, key),
                    ).fetchone()
                if existing:
                    return dict(existing)
            gateway_id = str(target.get("gateway_agent_id") or "")
            if gateway_id:
                self.gateway.ledger.reopen_agent(
                    gateway_id, now=self.gateway.clock.now()
                )
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE agents SET status='unknown',last_error='',updated_at=? "
                    "WHERE agent_id=? AND status='completed'",
                    (_now(), to_agent_id),
                )
                db.execute(
                    "UPDATE tasks SET status='unknown',completed_at=NULL,next_check_at=0 "
                    "WHERE agent_id=? AND status='completed'",
                    (to_agent_id,),
                )
        return dict(
            await super().send(
                from_agent_id,
                to_agent_id,
                message,
                interrupt_policy=interrupt_policy,
                idempotency_key=idempotency_key,
                purpose=purpose,
            )
        )

    async def status(self, agent_id: str) -> dict[str, Any]:
        result = await super().status(agent_id)
        gateway_id = str(result["agent"].get("gateway_agent_id") or "")
        if not gateway_id:
            agent = self.repository.agent(agent_id)
            gateway_id = str(agent.get("gateway_agent_id") or "") if agent else ""
        gateway_agent = (
            self.gateway.ledger.get_agent(gateway_id) if gateway_id else None
        )
        result["gateway"] = (
            {
                "agent_id": gateway_agent.id,
                "state": gateway_agent.state.value,
                "conversation_id": gateway_agent.conversation_id,
                "next_inspection_at": gateway_agent.next_inspection_at,
                "last_inspected_at": gateway_agent.last_inspected_at,
                "last_error": gateway_agent.last_error,
            }
            if gateway_agent
            else None
        )
        result["automations"] = [
            self._public_automation(row)
            for row in self.repository.automations(
                agent_id=agent_id, include_terminal=False
            )
        ]
        return dict(result)

    async def context(
        self,
        agent_id: str,
        *,
        since_cursor: str | None = None,
        max_events: int | None = None,
        max_chars: int | None = None,
    ) -> dict[str, Any]:
        agent = self.repository.agent(agent_id)
        if not agent:
            raise ValueError("agent was not found")
        gateway_id = str(agent.get("gateway_agent_id") or "")
        snapshot = self.gateway.latest_snapshot(gateway_id) if gateway_id else None
        events = self.repository.events_after(agent_id, 0)[
            -(max_events or self.config.context_max_events) :
        ]
        payload: dict[str, Any] = {
            "agent_id": agent_id,
            "cursor": str((snapshot or {}).get("current_node") or since_cursor or ""),
            "state": str(agent.get("status") or ""),
            "events": events,
            "turns": list((snapshot or {}).get("turns") or []),
        }
        limit = max_chars or self.config.context_max_chars
        encoded = _compact_json(payload)
        if len(encoded) > limit:
            payload["turns"] = payload["turns"][-8:]
            payload["events"] = payload["events"][-12:]
            payload["truncated"] = True
        return payload

    async def tail(
        self, agent_id: str, *, lines: int | None = None, max_chars: int | None = None
    ) -> dict[str, Any]:
        agent = self.repository.agent(agent_id)
        if not agent:
            raise ValueError("agent was not found")
        gateway_id = str(agent.get("gateway_agent_id") or "")
        snapshot = self.gateway.latest_snapshot(gateway_id) if gateway_id else None
        turns = list((snapshot or {}).get("turns") or [])[
            -(lines or self.config.tail_lines) :
        ]
        limit = max_chars or self.config.tail_max_chars
        while turns and len(_compact_json(turns)) > limit:
            turns.pop(0)
        return {"agent_id": agent_id, "turns": turns, "truncated": False}

    async def cancel(self, agent_id: str, *, interrupt: bool = False) -> dict[str, Any]:
        agent = self.repository.agent(agent_id)
        if not agent:
            raise ValueError("agent was not found")
        if str(agent.get("status") or "") == "cancelled":
            return {
                "agent_id": agent_id,
                "cancelled": True,
                "reason": "already cancelled",
            }
        task = self.repository.task_for_agent(agent_id)
        gateway_id = str(agent.get("gateway_agent_id") or "")
        if interrupt and gateway_id and agent.get("chat_id"):
            operation_id = self.gateway.enqueue_cancel(
                agent_id=gateway_id,
                idempotency_key=f"agent-cancel:{agent_id}",
            )
            with self.repository.transaction() as db:
                db.execute(
                    "UPDATE agents SET status='cancel_in_flight',gateway_control_operation_id=?,"
                    "updated_at=? WHERE agent_id=?",
                    (operation_id, _now(), agent_id),
                )
                if task:
                    db.execute(
                        "UPDATE tasks SET status='cancel_in_flight' WHERE task_id=?",
                        (task["task_id"],),
                    )
            return {
                "agent_id": agent_id,
                "cancelled": False,
                "queued": True,
                "operation_id": operation_id,
            }

        with self.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET status='cancelled',updated_at=? WHERE agent_id=?",
                (_now(), agent_id),
            )
            if task:
                db.execute(
                    "UPDATE tasks SET status='cancelled',completed_at=COALESCE(completed_at,?) "
                    "WHERE task_id=?",
                    (_now(), task["task_id"]),
                )
            db.execute(
                "UPDATE commands SET status='cancelled' WHERE to_agent_id=? AND status NOT IN "
                "('delivered','acknowledged','cancelled','superseded')",
                (agent_id,),
            )
            self._insert_event(
                db,
                agent_id,
                str(task["task_id"]) if task else None,
                "cancelled",
                {"reason": "orchestration cancelled without provider interrupt"},
                source_cursor=f"cancelled:{agent_id}",
            )
        if gateway_id:
            with self.gateway.ledger.transaction(immediate=True) as db:
                self.gateway.ledger.update_agent(
                    db,
                    agent_id=gateway_id,
                    now=self.gateway.clock.now(),
                    state=AgentState.CANCELLED,
                    completed_at=self.gateway.clock.now(),
                )
                self.gateway.ledger.cancel_pending_operations_for_agent(
                    db, agent_id=gateway_id, now=self.gateway.clock.now()
                )
        return {"agent_id": agent_id, "cancelled": True, "reason": ""}

    def dashboard_snapshot(self) -> dict[str, Any]:
        with self.repository.connect() as db:
            domain_rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM agents ORDER BY created_at,agent_id"
                )
            ]
            tasks = {
                str(row["agent_id"]): dict(row)
                for row in db.execute("SELECT * FROM tasks")
            }
            mailbox = {
                str(row["to_agent_id"]): int(row["count"])
                for row in db.execute(
                    "SELECT to_agent_id,COUNT(*) AS count FROM commands WHERE status NOT IN "
                    "('delivered','acknowledged','cancelled','superseded') GROUP BY to_agent_id"
                )
            }

        by_id = {str(row["agent_id"]): row for row in domain_rows}
        children: dict[str, list[str]] = {}
        roots: list[str] = []
        for row in domain_rows:
            agent_id = str(row["agent_id"])
            parent_id = str(row.get("parent_agent_id") or "")
            if parent_id and parent_id in by_id:
                children.setdefault(parent_id, []).append(agent_id)
            else:
                roots.append(agent_id)

        ordered: list[tuple[str, int, list[str]]] = []
        visited: set[str] = set()

        def walk(agent_id: str, depth: int, path: list[str]) -> None:
            if agent_id in visited:
                return
            visited.add(agent_id)
            current_path = [*path, agent_id]
            ordered.append((agent_id, depth, current_path))
            for child_id in children.get(agent_id, []):
                walk(child_id, depth + 1, current_path)

        for root_id in roots:
            walk(root_id, 0, [])
        for row in domain_rows:
            walk(str(row["agent_id"]), 0, [])

        agents: list[dict[str, Any]] = []
        for agent_id, depth, path in ordered:
            row = by_id[agent_id]
            gateway_id = str(row.get("gateway_agent_id") or "")
            gateway_agent = (
                self.gateway.ledger.get_agent(gateway_id) if gateway_id else None
            )
            agents.append(
                {
                    **self._public_agent(row),
                    "depth": depth,
                    "path": path,
                    "children_count": len(children.get(agent_id, [])),
                    "gateway_agent_id": gateway_id or None,
                    "gateway_state": gateway_agent.state.value
                    if gateway_agent
                    else None,
                    "next_inspection_at": gateway_agent.next_inspection_at
                    if gateway_agent
                    else None,
                    "last_inspected_at": gateway_agent.last_inspected_at
                    if gateway_agent
                    else None,
                    "task": tasks.get(agent_id),
                    "pending_mailbox": mailbox.get(agent_id, 0),
                }
            )

        operations = [
            {
                "id": operation.id,
                "agent_id": operation.agent_id,
                "type": operation.type.value,
                "lane": operation.lane.value,
                "state": operation.state.value,
                "due_at": operation.due_at,
                "attempts": operation.attempts,
                "last_error": operation.last_error,
            }
            for operation in self.gateway.ledger.list_operations()
            if operation.state in {OperationState.PENDING, OperationState.CLAIMED}
        ]
        circuits = [
            {
                "scope": circuit.scope,
                "state": circuit.state.value,
                "retry_at": circuit.retry_at,
                "probe_failures": circuit.probe_failures,
                "half_open_successes": circuit.half_open_successes,
            }
            for circuit in self.gateway.ledger.list_circuits()
        ]
        title_by_id = {
            str(agent["agent_id"]): str(agent.get("title") or "") for agent in agents
        }
        automations = []
        for row in self.repository.automations():
            item = self._public_automation(row)
            source_id = str(row.get("source_agent_id") or "")
            target_id = str(row.get("target_agent_id") or "")
            item.update(
                source_title=title_by_id.get(source_id, ""),
                target_title=title_by_id.get(target_id, ""),
            )
            automations.append(item)

        request_summary = self.gateway.ledger.request_stats(since=time.time() - 86400)
        requests = self.gateway.ledger.recent_request_events(limit=100)
        active = sum(
            1
            for agent in agents
            if agent.get("parent_agent_id")
            and agent.get("status") not in _TERMINAL_DOMAIN_STATES
        )
        backend = self.gateway.backend
        reasoning = {
            "model": str(getattr(backend, "model_slug", "") or ""),
            "thinking_effort": str(getattr(backend, "thinking_effort", "") or ""),
            "require_high_reasoning": bool(
                getattr(backend, "require_high_reasoning", False)
            ),
            "applies_to": ["create", "continue", "wakeup", "after_completion"],
        }
        active_automations = [
            item
            for item in automations
            if item.get("status") not in {"delivered", "cancelled", "failed"}
        ]
        return {
            "generated_at": _now(),
            "capacity": {
                "active_children": active,
                "maximum_active_children": self.maximum_active_children,
            },
            "counts": {
                "total": len(agents),
                "active": active,
                "terminal": sum(
                    1
                    for agent in agents
                    if agent.get("status") in _TERMINAL_DOMAIN_STATES
                ),
                "queued_operations": len(operations),
                "active_automations": len(active_automations),
                "scheduled_wakeups": sum(
                    1 for item in active_automations if item.get("kind") == "wakeup"
                ),
                "completion_triggers": sum(
                    1
                    for item in active_automations
                    if item.get("kind") == "after_completion"
                ),
            },
            "next_eligible_at": self.gateway.next_eligible_at(),
            "next_wakeup_at": self.next_wakeup_at(),
            "reasoning": reasoning,
            "circuits": circuits,
            "operations": operations,
            "automations": automations,
            "agents": agents,
            "request_summary": request_summary,
            "requests": requests,
        }


__all__ = ["GatewayChatAgentCoordinator"]
