from __future__ import annotations

import asyncio
import random
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Sequence

from .backend import BackendAdapter
from .circuit_breaker import CircuitManager, JitterFunction, scope_for_lane
from .clock import Clock, SystemClock
from .config import GatewayConfig
from .errors import BackendError, MalformedBackendResponse, RateLimitError
from .ledger import SQLiteLedger
from .models import (
    AgentRecord,
    AgentState,
    CircuitState,
    Lane,
    MutationResult,
    OperationRecord,
    OperationState,
    OperationType,
    Priority,
    Reservation,
    ThreadSnapshot,
    TickResult,
)
from .rate_limiter import DurableRateLimiter
from .status_reducer import reduce_snapshot

RetryJitter = Callable[[float, str, int], float]


@dataclass(frozen=True)
class _DispatchResult:
    value: Any
    effective_type: OperationType
    effective_payload: Mapping[str, Any]
    source_operation_id: Optional[str] = None


class ChatGateway:
    """Durable single-request scheduler.

    `tick()` performs arbitrary local work and at most one adapter invocation.
    Every physical request is reserved transactionally before the network call,
    so concurrent gateway processes cannot consume the same operation or slot.
    """

    def __init__(
        self,
        ledger: SQLiteLedger,
        backend: BackendAdapter,
        config: Optional[GatewayConfig] = None,
        clock: Optional[Clock] = None,
        *,
        instance_id: Optional[str] = None,
        circuit_jitter: Optional[JitterFunction] = None,
        retry_jitter: Optional[RetryJitter] = None,
    ) -> None:
        self.ledger = ledger
        self.backend = backend
        self.config = config or GatewayConfig(database_path=ledger.path)
        self.config.validate()
        self.clock = clock or SystemClock()
        self.instance_id = instance_id or f"gateway-{uuid.uuid4().hex}"
        self.rate_limiter = DurableRateLimiter(ledger, self.config)
        self.circuits = CircuitManager(
            ledger,
            self.config.circuit,
            jitter=circuit_jitter,
        )
        self.retry_jitter = retry_jitter or self._default_retry_jitter

    # ------------------------------------------------------------------
    # Public enqueue API
    # ------------------------------------------------------------------
    def register_existing(
        self,
        *,
        project_id: str,
        conversation_id: str,
        title: str = "",
        completion_marker: Optional[str] = None,
        state: AgentState = AgentState.UNKNOWN,
        snapshot: Optional[ThreadSnapshot] = None,
        agent_id: Optional[str] = None,
    ) -> str:
        """Register an existing conversation without spending a request slot."""

        now = self.clock.now()
        selected_id = self.ledger.register_existing_agent(
            project_id=project_id,
            conversation_id=conversation_id,
            title=title.strip(),
            completion_marker=completion_marker or self.config.completion_marker,
            state=state,
            now=now,
            agent_id=agent_id,
        )
        if snapshot is not None:
            self.ledger.cache_put(
                f"snapshot:{selected_id}",
                _serialize_snapshot(snapshot),
                now=now,
                ttl=315_360_000.0,
            )
        return selected_id

    def latest_snapshot(self, agent_id: str) -> Optional[Mapping[str, Any]]:
        value = self.ledger.cache_get(f"snapshot:{agent_id}", self.clock.now())
        return value if isinstance(value, Mapping) else None

    def enqueue_create(
        self,
        *,
        project_id: str,
        prompt: str,
        title: str = "",
        completion_marker: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        due_at: Optional[float] = None,
    ) -> tuple[str, str]:
        project_id = project_id.strip()
        prompt = prompt.strip()
        if not prompt:
            raise ValueError("prompt is required")
        now = self.clock.now()
        key = idempotency_key or f"create:{uuid.uuid4().hex}"
        return self.ledger.create_agent_with_operation(
            project_id=project_id,
            title=title.strip(),
            completion_marker=completion_marker or self.config.completion_marker,
            payload={
                "project_id": project_id,
                "prompt": prompt,
                "title": title.strip(),
            },
            idempotency_key=key,
            due_at=now if due_at is None else due_at,
            priority=int(Priority.CREATE),
            max_attempts=self.config.retry.maximum_attempts,
        )

    def enqueue_inspect(
        self,
        *,
        agent_id: str,
        due_at: Optional[float] = None,
        completion_sensitive: bool = False,
    ) -> str:
        agent = self._require_agent(agent_id)
        if not agent.conversation_id:
            raise ValueError("agent has no conversation_id")
        if agent.state.terminal:
            raise ValueError("terminal agents cannot be inspected")
        now = self.clock.now()
        return self.ledger.enqueue_operation(
            operation_type=OperationType.INSPECT,
            lane=Lane.READ,
            priority=int(
                Priority.COMPLETION_INSPECTION
                if completion_sensitive
                else Priority.INSPECTION
            ),
            idempotency_key=f"inspect:{agent_id}:{uuid.uuid4().hex}",
            coalesce_key=f"canonical:{agent_id}",
            payload={"conversation_id": agent.conversation_id},
            due_at=now if due_at is None else due_at,
            max_attempts=self.config.retry.maximum_attempts,
            agent_id=agent_id,
        )

    def enqueue_continue(
        self,
        *,
        agent_id: str,
        message: str,
        idempotency_key: Optional[str] = None,
        due_at: Optional[float] = None,
    ) -> str:
        agent = self._require_agent(agent_id)
        message = message.strip()
        if not agent.conversation_id:
            raise ValueError("agent has no conversation_id")
        if agent.state.terminal:
            self.ledger.reopen_agent(agent_id, now=self.clock.now())
            agent = self._require_agent(agent_id)
        if not message:
            raise ValueError("message is required")
        now = self.clock.now()
        key = idempotency_key or f"continue:{agent_id}:{uuid.uuid4().hex}"
        return self.ledger.enqueue_operation(
            operation_type=OperationType.CONTINUE,
            lane=Lane.HEAVY,
            priority=int(Priority.CONTINUE),
            idempotency_key=key,
            payload={"conversation_id": agent.conversation_id, "message": message},
            due_at=now if due_at is None else due_at,
            max_attempts=self.config.retry.maximum_attempts,
            agent_id=agent_id,
        )

    def enqueue_cancel(
        self,
        *,
        agent_id: str,
        idempotency_key: Optional[str] = None,
        due_at: Optional[float] = None,
    ) -> str:
        agent = self._require_agent(agent_id)
        if not agent.conversation_id:
            raise ValueError("agent has no conversation_id")
        now = self.clock.now()
        return self.ledger.enqueue_operation(
            operation_type=OperationType.CANCEL,
            lane=Lane.CLEANUP,
            priority=int(Priority.CANCEL),
            idempotency_key=idempotency_key or f"cancel:{agent_id}:{uuid.uuid4().hex}",
            payload={"conversation_id": agent.conversation_id},
            due_at=now if due_at is None else due_at,
            max_attempts=self.config.retry.maximum_attempts,
            agent_id=agent_id,
        )

    def enqueue_delete(
        self,
        *,
        agent_id: str,
        idempotency_key: Optional[str] = None,
        due_at: Optional[float] = None,
    ) -> str:
        agent = self._require_agent(agent_id)
        if not agent.conversation_id:
            raise ValueError("agent has no conversation_id")
        now = self.clock.now()
        return self.ledger.enqueue_operation(
            operation_type=OperationType.DELETE,
            lane=Lane.CLEANUP,
            priority=int(Priority.CANCEL),
            idempotency_key=idempotency_key or f"delete:{agent_id}:{uuid.uuid4().hex}",
            payload={"conversation_id": agent.conversation_id},
            due_at=now if due_at is None else due_at,
            max_attempts=self.config.retry.maximum_attempts,
            agent_id=agent_id,
        )

    def enqueue_project_list(
        self,
        *,
        project_id: str,
        idempotency_key: Optional[str] = None,
        due_at: Optional[float] = None,
    ) -> str:
        project_id = project_id.strip()
        if not project_id:
            raise ValueError("project_id is required")
        now = self.clock.now()
        return self.ledger.enqueue_operation(
            operation_type=OperationType.PROJECT_LIST,
            lane=Lane.METADATA,
            priority=int(Priority.METADATA),
            idempotency_key=idempotency_key
            or f"project-list:{project_id}:{uuid.uuid4().hex}",
            coalesce_key=f"project-list:{project_id}",
            payload={"project_id": project_id},
            due_at=now if due_at is None else due_at,
            max_attempts=self.config.retry.maximum_attempts,
        )

    # ------------------------------------------------------------------
    # Scheduler tick
    # ------------------------------------------------------------------
    async def tick(self) -> TickResult:
        now = self.clock.now()
        reservation, next_at, circuit_state = self._reserve(now)
        if reservation is None:
            return TickResult(
                status="idle",
                selected_operation=None,
                operation_type=None,
                lane=None,
                physical_requests=0,
                circuit_state=circuit_state,
                next_eligible_at=next_at,
            )

        started_at = self.clock.now()
        try:
            dispatch = await self._dispatch(reservation.operation)
        except BaseException as error:
            finished_at = self.clock.now()
            return self._finalize_error(
                reservation,
                error,
                started_at=started_at,
                finished_at=finished_at,
            )

        finished_at = self.clock.now()
        try:
            self._finalize_success(
                reservation,
                dispatch,
                started_at=started_at,
                finished_at=finished_at,
            )
        except BaseException as error:
            # The physical request succeeded but local finalization failed. The
            # claim lease and idempotency key make restart recovery conservative.
            return TickResult(
                status="local-finalization-error",
                selected_operation=reservation.operation.id,
                operation_type=reservation.operation.type.value,
                lane=reservation.operation.lane.value,
                physical_requests=1,
                circuit_state=self._circuit_state(reservation.circuit_scope),
                next_eligible_at=reservation.operation.claim_expires_at,
                agent_id=reservation.operation.agent_id,
                outcome="backend-success-local-error",
                error=f"{type(error).__name__}: {error}",
            )

        return TickResult(
            status="executed",
            selected_operation=reservation.operation.id,
            operation_type=reservation.operation.type.value,
            lane=reservation.operation.lane.value,
            physical_requests=1,
            circuit_state=self._circuit_state(reservation.circuit_scope),
            next_eligible_at=None,
            agent_id=reservation.operation.agent_id,
            outcome="success",
        )

    def tick_sync(self) -> TickResult:
        return asyncio.run(self.tick())

    def next_eligible_at(self) -> Optional[float]:
        """Return the earliest durable request slot without claiming work."""

        now = self.clock.now()
        pending = self.ledger.list_operations(state=OperationState.PENDING)
        if not pending:
            return None
        circuits = {record.scope: record for record in self.ledger.list_circuits()}
        candidates: list[float] = []
        with self.ledger.transaction() as connection:
            for operation in pending:
                eligible = max(now, operation.due_at)
                rate = self.rate_limiter.check(connection, lane=operation.lane, now=now)
                if rate.next_at is not None:
                    eligible = max(eligible, rate.next_at)
                circuit = circuits.get(scope_for_lane(operation.lane))
                if circuit is not None and circuit.retry_at is not None:
                    if circuit.state in {
                        CircuitState.OPEN,
                        CircuitState.HALF_OPEN,
                        CircuitState.PACED,
                    }:
                        eligible = max(eligible, circuit.retry_at)
                candidates.append(eligible)
        return min(candidates) if candidates else None

    def _reserve(
        self, now: float
    ) -> tuple[Optional[Reservation], Optional[float], str]:
        blocked_times: list[float] = []
        observed_state = CircuitState.CLOSED.value
        with self.ledger.transaction(immediate=True) as connection:
            self.ledger.recover_expired_claims(connection, now)
            candidates = self.ledger.due_operations(connection, now)
            for operation in candidates:
                if operation.type is OperationType.PROJECT_LIST:
                    project_id = str(operation.payload["project_id"])
                    cached = self.ledger.cache_get_tx(
                        connection, f"project-list:{project_id}", now
                    )
                    if cached is not None:
                        self.ledger.succeed_operation(
                            connection,
                            operation_id=operation.id,
                            now=now,
                            result={"cached": True, "count": len(cached)},
                        )
                        continue
                if (
                    operation.type is OperationType.CREATE
                    and self.ledger.capacity_agent_count(connection)
                    >= self.config.maximum_active_agents
                ):
                    continue

                rate = self.rate_limiter.check(
                    connection,
                    lane=operation.lane,
                    now=now,
                )
                if not rate.allowed:
                    if rate.next_at is not None:
                        blocked_times.append(rate.next_at)
                    continue

                allowed, circuit_next, circuit = self.circuits.admit(
                    connection,
                    lane=operation.lane,
                    operation_type=operation.type,
                    now=now,
                )
                observed_state = circuit.state.value
                if not allowed:
                    if circuit_next is not None:
                        blocked_times.append(circuit_next)
                    continue

                claim_token = f"{self.instance_id}:{uuid.uuid4().hex}"
                if not self.ledger.claim_operation(
                    connection,
                    operation_id=operation.id,
                    claim_token=claim_token,
                    claim_expires_at=now + self.config.retry.claim_ttl,
                    now=now,
                ):
                    continue
                event_id = self.ledger.reserve_request_event(
                    connection,
                    operation_id=operation.id,
                    lane=operation.lane,
                    circuit_scope=scope_for_lane(operation.lane),
                    now=now,
                )
                claimed = self.ledger.get_operation_tx(connection, operation.id)
                assert claimed is not None
                return (
                    Reservation(
                        operation=claimed,
                        request_event_id=event_id,
                        claim_token=claim_token,
                        circuit_scope=scope_for_lane(operation.lane),
                    ),
                    None,
                    circuit.state.value,
                )

            future_due = connection.execute(
                "SELECT MIN(due_at) FROM operations WHERE state='PENDING' AND due_at>?",
                (now,),
            ).fetchone()[0]
            if future_due is not None:
                blocked_times.append(float(future_due))
        return None, min(blocked_times) if blocked_times else None, observed_state

    # ------------------------------------------------------------------
    # Backend dispatch: exactly one adapter invocation
    # ------------------------------------------------------------------
    async def _dispatch(self, operation: OperationRecord) -> _DispatchResult:
        payload = dict(operation.payload)
        if operation.type is OperationType.RECOVERY_PROBE:
            source_operation_id = payload.get("source_operation_id")
            probe_action = str(payload.get("probe_action", "get_thread"))
            if probe_action == "get_thread":
                value = await self.backend.get_thread(
                    conversation_id=str(payload["conversation_id"])
                )
                return _DispatchResult(
                    value,
                    OperationType.INSPECT,
                    {"conversation_id": payload["conversation_id"]},
                    None,
                )
            if probe_action == "source":
                effective_type = OperationType(str(payload["source_type"]))
                source_payload = _mapping(payload.get("source_payload"))
                value = await self._invoke_adapter(
                    effective_type,
                    source_payload,
                    str(payload["source_idempotency_key"]),
                )
                return _DispatchResult(
                    value,
                    effective_type,
                    source_payload,
                    str(source_operation_id) if source_operation_id else None,
                )
            raise MalformedBackendResponse(
                f"unknown recovery probe action: {probe_action}"
            )

        value = await self._invoke_adapter(
            operation.type,
            payload,
            operation.idempotency_key,
        )
        return _DispatchResult(value, operation.type, payload)

    async def _invoke_adapter(
        self,
        operation_type: OperationType,
        payload: Mapping[str, Any],
        idempotency_key: str,
    ) -> Any:
        if operation_type is OperationType.CREATE:
            return await self.backend.create_thread(
                project_id=str(payload["project_id"]),
                prompt=str(payload["prompt"]),
                title=str(payload.get("title", "")),
                idempotency_key=idempotency_key,
            )
        if operation_type in {
            OperationType.VERIFY_CREATION,
            OperationType.INSPECT,
            OperationType.VERIFY_CONTINUE,
        }:
            return await self.backend.get_thread(
                conversation_id=str(payload["conversation_id"])
            )
        if operation_type is OperationType.CONTINUE:
            return await self.backend.continue_thread(
                conversation_id=str(payload["conversation_id"]),
                message=str(payload["message"]),
                idempotency_key=idempotency_key,
            )
        if operation_type is OperationType.CANCEL:
            return await self.backend.cancel_thread(
                conversation_id=str(payload["conversation_id"])
            )
        if operation_type is OperationType.DELETE:
            return await self.backend.delete_thread(
                conversation_id=str(payload["conversation_id"])
            )
        if operation_type is OperationType.PROJECT_LIST:
            return await self.backend.list_project_threads(
                project_id=str(payload["project_id"])
            )
        raise MalformedBackendResponse(
            f"no adapter dispatch exists for {operation_type.value}"
        )

    # ------------------------------------------------------------------
    # Success finalization
    # ------------------------------------------------------------------
    def _finalize_success(
        self,
        reservation: Reservation,
        dispatch: _DispatchResult,
        *,
        started_at: float,
        finished_at: float,
    ) -> None:
        with self.ledger.transaction(immediate=True) as connection:
            operation = self.ledger.get_operation_tx(
                connection, reservation.operation.id
            )
            if operation is None or operation.claim_token != reservation.claim_token:
                raise RuntimeError("operation claim was lost before finalization")

            self.ledger.finalize_request_event(
                connection,
                event_id=reservation.request_event_id,
                finished_at=finished_at,
                outcome="SUCCESS",
                details={"duration": max(0.0, finished_at - started_at)},
            )
            circuit = self.circuits.record_success(
                connection,
                scope=reservation.circuit_scope,
                operation_type=operation.type,
                now=finished_at,
            )

            if operation.type is OperationType.RECOVERY_PROBE:
                self.ledger.succeed_operation(
                    connection,
                    operation_id=operation.id,
                    now=finished_at,
                    result={"probe": "success"},
                )
                if dispatch.source_operation_id:
                    source = self.ledger.get_operation_tx(
                        connection, dispatch.source_operation_id
                    )
                    if source is not None and source.state is OperationState.PENDING:
                        self._apply_success(
                            connection,
                            source,
                            dispatch.effective_type,
                            dispatch.effective_payload,
                            dispatch.value,
                            finished_at,
                        )
                if circuit.state is CircuitState.HALF_OPEN:
                    self._schedule_next_recovery_probe(
                        connection,
                        scope=reservation.circuit_scope,
                        previous_probe=operation,
                        due_at=finished_at + self.config.circuit.half_open_spacing,
                        now=finished_at,
                        success_number=circuit.half_open_successes + 1,
                    )
                return

            self._apply_success(
                connection,
                operation,
                operation.type,
                operation.payload,
                dispatch.value,
                finished_at,
            )

    def _apply_success(
        self,
        connection: sqlite3.Connection,
        operation: OperationRecord,
        effective_type: OperationType,
        payload: Mapping[str, Any],
        value: Any,
        now: float,
    ) -> None:
        if effective_type is OperationType.CREATE:
            result = _require_mutation(value, "create")
            if not result.accepted or not result.conversation_id:
                raise MalformedBackendResponse(
                    "create_thread did not return an accepted conversation_id"
                )
            assert operation.agent_id is not None
            self.ledger.succeed_operation(
                connection,
                operation_id=operation.id,
                now=now,
                result={
                    "accepted": True,
                    "conversation_id": result.conversation_id,
                    "message_id": result.message_id,
                },
            )
            self.ledger.update_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                conversation_id=result.conversation_id,
                state=AgentState.CREATING,
                last_error=None,
            )
            self.ledger.enqueue_operation_tx(
                connection,
                operation_type=OperationType.VERIFY_CREATION,
                lane=Lane.READ,
                priority=int(Priority.RECONCILIATION),
                idempotency_key=f"verify-create:{operation.id}:1",
                coalesce_key=f"canonical:{operation.agent_id}",
                payload={
                    "conversation_id": result.conversation_id,
                    "verification_attempt": 1,
                },
                due_at=now + self.config.polling.first_creation_verification,
                max_attempts=self.config.retry.maximum_attempts,
                agent_id=operation.agent_id,
            )
            return

        if effective_type is OperationType.CONTINUE:
            result = _require_mutation(value, "continue")
            if not result.accepted:
                raise MalformedBackendResponse("continue_thread was not accepted")
            assert operation.agent_id is not None
            self.ledger.succeed_operation(
                connection,
                operation_id=operation.id,
                now=now,
                result={"accepted": True, "message_id": result.message_id},
            )
            self.ledger.update_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                state=AgentState.UNKNOWN,
                last_error=None,
            )
            self.ledger.enqueue_operation_tx(
                connection,
                operation_type=OperationType.VERIFY_CONTINUE,
                lane=Lane.READ,
                priority=int(Priority.RECONCILIATION),
                idempotency_key=f"verify-continue:{operation.id}:1",
                coalesce_key=f"canonical:{operation.agent_id}",
                payload={
                    "conversation_id": payload["conversation_id"],
                    "expected_message_id": result.message_id,
                    "verification_attempt": 1,
                },
                due_at=now + self.config.polling.continue_verification,
                max_attempts=self.config.retry.maximum_attempts,
                agent_id=operation.agent_id,
            )
            return

        if effective_type in {
            OperationType.VERIFY_CREATION,
            OperationType.VERIFY_CONTINUE,
            OperationType.INSPECT,
        }:
            if not isinstance(value, ThreadSnapshot):
                raise MalformedBackendResponse(
                    f"{effective_type.value} returned a non-ThreadSnapshot"
                )
            self._apply_snapshot(connection, operation, value, payload, now)
            return

        if effective_type is OperationType.CANCEL:
            result = _require_mutation(value, "cancel")
            if not result.accepted:
                raise MalformedBackendResponse("cancel_thread was not accepted")
            assert operation.agent_id is not None
            self.ledger.succeed_operation(
                connection,
                operation_id=operation.id,
                now=now,
                result={"accepted": True},
            )
            self.ledger.update_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                state=AgentState.CANCELLED,
                completed_at=now,
                last_error=None,
            )
            self.ledger.cancel_pending_operations_for_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                except_types=(OperationType.DELETE,),
            )
            return

        if effective_type is OperationType.DELETE:
            result = _require_mutation(value, "delete")
            if not result.accepted:
                raise MalformedBackendResponse("delete_thread was not accepted")
            assert operation.agent_id is not None
            self.ledger.succeed_operation(
                connection,
                operation_id=operation.id,
                now=now,
                result={"accepted": True},
            )
            self.ledger.update_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                state=AgentState.CANCELLED,
                completed_at=now,
                deleted_at=now,
                last_error=None,
            )
            self.ledger.cancel_pending_operations_for_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
            )
            return

        if effective_type is OperationType.PROJECT_LIST:
            if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
                raise MalformedBackendResponse("project list result was not a sequence")
            if not all(isinstance(item, ThreadSnapshot) for item in value):
                raise MalformedBackendResponse(
                    "project list contained a non-ThreadSnapshot item"
                )
            serialized = [
                {
                    "conversation_id": item.conversation_id,
                    "title": item.title,
                    "found": item.found,
                    "running": item.running,
                }
                for item in value
            ]
            self.ledger.succeed_operation(
                connection,
                operation_id=operation.id,
                now=now,
                result={"count": len(serialized)},
            )
            project_id = str(payload["project_id"])
            self.ledger.cache_put_tx(
                connection,
                f"project-list:{project_id}",
                serialized,
                now=now,
                ttl=self.config.metadata.cache_ttl,
            )
            return

        raise MalformedBackendResponse(
            f"no success finalizer exists for {effective_type.value}"
        )

    def _apply_snapshot(
        self,
        connection: sqlite3.Connection,
        operation: OperationRecord,
        snapshot: ThreadSnapshot,
        payload: Mapping[str, Any],
        now: float,
    ) -> None:
        if operation.agent_id is None:
            raise MalformedBackendResponse("canonical read operation has no agent")
        self.ledger.cache_put_tx(
            connection,
            f"snapshot:{operation.agent_id}",
            _serialize_snapshot(snapshot),
            now=now,
            ttl=315_360_000.0,
        )
        agent = self.ledger.get_agent_tx(connection, operation.agent_id)
        if agent is None:
            raise MalformedBackendResponse("canonical read references a missing agent")

        if operation.type is OperationType.VERIFY_CREATION and not snapshot.found:
            attempt = int(payload.get("verification_attempt", 1))
            if attempt < 2:
                updated = dict(payload)
                updated["verification_attempt"] = 2
                self.ledger.requeue_operation(
                    connection,
                    operation_id=operation.id,
                    due_at=now + self.config.polling.second_creation_verification,
                    now=now,
                    error="conversation not canonically visible after first verification",
                    increment_attempts=False,
                    payload=updated,
                )
                return
            self.ledger.succeed_operation(
                connection,
                operation_id=operation.id,
                now=now,
                result={"found": False, "verification_attempts": 2},
            )
            self.ledger.update_agent(
                connection,
                agent_id=agent.id,
                now=now,
                state=AgentState.UNKNOWN,
                last_inspected_at=now,
                next_inspection_at=now + self.config.polling.unchanged_interval,
                last_error="conversation not canonically visible after two verifications",
            )
            self._enqueue_inspect_tx(
                connection,
                agent=agent,
                conversation_id=str(payload["conversation_id"]),
                due_at=now + self.config.polling.unchanged_interval,
                now=now,
            )
            return

        expected_message_id = (
            str(payload["expected_message_id"])
            if payload.get("expected_message_id")
            else None
        )
        reduction = reduce_snapshot(
            snapshot,
            completion_marker=agent.completion_marker,
            expected_message_id=expected_message_id,
        )

        if (
            operation.type is OperationType.VERIFY_CONTINUE
            and expected_message_id
            and reduction.expected_message_present is False
        ):
            attempt = int(payload.get("verification_attempt", 1))
            if attempt < 2:
                updated = dict(payload)
                updated["verification_attempt"] = 2
                self.ledger.requeue_operation(
                    connection,
                    operation_id=operation.id,
                    due_at=now + self.config.polling.unchanged_once_interval,
                    now=now,
                    error="continued message not visible after first verification",
                    increment_attempts=False,
                    payload=updated,
                )
                return

        changed = reduction.snapshot_hash != agent.snapshot_hash
        unchanged_count = 0 if changed else agent.unchanged_count + 1
        self.ledger.succeed_operation(
            connection,
            operation_id=operation.id,
            now=now,
            result={
                "state": reduction.state.value,
                "terminal": reduction.terminal,
                "reason": reduction.reason,
                "expected_message_present": reduction.expected_message_present,
            },
        )

        if reduction.terminal:
            self.ledger.update_agent(
                connection,
                agent_id=agent.id,
                now=now,
                state=reduction.state,
                snapshot_hash=reduction.snapshot_hash,
                unchanged_count=unchanged_count,
                last_inspected_at=now,
                next_inspection_at=None,
                completed_at=now,
                last_error=None,
            )
            self.ledger.cancel_pending_operations_for_agent(
                connection,
                agent_id=agent.id,
                now=now,
                except_types=(OperationType.DELETE,),
            )
            return

        interval = self.config.polling.active_interval
        if not changed and unchanged_count == 1:
            interval = self.config.polling.unchanged_once_interval
        elif not changed and unchanged_count >= 2:
            interval = self.config.polling.unchanged_interval
        next_at = now + interval
        self.ledger.update_agent(
            connection,
            agent_id=agent.id,
            now=now,
            state=reduction.state,
            snapshot_hash=reduction.snapshot_hash,
            unchanged_count=unchanged_count,
            last_inspected_at=now,
            next_inspection_at=next_at,
            last_error=None,
        )
        self._enqueue_inspect_tx(
            connection,
            agent=agent,
            conversation_id=snapshot.conversation_id,
            due_at=next_at,
            now=now,
        )

    def _enqueue_inspect_tx(
        self,
        connection: sqlite3.Connection,
        *,
        agent: AgentRecord,
        conversation_id: str,
        due_at: float,
        now: float,
    ) -> str:
        return self.ledger.enqueue_operation_tx(
            connection,
            operation_type=OperationType.INSPECT,
            lane=Lane.READ,
            priority=int(Priority.INSPECTION),
            idempotency_key=f"inspect:{agent.id}:{due_at:.6f}:{uuid.uuid4().hex}",
            coalesce_key=f"canonical:{agent.id}",
            payload={"conversation_id": conversation_id},
            due_at=due_at,
            max_attempts=self.config.retry.maximum_attempts,
            agent_id=agent.id,
        )

    # ------------------------------------------------------------------
    # Error and circuit handling
    # ------------------------------------------------------------------
    def _finalize_error(
        self,
        reservation: Reservation,
        error: BaseException,
        *,
        started_at: float,
        finished_at: float,
    ) -> TickResult:
        backend_error = _normalize_error(error)
        rate_limited = isinstance(backend_error, RateLimitError) or (
            backend_error.status_code == 429
        )
        next_at: Optional[float] = None
        with self.ledger.transaction(immediate=True) as connection:
            operation = self.ledger.get_operation_tx(
                connection, reservation.operation.id
            )
            if operation is None or operation.claim_token != reservation.claim_token:
                raise RuntimeError("operation claim was lost before error finalization")

            self.ledger.finalize_request_event(
                connection,
                event_id=reservation.request_event_id,
                finished_at=finished_at,
                outcome="RATE_LIMITED" if rate_limited else "ERROR",
                status_code=backend_error.status_code,
                error_class=type(backend_error).__name__,
                is_rate_limit=rate_limited,
                details={"message": str(backend_error)[:500]},
            )

            if rate_limited:
                circuit = self.circuits.record_rate_limit(
                    connection,
                    scope=reservation.circuit_scope,
                    now=finished_at,
                )
                next_at = circuit.retry_at
                if operation.type is OperationType.RECOVERY_PROBE:
                    self.ledger.fail_operation(
                        connection,
                        operation_id=operation.id,
                        now=finished_at,
                        error=str(backend_error),
                    )
                else:
                    self.ledger.requeue_operation(
                        connection,
                        operation_id=operation.id,
                        due_at=next_at or finished_at,
                        now=finished_at,
                        error=str(backend_error),
                        increment_attempts=False,
                    )
                self._ensure_recovery_probe(
                    connection,
                    scope=reservation.circuit_scope,
                    failed_operation=operation,
                    due_at=next_at or finished_at,
                    now=finished_at,
                    generation=circuit.opened_at or finished_at,
                )
            elif operation.type is OperationType.RECOVERY_PROBE:
                circuit = self.circuits.record_probe_error(
                    connection,
                    scope=reservation.circuit_scope,
                    now=finished_at,
                )
                next_at = circuit.retry_at
                self.ledger.fail_operation(
                    connection,
                    operation_id=operation.id,
                    now=finished_at,
                    error=str(backend_error),
                )
                self._ensure_recovery_probe(
                    connection,
                    scope=reservation.circuit_scope,
                    failed_operation=operation,
                    due_at=next_at or finished_at,
                    now=finished_at,
                    generation=circuit.opened_at or finished_at,
                )
            elif (
                backend_error.permanent
                or operation.attempts + 1 >= operation.max_attempts
            ):
                self.ledger.fail_operation(
                    connection,
                    operation_id=operation.id,
                    now=finished_at,
                    error=str(backend_error),
                )
                self._terminalize_agent_for_operation(
                    connection, operation, backend_error, finished_at
                )
            else:
                attempt_number = operation.attempts + 1
                base = min(
                    self.config.retry.maximum_delay,
                    self.config.retry.base_delay * (2 ** (attempt_number - 1)),
                )
                delay = max(
                    0.0,
                    self.retry_jitter(base, operation.id, attempt_number),
                )
                next_at = finished_at + delay
                self.ledger.requeue_operation(
                    connection,
                    operation_id=operation.id,
                    due_at=next_at,
                    now=finished_at,
                    error=str(backend_error),
                    increment_attempts=True,
                )

        return TickResult(
            status="executed",
            selected_operation=reservation.operation.id,
            operation_type=reservation.operation.type.value,
            lane=reservation.operation.lane.value,
            physical_requests=1,
            circuit_state=self._circuit_state(reservation.circuit_scope),
            next_eligible_at=next_at,
            agent_id=reservation.operation.agent_id,
            outcome="rate-limited" if rate_limited else "error",
            error=f"{type(backend_error).__name__}: {backend_error}",
        )

    def _terminalize_agent_for_operation(
        self,
        connection: sqlite3.Connection,
        operation: OperationRecord,
        error: BackendError,
        now: float,
    ) -> None:
        if operation.agent_id is None:
            return
        if operation.type in {
            OperationType.CREATE,
            OperationType.VERIFY_CREATION,
            OperationType.INSPECT,
            OperationType.CONTINUE,
            OperationType.VERIFY_CONTINUE,
        }:
            self.ledger.update_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                state=AgentState.FAILED,
                completed_at=now,
                last_error=str(error),
            )
            self.ledger.cancel_pending_operations_for_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                except_types=(OperationType.DELETE,),
            )
        else:
            self.ledger.update_agent(
                connection,
                agent_id=operation.agent_id,
                now=now,
                last_error=str(error),
            )

    def _ensure_recovery_probe(
        self,
        connection: sqlite3.Connection,
        *,
        scope: str,
        failed_operation: OperationRecord,
        due_at: float,
        now: float,
        generation: float,
    ) -> str:
        payload = self._probe_payload(connection, failed_operation, scope)
        lane = _probe_lane(payload, failed_operation.lane)
        return self.ledger.enqueue_operation_tx(
            connection,
            operation_type=OperationType.RECOVERY_PROBE,
            lane=lane,
            priority=int(Priority.RECOVERY_PROBE),
            idempotency_key=f"recovery:{scope}:{generation:.6f}:0",
            coalesce_key=f"recovery:{scope}",
            payload=payload,
            due_at=due_at,
            max_attempts=self.config.retry.maximum_attempts,
            agent_id=failed_operation.agent_id,
        )

    def _schedule_next_recovery_probe(
        self,
        connection: sqlite3.Connection,
        *,
        scope: str,
        previous_probe: OperationRecord,
        due_at: float,
        now: float,
        success_number: int,
    ) -> str:
        payload = dict(previous_probe.payload)
        conversation_id = (
            self.ledger.first_conversation_id_tx(connection)
            if scope == "conversation"
            else None
        )
        if conversation_id:
            payload = {
                "scope": scope,
                "probe_action": "get_thread",
                "conversation_id": conversation_id,
                "source_operation_id": payload.get("source_operation_id"),
            }
        generation = payload.get("generation", previous_probe.created_at)
        payload["generation"] = generation
        return self.ledger.enqueue_operation_tx(
            connection,
            operation_type=OperationType.RECOVERY_PROBE,
            lane=_probe_lane(payload, previous_probe.lane),
            priority=int(Priority.RECOVERY_PROBE),
            idempotency_key=(
                f"recovery:{scope}:{float(generation):.6f}:{success_number}"
            ),
            coalesce_key=f"recovery:{scope}",
            payload=payload,
            due_at=due_at,
            max_attempts=self.config.retry.maximum_attempts,
            agent_id=previous_probe.agent_id,
        )

    def _probe_payload(
        self,
        connection: sqlite3.Connection,
        operation: OperationRecord,
        scope: str,
    ) -> dict[str, Any]:
        payload = dict(operation.payload)
        if operation.type is OperationType.RECOVERY_PROBE:
            payload["scope"] = scope
            payload.setdefault("generation", operation.created_at)
            return payload
        if scope != "conversation":
            return {
                "scope": scope,
                "probe_action": "source",
                "source_operation_id": operation.id,
                "source_type": operation.type.value,
                "source_payload": dict(operation.payload),
                "source_idempotency_key": operation.idempotency_key,
                "generation": self.clock.now(),
            }
        conversation_id = payload.get("conversation_id")
        if not conversation_id and operation.agent_id:
            agent = self.ledger.get_agent_tx(connection, operation.agent_id)
            conversation_id = agent.conversation_id if agent else None
        if not conversation_id:
            conversation_id = self.ledger.first_conversation_id_tx(connection)
        if conversation_id:
            return {
                "scope": scope,
                "probe_action": "get_thread",
                "conversation_id": str(conversation_id),
                "source_operation_id": operation.id,
                "generation": self.clock.now(),
            }
        return {
            "scope": scope,
            "probe_action": "source",
            "source_operation_id": operation.id,
            "source_type": operation.type.value,
            "source_payload": dict(operation.payload),
            "source_idempotency_key": operation.idempotency_key,
            "generation": self.clock.now(),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _require_agent(self, agent_id: str) -> AgentRecord:
        agent = self.ledger.get_agent(agent_id)
        if agent is None:
            raise KeyError(f"unknown agent: {agent_id}")
        return agent

    def _circuit_state(self, scope: str) -> str:
        with self.ledger.transaction() as connection:
            return self.ledger.get_circuit(
                connection, scope=scope, now=self.clock.now()
            ).state.value

    @staticmethod
    def _default_retry_jitter(delay: float, _operation_id: str, _attempt: int) -> float:
        return delay * (1.0 + random.uniform(-0.10, 0.10))


def _serialize_snapshot(snapshot: ThreadSnapshot) -> dict[str, Any]:
    return {
        "conversation_id": snapshot.conversation_id,
        "found": snapshot.found,
        "running": snapshot.running,
        "title": snapshot.title,
        "current_node": snapshot.current_node,
        "turns": [
            {
                "message_id": turn.message_id,
                "role": turn.role,
                "status": turn.status,
                "text": turn.text,
                "end_turn": turn.end_turn,
                "created_at": turn.created_at,
            }
            for turn in snapshot.turns
        ],
    }


def _normalize_error(error: BaseException) -> BackendError:
    if isinstance(error, BackendError):
        if error.status_code == 429 and not isinstance(error, RateLimitError):
            return RateLimitError(str(error))
        return error
    return BackendError(f"{type(error).__name__}: {error}", transient=True)


def _require_mutation(value: Any, operation: str) -> MutationResult:
    if not isinstance(value, MutationResult):
        raise MalformedBackendResponse(
            f"{operation} returned {type(value).__name__}, expected MutationResult"
        )
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _probe_lane(payload: Mapping[str, Any], fallback: Lane) -> Lane:
    scope = payload.get("scope")
    if scope == Lane.METADATA.value:
        return Lane.METADATA
    if scope == Lane.CLEANUP.value:
        return Lane.CLEANUP
    if payload.get("probe_action") == "get_thread":
        return Lane.READ
    source_type = payload.get("source_type")
    if source_type in {OperationType.CREATE.value, OperationType.CONTINUE.value}:
        return Lane.HEAVY
    return fallback


# Backward-compatible public name for the initial draft.
ChatScheduler = ChatGateway
