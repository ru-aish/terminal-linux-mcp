from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from chat_agent_orchestrator import CoordinatorConfig
from chat_gateway import (
    ChatGateway,
    CircuitConfig,
    CircuitState,
    FakeBackend,
    FakeClock,
    GatewayConfig,
    InfrastructureError,
    LaneLimit,
    AgentState,
    OperationState,
    OperationType,
    PollingConfig,
    RetryConfig,
    SQLiteLedger,
    ThreadSnapshot,
    TurnSnapshot,
    WindowLimit,
)
from gateway_agent_orchestrator import GatewayChatAgentCoordinator


def parent_snapshot() -> dict:
    return {
        "found": True,
        "canonical": True,
        "state_verified": True,
        "conversation_id": "parent-chat",
        "project_id": "g-p-project",
        "title": "Parent",
        "current_node": "parent-assistant",
        "running": False,
        "active_stream": False,
        "turns": [
            {
                "key": "parent-user",
                "role": "user",
                "text": "root",
                "status": "finished_successfully",
                "end_turn": None,
            },
            {
                "key": "parent-assistant",
                "role": "assistant",
                "text": "ready",
                "status": "finished_successfully",
                "end_turn": True,
            },
        ],
    }


class RegistrationRuntime:
    def __init__(self) -> None:
        self.get_calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get_thread(self, conversation_id: str):
        assert conversation_id == "parent-chat"
        self.get_calls += 1
        return parent_snapshot()

    async def list_projects(self, **_kwargs):
        return {"items": []}

    async def get_project(self, project_id: str):
        return {"id": project_id, "permissions": {"can_read": True, "can_write": True}}

    async def list_project_threads(self, project_id: str, **_kwargs):
        return {"items": [], "project_id": project_id}


@asynccontextmanager
async def runtime_factory(runtime: RegistrationRuntime):
    yield runtime


def fast_config(path: Path) -> GatewayConfig:
    wide = LaneLimit(0.0, (WindowLimit(100, 60.0),))
    return GatewayConfig(
        database_path=str(path),
        maximum_active_agents=64,
        global_limit=wide,
        conversation_read=wide,
        heavy=wide,
        metadata=LaneLimit(0.0, (WindowLimit(100, 60.0),), cache_ttl=1.0),
        cleanup=wide,
        polling=PollingConfig(
            first_creation_verification=1.0,
            second_creation_verification=1.0,
            continue_verification=1.0,
            active_interval=1.0,
            unchanged_once_interval=1.0,
            unchanged_interval=1.0,
        ),
        circuit=CircuitConfig(
            initial_cooldown=1.0,
            failed_probe_delays=(2.0, 4.0),
            half_open_successes=1,
            half_open_spacing=1.0,
            paced_interval=1.0,
            paced_duration=2.0,
            jitter_fraction=0.0,
        ),
        retry=RetryConfig(
            base_delay=1.0,
            maximum_delay=4.0,
            maximum_attempts=3,
            claim_ttl=30.0,
        ),
    )


def make_coordinator(tmp_path: Path):
    clock = FakeClock(1000.0)
    backend = FakeBackend()
    gateway_db = tmp_path / "gateway.sqlite"
    gateway = ChatGateway(
        SQLiteLedger(gateway_db),
        backend,
        fast_config(gateway_db),
        clock,
        circuit_jitter=lambda delay, _scope, _index: delay,
        retry_jitter=lambda delay, _operation, _attempt: delay,
    )
    runtime = RegistrationRuntime()
    coordinator = GatewayChatAgentCoordinator(
        CoordinatorConfig(
            tmp_path / "orchestration.sqlite",
            initial_check_seconds=1,
            progress_check_seconds=1,
            idle_check_seconds=1,
            heartbeat_seconds=1,
        ),
        lambda: runtime_factory(runtime),
        gateway,
        maximum_active_children=5,
    )
    return coordinator, gateway, backend, clock, runtime


def run(coro):
    return asyncio.run(coro)


def test_spawn_is_queued_and_every_sync_uses_at_most_one_request(tmp_path):
    coordinator, _gateway, backend, clock, runtime = make_coordinator(tmp_path)
    parent = run(
        coordinator.register_parent("parent-chat", working_directory=str(tmp_path))
    )
    child = run(
        coordinator.spawn(
            parent["agent_id"],
            "Inspect the code and report.",
            project_policy="inherit_parent",
            idempotency_key="child-one",
        )
    )

    assert runtime.get_calls == 1
    assert backend.calls == []
    assert child["task"]["status"] == "creating_thread"

    create_tick = run(coordinator.sync_once())
    assert create_tick["physical_requests"] == 1
    assert create_tick["operation_type"] == "CREATE"
    assert len(backend.calls) == 1

    blocked_tick = run(coordinator.sync_once())
    assert blocked_tick["physical_requests"] == 0
    assert len(backend.calls) == 1

    clock.advance(1)
    verify_tick = run(coordinator.sync_once())
    assert verify_tick["physical_requests"] == 1
    assert verify_tick["operation_type"] == "VERIFY_CREATION"
    assert len(backend.calls) == 2
    assert runtime.get_calls == 1
    assert all(
        result["physical_requests"] in {0, 1}
        for result in (create_tick, blocked_tick, verify_tick)
    )


def test_five_child_capacity_is_transactional(tmp_path):
    coordinator, _gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    parent = run(coordinator.register_parent("parent-chat"))
    for index in range(5):
        run(
            coordinator.spawn(
                parent["agent_id"],
                f"task {index}",
                idempotency_key=f"capacity-{index}",
            )
        )
    with pytest.raises(ValueError, match="maximum active managed sub-agents"):
        run(
            coordinator.spawn(
                parent["agent_id"],
                "sixth task",
                idempotency_key="capacity-six",
            )
        )
    assert backend.calls == []


def test_stopped_without_terminal_evidence_remains_unknown(tmp_path):
    coordinator, gateway, backend, clock, _runtime = make_coordinator(tmp_path)
    parent = run(coordinator.register_parent("parent-chat"))
    child = run(
        coordinator.spawn(
            parent["agent_id"],
            "long task",
            idempotency_key="unknown-child",
            notification_policy="notify_only",
        )
    )
    run(coordinator.sync_once())
    clock.advance(1)
    run(coordinator.sync_once())

    gateway_id = coordinator.repository.agent(child["agent"]["agent_id"])[
        "gateway_agent_id"
    ]
    conversation_id = gateway.ledger.get_agent(gateway_id).conversation_id
    backend.set_snapshot(
        ThreadSnapshot(
            conversation_id=conversation_id,
            found=True,
            running=False,
            current_node="assistant-still-working",
            turns=(
                TurnSnapshot(
                    "assistant-still-working",
                    "assistant",
                    "finished_successfully",
                    "stopped without the completion marker",
                    True,
                ),
            ),
        ),
        project_id="g-p-project",
    )
    clock.advance(1)
    tick = run(coordinator.sync_once())
    assert tick["physical_requests"] == 1
    assert gateway.ledger.get_agent(gateway_id).state.value == "UNKNOWN"
    status = run(coordinator.status(child["agent"]["agent_id"]))
    assert status["agent"]["status"] == "unknown"
    assert status["task"]["status"] == "unknown"


def test_command_continue_and_verification_are_separate_ticks(tmp_path):
    coordinator, gateway, backend, clock, _runtime = make_coordinator(tmp_path)
    parent = run(coordinator.register_parent("parent-chat"))
    child = run(
        coordinator.spawn(
            parent["agent_id"],
            "wait for follow-up",
            idempotency_key="continue-child",
            notification_policy="notify_only",
        )
    )
    child_id = child["agent"]["agent_id"]
    run(coordinator.sync_once())
    clock.advance(1)
    run(coordinator.sync_once())

    gateway_id = coordinator.repository.agent(child_id)["gateway_agent_id"]
    conversation_id = gateway.ledger.get_agent(gateway_id).conversation_id
    backend.set_snapshot(
        ThreadSnapshot(
            conversation_id=conversation_id,
            found=True,
            running=False,
            current_node="assistant-idle",
            turns=(
                TurnSnapshot(
                    "assistant-idle",
                    "assistant",
                    "finished_successfully",
                    "paused before marker",
                    True,
                ),
            ),
        ),
        project_id="g-p-project",
    )
    clock.advance(1)
    inspect = run(coordinator.sync_once())
    assert inspect["operation_type"] == "INSPECT"
    assert inspect["physical_requests"] == 1

    command = run(
        coordinator.send(
            parent["agent_id"],
            child_id,
            "Continue with the requested evidence.",
            idempotency_key="continue-command",
        )
    )
    assert command["status"] == "queued"

    dispatch = run(coordinator.sync_once())
    assert dispatch["operation_type"] == "CONTINUE"
    assert dispatch["physical_requests"] == 1
    assert coordinator.repository.queued_commands() == []

    before = len(backend.calls)
    too_early = run(coordinator.sync_once())
    assert too_early["physical_requests"] == 0
    assert len(backend.calls) == before

    clock.advance(1)
    verification = run(coordinator.sync_once())
    assert verification["operation_type"] == "VERIFY_CONTINUE"
    assert verification["physical_requests"] == 1


def test_child_message_to_running_root_schedules_canonical_parent_inspection(tmp_path):
    coordinator, gateway, backend, clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    parent_id = parent["agent_id"]
    child = run(
        coordinator.spawn(
            parent_id,
            "send a parent update",
            idempotency_key="running-root-child",
            notification_policy="notify_only",
        )
    )
    child_id = child["agent"]["agent_id"]

    run(coordinator.sync_once())
    clock.advance(1)
    run(coordinator.sync_once())

    parent_gateway_id = coordinator.repository.agent(parent_id)["gateway_agent_id"]
    with gateway.ledger.transaction(immediate=True) as db:
        gateway.ledger.update_agent(
            db,
            agent_id=parent_gateway_id,
            now=clock.now(),
            state=AgentState.RUNNING,
            snapshot_hash=None,
            last_inspected_at=None,
            next_inspection_at=None,
        )

    command = run(
        coordinator.send(
            child_id,
            parent_id,
            "child progress for the parent",
            idempotency_key="child-to-running-root",
        )
    )
    assert command["purpose"] == "progress"
    assert command["status"] == "queued"

    coordinator.prepare_local_work()
    parent_operations = [
        operation
        for operation in gateway.ledger.list_operations(state=OperationState.PENDING)
        if operation.agent_id == parent_gateway_id
    ]
    assert any(
        operation.type is OperationType.INSPECT for operation in parent_operations
    )

    inspection = run(coordinator.sync_once())
    assert inspection["operation_type"] == OperationType.INSPECT.value
    assert gateway.ledger.get_agent(parent_gateway_id).state is AgentState.UNKNOWN

    with coordinator.repository.transaction() as db:
        db.execute(
            "UPDATE commands SET next_attempt_at=0 WHERE command_id=?",
            (command["command_id"],),
        )
    dispatch = run(coordinator.sync_once())
    assert dispatch["operation_type"] == OperationType.CONTINUE.value
    with coordinator.repository.connect() as db:
        delivered = dict(
            db.execute(
                "SELECT * FROM commands WHERE command_id=?", (command["command_id"],)
            ).fetchone()
        )
    assert delivered["status"] == "delivered"
    assert backend.threads["parent-chat"].turns[-2].text == "child progress for the parent"


def test_child_completion_envelope_reaches_running_root_and_acks_events(tmp_path):
    coordinator, gateway, backend, clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    parent_id = parent["agent_id"]
    child = run(
        coordinator.spawn(
            parent_id,
            "finish and notify the parent",
            idempotency_key="completion-root-child",
            notification_policy="auto_resume",
        )
    )
    child_id = child["agent"]["agent_id"]
    task_id = child["task"]["task_id"]

    run(coordinator.sync_once())
    clock.advance(1)
    run(coordinator.sync_once())

    parent_gateway_id = coordinator.repository.agent(parent_id)["gateway_agent_id"]
    with gateway.ledger.transaction(immediate=True) as db:
        gateway.ledger.update_agent(
            db,
            agent_id=parent_gateway_id,
            now=clock.now(),
            state=AgentState.RUNNING,
            snapshot_hash=None,
            last_inspected_at=None,
            next_inspection_at=None,
        )
    coordinator._emit_event(
        child_id,
        task_id,
        "completed",
        {"summary": "child finished successfully"},
        source_cursor="test-child-completed",
    )

    coordinator.prepare_local_work()
    with coordinator.repository.connect() as db:
        completion = dict(
            db.execute(
                "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=? "
                "AND purpose='completion' ORDER BY sequence_no DESC LIMIT 1",
                (child_id, parent_id),
            ).fetchone()
        )
    assert completion["status"] == "queued"
    assert completion["ack_event_seq"]
    assert "[SUBAGENT UPDATE]" in completion["message"]

    inspection = run(coordinator.sync_once())
    assert inspection["operation_type"] == OperationType.INSPECT.value
    assert gateway.ledger.get_agent(parent_gateway_id).state is AgentState.UNKNOWN

    with coordinator.repository.transaction() as db:
        db.execute(
            "UPDATE commands SET next_attempt_at=0 WHERE command_id=?",
            (completion["command_id"],),
        )
    dispatch = run(coordinator.sync_once())
    assert dispatch["operation_type"] == OperationType.CONTINUE.value

    with coordinator.repository.connect() as db:
        completion_status = db.execute(
            "SELECT status FROM commands WHERE command_id=?",
            (completion["command_id"],),
        ).fetchone()[0]
        acked = db.execute(
            "SELECT last_acked_event_seq FROM subscriptions "
            "WHERE parent_agent_id=? AND child_agent_id=?",
            (parent_id, child_id),
        ).fetchone()[0]
    assert completion_status == "delivered"
    assert int(acked) == int(completion["ack_event_seq"])
    assert backend.threads["parent-chat"].turns[-2].text.startswith("[SUBAGENT UPDATE]")


def test_completed_child_reopens_for_new_parent_instruction(tmp_path):
    coordinator, gateway, _backend, _clock, _runtime = make_coordinator(tmp_path)
    parent = run(coordinator.register_parent("parent-chat"))
    child = run(
        coordinator.spawn(
            parent["agent_id"],
            "complete once",
            idempotency_key="reopen-child",
            notification_policy="notify_only",
        )
    )
    child_id = child["agent"]["agent_id"]
    run(coordinator.sync_once())
    _clock.advance(1)
    run(coordinator.sync_once())
    gateway_id = coordinator.repository.agent(child_id)["gateway_agent_id"]
    assert gateway.ledger.get_agent(gateway_id).conversation_id
    now = gateway.clock.now()
    with gateway.ledger.transaction(immediate=True) as db:
        gateway.ledger.update_agent(
            db,
            agent_id=gateway_id,
            now=now,
            state=AgentState.COMPLETED,
            completed_at=now,
        )
    with coordinator.repository.transaction() as db:
        db.execute(
            "UPDATE agents SET status='completed' WHERE agent_id=?",
            (child_id,),
        )
        db.execute(
            "UPDATE tasks SET status='completed',completed_at=? WHERE agent_id=?",
            (now, child_id),
        )

    with coordinator.repository.transaction() as db:
        db.execute(
            "UPDATE agents SET status='cancelled' WHERE agent_id=?",
            (parent["agent_id"],),
        )
    with pytest.raises(ValueError, match="sender agent is terminal"):
        run(
            coordinator.send(
                parent["agent_id"],
                child_id,
                "This invalid send must not reopen the child.",
                idempotency_key="invalid-reopen-command",
            )
        )
    assert gateway.ledger.get_agent(gateway_id).state is AgentState.COMPLETED
    with coordinator.repository.transaction() as db:
        db.execute(
            "UPDATE agents SET status='registered' WHERE agent_id=?",
            (parent["agent_id"],),
        )

    command = run(
        coordinator.send(
            parent["agent_id"],
            child_id,
            "Start the next bounded task.",
            idempotency_key="reopen-command",
        )
    )

    assert command["status"] == "queued"
    assert coordinator.repository.agent(child_id)["status"] == "unknown"
    assert coordinator.repository.task_for_agent(child_id)["completed_at"] is None
    coordinator.prepare_local_work()
    assert gateway.ledger.get_agent(gateway_id).state is AgentState.UNKNOWN
    assert any(
        operation.type is OperationType.CONTINUE
        for operation in gateway.ledger.list_operations(state=OperationState.PENDING)
    )


def test_runtime_outage_pauses_agents_without_consuming_retry_budget(tmp_path):
    coordinator, gateway, backend, clock, _runtime = make_coordinator(tmp_path)
    parent = run(coordinator.register_parent("parent-chat"))
    run(
        coordinator.spawn(
            parent["agent_id"],
            "Wait through a shared renderer outage.",
            idempotency_key="runtime-outage-child",
        )
    )
    gateway_id = next(
        agent.id
        for agent in gateway.ledger.list_agents()
        if agent.conversation_id is None
    )
    backend.queue("create_thread", InfrastructureError("renderer is unavailable"))

    outage = run(coordinator.sync_once())
    assert outage["outcome"] == "infrastructure-outage"
    assert outage["circuit_state"] == CircuitState.OPEN.value
    create = next(
        item
        for item in gateway.ledger.list_operations(state=OperationState.PENDING)
        if item.type is OperationType.CREATE
    )
    assert create.attempts == 0
    assert gateway.ledger.get_agent(gateway_id).state is AgentState.CREATING

    clock.advance(1)
    probe = run(coordinator.sync_once())
    assert probe["operation_type"] == OperationType.RECOVERY_PROBE.value
    assert probe["physical_requests"] == 1
    assert backend.calls[-1].method == "health"
    assert gateway.runtime_circuit() is CircuitState.PACED

    clock.advance(1)
    resumed = run(coordinator.sync_once())
    assert resumed["operation_type"] == OperationType.CREATE.value
    assert resumed["outcome"] == "success"
    assert sum(call.method == "create_thread" for call in backend.calls) == 2


def _register_parent_in_fake_backend(backend: FakeBackend) -> None:
    backend.set_snapshot(
        ThreadSnapshot(
            conversation_id="parent-chat",
            found=True,
            running=False,
            title="Parent",
            current_node="parent-assistant",
            turns=(
                TurnSnapshot(
                    "parent-assistant",
                    "assistant",
                    "finished_successfully",
                    "ready",
                    True,
                ),
            ),
        ),
        project_id="g-p-project",
    )


def test_wakeup_timer_materializes_once_and_uses_gateway_rail(tmp_path, monkeypatch):
    import chat_agent_orchestrator
    import gateway_agent_orchestrator

    wall = [10_000.0]
    monkeypatch.setattr(chat_agent_orchestrator, "_now", lambda: wall[0])
    monkeypatch.setattr(gateway_agent_orchestrator, "_now", lambda: wall[0])
    coordinator, gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))

    scheduled = run(
        coordinator.schedule_wakeup(
            "parent-chat",
            wall[0] + 5,
            prompt="continue",
            idempotency_key="parent-review",
        )
    )
    replay = run(
        coordinator.schedule_wakeup(
            parent["agent_id"],
            wall[0] + 500,
            prompt="This duplicate must not replace the first timer.",
            idempotency_key="parent-review",
        )
    )
    assert replay["automation_id"] == scheduled["automation_id"]
    assert replay["due_at"] == wall[0] + 5
    assert coordinator.next_wakeup_at() == wall[0] + 5

    coordinator.prepare_local_work()
    assert (
        coordinator.repository.automation(scheduled["automation_id"])["status"]
        == "scheduled"
    )
    assert not any(
        operation.type is OperationType.CONTINUE
        for operation in gateway.ledger.list_operations(state=OperationState.PENDING)
    )

    wall[0] += 5
    coordinator.prepare_local_work()
    automation = coordinator.repository.automation(scheduled["automation_id"])
    assert automation["status"] == "queued"
    assert automation["triggered_at"] == wall[0]
    assert any(
        operation.type is OperationType.INSPECT
        for operation in gateway.ledger.list_operations(state=OperationState.PENDING)
    )
    assert backend.calls == []

    inspection = run(coordinator.sync_once())
    assert inspection["operation_type"] == OperationType.INSPECT.value
    wall[0] += 1
    coordinator.prepare_local_work()
    assert any(
        operation.type is OperationType.CONTINUE
        for operation in gateway.ledger.list_operations(state=OperationState.PENDING)
    )
    tick = run(coordinator.sync_once())
    assert tick["operation_type"] == OperationType.CONTINUE.value
    assert tick["physical_requests"] == 1
    assert backend.calls[-1].method == "continue_thread"
    assert backend.threads["parent-chat"].turns[-2].text == "continue"
    assert (
        coordinator.repository.automation(scheduled["automation_id"])["status"]
        == "delivered"
    )
    continue_calls = sum(call.method == "continue_thread" for call in backend.calls)
    wall[0] += 60
    coordinator.prepare_local_work()
    run(coordinator.sync_once())
    coordinator.prepare_local_work()
    run(coordinator.sync_once())
    assert sum(call.method == "continue_thread" for call in backend.calls) == continue_calls == 1


def test_completion_prompt_requires_exact_marker_before_queueing(tmp_path, monkeypatch):
    import chat_agent_orchestrator
    import gateway_agent_orchestrator

    wall = [20_000.0]
    monkeypatch.setattr(chat_agent_orchestrator, "_now", lambda: wall[0])
    monkeypatch.setattr(gateway_agent_orchestrator, "_now", lambda: wall[0])
    coordinator, gateway, backend, clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    child = run(
        coordinator.spawn(
            parent["agent_id"],
            "Produce verified evidence.",
            idempotency_key="completion-source",
            notification_policy="notify_only",
        )
    )
    child_id = child["agent"]["agent_id"]
    run(coordinator.sync_once())
    clock.advance(1)
    run(coordinator.sync_once())
    gateway_id = coordinator.repository.agent(child_id)["gateway_agent_id"]
    conversation_id = gateway.ledger.get_agent(gateway_id).conversation_id
    with coordinator.repository.transaction() as db:
        db.execute(
            "UPDATE tasks SET status='waiting_for_parent' WHERE agent_id=?",
            (child_id,),
        )

    trigger = run(
        coordinator.queue_after_completion(
            conversation_id,
            "parent-chat",
            "Synthesize the completed child result.",
            idempotency_key="synthesize-after-child",
        )
    )
    backend.set_snapshot(
        ThreadSnapshot(
            conversation_id=conversation_id,
            found=True,
            running=False,
            current_node="assistant-no-marker",
            turns=(
                TurnSnapshot(
                    "assistant-no-marker",
                    "assistant",
                    "finished_successfully",
                    "The work looks complete, but the exact contract is absent.",
                    True,
                ),
            ),
        ),
        project_id="g-p-project",
    )
    clock.advance(1)
    without_marker = run(coordinator.sync_once())
    assert without_marker["operation_type"] == OperationType.INSPECT.value
    coordinator.prepare_local_work()
    assert (
        coordinator.repository.automation(trigger["automation_id"])["status"]
        == "waiting"
    )
    assert not any(
        row["purpose"] == "after_completion"
        for row in coordinator.repository.queued_commands(include_future=True)
    )

    marker = child["task"]["completion_marker"]
    backend.set_snapshot(
        ThreadSnapshot(
            conversation_id=conversation_id,
            found=True,
            running=False,
            current_node="assistant-with-marker",
            turns=(
                TurnSnapshot(
                    "assistant-with-marker",
                    "assistant",
                    "finished_successfully",
                    f"All evidence is verified.\n{marker}",
                    True,
                ),
            ),
        ),
        project_id="g-p-project",
    )
    gateway.enqueue_inspect(
        agent_id=gateway_id,
        due_at=clock.now(),
        completion_sensitive=True,
    )
    marker_tick = run(coordinator.sync_once())
    assert marker_tick["operation_type"] == OperationType.INSPECT.value
    coordinator.prepare_local_work()
    queued = coordinator.repository.automation(trigger["automation_id"])
    assert queued["status"] == "queued"
    assert queued["triggered_at"] == wall[0]
    assert (
        len(
            [
                row
                for row in coordinator.repository.queued_commands(include_future=True)
                if row["purpose"] == "after_completion"
            ]
        )
        <= 1
    )

    delivered = None
    for _ in range(6):
        tick = run(coordinator.sync_once())
        if tick["operation_type"] == OperationType.CONTINUE.value:
            delivered = tick
            break
        wall[0] += 1
        clock.advance(1)
        coordinator.prepare_local_work()
    assert delivered is not None
    assert (
        coordinator.repository.automation(trigger["automation_id"])["status"]
        == "delivered"
    )


def test_cancel_automation_is_idempotent_and_blocks_future_materialization(
    tmp_path, monkeypatch
):
    import chat_agent_orchestrator
    import gateway_agent_orchestrator

    wall = [30_000.0]
    monkeypatch.setattr(chat_agent_orchestrator, "_now", lambda: wall[0])
    monkeypatch.setattr(gateway_agent_orchestrator, "_now", lambda: wall[0])
    coordinator, _gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    timer = run(
        coordinator.schedule_wakeup(
            parent["agent_id"], wall[0] + 10, idempotency_key="cancel-me"
        )
    )
    first = run(coordinator.cancel_automation(timer["automation_id"]))
    second = run(coordinator.cancel_automation(timer["automation_id"]))
    assert first["status"] == second["status"] == "cancelled"
    wall[0] += 20
    coordinator.prepare_local_work()
    assert (
        coordinator.repository.automation(timer["automation_id"])["status"]
        == "cancelled"
    )
    assert coordinator.repository.queued_commands(include_future=True) == []


def test_background_service_waits_for_near_wakeup_without_thirty_second_floor(
    monkeypatch,
):
    import chat_agent_orchestrator
    from chat_agent_orchestrator import ChatAgentService

    monkeypatch.setattr(chat_agent_orchestrator, "_now", lambda: 100.0)

    class Coordinator:
        def next_due_at(self):
            return None

        def next_wakeup_at(self):
            return 104.5

    service = ChatAgentService(Coordinator(), interval_seconds=600)
    assert service._next_wait_seconds() == 4.5


def test_completion_marker_must_be_an_exact_standalone_line(tmp_path):
    coordinator, gateway, backend, clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    child = run(
        coordinator.spawn(
            parent["agent_id"],
            "Complete with an exact marker.",
            idempotency_key="standalone-marker-source",
            notification_policy="notify_only",
        )
    )
    child_id = child["agent"]["agent_id"]
    run(coordinator.sync_once())
    clock.advance(1)
    run(coordinator.sync_once())
    gateway_id = coordinator.repository.agent(child_id)["gateway_agent_id"]
    conversation_id = gateway.ledger.get_agent(gateway_id).conversation_id
    marker = child["task"]["completion_marker"]
    trigger = run(
        coordinator.queue_after_completion(
            child_id,
            parent["agent_id"],
            "This must never queue from embedded evidence.",
            idempotency_key="standalone-marker-gate",
        )
    )
    backend.set_snapshot(
        ThreadSnapshot(
            conversation_id=conversation_id,
            found=True,
            running=False,
            current_node="assistant-embedded-marker",
            turns=(
                TurnSnapshot(
                    "assistant-embedded-marker",
                    "assistant",
                    "finished_successfully",
                    "A sentence containing " + marker + " inside other text.",
                    True,
                ),
            ),
        ),
        project_id="g-p-project",
    )
    clock.advance(1)
    run(coordinator.sync_once())
    coordinator.prepare_local_work()
    automation = coordinator.repository.automation(trigger["automation_id"])
    assert automation["status"] == "waiting"
    assert automation["last_error"] == ""
    assert gateway.ledger.get_agent(gateway_id).state is AgentState.UNKNOWN
    assert not any(
        row["purpose"] == "after_completion"
        for row in coordinator.repository.queued_commands(include_future=True)
    )


def test_wake_timestamp_requires_timezone_for_iso_values(tmp_path):
    coordinator, _gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    with pytest.raises(ValueError, match="include a timezone"):
        run(coordinator.schedule_wakeup(parent["agent_id"], "2026-07-22T09:30:00"))


def test_dashboard_snapshot_exposes_tree_automations_and_reasoning(tmp_path):
    coordinator, _gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    backend.model_slug = "gpt-5.6-terra"
    backend.thinking_effort = "extended"
    backend.require_high_reasoning = True
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    child = run(
        coordinator.spawn(
            parent["agent_id"],
            "Create one tree child.",
            idempotency_key="dashboard-child",
            notification_policy="notify_only",
        )
    )
    run(
        coordinator.schedule_wakeup(
            child["agent"]["agent_id"],
            40_000,
            idempotency_key="dashboard-timer",
        )
    )
    snapshot = coordinator.dashboard_snapshot()
    agents = {agent["agent_id"]: agent for agent in snapshot["agents"]}
    assert agents[parent["agent_id"]]["depth"] == 0
    assert agents[parent["agent_id"]]["children_count"] == 1
    assert agents[child["agent"]["agent_id"]]["depth"] == 1
    assert agents[child["agent"]["agent_id"]]["path"] == [
        parent["agent_id"],
        child["agent"]["agent_id"],
    ]
    assert snapshot["automations"][0]["kind"] == "wakeup"
    assert snapshot["counts"]["scheduled_wakeups"] == 1
    assert snapshot["reasoning"] == {
        "model": "gpt-5.6-terra",
        "thinking_effort": "extended",
        "require_high_reasoning": True,
        "applies_to": ["create", "continue", "wakeup", "after_completion"],
    }


def test_completion_trigger_rejects_multiline_marker(tmp_path):
    coordinator, _gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    with pytest.raises(ValueError, match="must be a single line"):
        run(
            coordinator.queue_after_completion(
                parent["agent_id"],
                parent["agent_id"],
                "Do not queue this.",
                completion_marker="FIRST\nSECOND",
            )
        )


def test_wakeup_idempotency_is_transactional_under_concurrent_callers(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    coordinator, _gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))

    def schedule():
        return run(
            coordinator.schedule_wakeup(
                parent["agent_id"],
                50_000,
                prompt="One durable timer only.",
                idempotency_key="concurrent-timer",
            )
        )

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _index: schedule(), range(8)))
    assert len({result["automation_id"] for result in results}) == 1
    rows = coordinator.repository.automations(
        agent_id=parent["agent_id"], include_terminal=True
    )
    assert len(rows) == 1


def test_gateway_reducer_rejects_marker_embedded_in_other_text() -> None:
    from chat_gateway.status_reducer import reduce_snapshot as reduce_gateway_snapshot

    marker = "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"
    snapshot = ThreadSnapshot(
        conversation_id="embedded-marker",
        found=True,
        running=False,
        current_node="assistant-embedded",
        turns=(
            TurnSnapshot(
                "assistant-embedded",
                "assistant",
                "finished_successfully",
                "This sentence mentions " + marker + " but is not the contract line.",
                True,
            ),
        ),
    )
    reduction = reduce_gateway_snapshot(snapshot, completion_marker=marker)
    assert reduction.state is AgentState.UNKNOWN
    assert reduction.marker_present is False
    assert reduction.terminal is False


def test_completion_gate_actively_monitors_existing_root_without_cached_snapshot(
    tmp_path,
):
    coordinator, gateway, backend, _clock, _runtime = make_coordinator(tmp_path)
    _register_parent_in_fake_backend(backend)
    parent = run(coordinator.register_parent("parent-chat"))
    gateway_id = coordinator.repository.agent(parent["agent_id"])["gateway_agent_id"]
    assert gateway.latest_snapshot(gateway_id) is None

    marker = gateway.config.completion_marker
    backend.set_snapshot(
        ThreadSnapshot(
            conversation_id="parent-chat",
            found=True,
            running=False,
            title="Parent",
            current_node="parent-complete",
            turns=(
                TurnSnapshot(
                    "parent-complete",
                    "assistant",
                    "finished_successfully",
                    "Root work is complete.\n" + marker,
                    True,
                ),
            ),
        ),
        project_id="g-p-project",
    )
    trigger = run(
        coordinator.queue_after_completion(
            "parent-chat",
            "parent-chat",
            "Begin the next verified phase.",
            idempotency_key="root-completion-monitor",
        )
    )
    pending = gateway.ledger.list_operations(state=OperationState.PENDING)
    assert any(operation.type is OperationType.INSPECT for operation in pending)

    inspection = run(coordinator.sync_once())
    assert inspection["operation_type"] == OperationType.INSPECT.value
    assert gateway.ledger.get_agent(gateway_id).state is AgentState.COMPLETED

    coordinator.prepare_local_work()
    automation = coordinator.repository.automation(trigger["automation_id"])
    assert automation["status"] == "queued"
    assert any(
        operation.type is OperationType.CONTINUE
        for operation in gateway.ledger.list_operations(state=OperationState.PENDING)
    )
