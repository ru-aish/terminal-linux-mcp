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
    FakeBackend,
    FakeClock,
    GatewayConfig,
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
