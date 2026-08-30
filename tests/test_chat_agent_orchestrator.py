from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from chat_agent_orchestrator import (
    DEFAULT_AGENT_COMPLETION_MARKER,
    ChatAgentCoordinator,
    ChatAgentService,
    CoordinatorConfig,
)


def terminal_snapshot(
    chat_id: str,
    *,
    running: bool = False,
    marker: str = "",
    project_id: str | None = "g-p-project",
) -> dict:
    assistant = marker or ("working" if running else "idle result")
    return {
        "found": True,
        "conversation_id": chat_id,
        "project_id": project_id,
        "title": chat_id,
        "current_node": f"a-{chat_id}",
        "state_verified": True,
        "canonical": True,
        "running": running,
        "active_stream": False,
        "turns": [
            {
                "key": f"u-{chat_id}",
                "role": "user",
                "text": "task",
                "status": "finished_successfully",
                "end_turn": None,
            },
            {
                "key": f"a-{chat_id}",
                "role": "assistant",
                "text": assistant,
                "status": "in_progress" if running else "finished_successfully",
                "end_turn": False if running else True,
            },
        ],
    }


class FakeRuntime:
    def __init__(self):
        self.projects = {
            "g-p-project": {
                "id": "g-p-project",
                "name": "Project",
                "permissions": {"can_read": True, "can_write": True},
            }
        }
        self.snapshots: dict[str, dict] = {"parent-chat": terminal_snapshot("parent-chat")}
        self.context_events: dict[str, list[dict]] = {}
        self.create_calls: list[tuple[str, str | None, str | None]] = []
        self.get_calls: list[str] = []
        self.context_calls: list[str] = []
        self.enter_calls = 0
        self.continue_calls: list[tuple[str, str, str]] = []
        self.continue_message_ids: list[str | None] = []
        self.cancel_calls: list[str] = []
        self.cancel_result = {"cancelled": True, "reason": ""}
        self.get_errors: set[str] = set()
        self.continue_errors: set[str] = set()
        self.continue_results: dict[str, dict] = {}
        self.create_delay = 0.0
        self.create_result: dict | None = None
        self.before_create = None
        self.before_continue = None
        self.before_cancel = None

    async def __aenter__(self):
        self.enter_calls += 1
        return self

    async def __aexit__(self, *_args):
        return None

    async def list_projects(self, *, limit=20, cursor=None, owned_only=True):
        return {"items": list(self.projects.values())[:limit], "cursor": None}

    async def get_project(self, project_id):
        return self.projects.get(project_id, {})

    async def list_project_threads(self, project_id, *, limit=20, cursor=None, owned_only=True):
        return {
            "items": [
                {"conversation_id": key, "title": key, "project_id": project_id}
                for key in self.snapshots
                if key != "parent-chat"
            ][:limit],
            "cursor": None,
        }

    async def create_thread(self, prompt, *, project_id=None, title=None):
        if self.before_create is not None:
            self.before_create()
        if self.create_delay:
            await asyncio.sleep(self.create_delay)
        chat_id = f"child-chat-{len(self.create_calls) + 1}"
        self.create_calls.append((prompt, project_id, title))
        self.snapshots[chat_id] = terminal_snapshot(chat_id, running=True)
        result = {
            "sent": True,
            "observed": True,
            "running": True,
            "conversation_id": chat_id,
            "current_node": f"a-{chat_id}",
            "user_message_id": f"u-{chat_id}",
            "chat_url": f"https://chatgpt.com/c/{chat_id}",
        }
        if self.create_result is not None:
            result.update(self.create_result)
        return result

    async def get_thread(self, conversation_id):
        self.get_calls.append(conversation_id)
        if conversation_id in self.get_errors:
            raise RuntimeError(f"read failed for {conversation_id}")
        return self.snapshots[conversation_id]

    async def continue_thread(
        self,
        conversation_id,
        message,
        *,
        expected_current_node,
        wait_for_completion=True,
        user_message_id=None,
    ):
        assert wait_for_completion is False
        if self.before_continue is not None:
            self.before_continue(conversation_id)
        self.continue_calls.append((conversation_id, message, expected_current_node))
        self.continue_message_ids.append(user_message_id)
        if conversation_id in self.continue_errors:
            raise RuntimeError(f"send failed for {conversation_id}")
        result = dict(
            self.continue_results.get(
                conversation_id,
                {"sent": True, "observed": True, "running": True, "reason": ""},
            )
        )
        if user_message_id:
            result.setdefault("user_message_id", user_message_id)
        result.setdefault("parent_message_id", expected_current_node)
        if result.get("sent"):
            self.snapshots[conversation_id] = terminal_snapshot(
                conversation_id, running=True
            )
        return result

    async def thread_context(self, conversation_id, *, since_cursor=None, max_events=60, max_chars=12000):
        self.context_calls.append(conversation_id)
        events = self.context_events.get(conversation_id, [])
        if since_cursor:
            index = next((i for i, event in enumerate(events) if event["cursor"] == since_cursor), None)
            events = events[index + 1 :] if index is not None else events
        return {
            "conversation_id": conversation_id,
            "events": events[-max_events:],
            "next_cursor": events[-1]["cursor"] if events else since_cursor,
            "running": self.snapshots[conversation_id]["running"],
        }

    async def thread_tail(self, conversation_id, *, lines=60, max_chars=12000):
        return {"conversation_id": conversation_id, "lines": [f"assistant: {conversation_id}"]}

    async def cancel(self, conversation_id):
        if self.before_cancel is not None:
            self.before_cancel(conversation_id)
        self.cancel_calls.append(conversation_id)
        if self.cancel_result.get("cancelled"):
            self.snapshots[conversation_id] = terminal_snapshot(conversation_id)
        return dict(self.cancel_result)


def coordinator(
    tmp_path: Path,
    runtime: FakeRuntime,
    *,
    max_continue_attempts: int = 20,
    stale_after_seconds: float = 600.0,
) -> ChatAgentCoordinator:
    return ChatAgentCoordinator(
        CoordinatorConfig(
            tmp_path / "agents.db",
            context_max_events=60,
            context_max_chars=12000,
            max_continue_attempts=max_continue_attempts,
            stale_after_seconds=stale_after_seconds,
        ),
        lambda: runtime,
    )


async def make_parent_and_child(
    tmp_path: Path,
    runtime: FakeRuntime,
    *,
    notification_policy: str = "notify_only",
    completion_marker: str = "",
):
    service = coordinator(tmp_path, runtime)
    parent = await service.register_parent(
        "parent-chat", project_id="g-p-project", title="Parent"
    )
    child = await service.spawn(
        parent["agent_id"],
        "child task",
        project_policy="explicit",
        project_id="g-p-project",
        notification_policy=notification_policy,
        completion_marker=completion_marker,
        idempotency_key="spawn-one",
    )
    return service, parent, child


def test_register_parent_and_spawn_are_persistent_and_idempotent(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        duplicate_parent = await service.register_parent("parent-chat")
        duplicate_child = await service.spawn(
            parent["agent_id"],
            "ignored duplicate prompt",
            project_policy="explicit",
            project_id="g-p-project",
            idempotency_key="spawn-one",
        )
        assert duplicate_parent["agent_id"] == parent["agent_id"]
        assert duplicate_child["agent"]["agent_id"] == child["agent"]["agent_id"]
        assert len(runtime.create_calls) == 1
        assert runtime.create_calls[0][1] == "g-p-project"

        recreated = coordinator(tmp_path, runtime)
        persisted = await recreated.status(child["agent"]["agent_id"])
        assert persisted["agent"]["chat_id"] == "child-chat-1"
        assert persisted["agent"]["parent_agent_id"] == parent["agent_id"]
        tree = await recreated.children(parent["agent_id"], recursive=True)
        assert tree["children"][0]["agent_id"] == child["agent"]["agent_id"]

    asyncio.run(run())


def test_project_policy_is_explicit_and_never_guessed(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        with pytest.raises(ValueError, match="project_id is required"):
            await service.spawn(parent["agent_id"], "task", project_policy="explicit")
        with pytest.raises(ValueError, match="not allowed"):
            await service.spawn(
                parent["agent_id"], "task", project_policy="none", project_id="g-p-project"
            )
        assert runtime.create_calls == []

    asyncio.run(run())


def test_active_child_queues_followup_then_terminal_child_receives_it(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        first = await service.send(
            parent["agent_id"], child_id, "follow-up", idempotency_key="message-one"
        )
        duplicate = await service.send(
            parent["agent_id"], child_id, "follow-up", idempotency_key="message-one"
        )
        assert duplicate["command_id"] == first["command_id"]

        result = await service.sync_once()
        assert result["write_count"] == 0
        assert runtime.continue_calls == []

        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        result = await service.sync_once()
        assert result["write_count"] == 1
        assert runtime.continue_calls == [
            ("child-chat-1", "follow-up", "a-child-chat-1")
        ]
        recreated = coordinator(tmp_path, runtime)
        assert (await recreated.status(child_id))["mailbox"]["delivered"] == 1

    asyncio.run(run())


def test_commands_are_delivered_before_unrelated_agent_monitoring(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat", project_id="g-p-project")
        child_a = await service.spawn(
            parent["agent_id"], "a", project_policy="explicit", project_id="g-p-project"
        )
        child_b = await service.spawn(
            parent["agent_id"], "b", project_policy="explicit", project_id="g-p-project"
        )
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        runtime.snapshots["child-chat-2"] = terminal_snapshot("child-chat-2")
        await service.send(parent["agent_id"], child_a["agent"]["agent_id"], "A")
        await service.send(parent["agent_id"], child_b["agent"]["agent_id"], "B")
        runtime.get_calls.clear()

        first = await service.sync_once()
        assert first["inspected"] == []
        assert first["write_count"] == 1
        assert runtime.get_calls == ["child-chat-1"]
        assert len(runtime.continue_calls) == 1

        second = await service.sync_once()
        assert second["inspected"] == []
        assert second["write_count"] == 1
        assert runtime.get_calls == ["child-chat-1", "child-chat-2"]
        assert len(runtime.continue_calls) == 2

    asyncio.run(run())


def test_auto_resume_queues_parent_update_and_waits_for_parent_terminal(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path, runtime, notification_policy="auto_resume"
        )
        child_id = child["agent"]["agent_id"]
        runtime.context_events["child-chat-1"] = [
            {"cursor": "evt-node", "kind": "progress", "summary": "Tests passed"}
        ]
        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat", running=True)
        result = await service.sync_once()
        assert result["write_count"] == 0
        parent_status = await service.status(parent["agent_id"])
        assert parent_status["mailbox"]["queued"] >= 1

        waited = await service.wait(child_id)
        assert waited["events"]

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        result = await service.sync_once()
        assert result["write_count"] == 1
        assert runtime.continue_calls[0][0] == "parent-chat"
        assert "[SUBAGENT UPDATE]" in runtime.continue_calls[0][1]

    asyncio.run(run())


def test_completion_marker_and_cancel_are_durable(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, _parent, child = await make_parent_and_child(
            tmp_path, runtime, completion_marker="DONE"
        )
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", marker="summary\nDONE"
        )
        await service.sync_once()
        assert (await service.status(child_id))["agent"]["status"] == "completed"

        other = await service.spawn(
            child["agent"]["parent_agent_id"],
            "second",
            project_policy="explicit",
            project_id="g-p-project",
        )
        other_id = other["agent"]["agent_id"]
        result = await service.cancel(other_id, interrupt=True)
        assert result["cancelled"] is True
        assert runtime.cancel_calls == ["child-chat-2"]
        assert (await service.status(other_id))["agent"]["status"] == "cancelled"

    asyncio.run(run())


def test_parent_registration_discovers_project_and_rejects_mismatch(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        assert parent["project_id"] == "g-p-project"
        assert parent["title"] == "parent-chat"

        other_runtime = FakeRuntime()
        other_service = coordinator(tmp_path / "other", other_runtime)
        with pytest.raises(ValueError, match="does not match"):
            await other_service.register_parent(
                "parent-chat", project_id="g-p-wrong"
            )

    asyncio.run(run())


def test_default_completion_contract_and_automatic_continuation(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent(
            "parent-chat", working_directory=str(tmp_path)
        )
        child = await service.spawn(
            parent["agent_id"],
            "complete the implementation",
            project_policy="inherit_parent",
        )
        prompt = runtime.create_calls[0][0]
        assert "Completion contract" in prompt
        assert DEFAULT_AGENT_COMPLETION_MARKER in prompt
        child_id = child["agent"]["agent_id"]
        assert f"agent_id: {child_id}" in prompt
        assert f"task_id: {child['task']['task_id']}" in prompt
        assert f"parent_agent_id: {parent['agent_id']}" in prompt
        assert f"root_agent_id: {parent['agent_id']}" in prompt
        assert f"orchestration_id: {parent['orchestration_id']}" in prompt
        assert f"working_directory: {tmp_path}" in prompt
        assert "call bootstrap_thread with thread_id equal to your agent_id" in prompt
        assert child["agent"]["working_directory"] == str(tmp_path)

        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        result = await service.sync_once()
        assert result["write_count"] == 1
        assert runtime.continue_calls[-1][0] == "child-chat-1"
        assert DEFAULT_AGENT_COMPLETION_MARKER in runtime.continue_calls[-1][1]
        status = await service.status(child_id)
        assert status["task"]["continue_attempts"] == 1
        assert status["task"]["status"] == "running"

    asyncio.run(run())


def test_command_read_failure_yields_after_one_request_then_advances(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        first = await service.spawn(parent["agent_id"], "one")
        second = await service.spawn(parent["agent_id"], "two")
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        runtime.snapshots["child-chat-2"] = terminal_snapshot("child-chat-2")
        await service.send(parent["agent_id"], first["agent"]["agent_id"], "first")
        await service.send(parent["agent_id"], second["agent"]["agent_id"], "second")
        runtime.get_errors.add("child-chat-1")
        runtime.get_calls.clear()

        first_sync = await service.sync_once()
        assert first_sync["write_count"] == 0
        assert runtime.get_calls == ["child-chat-1"]
        assert runtime.continue_calls == []

        second_sync = await service.sync_once()
        assert second_sync["write_count"] == 1
        assert runtime.get_calls == ["child-chat-1", "child-chat-2"]
        assert runtime.continue_calls == [
            ("child-chat-2", "second", "a-child-chat-2")
        ]

    asyncio.run(run())


def test_uncertain_delivery_is_not_retried_and_later_command_runs(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        first = await service.spawn(parent["agent_id"], "one")
        second = await service.spawn(parent["agent_id"], "two")
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        runtime.snapshots["child-chat-2"] = terminal_snapshot("child-chat-2")
        await service.send(parent["agent_id"], first["agent"]["agent_id"], "first")
        await service.send(parent["agent_id"], second["agent"]["agent_id"], "second")
        runtime.continue_results["child-chat-1"] = {
            "sent": True,
            "observed": False,
            "running": True,
            "reason": "confirmation timed out",
        }

        first_scan = await service.sync_once()
        assert first_scan["write_count"] == 1
        assert runtime.continue_calls[0][0] == "child-chat-1"
        first_status = await service.status(first["agent"]["agent_id"] )
        assert first_status["mailbox"]["delivery_uncertain"] == 1

        second_scan = await service.sync_once()
        assert second_scan["write_count"] == 1
        assert [call[0] for call in runtime.continue_calls] == [
            "child-chat-1",
            "child-chat-2",
        ]

    asyncio.run(run())


def test_auto_resume_batches_events_and_acks_only_after_delivery(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path, runtime, notification_policy="auto_resume"
        )
        child_id = child["agent"]["agent_id"]
        runtime.context_events["child-chat-1"] = [
            {"cursor": "evt-1", "kind": "progress", "summary": "Tests running"},
            {"cursor": "evt-2", "kind": "progress", "summary": "Tests passed"},
        ]
        runtime.snapshots["parent-chat"] = terminal_snapshot(
            "parent-chat", running=True
        )
        await service.sync_once()
        parent_status = await service.status(parent["agent_id"] )
        assert parent_status["mailbox"]["queued"] == 1

        events = (await service.wait(child_id))["events"]
        last_seq = max(event["event_seq"] for event in events)
        with pytest.raises(ValueError, match="does not exist"):
            await service.ack(parent["agent_id"], child_id, last_seq + 100)

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        delivered = await service.sync_once()
        assert delivered["write_count"] == 1
        assert "evt-1" in runtime.continue_calls[-1][1]
        assert "evt-2" in runtime.continue_calls[-1][1]
        with service.repository.connect() as db:
            acked = db.execute(
                "SELECT last_acked_event_seq FROM subscriptions "
                "WHERE parent_agent_id=? AND child_agent_id=?",
                (parent["agent_id"], child_id),
            ).fetchone()[0]
        assert int(acked) == last_seq

    asyncio.run(run())


def test_automatic_continuation_attempt_limit_emits_failure(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime, max_continue_attempts=1)
        parent = await service.register_parent("parent-chat")
        child = await service.spawn(parent["agent_id"], "task")
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        assert (await service.sync_once())["write_count"] == 1

        next_snapshot = terminal_snapshot("child-chat-1")
        next_snapshot["current_node"] = "a-child-chat-1-second"
        next_snapshot["turns"][-1]["key"] = "a-child-chat-1-second"
        runtime.snapshots["child-chat-1"] = next_snapshot
        result = await service.sync_once()
        assert result["write_count"] == 1
        assert [call[0] for call in runtime.continue_calls].count("child-chat-1") == 1
        assert runtime.continue_calls[-1][0] == "parent-chat"
        assert "maximum automatic continuation attempts reached" in runtime.continue_calls[-1][1]
        status = await service.status(child_id)
        assert status["agent"]["status"] == "failed"
        assert status["task"]["status"] == "failed"
        assert any(
            event["kind"] == "failed"
            for event in (await service.wait(child_id))["events"]
        )

    asyncio.run(run())


def test_structurally_stale_child_notifies_parent_without_resend(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime, stale_after_seconds=1)
        parent = await service.register_parent("parent-chat")
        child = await service.spawn(parent["agent_id"], "long task")
        child_id = child["agent"]["agent_id"]
        stale_snapshot = terminal_snapshot("child-chat-1", running=True)
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET last_progress_at=0,progress_signature=? WHERE agent_id=?",
                (service._progress_signature(stale_snapshot), child_id),
            )
        runtime.snapshots["child-chat-1"] = stale_snapshot

        result = await service.sync_once()
        assert result["write_count"] == 1
        assert all(call[0] != "child-chat-1" for call in runtime.continue_calls)
        assert runtime.continue_calls[-1][0] == "parent-chat"
        assert "canonical thread made no structural progress" in runtime.continue_calls[-1][1]
        status = await service.status(child_id)
        assert status["agent"]["status"] == "stale"
        assert any(
            event["kind"] == "blocked"
            for event in (await service.wait(child_id))["events"]
        )

    asyncio.run(run())


def test_background_service_does_not_open_runtime_without_pending_work(tmp_path):
    class UnavailableRuntime:
        async def __aenter__(self):
            raise AssertionError("runtime should not be opened")

        async def __aexit__(self, *_args):
            return None

    coordinator_instance = ChatAgentCoordinator(
        CoordinatorConfig(tmp_path / "idle.db"),
        lambda: UnavailableRuntime(),
    )
    service = ChatAgentService(coordinator_instance)
    result = asyncio.run(service.sync_now())
    assert result == {"status": "idle", "write_count": 0, "inspected": []}


def test_manual_ack_cancels_queued_parent_notification(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path, runtime, notification_policy="auto_resume"
        )
        child_id = child["agent"]["agent_id"]
        runtime.context_events["child-chat-1"] = [
            {"cursor": "manual-ack", "kind": "progress", "summary": "review ready"}
        ]
        runtime.snapshots["parent-chat"] = terminal_snapshot(
            "parent-chat", running=True
        )
        await service.sync_once()
        events = (await service.wait(child_id))["events"]
        last_seq = max(event["event_seq"] for event in events)
        await service.ack(parent["agent_id"], child_id, last_seq)
        parent_status = await service.status(parent["agent_id"] )
        assert parent_status["mailbox"]["acknowledged"] == 1

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        result = await service.sync_once()
        assert result["write_count"] == 0
        assert all(call[0] != "parent-chat" for call in runtime.continue_calls)

    asyncio.run(run())


def test_concurrent_spawn_retry_reuses_reserved_agent(tmp_path):
    async def run():
        runtime = FakeRuntime()
        runtime.create_delay = 0.02
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        first, second = await asyncio.gather(
            service.spawn(
                parent["agent_id"],
                "same task",
                idempotency_key="same-spawn",
            ),
            service.spawn(
                parent["agent_id"],
                "same task",
                idempotency_key="same-spawn",
            ),
        )
        assert first["agent"]["agent_id"] == second["agent"]["agent_id"]
        assert len(runtime.create_calls) == 1

    asyncio.run(run())


def test_existing_parent_can_fill_missing_working_directory_once(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        assert parent["working_directory"] is None

        rebound = await service.register_parent(
            "parent-chat", working_directory=str(tmp_path)
        )
        assert rebound["agent_id"] == parent["agent_id"]
        assert rebound["working_directory"] == str(tmp_path)

        other = tmp_path / "other"
        other.mkdir()
        with pytest.raises(ValueError, match="different working_directory"):
            await service.register_parent(
                "parent-chat", working_directory=str(other)
            )

    asyncio.run(run())


def test_terminal_agents_cannot_send_or_receive_new_messages(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", marker=f"summary\n{DEFAULT_AGENT_COMPLETION_MARKER}"
        )
        await service.sync_once()
        assert (await service.status(child_id))["agent"]["status"] == "completed"

        with pytest.raises(ValueError, match="target agent is terminal"):
            await service.send(parent["agent_id"], child_id, "new work")
        with pytest.raises(ValueError, match="sender agent is terminal"):
            await service.send(child_id, parent["agent_id"], "late update")

    asyncio.run(run())


def test_same_node_text_growth_counts_as_structural_progress(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime, stale_after_seconds=1)
        parent = await service.register_parent("parent-chat")
        child = await service.spawn(parent["agent_id"], "long task")
        child_id = child["agent"]["agent_id"]
        before = terminal_snapshot("child-chat-1", running=True)
        after = terminal_snapshot("child-chat-1", running=True)
        after["turns"][-1]["text"] = "working with additional output"
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET last_progress_at=0,progress_signature=?,current_node=? WHERE agent_id=?",
                (
                    service._progress_signature(before),
                    before["current_node"],
                    child_id,
                ),
            )
        runtime.snapshots["child-chat-1"] = after

        result = await service.sync_once()
        status = await service.status(child_id)
        assert status["agent"]["status"] == "running"
        assert status["agent"]["last_progress_at"] > 0
        assert not any(
            event["kind"] == "blocked"
            for event in (await service.wait(child_id))["events"]
        )
        assert all(call[0] != "child-chat-1" for call in runtime.continue_calls)
        assert result["write_count"] in {0, 1}

    asyncio.run(run())


def test_interrupt_directly_cancels_and_steers_in_one_sync(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", running=True
        )
        command = await service.send(
            parent["agent_id"],
            child_id,
            "urgent correction",
            interrupt_policy="interrupt",
        )

        result = await service.sync_once()
        assert result["write_count"] == 2
        assert runtime.cancel_calls == ["child-chat-1"]
        assert runtime.continue_calls == [
            ("child-chat-1", "urgent correction", "a-child-chat-1")
        ]
        with service.repository.connect() as db:
            status = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()[0]
        assert status == "delivered"

        assert (await service.sync_once())["write_count"] == 0
        assert runtime.cancel_calls == ["child-chat-1"]

    asyncio.run(run())


def test_queued_instruction_survives_completion_and_reopens_thread(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", running=True
        )
        command = await service.send(
            parent["agent_id"], child_id, "late follow-up"
        )

        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1",
            marker=f"done\n{DEFAULT_AGENT_COMPLETION_MARKER}",
        )
        result = await service.sync_once()
        assert result["write_count"] == 1
        assert runtime.continue_calls == [
            ("child-chat-1", "late follow-up", "a-child-chat-1")
        ]
        with service.repository.connect() as db:
            row = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()
        assert row["status"] == "delivered"
        status = await service.status(child_id)
        assert status["agent"]["status"] == "waiting_assistant"
        assert status["task"]["status"] == "waiting_assistant"
        assert status["task"]["completed_at"] is None

    asyncio.run(run())


def test_reserved_creation_is_resumed_after_restart(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, _parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET chat_id=NULL,status='creating_thread' WHERE agent_id=?",
                (child_id,),
            )
            db.execute(
                "UPDATE tasks SET status='creating_thread' WHERE agent_id=?",
                (child_id,),
            )
        restarted = coordinator(tmp_path, runtime)
        result = await restarted.sync_once()
        assert result["write_count"] == 1
        status = await restarted.status(child_id)
        assert status["agent"]["chat_id"] == "child-chat-2"
        assert status["agent"]["status"] == "running"
        assert len(runtime.create_calls) == 2

    asyncio.run(run())


def test_creation_in_flight_becomes_failed_uncertain_after_restart(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, _parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET chat_id=NULL,status='creation_in_flight' WHERE agent_id=?",
                (child_id,),
            )
            db.execute(
                "UPDATE tasks SET status='creation_in_flight' WHERE agent_id=?",
                (child_id,),
            )
        restarted = coordinator(tmp_path, runtime)
        status = await restarted.status(child_id)
        assert status["agent"]["status"] == "failed"
        assert status["task"]["status"] == "failed"
        assert "stopped while creating" in status["agent"]["last_error"]
        assert any(
            event["kind"] == "failed"
            for event in (await restarted.wait(child_id))["events"]
        )
        assert len(runtime.create_calls) == 1

    asyncio.run(run())


def test_delivery_in_flight_becomes_uncertain_after_restart(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        command = await service.send(
            parent["agent_id"], child["agent"]["agent_id"], "message"
        )
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='delivery_in_flight' WHERE command_id=?",
                (command["command_id"],),
            )
        restarted = coordinator(tmp_path, runtime)
        with restarted.repository.connect() as db:
            row = db.execute(
                "SELECT status,last_error FROM commands WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()
        assert row["status"] == "delivery_uncertain"
        assert "stopped during" in row["last_error"]
        await restarted.sync_once()
        assert runtime.continue_calls == []

    asyncio.run(run())


def test_continuation_in_flight_is_not_retried_after_restart(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, _parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        current_node = runtime.snapshots["child-chat-1"]["current_node"]
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE tasks SET status='continuation_in_flight',continue_attempts=1,last_continue_node=?,last_continue_at=1 WHERE agent_id=?",
                (current_node, child_id),
            )
            db.execute(
                "UPDATE agents SET status='continuation_in_flight' WHERE agent_id=?",
                (child_id,),
            )
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        restarted = coordinator(tmp_path, runtime)
        status = await restarted.status(child_id)
        assert status["task"]["status"] == "continuation_uncertain"
        assert status["agent"]["status"] == "error"
        result = await restarted.sync_once()
        assert result["write_count"] == 0
        assert runtime.continue_calls == []
        assert (await restarted.status(child_id))["task"]["status"] == "waiting_after_continue"

    asyncio.run(run())


def test_external_writes_are_preceded_by_in_flight_persistence(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")

        def check_creation():
            with service.repository.connect() as db:
                row = db.execute(
                    "SELECT a.status AS agent_status,t.status AS task_status "
                    "FROM agents a JOIN tasks t ON t.agent_id=a.agent_id "
                    "WHERE a.parent_agent_id=? ORDER BY a.created_at DESC LIMIT 1",
                    (parent["agent_id"],),
                ).fetchone()
            assert row["agent_status"] == "creation_in_flight"
            assert row["task_status"] == "creation_in_flight"

        runtime.before_create = check_creation
        child = await service.spawn(parent["agent_id"], "task")
        child_id = child["agent"]["agent_id"]
        runtime.before_create = None
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")

        command = await service.send(parent["agent_id"], child_id, "follow-up")

        def check_delivery(_conversation_id):
            with service.repository.connect() as db:
                row = db.execute(
                    "SELECT status FROM commands WHERE command_id=?",
                    (command["command_id"],),
                ).fetchone()
            assert row["status"] == "delivery_in_flight"

        runtime.before_continue = check_delivery
        assert (await service.sync_once())["write_count"] == 1
        runtime.before_continue = None

        next_snapshot = terminal_snapshot("child-chat-1")
        next_snapshot["current_node"] = "assistant-second"
        next_snapshot["turns"][-1]["key"] = "assistant-second"
        runtime.snapshots["child-chat-1"] = next_snapshot

        def check_continuation(_conversation_id):
            with service.repository.connect() as db:
                row = db.execute(
                    "SELECT status FROM tasks WHERE agent_id=?",
                    (child_id,),
                ).fetchone()
            assert row["status"] == "continuation_in_flight"

        runtime.before_continue = check_continuation
        assert (await service.sync_once())["write_count"] == 1

    asyncio.run(run())


def test_unavailable_interrupt_is_attempted_once_then_waits_for_terminal(tmp_path):
    async def run():
        runtime = FakeRuntime()
        runtime.cancel_result = {
            "cancelled": False,
            "reason": "no owned stream handle",
        }
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", running=True
        )
        command = await service.send(
            parent["agent_id"],
            child_id,
            "urgent correction",
            interrupt_policy="interrupt",
        )
        assert (await service.sync_once())["write_count"] == 1
        assert runtime.cancel_calls == ["child-chat-1"]
        with service.repository.connect() as db:
            row = db.execute(
                "SELECT status,last_error FROM commands WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()
        assert row["status"] == "waiting_after_cancel"
        assert "no owned stream" in row["last_error"]

        assert (await service.sync_once())["write_count"] == 0
        assert runtime.cancel_calls == ["child-chat-1"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        assert (await service.sync_once())["write_count"] == 1
        assert runtime.continue_calls[-1][0] == "child-chat-1"

    asyncio.run(run())


def test_cancel_in_flight_becomes_cancelled_after_restart(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, _parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET status='cancel_in_flight' WHERE agent_id=?",
                (child_id,),
            )
            db.execute(
                "UPDATE tasks SET status='cancel_in_flight' WHERE agent_id=?",
                (child_id,),
            )
        restarted = coordinator(tmp_path, runtime)
        status = await restarted.status(child_id)
        assert status["agent"]["status"] == "cancelled"
        assert status["task"]["status"] == "cancelled"
        assert any(
            event["kind"] == "cancelled"
            for event in (await restarted.wait(child_id))["events"]
        )

    asyncio.run(run())


def test_direct_cancel_persists_in_flight_before_runtime_call(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, _parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]

        def check_cancel(_conversation_id):
            with service.repository.connect() as db:
                row = db.execute(
                    "SELECT a.status AS agent_status,t.status AS task_status "
                    "FROM agents a JOIN tasks t ON t.agent_id=a.agent_id "
                    "WHERE a.agent_id=?",
                    (child_id,),
                ).fetchone()
            assert row["agent_status"] == "cancel_in_flight"
            assert row["task_status"] == "cancel_in_flight"

        runtime.before_cancel = check_cancel
        result = await service.cancel(child_id, interrupt=True)
        assert result["cancelled"] is True
        assert (await service.status(child_id))["agent"]["status"] == "cancelled"

    asyncio.run(run())


def test_cancelling_parent_drops_and_acks_queued_child_notifications(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path, runtime, notification_policy="auto_resume"
        )
        child_id = child["agent"]["agent_id"]
        runtime.context_events["child-chat-1"] = [
            {"cursor": "done-event", "kind": "final", "summary": "done"}
        ]
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1",
            marker=f"done\n{DEFAULT_AGENT_COMPLETION_MARKER}",
        )
        runtime.snapshots["parent-chat"] = terminal_snapshot(
            "parent-chat", running=True
        )
        assert (await service.sync_once())["write_count"] == 0
        parent_status = await service.status(parent["agent_id"] )
        assert parent_status["mailbox"]["queued"] == 1
        events = (await service.wait(child_id))["events"]
        last_seq = max(event["event_seq"] for event in events)

        await service.cancel(parent["agent_id"])
        with service.repository.connect() as db:
            acked = db.execute(
                "SELECT last_acked_event_seq FROM subscriptions "
                "WHERE parent_agent_id=? AND child_agent_id=?",
                (parent["agent_id"], child_id),
            ).fetchone()[0]
        assert int(acked) == last_seq
        assert service.has_pending_work() is False

    asyncio.run(run())


def test_uncertain_parent_notification_reconciles_from_side_branch(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path, runtime, notification_policy="auto_resume"
        )
        child_id = child["agent"]["agent_id"]
        runtime.context_events["child-chat-1"] = [
            {"cursor": "progress-1", "kind": "progress", "summary": "working"}
        ]
        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        runtime.continue_results["parent-chat"] = {
            "sent": True,
            "observed": False,
            "running": True,
            "reason": "active branch changed before confirmation",
            "request_id": "request-parent",
            "user_message_id": "generated-parent-message",
            "parent_message_id": "a-parent-chat",
        }

        first = await service.sync_once()
        assert first["write_count"] == 1
        with service.repository.connect() as db:
            command = dict(
                db.execute(
                    "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=? "
                    "AND purpose='progress'",
                    (child_id, parent["agent_id"]),
                ).fetchone()
            )
        assert command["status"] == "delivery_uncertain"
        assert command["user_message_id"] == "generated-parent-message"

        parent_snapshot = terminal_snapshot("parent-chat")
        parent_snapshot["all_message_ids"] = ["generated-parent-message"]
        runtime.snapshots["parent-chat"] = parent_snapshot
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET next_attempt_at=0 WHERE command_id=?",
                (command["command_id"],),
            )
        second = await service.sync_once()
        assert second["write_count"] == 1

        with service.repository.connect() as db:
            status = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()[0]
            acked = db.execute(
                "SELECT last_acked_event_seq FROM subscriptions "
                "WHERE parent_agent_id=? AND child_agent_id=?",
                (parent["agent_id"], child_id),
            ).fetchone()[0]
        assert status == "delivered"
        assert int(acked) == int(command["ack_event_seq"])
        assert len(runtime.continue_calls) == 1

    asyncio.run(run())


def test_restart_preserves_send_evidence_and_reconciles_without_resend(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        command = await service.send(parent["agent_id"], child_id, "follow-up")
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='delivery_in_flight',request_id=?,"
                "user_message_id=?,parent_message_id=? WHERE command_id=?",
                ("request-1", "persisted-user-message", "a-child-chat-1", command["command_id"]),
            )

        child_snapshot = terminal_snapshot("child-chat-1")
        child_snapshot["all_message_ids"] = ["persisted-user-message"]
        runtime.snapshots["child-chat-1"] = child_snapshot
        restarted = coordinator(tmp_path, runtime)
        with restarted.repository.connect() as db:
            recovered = dict(
                db.execute(
                    "SELECT * FROM commands WHERE command_id=?",
                    (command["command_id"],),
                ).fetchone()
            )
        assert recovered["status"] == "delivery_uncertain"
        assert recovered["request_id"] == "request-1"
        assert recovered["user_message_id"] == "persisted-user-message"

        runtime.get_calls.clear()
        cooling_down = await restarted.sync_once()
        assert cooling_down["write_count"] == 0
        assert runtime.get_calls == []
        with restarted.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET next_attempt_at=0 WHERE command_id=?",
                (command["command_id"],),
            )

        result = await restarted.sync_once()
        assert result["write_count"] == 1
        assert runtime.get_calls == ["child-chat-1"]
        with restarted.repository.connect() as db:
            final_status = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (command["command_id"],),
            ).fetchone()[0]
        assert final_status == "delivered"
        assert runtime.continue_calls == []

    asyncio.run(run())


def test_completion_supersedes_uncertain_progress_and_drains_cursor(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path,
            runtime,
            notification_policy="auto_resume",
            completion_marker="DONE",
        )
        child_id = child["agent"]["agent_id"]
        runtime.context_events["child-chat-1"] = [
            {"cursor": "progress-1", "kind": "progress", "summary": "working"}
        ]
        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        runtime.continue_results["parent-chat"] = {
            "sent": True,
            "observed": False,
            "running": True,
            "reason": "confirmation timed out",
            "user_message_id": "uncertain-progress-message",
            "parent_message_id": "a-parent-chat",
        }
        await service.sync_once()

        with service.repository.connect() as db:
            progress = dict(
                db.execute(
                    "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=? "
                    "AND purpose='progress'",
                    (child_id, parent["agent_id"]),
                ).fetchone()
            )
        assert progress["status"] == "delivery_uncertain"

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", marker="final result\nDONE"
        )
        runtime.continue_results["parent-chat"] = {
            "sent": True,
            "observed": True,
            "running": True,
            "reason": "",
            "user_message_id": "completion-message",
            "parent_message_id": "a-parent-chat",
        }
        second = await service.sync_once()
        assert second["write_count"] == 1

        with service.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=? "
                    "ORDER BY sequence_no",
                    (child_id, parent["agent_id"]),
                )
            ]
            acked = int(
                db.execute(
                    "SELECT last_acked_event_seq FROM subscriptions "
                    "WHERE parent_agent_id=? AND child_agent_id=?",
                    (parent["agent_id"], child_id),
                ).fetchone()[0]
            )
        assert [row["purpose"] for row in rows] == ["progress", "completion"]
        assert rows[0]["status"] == "superseded"
        assert rows[1]["status"] == "delivered"
        assert acked == int(rows[1]["ack_event_seq"])
        assert '"kind":"completed"' in rows[1]["message"]

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        third = await service.sync_once()
        assert third["write_count"] == 0
        with service.repository.connect() as db:
            completion_count = db.execute(
                "SELECT COUNT(*) FROM commands WHERE from_agent_id=? AND to_agent_id=? "
                "AND purpose='completion'",
                (child_id, parent["agent_id"]),
            ).fetchone()[0]
        assert completion_count == 1
        assert len(runtime.continue_calls) == 2

    asyncio.run(run())


def test_completion_envelope_is_selected_even_after_oversized_progress_history(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = ChatAgentCoordinator(
            CoordinatorConfig(
                tmp_path / "agents.db",
                context_max_events=60,
                context_max_chars=500,
            ),
            lambda: runtime,
        )
        parent = await service.register_parent("parent-chat")
        child = await service.spawn(
            parent["agent_id"],
            "long child task",
            notification_policy="auto_resume",
        )
        child_id = child["agent"]["agent_id"]
        task_id = child["task"]["task_id"]
        for index in range(10):
            service._emit_event(
                child_id,
                task_id,
                "progress",
                {"summary": f"{index}:" + "x" * 300},
                source_cursor=f"large-progress-{index}",
            )
        completion_id = service._emit_event(
            child_id,
            task_id,
            "completed",
            {"summary": "final"},
            source_cursor="large-completed",
        )
        assert completion_id is not None

        service._queue_parent_notifications()
        with service.repository.connect() as db:
            commands = [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM commands WHERE from_agent_id=? AND to_agent_id=?",
                    (child_id, parent["agent_id"]),
                )
            ]
            completion_seq = int(
                db.execute(
                    "SELECT event_seq FROM events WHERE event_id=?",
                    (completion_id,),
                ).fetchone()[0]
            )
        assert len(commands) == 1
        assert commands[0]["purpose"] == "completion"
        assert int(commands[0]["ack_event_seq"]) == completion_seq
        assert '"kind":"completed"' in commands[0]["message"]

    asyncio.run(run())


def test_blocking_question_pauses_child_until_parent_answer(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")

        question = await service.send(
            child_id,
            parent["agent_id"],
            "Which migration strategy should I use?",
            purpose="question",
            idempotency_key="blocking-question-1",
        )
        assert question["purpose"] == "question"
        child_status = await service.status(child_id)
        assert child_status["agent"]["status"] == "waiting_for_parent"
        assert child_status["task"]["status"] == "waiting_for_parent"

        first = await service.sync_once()
        assert first["write_count"] == 1
        assert runtime.continue_calls[-1][0] == "parent-chat"
        assert runtime.continue_calls[-1][1] == "Which migration strategy should I use?"
        child_status = await service.status(child_id)
        assert child_status["task"]["status"] == "waiting_for_parent"
        assert all(
            not (call[0] == "child-chat-1" and "Continue working" in call[1])
            for call in runtime.continue_calls
        )

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        answer = await service.send(
            parent["agent_id"],
            child_id,
            "Use the backward-compatible two-step migration.",
            purpose="answer",
            idempotency_key="answer-1",
        )
        assert answer["purpose"] == "answer"

        second = await service.sync_once()
        assert second["write_count"] == 1
        assert runtime.continue_calls[-1][0] == "child-chat-1"
        assert runtime.continue_calls[-1][1] == (
            "Use the backward-compatible two-step migration."
        )
        child_status = await service.status(child_id)
        assert child_status["agent"]["status"] == "waiting_assistant"
        assert child_status["task"]["status"] == "waiting_assistant"
        assert len(
            [call for call in runtime.continue_calls if call[0] == "child-chat-1"]
        ) == 1

    asyncio.run(run())


def test_question_and_answer_require_direct_parent_child_direction(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]

        with pytest.raises(ValueError, match="question must be sent"):
            await service.send(
                parent["agent_id"], child_id, "wrong direction", purpose="question"
            )
        with pytest.raises(ValueError, match="answer must be sent"):
            await service.send(
                child_id, parent["agent_id"], "wrong direction", purpose="answer"
            )
        with pytest.raises(ValueError, match="purpose must be"):
            await service.send(
                parent["agent_id"], child_id, "unknown", purpose="unsupported"
            )

    asyncio.run(run())


def test_blocking_question_and_answer_survive_coordinator_restart(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        question = await service.send(
            child_id,
            parent["agent_id"],
            "Need a decision",
            purpose="question",
            idempotency_key="restart-question",
        )

        restarted = coordinator(tmp_path, runtime)
        assert (await restarted.status(child_id))["task"]["status"] == (
            "waiting_for_parent"
        )
        assert (await restarted.sync_once())["write_count"] == 1
        assert runtime.continue_calls[-1][1] == "Need a decision"

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        answer = await restarted.send(
            parent["agent_id"],
            child_id,
            "Proceed with option A",
            purpose="answer",
            idempotency_key="restart-answer",
        )
        again = coordinator(tmp_path, runtime)
        assert (await again.sync_once())["write_count"] == 1
        assert runtime.continue_calls[-1][1] == "Proceed with option A"
        with again.repository.connect() as db:
            statuses = {
                row["command_id"]: row["status"]
                for row in db.execute(
                    "SELECT command_id,status FROM commands WHERE command_id IN (?,?)",
                    (question["command_id"], answer["command_id"]),
                )
            }
        assert statuses == {
            question["command_id"]: "delivered",
            answer["command_id"]: "delivered",
        }

    asyncio.run(run())


def test_cancel_in_flight_command_recovers_without_repeating_cancel(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        command = await service.send(
            parent["agent_id"],
            child_id,
            "urgent correction",
            interrupt_policy="interrupt",
        )
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='cancel_in_flight' WHERE command_id=?",
                (command["command_id"],),
            )

        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", running=True
        )
        restarted = coordinator(tmp_path, runtime)
        with restarted.repository.connect() as db:
            recovered = dict(
                db.execute(
                    "SELECT status,last_error FROM commands WHERE command_id=?",
                    (command["command_id"],),
                ).fetchone()
            )
        assert recovered["status"] == "waiting_after_cancel"
        assert "waiting for a verified terminal target" in recovered["last_error"]

        assert (await restarted.sync_once())["write_count"] == 0
        assert runtime.cancel_calls == []
        assert runtime.continue_calls == []

        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        assert (await restarted.sync_once())["write_count"] == 1
        assert runtime.cancel_calls == []
        assert runtime.continue_calls[-1][1] == "urgent correction"

    asyncio.run(run())


def test_uncertain_command_blocks_later_command_for_same_target(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        first = await service.send(
            parent["agent_id"], child_id, "first", idempotency_key="ordered-first"
        )
        second = await service.send(
            parent["agent_id"], child_id, "second", idempotency_key="ordered-second"
        )
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='delivery_uncertain' WHERE command_id=?",
                (first["command_id"],),
            )

        result = await service.sync_once()
        assert result["write_count"] == 0
        assert runtime.continue_calls == []

        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='cancelled' WHERE command_id=?",
                (first["command_id"],),
            )
        assert (await service.sync_once())["write_count"] == 1
        assert runtime.continue_calls == [
            ("child-chat-1", "second", "a-child-chat-1")
        ]
        with service.repository.connect() as db:
            status = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (second["command_id"],),
            ).fetchone()[0]
        assert status == "delivered"

    asyncio.run(run())


def test_default_child_update_is_progress_and_completion_supersedes_it(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path,
            runtime,
            notification_policy="auto_resume",
            completion_marker="DONE",
        )
        child_id = child["agent"]["agent_id"]
        update = await service.send(
            child_id,
            parent["agent_id"],
            "non-blocking child update",
            idempotency_key="child-update",
        )
        assert update["purpose"] == "progress"

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", marker="finished\nDONE"
        )
        result = await service.sync_once()
        assert result["write_count"] == 1

        with service.repository.connect() as db:
            rows = [
                dict(row)
                for row in db.execute(
                    "SELECT purpose,status,message FROM commands "
                    "WHERE from_agent_id=? AND to_agent_id=? ORDER BY sequence_no",
                    (child_id, parent["agent_id"]),
                )
            ]
        assert rows[0]["purpose"] == "progress"
        assert rows[0]["status"] == "superseded"
        assert rows[1]["purpose"] == "completion"
        assert rows[1]["status"] == "delivered"
        assert runtime.continue_calls[-1][1].startswith("[SUBAGENT UPDATE]")
        assert all(call[1] != "non-blocking child update" for call in runtime.continue_calls)

    asyncio.run(run())


def test_completion_supersedes_undelivered_blocking_question(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path,
            runtime,
            notification_policy="auto_resume",
            completion_marker="DONE",
        )
        child_id = child["agent"]["agent_id"]
        question = await service.send(
            child_id,
            parent["agent_id"],
            "This question became obsolete",
            purpose="question",
            idempotency_key="obsolete-question",
        )
        assert question["purpose"] == "question"

        runtime.snapshots["parent-chat"] = terminal_snapshot("parent-chat")
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", marker="resolved independently\nDONE"
        )
        assert (await service.sync_once())["write_count"] == 1
        with service.repository.connect() as db:
            statuses = [
                tuple(row)
                for row in db.execute(
                    "SELECT purpose,status FROM commands WHERE from_agent_id=? "
                    "AND to_agent_id=? ORDER BY sequence_no",
                    (child_id, parent["agent_id"]),
                )
            ]
        assert statuses == [
            ("question", "superseded"),
            ("completion", "delivered"),
        ]
        assert all(
            call[1] != "This question became obsolete"
            for call in runtime.continue_calls
        )

    asyncio.run(run())


def test_delivery_uncertainty_keeps_background_reconciliation_awake(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path, runtime, notification_policy="notify_only"
        )
        command = await service.send(
            parent["agent_id"], child["agent"]["agent_id"], "message"
        )
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='delivery_uncertain' WHERE command_id=?",
                (command["command_id"],),
            )
            db.execute(
                "UPDATE agents SET status='completed' WHERE agent_id IN (?,?)",
                (parent["agent_id"], child["agent"]["agent_id"]),
            )
            db.execute(
                "UPDATE tasks SET status='completed',completed_at=1 WHERE agent_id=?",
                (child["agent"]["agent_id"],),
            )
        assert service.has_pending_work() is True

    asyncio.run(run())


def test_legacy_uncertain_without_evidence_is_cancelled_without_remote_read(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        legacy = await service.send(parent["agent_id"], child_id, "legacy")
        current = await service.send(parent["agent_id"], child_id, "current")
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='delivery_uncertain',user_message_id=NULL,"
                "parent_message_id=NULL,request_id=NULL,next_attempt_at=0 "
                "WHERE command_id=?",
                (legacy["command_id"],),
            )
        runtime.get_calls.clear()

        first = await service.sync_once()

        assert first["write_count"] == 0
        assert runtime.get_calls == []
        with service.repository.connect() as db:
            legacy_row = dict(
                db.execute(
                    "SELECT status,last_error FROM commands WHERE command_id=?",
                    (legacy["command_id"],),
                ).fetchone()
            )
        assert legacy_row["status"] == "cancelled"
        assert "lacks durable reconciliation evidence" in legacy_row["last_error"]

        second = await service.sync_once()
        assert second["write_count"] == 1
        assert runtime.continue_calls[-1][1] == "current"
        with service.repository.connect() as db:
            current_status = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (current["command_id"],),
            ).fetchone()[0]
        assert current_status == "delivered"

    asyncio.run(run())


def test_terminal_parent_cancels_question_and_fails_waiting_child(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(
            tmp_path, runtime, notification_policy="notify_only"
        )
        child_id = child["agent"]["agent_id"]
        question = await service.send(
            child_id,
            parent["agent_id"],
            "I cannot continue without this answer",
            purpose="question",
        )
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET status='completed' WHERE agent_id=?",
                (parent["agent_id"],),
            )

        await service.sync_once()
        status = await service.status(child_id)
        assert status["agent"]["status"] == "failed"
        assert status["task"]["status"] == "failed"
        assert "terminal parent" in status["agent"]["last_error"]
        with service.repository.connect() as db:
            command_status = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (question["command_id"],),
            ).fetchone()[0]
        assert command_status == "cancelled"
        events = (await service.wait(child_id))["events"]
        assert any(
            event["kind"] == "blocked"
            and "terminal parent" in event["payload"]["reason"]
            for event in events
        )

    asyncio.run(run())


def test_scheduled_sync_does_not_open_runtime_before_due(tmp_path):
    async def run():
        runtime = FakeRuntime()
        coordinator_instance, _parent, _child = await make_parent_and_child(
            tmp_path,
            runtime,
        )
        runtime.enter_calls = 0
        service = ChatAgentService(coordinator_instance, interval_seconds=600)

        result = await service.sync_now(force=False)

        assert result == {"status": "idle", "write_count": 0, "inspected": []}
        assert runtime.enter_calls == 0

    asyncio.run(run())


def test_background_service_survives_wait_calculation_error():
    class Coordinator:
        def prepare_local_work(self):
            pass

        def next_due_at(self):
            return None

    async def run():
        service = ChatAgentService(Coordinator(), interval_seconds=30)
        calls = 0

        def flaky_wait_seconds():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary scheduler error")
            return 0.01

        service._next_wait_seconds = flaky_wait_seconds
        await service.start()
        try:
            await asyncio.sleep(0.05)
            assert service._task is not None
            assert not service._task.done()
        finally:
            await service.stop()

    asyncio.run(run())


def test_scheduled_sync_reads_only_due_child_once_when_unchanged(tmp_path):
    async def run():
        runtime = FakeRuntime()
        coordinator_instance, parent, child = await make_parent_and_child(
            tmp_path,
            runtime,
        )
        child_id = child["agent"]["agent_id"]
        await coordinator_instance.sync_once()
        runtime.get_calls.clear()
        runtime.context_calls.clear()
        with coordinator_instance.repository.transaction() as db:
            db.execute(
                "UPDATE tasks SET next_check_at=0 WHERE agent_id=?",
                (child_id,),
            )

        result = await ChatAgentService(coordinator_instance).sync_now(force=False)

        assert result["inspected"] == [{"agent_id": child_id, "status": "running"}]
        assert runtime.get_calls == ["child-chat-1"]
        assert runtime.context_calls == []
        assert "parent-chat" not in runtime.get_calls
        assert parent["agent_id"] != child_id

    asyncio.run(run())


def test_scheduled_sync_fetches_context_only_after_structural_change(tmp_path):
    async def run():
        runtime = FakeRuntime()
        coordinator_instance, _parent, child = await make_parent_and_child(
            tmp_path,
            runtime,
        )
        child_id = child["agent"]["agent_id"]
        await coordinator_instance.sync_once()
        changed = terminal_snapshot("child-chat-1", running=True)
        changed["current_node"] = "a-child-chat-1-progress"
        changed["turns"][-1]["key"] = "a-child-chat-1-progress"
        runtime.snapshots["child-chat-1"] = changed
        runtime.get_calls.clear()
        runtime.context_calls.clear()
        with coordinator_instance.repository.transaction() as db:
            db.execute(
                "UPDATE tasks SET next_check_at=0 WHERE agent_id=?",
                (child_id,),
            )

        await ChatAgentService(coordinator_instance).sync_now(force=False)

        assert runtime.get_calls == ["child-chat-1"]
        assert runtime.context_calls == ["child-chat-1"]

    asyncio.run(run())


def test_busy_command_is_deferred_without_reopening_runtime(tmp_path):
    async def run():
        runtime = FakeRuntime()
        coordinator_instance, parent, child = await make_parent_and_child(
            tmp_path,
            runtime,
        )
        child_id = child["agent"]["agent_id"]
        await coordinator_instance.sync_once()
        await coordinator_instance.send(
            parent["agent_id"],
            child_id,
            "queued while running",
        )
        service = ChatAgentService(coordinator_instance)

        first = await service.sync_now(force=False)
        assert first["write_count"] == 0
        runtime.enter_calls = 0
        runtime.get_calls.clear()

        second = await service.sync_now(force=False)

        assert second == {"status": "idle", "write_count": 0, "inspected": []}
        assert runtime.enter_calls == 0
        assert runtime.get_calls == []
        with coordinator_instance.repository.connect() as db:
            next_attempt_at = db.execute(
                "SELECT next_attempt_at FROM commands WHERE to_agent_id=?",
                (child_id,),
            ).fetchone()[0]
        assert float(next_attempt_at) > 0

    asyncio.run(run())


def test_waiting_for_parent_task_is_not_scheduled_after_question_delivery(tmp_path):
    async def run():
        runtime = FakeRuntime()
        coordinator_instance, parent, child = await make_parent_and_child(
            tmp_path,
            runtime,
        )
        child_id = child["agent"]["agent_id"]
        await coordinator_instance.send(
            child_id,
            parent["agent_id"],
            "Need one answer",
            purpose="question",
        )
        await coordinator_instance.sync_once()
        runtime.enter_calls = 0
        runtime.get_calls.clear()

        result = await ChatAgentService(coordinator_instance).sync_now(force=False)

        assert result == {"status": "idle", "write_count": 0, "inspected": []}
        assert runtime.enter_calls == 0
        assert runtime.get_calls == []
        assert (await coordinator_instance.status(child_id))["task"]["status"] == "waiting_for_parent"

    asyncio.run(run())


def test_delivery_identity_is_persisted_before_dispatch_and_survives_failure(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        command = await service.send(parent["agent_id"], child_id, "follow-up")
        observed = {}

        def before_continue(_conversation_id):
            with service.repository.connect() as db:
                observed.update(
                    dict(
                        db.execute(
                            "SELECT status,user_message_id,parent_message_id "
                            "FROM commands WHERE command_id=?",
                            (command["command_id"],),
                        ).fetchone()
                    )
                )

        runtime.before_continue = before_continue
        runtime.continue_errors.add("child-chat-1")

        result = await service.sync_once()

        assert result["write_count"] == 1
        assert observed["status"] == "delivery_in_flight"
        assert observed["user_message_id"]
        assert observed["parent_message_id"] == "a-child-chat-1"
        with service.repository.connect() as db:
            stored = dict(
                db.execute(
                    "SELECT status,user_message_id,parent_message_id,next_attempt_at "
                    "FROM commands WHERE command_id=?",
                    (command["command_id"],),
                ).fetchone()
            )
        assert stored["status"] == "delivery_uncertain"
        assert stored["user_message_id"] == observed["user_message_id"]
        assert stored["parent_message_id"] == observed["parent_message_id"]
        assert float(stored["next_attempt_at"]) > 0
        assert runtime.continue_message_ids == [stored["user_message_id"]]

    asyncio.run(run())


def test_verified_absence_requeues_and_reuses_the_same_delivery_identity(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot("child-chat-1")
        command = await service.send(parent["agent_id"], child_id, "follow-up")
        runtime.continue_errors.add("child-chat-1")

        assert (await service.sync_once())["write_count"] == 1
        with service.repository.connect() as db:
            uncertain = dict(
                db.execute(
                    "SELECT status,user_message_id FROM commands WHERE command_id=?",
                    (command["command_id"],),
                ).fetchone()
            )
        stable_id = uncertain["user_message_id"]
        assert uncertain["status"] == "delivery_uncertain"
        assert stable_id

        runtime.continue_errors.clear()
        runtime.get_calls.clear()

        cooling_down = await service.sync_once()
        assert cooling_down["write_count"] == 0
        assert runtime.get_calls == []
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET next_attempt_at=0 WHERE command_id=?",
                (command["command_id"],),
            )

        reconciled = await service.sync_once()
        assert reconciled["write_count"] == 0
        assert runtime.get_calls == ["child-chat-1"]
        with service.repository.connect() as db:
            requeued = dict(
                db.execute(
                    "SELECT status,user_message_id,next_attempt_at,last_error "
                    "FROM commands WHERE command_id=?",
                    (command["command_id"],),
                ).fetchone()
            )
        assert requeued["status"] == "queued"
        assert requeued["user_message_id"] == stable_id
        assert float(requeued["next_attempt_at"]) == 0
        assert "not found" in requeued["last_error"]

        delivered = await service.sync_once()
        assert delivered["write_count"] == 1
        assert runtime.continue_message_ids == [stable_id, stable_id]
        with service.repository.connect() as db:
            final = dict(
                db.execute(
                    "SELECT status,user_message_id FROM commands WHERE command_id=?",
                    (command["command_id"],),
                ).fetchone()
            )
        assert final == {"status": "delivered", "user_message_id": stable_id}

    asyncio.run(run())


def test_scheduled_sync_spreads_multiple_due_children_across_cycles(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        first = await service.spawn(parent["agent_id"], "one")
        second = await service.spawn(parent["agent_id"], "two")
        first_id = first["agent"]["agent_id"]
        second_id = second["agent"]["agent_id"]
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE tasks SET next_check_at=0 WHERE agent_id IN (?,?)",
                (first_id, second_id),
            )
        runtime.get_calls.clear()
        runtime.context_calls.clear()

        result = await ChatAgentService(service).sync_now(force=False)

        assert len(result["inspected"]) == 1
        assert result["inspected"][0]["agent_id"] == first_id
        assert runtime.get_calls == ["child-chat-1"]
        assert runtime.context_calls == ["child-chat-1"]
        assert second_id != first_id

    asyncio.run(run())



def test_manual_sync_also_limits_monitoring_to_one_conversation(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent("parent-chat")
        first = await service.spawn(parent["agent_id"], "one")
        second = await service.spawn(parent["agent_id"], "two")
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE tasks SET next_check_at=0 WHERE agent_id IN (?,?)",
                (first["agent"]["agent_id"], second["agent"]["agent_id"]),
            )
        runtime.get_calls.clear()
        runtime.context_calls.clear()

        result = await service.sync_once()

        assert len(result["inspected"]) == 1
        assert runtime.get_calls == ["child-chat-1"]
        assert runtime.context_calls == ["child-chat-1"]

    asyncio.run(run())


def test_uncertain_creation_with_known_chat_id_is_reconciled_without_duplicate(tmp_path):
    async def run():
        runtime = FakeRuntime()
        runtime.create_result = {
            "observed": False,
            "reason": "created conversation could not be read back",
        }
        service = coordinator(tmp_path, runtime)
        parent = await service.register_parent(
            "parent-chat", project_id="g-p-project"
        )
        child = await service.spawn(
            parent["agent_id"],
            "child task",
            project_policy="explicit",
            project_id="g-p-project",
            idempotency_key="uncertain-spawn",
        )
        child_id = child["agent"]["agent_id"]
        assert child["agent"]["chat_id"] == "child-chat-1"
        assert child["agent"]["status"] == "creation_uncertain"
        assert child["task"]["status"] == "creation_uncertain"
        assert len(runtime.create_calls) == 1

        with service.repository.transaction() as db:
            db.execute(
                "UPDATE tasks SET next_check_at=0 WHERE agent_id=?",
                (child_id,),
            )
        result = await service.sync_once()
        assert result["write_count"] == 0
        recovered = await service.status(child_id)
        assert recovered["agent"]["status"] == "running"
        assert recovered["task"]["status"] == "running"
        assert recovered["agent"]["chat_id"] == "child-chat-1"
        assert len(runtime.create_calls) == 1
        assert any(
            event["kind"] == "started"
            and event["payload"].get("recovered") is True
            for event in (await service.wait(child_id))["events"]
        )

    asyncio.run(run())


def test_restart_preserves_known_creation_in_flight_for_reconciliation(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, _parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE agents SET status='creation_in_flight' WHERE agent_id=?",
                (child_id,),
            )
            db.execute(
                "UPDATE tasks SET status='creation_in_flight' WHERE agent_id=?",
                (child_id,),
            )

        restarted = coordinator(tmp_path, runtime)
        uncertain = await restarted.status(child_id)
        assert uncertain["agent"]["status"] == "creation_uncertain"
        assert uncertain["task"]["status"] == "creation_uncertain"
        assert uncertain["agent"]["chat_id"] == "child-chat-1"
        assert len(runtime.create_calls) == 1

        assert (await restarted.sync_once())["write_count"] == 0
        recovered = await restarted.status(child_id)
        assert recovered["agent"]["status"] == "running"
        assert recovered["task"]["status"] == "running"
        assert len(runtime.create_calls) == 1

    asyncio.run(run())


def test_send_idempotency_replay_precedes_terminal_target_validation(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        command = await service.send(
            parent["agent_id"],
            child_id,
            "queued work",
            idempotency_key="terminal-replay",
        )
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='cancelled' WHERE command_id=?",
                (command["command_id"],),
            )
            db.execute(
                "UPDATE agents SET status='completed' WHERE agent_id=?",
                (child_id,),
            )
            db.execute(
                "UPDATE tasks SET status='completed',completed_at=1 WHERE agent_id=?",
                (child_id,),
            )

        replay = await service.send(
            parent["agent_id"],
            child_id,
            "different retry body",
            idempotency_key="terminal-replay",
        )
        assert replay["command_id"] == command["command_id"]
        assert replay["status"] == "cancelled"
        assert replay["message"] == "queued work"

    asyncio.run(run())


def test_interrupt_supersedes_older_unsent_queue_instruction(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        runtime.snapshots["child-chat-1"] = terminal_snapshot(
            "child-chat-1", running=True
        )
        queued = await service.send(
            parent["agent_id"],
            child_id,
            "older queued work",
            interrupt_policy="queue",
            idempotency_key="older-queue",
        )
        interrupt = await service.send(
            parent["agent_id"],
            child_id,
            "direct steering",
            interrupt_policy="interrupt",
            idempotency_key="direct-steering",
        )

        with service.repository.connect() as db:
            older = dict(
                db.execute(
                    "SELECT status,last_error FROM commands WHERE command_id=?",
                    (queued["command_id"],),
                ).fetchone()
            )
        assert older == {
            "status": "superseded",
            "last_error": "superseded by direct interrupt steering",
        }

        result = await service.sync_once()
        assert result["write_count"] == 2
        assert runtime.cancel_calls == ["child-chat-1"]
        assert runtime.continue_calls == [
            ("child-chat-1", "direct steering", "a-child-chat-1")
        ]
        with service.repository.connect() as db:
            delivered = db.execute(
                "SELECT status FROM commands WHERE command_id=?",
                (interrupt["command_id"],),
            ).fetchone()[0]
        assert delivered == "delivered"

    asyncio.run(run())


def test_reconciled_instruction_reopens_completed_target(tmp_path):
    async def run():
        runtime = FakeRuntime()
        service, parent, child = await make_parent_and_child(tmp_path, runtime)
        child_id = child["agent"]["agent_id"]
        command = await service.send(
            parent["agent_id"],
            child_id,
            "queued next turn",
            idempotency_key="reconciled-next-turn",
        )
        with service.repository.transaction() as db:
            db.execute(
                "UPDATE commands SET status='delivery_uncertain',user_message_id=?,"
                "parent_message_id=?,next_attempt_at=0 WHERE command_id=?",
                ("persisted-next-turn", "a-child-chat-1", command["command_id"]),
            )
            db.execute(
                "UPDATE agents SET status='completed' WHERE agent_id=?",
                (child_id,),
            )
            db.execute(
                "UPDATE tasks SET status='completed',completed_at=123 WHERE agent_id=?",
                (child_id,),
            )
        snapshot = terminal_snapshot("child-chat-1")
        snapshot["all_message_ids"] = ["persisted-next-turn"]
        runtime.snapshots["child-chat-1"] = snapshot

        result = await service.sync_once()
        assert result["write_count"] == 1
        assert runtime.continue_calls == []
        status = await service.status(child_id)
        assert status["agent"]["status"] == "waiting_assistant"
        assert status["task"]["status"] == "waiting_assistant"
        assert status["task"]["completed_at"] is None
        assert status["mailbox"]["delivered"] == 1

    asyncio.run(run())
