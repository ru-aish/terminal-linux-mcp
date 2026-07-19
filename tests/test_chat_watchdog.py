from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from chat_watchdog import (
    ChatLink,
    ChatWatchdog,
    ConversationTurn,
    ChatWatchdogConfig,
    DEFAULT_CONTINUE_MESSAGE,
    CodexInternalChatAdapter,
    ModelOption,
    ThreadDecisionState,
    classify_runtime_error,
    classify_thread_state,
    desktop_launch_environment,
    select_model_option,
    QueueConflictError,
    RefreshResult,
    SendResult,
    ThreadSnapshot,
    install_chat_watchdog_lifespan,
    parse_chat_link,
)
from chat_internal_client import (
    RuntimeNotReadyError,
    RuntimeProbe,
    RuntimeProtocolError,
    RuntimeUnavailableError,
)
from gpt_thread_store import GPTThreadStore
from usage_dashboard import install_usage_dashboard


def link(value: str = "11111111-1111-1111-1111-111111111111") -> ChatLink:
    return parse_chat_link(f"https://chatgpt.com/c/{value}")


def turn(key: str, role: str, text: str) -> ConversationTurn:
    return ConversationTurn(key=key, role=role, text=text)


def canonical_snapshot(
    *,
    conversation_id: str | None = None,
    assistant_text: str = "partial response",
    assistant_status: str = "finished_successfully",
    assistant_end_turn: bool | None = True,
    assistant_key: str = "a-terminal",
    current_node: str | None = None,
    running: bool = False,
    active_stream: bool = False,
    update_time: float = 1000.0,
    include_assistant: bool = True,
    latest_user_text: str | None = None,
) -> ThreadSnapshot:
    item_id = conversation_id or link().conversation_id
    turns: list[ConversationTurn] = [
        ConversationTurn(
            key="u-task",
            role="user",
            text="finish the task",
            status="finished_successfully",
            end_turn=None,
        )
    ]
    if include_assistant:
        turns.append(
            ConversationTurn(
                key=assistant_key,
                role="assistant",
                text=assistant_text,
                parent_id="u-task",
                status=assistant_status,
                end_turn=assistant_end_turn,
                create_time=update_time,
                model_slug="gpt-5-6-thinking",
            )
        )
    if latest_user_text is not None:
        turns.append(
            ConversationTurn(
                key="u-latest",
                role="user",
                text=latest_user_text,
                parent_id=turns[-1].key,
                status="finished_successfully",
                end_turn=None,
                create_time=update_time,
            )
        )
    node = current_node if current_node is not None else turns[-1].key
    return ThreadSnapshot(
        found=True,
        conversation_id=item_id,
        running=running,
        assistant_messages=tuple(
            item.text for item in turns if item.role == "assistant" and item.text
        ),
        user_messages=tuple(item.text for item in turns if item.role == "user" and item.text),
        turns=tuple(turns),
        current_node=node,
        canonical=True,
        state_verified=True,
        update_time=update_time,
        active_stream=active_stream,
    )


class FakeAdapter:
    def __init__(
        self,
        snapshot: ThreadSnapshot,
        *,
        refresh: RefreshResult | None = None,
        current_conversation_id: str | None = None,
        snapshots: dict[str, ThreadSnapshot] | None = None,
        inspect_errors: set[str] | None = None,
        send_errors: set[str] | None = None,
        send_results: dict[str, SendResult] | None = None,
        restore_result: bool = True,
        preserve_empty_transcript: bool = False,
        snapshot_sequences: dict[str, list[ThreadSnapshot]] | None = None,
    ):
        self.snapshot = snapshot
        self.snapshots = snapshots or {}
        self.inspect_errors = inspect_errors or set()
        self.send_errors = send_errors or set()
        self.send_results = send_results or {}
        self.current_id = current_conversation_id
        self.restore_result = restore_result
        self.preserve_empty_transcript = preserve_empty_transcript
        self.snapshot_sequences = {
            key: list(value)
            for key, value in (snapshot_sequences or {}).items()
        }
        self.refresh_result = refresh or RefreshResult(True)
        self.sent: list[str] = []
        self.sent_conversation_ids: list[str] = []
        self.refreshes = 0
        self.inspections = 0
        self.inspected_conversation_ids: list[str] = []
        self.restored: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def current_conversation_id(self):
        return self.current_id

    async def refresh_catalog(self):
        self.refreshes += 1
        return self.refresh_result

    def _hydrated(self, snapshot: ThreadSnapshot) -> ThreadSnapshot:
        if snapshot.turns or self.preserve_empty_transcript or not snapshot.found:
            return snapshot
        turns: list[ConversationTurn] = []
        users = snapshot.user_messages or ("test task",)
        assistants = snapshot.assistant_messages or ("partial response",)
        for index, text in enumerate(users):
            turns.append(turn(f"fake-user-{index}", "user", text))
        for index, text in enumerate(assistants):
            turns.append(turn(f"fake-assistant-{index}", "assistant", text))
        return replace(
            snapshot,
            assistant_messages=tuple(assistants),
            turns=tuple(turns),
        )

    async def inspect(self, item):
        self.inspections += 1
        self.inspected_conversation_ids.append(item.conversation_id)
        if item.conversation_id in self.inspect_errors:
            raise RuntimeError("simulated inspection failure")
        sequence = self.snapshot_sequences.get(item.conversation_id)
        if sequence:
            snapshot = sequence.pop(0) if len(sequence) > 1 else sequence[0]
        else:
            snapshot = self.snapshots.get(item.conversation_id, self.snapshot)
        return self._hydrated(snapshot)

    async def send_continue(self, item, message, *, expected_current_node=""):
        if item.conversation_id in self.send_errors:
            raise RuntimeError("simulated send failure")
        self.sent.append(message)
        self.sent_conversation_ids.append(item.conversation_id)
        result = self.send_results.get(item.conversation_id, SendResult(True, True, False))
        if result.clicked and not result.parent_message_id:
            result = replace(result, parent_message_id=expected_current_node)
        return result

    async def restore(self, conversation_id):
        self.restored.append(conversation_id)
        return self.restore_result


def make_watchdog(
    tmp_path: Path,
    adapter: FakeAdapter,
    *,
    dry_run: bool = False,
    clock=lambda: 1000.0,
) -> ChatWatchdog:
    config = ChatWatchdogConfig(
        True,
        tmp_path / "threads.txt",
        tmp_path / "state.json",
        tmp_path / "done.jsonl",
        retry_cooldown_seconds=300,
        dry_run=dry_run,
        pre_send_confirmation_seconds=0,
    )
    watchdog = ChatWatchdog(config, adapter_factory=lambda: adapter, clock=clock)
    watchdog.add_url(link().url)
    return watchdog


def test_url_parsing_and_strict_paths():
    project = parse_chat_link(
        "https://www.chatgpt.com/g/g-p-"
        + "a" * 32
        + "/c/11111111-1111-1111-1111-111111111111"
    )
    assert project.project_id == "g-p-" + "a" * 32
    assert project.url.startswith("https://chatgpt.com/g/")
    with pytest.raises(ValueError):
        parse_chat_link("https://chatgpt.com/c/not-a-uuid")
    with pytest.raises(ValueError):
        parse_chat_link(project.url + "?x=1")
    with pytest.raises(ValueError):
        parse_chat_link(project.url + "/extra")



def test_completion_requires_exact_line_in_latest_assistant_turn(tmp_path):
    async def run():
        cases = [
            (
                "user-marker",
                ThreadSnapshot(
                    True,
                    link().conversation_id,
                    user_messages=("DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",),
                ),
                False,
            ),
            (
                "old-assistant-marker",
                ThreadSnapshot(
                    True,
                    link().conversation_id,
                    assistant_messages=(
                        "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",
                        "Still working on the remaining steps.",
                    ),
                ),
                False,
            ),
            (
                "substring-marker",
                ThreadSnapshot(
                    True,
                    link().conversation_id,
                    assistant_messages=("Not yet DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",),
                ),
                False,
            ),
            (
                "exact-latest-marker",
                ThreadSnapshot(
                    True,
                    link().conversation_id,
                    assistant_messages=(
                        "Implementation summary\nDONE_I_HAVE_COMPLETED_ALL_THE_STEPS",
                    ),
                ),
                True,
            ),
        ]
        for name, snapshot, should_complete in cases:
            fake = FakeAdapter(snapshot)
            watchdog = make_watchdog(tmp_path / name, fake)
            await watchdog.scan_once()
            remaining, _ = watchdog.queue.entries()
            assert (not remaining) is should_complete
            assert bool(fake.sent) is not should_complete

    asyncio.run(run())


def test_running_draft_cooldown_and_dry_run_do_not_duplicate(tmp_path):
    async def run():
        for name, snapshot, expected in [
            ("running", ThreadSnapshot(True, link().conversation_id, running=True), "running_canonical"),
            (
                "draft",
                ThreadSnapshot(True, link().conversation_id, composer_text="keep me"),
                "draft_present",
            ),
        ]:
            fake = FakeAdapter(snapshot)
            watchdog = make_watchdog(tmp_path / name, fake)
            result = await watchdog.scan_once()
            assert result["queue"]["entries"][0]["state"]["status"] == expected
            assert not fake.sent

        fake = FakeAdapter(ThreadSnapshot(True, link().conversation_id))
        watchdog = make_watchdog(tmp_path / "cooldown", fake)
        await watchdog.scan_once()
        await watchdog.scan_once()
        assert len(fake.sent) == 1

        dry = FakeAdapter(ThreadSnapshot(True, link().conversation_id))
        dry_watchdog = make_watchdog(tmp_path / "dry", dry, dry_run=True)
        await dry_watchdog.scan_once()
        assert not dry.sent

    asyncio.run(run())


def test_refresh_deferral_never_inspects_or_navigates(tmp_path):
    async def run():
        fake = FakeAdapter(
            ThreadSnapshot(True, link().conversation_id),
            refresh=RefreshResult(False, reason="active composer contains a draft"),
        )
        watchdog = make_watchdog(tmp_path, fake)
        snapshot = await watchdog.scan_once()
        assert fake.refreshes == 1
        assert fake.inspections == 0
        assert fake.sent == []
        assert snapshot["runtime"]["scanning"] is False
        assert snapshot["runtime"]["last_scan_finished_at"]
        assert snapshot["runtime"]["last_refresh"]["refreshed"] is False
        assert snapshot["queue"]["entries"][0]["state"]["status"] == "waiting_refresh"

    asyncio.run(run())


def test_failed_hard_reload_still_restores_the_original_chat(tmp_path):
    async def run():
        original = "33333333-3333-3333-3333-333333333333"
        fake = FakeAdapter(
            ThreadSnapshot(True, link().conversation_id),
            refresh=RefreshResult(False, hard_reload=True, reason="renderer reload timed out"),
            current_conversation_id=original,
        )
        watchdog = make_watchdog(tmp_path, fake)
        snapshot = await watchdog.scan_once()
        assert fake.inspections == 0
        assert fake.restored == [original]
        assert snapshot["runtime"]["last_restore"]["restored"] is True
        assert snapshot["queue"]["entries"][0]["state"]["status"] == "waiting_refresh"

    asyncio.run(run())


def test_confirmed_continuation_is_not_retried_for_unchanged_transcript_after_cooldown(tmp_path):
    async def run():
        now = [1000.0]
        fake = FakeAdapter(ThreadSnapshot(True, link().conversation_id))
        watchdog = make_watchdog(tmp_path, fake, clock=lambda: now[0])
        first = await watchdog.scan_once()
        assert first["queue"]["entries"][0]["state"]["status"] == "continue_sent"
        assert len(fake.sent) == 1

        now[0] += 3600
        second = await watchdog.scan_once()
        state = second["queue"]["entries"][0]["state"]
        assert state["status"] == "waiting_after_continue"
        assert "exact transcript" in state["last_error"]
        assert len(fake.sent) == 1

    asyncio.run(run())


def test_unobserved_submission_is_recorded_separately(tmp_path):
    async def run():
        item = link()
        fake = FakeAdapter(
            ThreadSnapshot(True, item.conversation_id),
            send_results={item.conversation_id: SendResult(True, False, False, "submission was not observed")},
        )
        watchdog = make_watchdog(tmp_path, fake)
        snapshot = await watchdog.scan_once()
        state = snapshot["queue"]["entries"][0]["state"]
        assert state["status"] == "continue_unconfirmed"
        assert state["continue_attempts"] == 1
        assert state["last_error"] == "submission was not observed"

        second = await watchdog.scan_once()
        second_state = second["queue"]["entries"][0]["state"]
        assert second_state["status"] == "waiting_unconfirmed_submission"
        assert second_state["continue_attempts"] == 1
        assert len(fake.sent) == 1

    asyncio.run(run())


def test_empty_transcript_never_sends_a_continuation(tmp_path):
    async def run():
        item = link()
        fake = FakeAdapter(
            ThreadSnapshot(True, item.conversation_id),
            preserve_empty_transcript=True,
        )
        watchdog = make_watchdog(tmp_path, fake)
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "waiting_transcript"
        assert "transcript is empty" in state["last_error"]
        assert state["continue_attempts"] == 0
        assert fake.sent == []
        assert fake.inspections == 1

    asyncio.run(run())


def test_late_completion_during_pre_send_confirmation_removes_task_without_sending(tmp_path):
    async def run():
        item = link()
        initial = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("Work is nearly complete.",),
            user_messages=("finish the task",),
            turns=(
                turn("u-task", "user", "finish the task"),
                turn("a-partial", "assistant", "Work is nearly complete."),
            ),
        )
        completed = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",),
            user_messages=("finish the task",),
            turns=(
                turn("u-task", "user", "finish the task"),
                turn("a-done", "assistant", "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"),
            ),
        )
        fake = FakeAdapter(
            initial,
            snapshot_sequences={item.conversation_id: [initial, completed]},
        )
        watchdog = make_watchdog(tmp_path, fake)
        result = await watchdog.scan_once()
        assert result["queue"]["entries"] == []
        assert fake.sent == []
        assert fake.inspections == 2
        state = watchdog.state.get(item.conversation_id)
        assert state["status"] == "completed"
        assert state["completion_turn_key"] == "a-done"

    asyncio.run(run())


def test_transcript_change_during_pre_send_confirmation_defers_send(tmp_path):
    async def run():
        item = link()
        initial = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("First partial output",),
            user_messages=("finish the task",),
            turns=(
                turn("u-task", "user", "finish the task"),
                turn("a-partial", "assistant", "First partial output"),
            ),
        )
        changed = replace(
            initial,
            assistant_messages=("A newer partial output",),
            turns=(
                turn("u-task", "user", "finish the task"),
                turn("a-partial", "assistant", "A newer partial output"),
            ),
        )
        fake = FakeAdapter(
            initial,
            snapshot_sequences={item.conversation_id: [initial, changed]},
        )
        watchdog = make_watchdog(tmp_path, fake)
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "waiting_transcript_change"
        assert fake.sent == []

    asyncio.run(run())


def test_final_send_guard_completion_result_does_not_count_as_attempt(tmp_path):
    async def run():
        item = link()
        snapshot = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("Still working",),
            user_messages=("finish the task",),
            turns=(
                turn("u-task", "user", "finish the task"),
                turn("a-partial", "assistant", "Still working"),
            ),
        )
        fake = FakeAdapter(
            snapshot,
            send_results={
                item.conversation_id: SendResult(
                    False,
                    False,
                    False,
                    "completion marker appeared before send",
                )
            },
        )
        watchdog = make_watchdog(tmp_path, fake)
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "waiting_completion_confirmation"
        assert state["continue_attempts"] == 0
        assert fake.sent == [DEFAULT_CONTINUE_MESSAGE]

    asyncio.run(run())


def test_existing_watchdog_prompt_as_latest_user_turn_is_not_duplicated(tmp_path):
    async def run():
        item = link()
        snapshot = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("Still working",),
            user_messages=("finish the task", DEFAULT_CONTINUE_MESSAGE),
            turns=(
                turn("u-task", "user", "finish the task"),
                turn("a-partial", "assistant", "Still working"),
                turn("u-watchdog", "user", DEFAULT_CONTINUE_MESSAGE),
            ),
        )
        fake = FakeAdapter(snapshot)
        watchdog = make_watchdog(tmp_path, fake)
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "waiting_after_continue"
        assert fake.sent == []
        assert state["task_start_user_turn_key"] == "u-task"

    asyncio.run(run())


def test_send_failure_uses_one_write_per_scan_and_rotates_to_next_candidate(tmp_path):
    async def run():
        first = link()
        second = link("22222222-2222-2222-2222-222222222222")
        original = "33333333-3333-3333-3333-333333333333"
        fake = FakeAdapter(
            ThreadSnapshot(True, first.conversation_id),
            current_conversation_id=original,
            snapshots={
                first.conversation_id: ThreadSnapshot(True, first.conversation_id),
                second.conversation_id: ThreadSnapshot(True, second.conversation_id),
            },
            send_errors={first.conversation_id},
        )
        watchdog = make_watchdog(tmp_path, fake)
        watchdog.add_url(second.url)

        first_scan = await watchdog.scan_once()
        first_states = {
            entry["conversation_id"]: entry["state"]["status"]
            for entry in first_scan["queue"]["entries"]
        }
        assert first_states[first.conversation_id] == "retryable_error"
        assert first_states[second.conversation_id] == "ready_to_continue"
        assert fake.sent_conversation_ids == []

        second_scan = await watchdog.scan_once()
        second_states = {
            entry["conversation_id"]: entry["state"]["status"]
            for entry in second_scan["queue"]["entries"]
        }
        assert second_states[second.conversation_id] == "continue_sent"
        assert fake.sent_conversation_ids == [second.conversation_id]
        assert fake.inspected_conversation_ids == [
            first.conversation_id,
            second.conversation_id,
            first.conversation_id,
            first.conversation_id,
            second.conversation_id,
            second.conversation_id,
        ]
        assert fake.restored == [original, original]
        assert second_scan["runtime"]["last_restore"]["restored"] is True

    asyncio.run(run())


def test_original_chat_is_restored_when_processing_raises(tmp_path, monkeypatch):
    async def run():
        original = "33333333-3333-3333-3333-333333333333"
        completed = ThreadSnapshot(
            True,
            link().conversation_id,
            assistant_messages=("DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",),
        )
        fake = FakeAdapter(completed, current_conversation_id=original)
        watchdog = make_watchdog(tmp_path, fake)

        def fail_complete(*_args, **_kwargs):
            raise OSError("simulated completion log failure")

        monkeypatch.setattr(watchdog.queue, "complete", fail_complete)
        snapshot = await watchdog.scan_once()
        remaining, _ = watchdog.queue.entries()
        assert remaining
        assert fake.restored == [original]
        assert snapshot["runtime"]["last_restore"]["restored"] is True
        assert "OSError" in snapshot["runtime"]["last_error"]

    asyncio.run(run())


def test_readding_same_url_creates_fresh_task_generation(tmp_path):
    fake = FakeAdapter(ThreadSnapshot(True, link().conversation_id, running=True))
    watchdog = make_watchdog(tmp_path, fake)
    first = watchdog.state.get(link().conversation_id)
    watchdog.state.update(
        link().conversation_id,
        last_assistant_hash="old",
        last_continue_epoch=999,
        continue_attempts=7,
        completion_turn_key="assistant-old",
        status="completed",
    )
    assert watchdog.remove_url(link().conversation_id)
    watchdog.add_url(link().url)
    second = watchdog.state.get(link().conversation_id)
    assert second["task_generation"] == first["task_generation"] + 1
    assert second["task_id"] != first["task_id"]
    assert second["queued"] is True
    assert second["status"] == "queued"
    assert second["last_assistant_hash"] == ""
    assert second["last_continue_epoch"] == 0
    assert second["continue_attempts"] == 0
    assert second["completion_turn_key"] == ""


def test_direct_file_remove_and_readd_creates_new_generation(tmp_path):
    async def run():
        fake = FakeAdapter(ThreadSnapshot(True, link().conversation_id, running=True))
        watchdog = make_watchdog(tmp_path, fake)
        first = watchdog.state.get(link().conversation_id)
        watchdog.config.queue_path.write_text("# no active tasks\n", encoding="utf-8")
        await watchdog.scan_once()
        removed = watchdog.state.get(link().conversation_id)
        assert removed["queued"] is False
        watchdog.config.queue_path.write_text(f"{link().url}\n", encoding="utf-8")
        await watchdog.scan_once()
        second = watchdog.state.get(link().conversation_id)
        assert second["queued"] is True
        assert second["task_generation"] == first["task_generation"] + 1
        assert second["task_id"] != first["task_id"]

    asyncio.run(run())


def readd_after_completion(watchdog: ChatWatchdog, completion_key: str = "a-old-done") -> dict:
    item = link()
    watchdog.state.update(
        item.conversation_id,
        status="completed",
        queued=False,
        completed_at="2026-01-01T00:00:00+00:00",
        completion_turn_key=completion_key,
    )
    watchdog.remove_url(item.conversation_id)
    watchdog.add_url(item.url)
    return watchdog.state.get(item.conversation_id)


def test_first_generation_ordered_marker_completes(tmp_path):
    async def run():
        item = link()
        snapshot = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",),
            user_messages=("first task",),
            turns=(
                turn("u-first", "user", "first task"),
                turn("a-first-done", "assistant", "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"),
            ),
        )
        watchdog = make_watchdog(tmp_path, FakeAdapter(snapshot))
        await watchdog.scan_once()
        remaining, _ = watchdog.queue.entries()
        assert not remaining
        state = watchdog.state.get(item.conversation_id)
        assert state["task_generation"] == 1
        assert state["task_start_user_turn_key"] == "u-first"
        assert state["completion_turn_key"] == "a-first-done"

    asyncio.run(run())


def test_old_completion_without_new_user_waits_for_new_task(tmp_path):
    async def run():
        item = link()
        snapshot = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",),
            user_messages=("old task",),
            turns=(
                turn("u-old", "user", "old task"),
                turn("a-done", "assistant", "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"),
            ),
        )
        fake = FakeAdapter(snapshot)
        watchdog = make_watchdog(tmp_path, fake)
        readd_after_completion(watchdog, "a-done")
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "awaiting_new_task"
        assert "new user task" in state["last_error"]
        assert not fake.sent
        remaining, _ = watchdog.queue.entries()
        assert remaining

    asyncio.run(run())


def test_old_completion_new_task_partial_output_continues_current_generation(tmp_path):
    async def run():
        item = link()
        snapshot = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=("DONE_I_HAVE_COMPLETED_ALL_THE_STEPS", "Working on the new task"),
            user_messages=("old task", "new large task"),
            turns=(
                turn("u-old", "user", "old task"),
                turn("a-old-done", "assistant", "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"),
                turn("u-new", "user", "new large task"),
                turn("a-partial", "assistant", "Working on the new task"),
            ),
        )
        fake = FakeAdapter(snapshot)
        watchdog = make_watchdog(tmp_path, fake, dry_run=True)
        readd_after_completion(watchdog)
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "would_continue"
        assert state["task_start_user_turn_key"] == "u-new"
        remaining, _ = watchdog.queue.entries()
        assert remaining

    asyncio.run(run())


def test_new_task_latest_marker_completes_with_task_metadata(tmp_path):
    async def run():
        item = link()
        snapshot = ThreadSnapshot(
            True,
            item.conversation_id,
            assistant_messages=(
                "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",
                "New task summary\nDONE_I_HAVE_COMPLETED_ALL_THE_STEPS",
            ),
            user_messages=("old task", "new large task"),
            turns=(
                turn("u-old", "user", "old task"),
                turn("a-old-done", "assistant", "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"),
                turn("u-new", "user", "new large task"),
                turn("a-new-done", "assistant", "New task summary\nDONE_I_HAVE_COMPLETED_ALL_THE_STEPS"),
            ),
        )
        fake = FakeAdapter(snapshot)
        watchdog = make_watchdog(tmp_path, fake)
        task = readd_after_completion(watchdog)
        await watchdog.scan_once()
        remaining, _ = watchdog.queue.entries()
        assert not remaining
        state = watchdog.state.get(item.conversation_id)
        assert state["status"] == "completed"
        assert state["queued"] is False
        assert state["task_start_user_turn_key"] == "u-new"
        assert state["completion_turn_key"] == "a-new-done"
        record = __import__("json").loads((tmp_path / "done.jsonl").read_text(encoding="utf-8").splitlines()[-1])
        assert record["task_id"] == task["task_id"]
        assert record["task_generation"] == task["task_generation"]
        assert record["task_start_user_turn_key"] == "u-new"
        assert record["completion_turn_key"] == "a-new-done"

    asyncio.run(run())




def test_managed_runtime_probe_requires_the_internal_client(monkeypatch, tmp_path):
    async def run():
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
            )
        )

        async def target_ready(*_args, **_kwargs):
            return RuntimeProbe(True, True)

        monkeypatch.setattr("chat_watchdog.probe_runtime", target_ready)

        class HealthyClient:
            def __init__(self, *_args, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return None

            async def health(self):
                return {"ready": True}

        monkeypatch.setattr("chat_watchdog.InternalChatClient", HealthyClient)
        assert (await watchdog._probe_managed_runtime()).ready is True

        class BrokenClient(HealthyClient):
            async def health(self):
                raise RuntimeProtocolError("Authorization: secret-value")

        monkeypatch.setattr("chat_watchdog.InternalChatClient", BrokenClient)
        failed = await watchdog._probe_managed_runtime()
        assert failed.available is True
        assert failed.ready is False
        assert "secret-value" not in failed.reason
        assert "[redacted]" in failed.reason

        created = False

        async def endpoint_offline(*_args, **_kwargs):
            return RuntimeProbe(False, False, "offline")

        class ShouldNotStart(HealthyClient):
            def __init__(self, *_args, **_kwargs):
                nonlocal created
                created = True

        monkeypatch.setattr("chat_watchdog.probe_runtime", endpoint_offline)
        monkeypatch.setattr("chat_watchdog.InternalChatClient", ShouldNotStart)
        offline = await watchdog._probe_managed_runtime()
        assert offline.available is False
        assert created is False

    asyncio.run(run())

def test_auto_start_app_success_and_disabled(tmp_path, monkeypatch):
    async def run():
        config = ChatWatchdogConfig(
            True,
            tmp_path / "threads.txt",
            tmp_path / "state.json",
            tmp_path / "done.jsonl",
            auto_start_app=True,
            app_command="/usr/bin/codex-desktop",
            app_start_timeout_seconds=2,
        )
        watchdog = ChatWatchdog(config)
        probes = iter(
            [
                RuntimeProbe(False, False, "offline"),
                RuntimeProbe(True, True),
            ]
        )

        async def runtime_probe():
            return next(probes)

        launched = []

        class Process:
            async def wait(self):
                return 0

        async def create_process(*args, **kwargs):
            launched.append((args, kwargs))
            return Process()

        async def no_sleep(_seconds):
            return None

        monkeypatch.setattr(watchdog, "_probe_managed_runtime", runtime_probe)
        monkeypatch.setattr("chat_watchdog.asyncio.create_subprocess_exec", create_process)
        monkeypatch.setattr("chat_watchdog.asyncio.sleep", no_sleep)
        monkeypatch.setattr(
            "chat_watchdog.desktop_launch_environment",
            lambda: {"DISPLAY": ":test"},
        )
        assert await watchdog._ensure_background_runtime() is True
        assert launched[0][0] == ("/usr/bin/codex-desktop",)
        assert launched[0][1]["start_new_session"] is True
        assert launched[0][1]["env"] == {"DISPLAY": ":test"}
        assert watchdog._runtime["last_app_start"]["action"] == "start"
        assert watchdog._runtime["last_app_start"]["started"] is True

        reopen = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "reopen-threads.txt",
                tmp_path / "reopen-state.json",
                tmp_path / "reopen-done.jsonl",
                auto_start_app=True,
                app_command="/usr/bin/codex-desktop",
                app_start_timeout_seconds=2,
            )
        )
        reopen_probes = iter(
            [
                RuntimeProbe(True, False, "primary renderer is unavailable"),
                RuntimeProbe(True, True),
            ]
        )

        async def reopen_probe():
            return next(reopen_probes)

        monkeypatch.setattr(reopen, "_probe_managed_runtime", reopen_probe)
        launched.clear()
        assert await reopen._ensure_background_runtime() is True
        assert launched[0][0] == ("/usr/bin/codex-desktop", "--new-chat")
        assert reopen._runtime["last_app_start"]["action"] == "reopen"

        ready = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "ready-threads.txt",
                tmp_path / "ready-state.json",
                tmp_path / "ready-done.jsonl",
            )
        )

        async def ready_probe():
            return RuntimeProbe(True, True)

        monkeypatch.setattr(ready, "_probe_managed_runtime", ready_probe)
        launched.clear()
        assert await ready._ensure_background_runtime() is False
        assert launched == []

        disabled = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "disabled-threads.txt",
                tmp_path / "disabled-state.json",
                tmp_path / "disabled-done.jsonl",
                auto_start_app=False,
            )
        )

        async def unavailable_probe():
            return RuntimeProbe(False, False, "offline")

        monkeypatch.setattr(disabled, "_probe_managed_runtime", unavailable_probe)
        with pytest.raises(RuntimeUnavailableError, match="disabled"):
            await disabled._ensure_background_runtime()

        failed = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "failed-threads.txt",
                tmp_path / "failed-state.json",
                tmp_path / "failed-done.jsonl",
                auto_start_app=True,
            )
        )
        monkeypatch.setattr(failed, "_probe_managed_runtime", unavailable_probe)

        async def fail_launch(*_args, **_kwargs):
            raise OSError("launcher missing")

        monkeypatch.setattr("chat_watchdog.asyncio.create_subprocess_exec", fail_launch)
        with pytest.raises(RuntimeUnavailableError, match="could not launch ChatGPT"):
            await failed._ensure_background_runtime()
        assert "launcher missing" in failed._runtime["last_app_start"]["reason"]

    asyncio.run(run())


def test_desktop_launch_environment_inherits_only_graphical_session_fields(tmp_path, monkeypatch):
    proc = tmp_path / "123"
    proc.mkdir()
    (proc / "comm").write_text("plasmashell\n", encoding="utf-8")
    (proc / "environ").write_bytes(
        b"DISPLAY=:0\0WAYLAND_DISPLAY=wayland-0\0XDG_RUNTIME_DIR=/run/user/1000\0"
        b"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/1000/bus\0SECRET=do-not-copy\0"
    )
    monkeypatch.setattr("chat_watchdog.os.getuid", lambda: proc.stat().st_uid)

    environment = desktop_launch_environment(
        {"PATH": "/usr/bin"},
        proc_root=tmp_path,
    )

    assert environment["DISPLAY"] == ":0"
    assert environment["WAYLAND_DISPLAY"] == "wayland-0"
    assert environment["DBUS_SESSION_BUS_ADDRESS"] == "unix:path=/run/user/1000/bus"
    assert "SECRET" not in environment

def test_high_quality_block_is_persisted_without_sending(tmp_path):
    async def run():
        item = link()
        fake = FakeAdapter(
            ThreadSnapshot(True, item.conversation_id),
            send_results={
                item.conversation_id: SendResult(
                    False,
                    False,
                    False,
                    "High reasoning option was not found",
                    quality_verified=False,
                    quality_changed=False,
                    quality_label="Medium",
                )
            },
        )
        watchdog = make_watchdog(tmp_path, fake)
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "continue_blocked"
        assert state["quality_verified"] is False
        assert state["quality_label"] == "Medium"
        assert state["continue_attempts"] == 0

    asyncio.run(run())


def test_queue_editor_detects_external_file_change(tmp_path):
    fake = FakeAdapter(ThreadSnapshot(True, link().conversation_id))
    watchdog = make_watchdog(tmp_path, fake)
    initial = watchdog.queue.snapshot()
    watchdog.add_url(link("22222222-2222-2222-2222-222222222222").url)
    with pytest.raises(QueueConflictError):
        watchdog.replace_queue(initial["text"], initial["version"])


def test_completion_log_is_durable_before_queue_removal(tmp_path, monkeypatch):
    fake = FakeAdapter(ThreadSnapshot(True, link().conversation_id))
    watchdog = make_watchdog(tmp_path, fake)

    def fail_remove(_conversation_id: str) -> bool:
        raise OSError("simulated queue write failure")

    monkeypatch.setattr(watchdog.queue, "remove", fail_remove)
    with pytest.raises(OSError):
        watchdog.queue.complete(link(), {"title": "test"})
    assert link().conversation_id in (tmp_path / "done.jsonl").read_text(encoding="utf-8")


def test_dashboard_watchdog_mutations_are_auth_csrf_and_version_protected(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("MCP_DASHBOARD_TOKEN", "secret")
    monkeypatch.setenv("TEST_WATCHDOG_HOME", str(tmp_path / "gpt-home"))
    store = GPTThreadStore(lambda: tmp_path / "workspace", home_env="TEST_WATCHDOG_HOME")
    watchdog = ChatWatchdog(
        ChatWatchdogConfig(
            False,
            tmp_path / "threads.txt",
            tmp_path / "state.json",
            tmp_path / "done.jsonl",
        )
    )
    app = Starlette()
    install_usage_dashboard(app, store, watchdog)
    with TestClient(app) as client:
        assert client.get("/dashboard/watchdog").status_code == 401
        auth = {"authorization": "Bearer secret"}
        headers = {**auth, "x-mcp-dashboard-csrf": "chat-watchdog"}
        assert (
            client.post(
                "/dashboard/watchdog/queue/add",
                json={"url": link().url},
                headers=auth,
            ).status_code
            == 403
        )
        added = client.post(
            "/dashboard/watchdog/queue/add",
            json={"url": link().url},
            headers=headers,
        )
        assert added.status_code == 201
        assert added.json()["queue"]["entries"][0]["conversation_id"] == link().conversation_id

        loaded = client.get("/dashboard/watchdog", headers=auth).json()
        stale_version = loaded["queue"]["version"]
        watchdog.add_url(link("22222222-2222-2222-2222-222222222222").url)
        conflict = client.put(
            "/dashboard/watchdog/queue",
            content=loaded["queue"]["text"],
            headers={
                **headers,
                "content-type": "text/plain",
                "x-watchdog-queue-version": stale_version,
            },
        )
        assert conflict.status_code == 409
        assert "changed on disk" in conflict.json()["error"]

        removed = client.delete(
            f"/dashboard/watchdog/queue/{link().conversation_id}",
            headers=headers,
        )
        assert removed.status_code == 200
        assert removed.json()["removed"] is True
        assert "queue" in removed.json()


def test_lifecycle_starts_and_stops_watchdog(tmp_path):
    watchdog = ChatWatchdog(
        ChatWatchdogConfig(
            True,
            tmp_path / "threads.txt",
            tmp_path / "state.json",
            tmp_path / "done.jsonl",
            initial_delay_seconds=0,
        )
    )
    app = Starlette()
    install_chat_watchdog_lifespan(app, watchdog)
    with TestClient(app):
        assert watchdog._task is not None
    assert watchdog._task is None


def test_canonical_decision_matrix_is_fail_closed():
    marker = "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"

    owned = canonical_snapshot(active_stream=True)
    assert classify_thread_state(
        owned,
        task_start_index=0,
        completion_marker=marker,
    ).state is ThreadDecisionState.RUNNING_OWNED_STREAM

    running = canonical_snapshot(
        assistant_status="in_progress",
        assistant_end_turn=False,
        running=True,
    )
    assert classify_thread_state(
        running,
        task_start_index=0,
        completion_marker=marker,
        now=1010.0,
        stale_after_seconds=60,
    ).state is ThreadDecisionState.RUNNING_CANONICAL

    stale = replace(running, update_time=100.0)
    assert classify_thread_state(
        stale,
        task_start_index=0,
        completion_marker=marker,
        now=1000.0,
        stale_after_seconds=60,
    ).state is ThreadDecisionState.INTERRUPTED_OR_STALE

    complete = canonical_snapshot(assistant_text=f"summary\n{marker}")
    completion = classify_thread_state(
        complete,
        task_start_index=0,
        completion_marker=marker,
    )
    assert completion.state is ThreadDecisionState.COMPLETED
    assert completion.completion_turn is not None
    assert completion.completion_turn.key == complete.current_node

    for status in ("finished_successfully", "failed", "interrupted"):
        stopped = canonical_snapshot(assistant_status=status)
        decision = classify_thread_state(
            stopped,
            task_start_index=0,
            completion_marker=marker,
        )
        assert decision.state is ThreadDecisionState.STOPPED_INCOMPLETE
        assert decision.can_continue is True

    latest_user = canonical_snapshot(latest_user_text="additional request")
    assert classify_thread_state(
        latest_user,
        task_start_index=0,
        completion_marker=marker,
    ).state is ThreadDecisionState.AWAITING_ASSISTANT

    duplicate = canonical_snapshot(latest_user_text=DEFAULT_CONTINUE_MESSAGE)
    assert classify_thread_state(
        duplicate,
        task_start_index=0,
        completion_marker=marker,
        continuation_message=DEFAULT_CONTINUE_MESSAGE,
    ).state is ThreadDecisionState.DUPLICATE_PENDING

    unknown_status = canonical_snapshot(assistant_status="mystery")
    assert classify_thread_state(
        unknown_status,
        task_start_index=0,
        completion_marker=marker,
    ).state is ThreadDecisionState.UNKNOWN

    missing_end = canonical_snapshot(assistant_end_turn=None)
    assert classify_thread_state(
        missing_end,
        task_start_index=0,
        completion_marker=marker,
    ).state is ThreadDecisionState.UNKNOWN

    wrong_node = canonical_snapshot(current_node="different-node")
    assert classify_thread_state(
        wrong_node,
        task_start_index=0,
        completion_marker=marker,
    ).state is ThreadDecisionState.UNKNOWN


def test_old_or_user_marker_never_completes_current_generation():
    marker = "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"
    snapshot = ThreadSnapshot(
        found=True,
        conversation_id=link().conversation_id,
        turns=(
            ConversationTurn("u-old", "user", "old task"),
            ConversationTurn(
                "a-old",
                "assistant",
                marker,
                status="finished_successfully",
                end_turn=True,
            ),
            ConversationTurn("u-new", "user", marker),
            ConversationTurn(
                "a-new",
                "assistant",
                "current task is incomplete",
                status="finished_successfully",
                end_turn=True,
            ),
        ),
        current_node="a-new",
        canonical=True,
        state_verified=True,
    )
    decision = classify_thread_state(
        snapshot,
        task_start_index=2,
        completion_marker=marker,
    )
    assert decision.state is ThreadDecisionState.STOPPED_INCOMPLETE
    assert decision.completion_turn is None


def test_runtime_error_classification_is_deterministic_and_sanitized():
    unavailable = classify_runtime_error(RuntimeUnavailableError("offline"))
    assert unavailable.state is ThreadDecisionState.RUNTIME_UNAVAILABLE

    not_ready = classify_runtime_error(RuntimeNotReadyError("main renderer missing"))
    assert not_ready.state is ThreadDecisionState.RUNTIME_NOT_READY

    retryable = classify_runtime_error(RuntimeProtocolError("temporary protocol failure"))
    assert retryable.state is ThreadDecisionState.RETRYABLE_ERROR

    terminal = classify_runtime_error(ValueError("invalid configuration"))
    assert terminal.state is ThreadDecisionState.TERMINAL_ERROR

    secret = classify_runtime_error(
        RuntimeProtocolError("Authorization=private Cookie=session Bearer abc.def")
    )
    assert "private" not in secret.reason
    assert "session" not in secret.reason
    assert "abc.def" not in secret.reason


def test_model_policy_selects_compatible_reasoning_without_downgrade():
    options = [
        ModelOption("gpt-5-5", is_default=True),
        ModelOption(
            "gpt-5-6-thinking",
            thinking_efforts=("standard", "extended"),
        ),
    ]
    selected = select_model_option(
        options,
        thinking_effort="extended",
        require_high_reasoning=True,
    )
    assert selected.slug == "gpt-5-6-thinking"
    assert selected.thinking_effort == "extended"
    assert selected.source == "required-reasoning"

    latest = select_model_option(
        options,
        latest_model="gpt-5-6-thinking",
        thinking_effort="extended",
        require_high_reasoning=True,
    )
    assert latest.source == "latest-thread"

    with pytest.raises(ValueError, match="configured model is unavailable"):
        select_model_option(
            options,
            preferred_model="gpt-missing",
            thinking_effort="extended",
            require_high_reasoning=True,
        )

    with pytest.raises(ValueError, match="reasoning effort"):
        select_model_option(
            options,
            preferred_model="gpt-5-5",
            thinking_effort="extended",
            require_high_reasoning=True,
        )


def test_default_adapter_is_internal_and_ui_adapters_are_rejected(tmp_path):
    default = ChatWatchdog(
        ChatWatchdogConfig(
            False,
            tmp_path / "default-q",
            tmp_path / "default-s",
            tmp_path / "default-d",
        )
    )
    assert isinstance(default.adapter_factory(), CodexInternalChatAdapter)

    for mode in ("legacy-desktop", "desktop", "web"):
        with pytest.raises(ValueError, match="unsupported"):
            ChatWatchdog(
                ChatWatchdogConfig(
                    False,
                    tmp_path / f"{mode}-q",
                    tmp_path / f"{mode}-s",
                    tmp_path / f"{mode}-d",
                    adapter_mode=mode,
                )
            )


def test_current_node_change_during_confirmation_blocks_send(tmp_path):
    async def run():
        item = link()
        initial = canonical_snapshot(
            conversation_id=item.conversation_id,
            assistant_key="a-first",
            current_node="a-first",
        )
        changed = canonical_snapshot(
            conversation_id=item.conversation_id,
            assistant_key="a-second",
            current_node="a-second",
        )
        fake = FakeAdapter(
            initial,
            snapshot_sequences={item.conversation_id: [initial, changed]},
        )
        watchdog = make_watchdog(tmp_path, fake)
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "waiting_current_node_change"
        assert fake.sent == []

    asyncio.run(run())


def test_duplicate_parent_and_attempt_limit_block_send(tmp_path):
    async def run():
        snapshot = canonical_snapshot()
        fake = FakeAdapter(snapshot)
        watchdog = make_watchdog(tmp_path / "parent", fake)
        watchdog.state.update(
            link().conversation_id,
            last_continue_parent_node=snapshot.current_node,
        )
        result = await watchdog.scan_once()
        state = result["queue"]["entries"][0]["state"]
        assert state["status"] == "waiting_after_continue"
        assert "canonical parent node" in state["last_error"]
        assert fake.sent == []

        limited_fake = FakeAdapter(snapshot)
        limited = make_watchdog(tmp_path / "limit", limited_fake)
        limited.state.update(
            link().conversation_id,
            continue_attempts=limited.config.max_continue_attempts,
        )
        limited_result = await limited.scan_once()
        limited_state = limited_result["queue"]["entries"][0]["state"]
        assert limited_state["status"] == ThreadDecisionState.TERMINAL_ERROR.value
        assert "maximum continuation attempts" in limited_state["last_error"]
        assert limited_fake.sent == []

    asyncio.run(run())


def test_running_conversation_does_not_starve_later_candidate(tmp_path):
    async def run():
        first = link()
        second = link("22222222-2222-2222-2222-222222222222")
        first_snapshot = canonical_snapshot(
            conversation_id=first.conversation_id,
            assistant_status="in_progress",
            assistant_end_turn=False,
            running=True,
        )
        second_snapshot = canonical_snapshot(conversation_id=second.conversation_id)
        fake = FakeAdapter(
            first_snapshot,
            snapshots={
                first.conversation_id: first_snapshot,
                second.conversation_id: second_snapshot,
            },
        )
        watchdog = make_watchdog(tmp_path, fake)
        watchdog.add_url(second.url)
        result = await watchdog.scan_once()
        assert fake.inspected_conversation_ids == [
            first.conversation_id,
            second.conversation_id,
            second.conversation_id,
        ]
        assert fake.sent_conversation_ids == [second.conversation_id]
        states = {
            entry["conversation_id"]: entry["state"]["status"]
            for entry in result["queue"]["entries"]
        }
        assert states[first.conversation_id] == ThreadDecisionState.RUNNING_CANONICAL.value
        assert states[second.conversation_id] == "continue_sent"

    asyncio.run(run())


def test_public_runtime_ensure_serializes_concurrent_launch_attempts(tmp_path, monkeypatch):
    async def run():
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "q",
                tmp_path / "s",
                tmp_path / "d",
            )
        )
        active = 0
        maximum = 0
        calls = 0

        async def fake_ensure():
            nonlocal active, maximum, calls
            calls += 1
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.02)
            active -= 1
            return True

        monkeypatch.setattr(watchdog, "_ensure_background_runtime", fake_ensure)
        results = await asyncio.gather(
            watchdog.ensure_background_runtime(),
            watchdog.ensure_background_runtime(),
        )
        assert results == [True, True]
        assert calls == 2
        assert maximum == 1

    asyncio.run(run())
