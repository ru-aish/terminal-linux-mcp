from __future__ import annotations

import asyncio
import json
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
    DirectChatAdapter,
    ModelOption,
    ThreadDecisionState,
    classify_runtime_error,
    classify_thread_state,
    desktop_launch_environment,
    model_options_from_payload,
    select_model_option,
    QueueConflictError,
    RefreshResult,
    SendResult,
    ThreadSnapshot,
    install_chat_watchdog_lifespan,
    parse_chat_link,
)
from durable_ledger import DurableLedger

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


def project_id(value: str = "a") -> str:
    return "g-p-" + value * 32


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
        projects: list[dict] | None = None,
        project_threads: dict[str, list[dict]] | None = None,
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
        self.forced_sends: list[bool] = []
        self.projects = list(projects or [])
        self.project_threads = {key: list(value) for key, value in (project_threads or {}).items()}
        self.project_list_calls = 0
        self.project_thread_calls: list[tuple[str, str | None]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def current_conversation_id(self):
        return self.current_id

    async def refresh_catalog(self):
        self.refreshes += 1
        return self.refresh_result

    async def list_projects(self, *, limit=50, cursor=None):
        self.project_list_calls += 1
        offset = int(cursor or 0)
        page = self.projects[offset:offset + limit]
        next_cursor = str(offset + limit) if offset + limit < len(self.projects) else None
        return {"items": page, "cursor": next_cursor}

    async def list_project_threads(self, project_id, *, limit=50, cursor=None):
        self.project_thread_calls.append((project_id, cursor))
        items = self.project_threads.get(project_id, [])
        offset = int(cursor or 0)
        page = items[offset:offset + limit]
        next_cursor = str(offset + limit) if offset + limit < len(items) else None
        return {"items": page, "cursor": next_cursor}

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

    async def send_continue(
        self,
        item,
        message,
        *,
        expected_current_node="",
        force=False,
    ):
        if item.conversation_id in self.send_errors:
            raise RuntimeError("simulated send failure")
        self.sent.append(message)
        self.sent_conversation_ids.append(item.conversation_id)
        self.forced_sends.append(force)
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


def test_forced_thread_continues_every_twenty_minutes_regardless_of_running_state(tmp_path):
    async def run():
        item = link()
        now = [1000.0]
        fake = FakeAdapter(
            canonical_snapshot(
                conversation_id=item.conversation_id,
                assistant_status="in_progress",
                assistant_end_turn=False,
                running=True,
            )
        )
        config = ChatWatchdogConfig(
            True,
            tmp_path / "threads.txt",
            tmp_path / "state.json",
            tmp_path / "done.jsonl",
            forced_continue_conversation_ids=(item.conversation_id,),
            forced_continue_interval_seconds=1200,
            pre_send_confirmation_seconds=0,
        )
        watchdog = ChatWatchdog(
            config,
            adapter_factory=lambda: fake,
            clock=lambda: now[0],
        )
        watchdog.add_url(item.url)

        await watchdog.scan_once()
        assert fake.sent == [DEFAULT_CONTINUE_MESSAGE]
        assert fake.forced_sends == [True]

        now[0] = 2199.0
        before_due = await watchdog.scan_once()
        assert fake.sent == [DEFAULT_CONTINUE_MESSAGE]
        assert before_due["queue"]["entries"][0]["state"]["status"] == (
            "waiting_forced_interval"
        )

        now[0] = 2200.0
        await watchdog.scan_once()
        assert fake.sent == [DEFAULT_CONTINUE_MESSAGE, DEFAULT_CONTINUE_MESSAGE]
        assert fake.forced_sends == [True, True]

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
            adapter_mode="codex-internal",
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
                adapter_mode="codex-internal",
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
                adapter_mode="codex-internal",
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
                adapter_mode="codex-internal",
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
                adapter_mode="codex-internal",
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



def test_hidden_raw_leaf_does_not_block_visible_thread_classification():
    snapshot = replace(
        canonical_snapshot(),
        current_node="hidden-tail",
        visible_current_node="a-terminal",
    )

    decision = classify_thread_state(
        snapshot,
        task_start_index=0,
        completion_marker="DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",
    )

    assert decision.state is ThreadDecisionState.STOPPED_INCOMPLETE
    assert decision.can_continue is True

    contradictory = replace(snapshot, visible_current_node="different-visible-node")
    assert classify_thread_state(
        contradictory,
        task_start_index=0,
        completion_marker="DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",
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


def test_model_catalog_normalizes_object_and_string_thinking_efforts():
    options = model_options_from_payload(
        {
            "models": [
                {
                    "slug": "gpt-5-6-thinking",
                    "thinking_efforts": [
                        {"thinking_effort": "standard"},
                        {"thinking_effort": "extended"},
                    ],
                },
                {
                    "slug": "legacy-model",
                    "thinking_efforts": ["standard", "high"],
                },
            ]
        }
    )

    assert options[0].thinking_efforts == ("standard", "extended")
    assert options[1].thinking_efforts == ("standard", "high")


def test_default_adapter_is_direct_and_ui_adapters_are_rejected(tmp_path):
    default = ChatWatchdog(
        ChatWatchdogConfig(
            False,
            tmp_path / "default-q",
            tmp_path / "default-s",
            tmp_path / "default-d",
        )
    )
    assert isinstance(default.adapter_factory(), DirectChatAdapter)

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


def test_empty_latest_user_turn_is_awaiting_without_adapter_crash():
    snapshot = ThreadSnapshot(
        found=True,
        conversation_id=link().conversation_id,
        turns=(
            ConversationTurn(
                key="u-empty",
                role="user",
                text="",
                status="finished_successfully",
                end_turn=True,
            ),
        ),
        current_node="u-empty",
        canonical=True,
        state_verified=True,
    )
    decision = classify_thread_state(
        snapshot,
        task_start_index=-1,
        completion_marker="DONE",
        continuation_message=DEFAULT_CONTINUE_MESSAGE,
    )
    assert decision.state is ThreadDecisionState.AWAITING_ASSISTANT


def test_empty_noncanonical_snapshot_is_never_continuation_eligible():
    snapshot = ThreadSnapshot(
        found=True,
        conversation_id=link().conversation_id,
        canonical=False,
        state_verified=False,
    )
    decision = classify_thread_state(
        snapshot,
        task_start_index=-1,
        completion_marker="DONE",
    )
    assert decision.state is ThreadDecisionState.UNKNOWN
    assert decision.can_continue is False


def test_legacy_watchdog_json_imports_once_into_durable_ledger(tmp_path):
    conversation_id = link().conversation_id
    state_path = tmp_path / "state.json"
    state_path.write_text(
        json.dumps(
            {
                conversation_id: {
                    "status": "legacy-running",
                    "task_generation": 3,
                    "queued": True,
                }
            }
        ),
        encoding="utf-8",
    )
    ledger_path = tmp_path / "shared.db"
    watchdog = ChatWatchdog(
        ChatWatchdogConfig(
            True,
            tmp_path / "threads.txt",
            state_path,
            tmp_path / "done.jsonl",
            ledger_path=ledger_path,
        ),
        adapter_factory=lambda: FakeAdapter(ThreadSnapshot(False, conversation_id)),
    )
    assert watchdog.state.get(conversation_id)["task_generation"] == 3
    assert DurableLedger(ledger_path).watchdog_state(conversation_id)["status"] == (
        "legacy-running"
    )

    # Later edits to the compatibility mirror cannot overwrite authoritative
    # SQLite state on restart.
    state_path.write_text(
        json.dumps({conversation_id: {"status": "stale-file", "task_generation": 99}}),
        encoding="utf-8",
    )
    restarted = ChatWatchdog(
        ChatWatchdogConfig(
            True,
            tmp_path / "threads.txt",
            state_path,
            tmp_path / "done.jsonl",
            ledger_path=ledger_path,
        ),
        adapter_factory=lambda: FakeAdapter(ThreadSnapshot(False, conversation_id)),
    )
    assert restarted.state.get(conversation_id)["status"] == "legacy-running"
    mirrored = json.loads(state_path.read_text(encoding="utf-8"))
    assert mirrored[conversation_id]["task_generation"] == 3


def test_watchdog_and_agent_orchestration_share_one_sqlite_ledger(tmp_path):
    shared = tmp_path / "orchestration.db"
    item = link()
    watchdog = ChatWatchdog(
        ChatWatchdogConfig(
            True,
            tmp_path / "threads.txt",
            tmp_path / "state.json",
            tmp_path / "done.jsonl",
            ledger_path=shared,
        ),
        adapter_factory=lambda: FakeAdapter(ThreadSnapshot(False, item.conversation_id)),
    )
    watchdog.state.start_task(item, source="test")

    from chat_agent_orchestrator import ChatAgentCoordinator, CoordinatorConfig

    runtime = FakeAdapter(ThreadSnapshot(False, item.conversation_id))
    # Constructing the coordinator against the same file must preserve the
    # generic watchdog task family and initialize the agent tables safely.
    coordinator = ChatAgentCoordinator(CoordinatorConfig(shared), lambda: runtime)
    assert coordinator.repository.watchdog_state(item.conversation_id)["queued"] is True
    with coordinator.repository.connect() as db:
        tables = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert {"watchdog_tasks", "agents", "tasks", "commands"} <= tables


def test_watchdog_skips_conversation_owned_by_active_orchestration_task(tmp_path):
    async def run():
        item = link()
        fake = FakeAdapter(ThreadSnapshot(True, item.conversation_id))
        watchdog = make_watchdog(tmp_path, fake)
        with watchdog.ledger.transaction() as db:
            db.execute(
                "INSERT INTO orchestrations(orchestration_id,root_agent_id,notification_policy,created_at) "
                "VALUES('orch-one',NULL,'notify_only',1)"
            )
            db.execute(
                "INSERT INTO agents(agent_id,orchestration_id,parent_agent_id,root_agent_id,chat_id,title,status,notification_policy,created_at,updated_at) "
                "VALUES('agent-one','orch-one',NULL,'agent-one',?,'owned','running','notify_only',1,1)",
                (item.conversation_id,),
            )
            db.execute(
                "UPDATE orchestrations SET root_agent_id='agent-one' WHERE orchestration_id='orch-one'"
            )
            db.execute(
                "INSERT INTO tasks(task_id,agent_id,prompt,completion_marker,status,next_check_at,created_at) "
                "VALUES('task-one','agent-one','task','DONE','running',0,1)"
            )

        snapshot = await watchdog.scan_once()

        assert fake.refreshes == 0
        assert fake.inspections == 0
        assert fake.sent == []
        assert snapshot["runtime"]["skipped_orchestrated_conversation_ids"] == [
            item.conversation_id
        ]

    asyncio.run(run())


def test_selected_project_baselines_existing_threads_and_adds_only_new_threads(tmp_path):
    async def run():
        selected = project_id("a")
        unselected = project_id("b")
        old_id = "11111111-1111-1111-1111-111111111111"
        new_id = "22222222-2222-2222-2222-222222222222"
        ignored_id = "33333333-3333-3333-3333-333333333333"
        new_snapshot = canonical_snapshot(
            conversation_id=new_id,
            assistant_status="in_progress",
            assistant_end_turn=False,
            running=True,
        )
        fake = FakeAdapter(
            new_snapshot,
            snapshots={new_id: new_snapshot},
            project_threads={
                selected: [
                    {"conversation_id": old_id, "title": "Existing task", "project_id": selected}
                ],
                unselected: [
                    {"conversation_id": ignored_id, "title": "Ignored task", "project_id": unselected}
                ],
            },
        )
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
                pre_send_confirmation_seconds=0,
            ),
            adapter_factory=lambda: fake,
        )

        selected_snapshot = await watchdog.select_project(selected, name="Selected project")
        assert selected_snapshot["projects"]["items"][0]["seen_thread_count"] == 1
        assert selected_snapshot["queue"]["entries"] == []
        assert fake.project_thread_calls == [(selected, None)]

        fake.project_threads[selected] = [
            {"conversation_id": new_id, "title": "New task", "project_id": selected},
            {"conversation_id": old_id, "title": "Existing task", "project_id": selected},
        ]
        result = await watchdog.scan_once(trigger="test")

        assert result["runtime"]["project_scan"]["physical_list_requests"] == 1
        assert result["runtime"]["project_scan"]["new_thread_ids"] == [new_id]
        assert [item["conversation_id"] for item in result["queue"]["entries"]] == [new_id]
        assert result["queue"]["entries"][0]["state"]["task_source"] == f"project-watch:{selected}"
        assert result["queue"]["entries"][0]["state"]["auto_project_id"] == selected
        assert result["queue"]["entries"][0]["state"]["title"] == "New task"
        assert all(project != unselected for project, _ in fake.project_thread_calls)
        assert ignored_id not in {item["conversation_id"] for item in result["queue"]["entries"]}

    asyncio.run(run())


def test_project_baseline_pages_all_old_threads_but_normal_scan_stops_after_seen_page(tmp_path):
    async def run():
        selected = project_id("c")
        old_ids = [f"00000000-0000-0000-0000-{index:012d}" for index in range(1, 52)]
        fake = FakeAdapter(
            canonical_snapshot(conversation_id=old_ids[0], running=True),
            project_threads={
                selected: [
                    {"conversation_id": conversation_id, "title": f"Old {index}", "project_id": selected}
                    for index, conversation_id in enumerate(old_ids)
                ]
            },
        )
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
                pre_send_confirmation_seconds=0,
            ),
            adapter_factory=lambda: fake,
        )

        baseline = await watchdog.select_project(selected, name="Large project")
        assert baseline["projects"]["items"][0]["seen_thread_count"] == 51
        assert baseline["runtime"]["project_baseline"]["physical_list_requests"] == 2
        assert fake.project_thread_calls == [(selected, None), (selected, "50")]

        fake.project_thread_calls.clear()
        result = await watchdog.scan_once(trigger="test")
        assert fake.project_thread_calls == [(selected, None)]
        assert result["runtime"]["project_scan"]["physical_list_requests"] == 1
        assert result["runtime"]["project_scan"]["new_threads_added"] == 0
        assert result["queue"]["entries"] == []

    asyncio.run(run())


def test_removing_watched_project_stops_automatic_discovery(tmp_path):
    async def run():
        selected = project_id("d")
        new_id = "44444444-4444-4444-4444-444444444444"
        fake = FakeAdapter(
            canonical_snapshot(conversation_id=new_id, running=True),
            project_threads={selected: []},
        )
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
            ),
            adapter_factory=lambda: fake,
        )
        await watchdog.select_project(selected, name="Temporary")
        assert watchdog.remove_project(selected) is True
        fake.project_threads[selected] = [
            {"conversation_id": new_id, "title": "Should stay out", "project_id": selected}
        ]
        calls_before = len(fake.project_thread_calls)
        result = await watchdog.scan_once(trigger="test")
        assert len(fake.project_thread_calls) == calls_before
        assert result["queue"]["entries"] == []
        assert result["projects"]["count"] == 0

    asyncio.run(run())


def test_dashboard_can_list_select_and_remove_watchdog_projects(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_DASHBOARD_TOKEN", "secret")
    monkeypatch.setenv("TEST_PROJECT_WATCHDOG_HOME", str(tmp_path / "gpt-home"))
    store = GPTThreadStore(
        lambda: tmp_path / "workspace",
        home_env="TEST_PROJECT_WATCHDOG_HOME",
    )
    selected = project_id("e")
    old_id = "55555555-5555-5555-5555-555555555555"
    fake = FakeAdapter(
        canonical_snapshot(conversation_id=old_id, running=True),
        projects=[
            {
                "id": selected,
                "name": "Project E",
                "description": "Watch this project",
                "permissions": {"can_read": True, "can_write": True, "can_delete": False},
            }
        ],
        project_threads={
            selected: [
                {"conversation_id": old_id, "title": "Already here", "project_id": selected}
            ]
        },
    )
    watchdog = ChatWatchdog(
        ChatWatchdogConfig(
            False,
            tmp_path / "threads.txt",
            tmp_path / "state.json",
            tmp_path / "done.jsonl",
        ),
        adapter_factory=lambda: fake,
    )
    app = Starlette()
    install_usage_dashboard(app, store, watchdog)
    auth = {"authorization": "Bearer secret"}
    mutate = {**auth, "x-mcp-dashboard-csrf": "chat-watchdog"}

    with TestClient(app) as client:
        available = client.get("/dashboard/watchdog/projects/available", headers=auth)
        assert available.status_code == 200
        assert available.json()["items"][0]["id"] == selected
        assert available.json()["items"][0]["selected"] is False
        assert fake.project_list_calls == 1

        rejected = client.post(
            "/dashboard/watchdog/projects",
            json={"project_id": selected, "name": "Project E"},
            headers=auth,
        )
        assert rejected.status_code == 403

        added = client.post(
            "/dashboard/watchdog/projects",
            json={"project_id": selected, "name": "Project E"},
            headers=mutate,
        )
        assert added.status_code == 201
        assert added.json()["projects"]["items"][0]["project_id"] == selected
        assert added.json()["projects"]["items"][0]["seen_thread_count"] == 1
        assert added.json()["queue"]["entries"] == []

        removed = client.delete(
            f"/dashboard/watchdog/projects/{selected}",
            headers=mutate,
        )
        assert removed.status_code == 200
        assert removed.json()["removed"] is True
        assert removed.json()["projects"]["count"] == 0


def test_selected_project_autodiscovers_only_new_project_threads(tmp_path):
    async def run():
        project_id = "g-p-" + "a" * 32
        other_project_id = "g-p-" + "b" * 32
        old_id = "11111111-1111-1111-1111-111111111111"
        new_id = "22222222-2222-2222-2222-222222222222"
        other_id = "33333333-3333-3333-3333-333333333333"
        new_snapshot = canonical_snapshot(
            conversation_id=new_id,
            assistant_status="in_progress",
            assistant_end_turn=False,
            running=True,
        )
        fake = FakeAdapter(
            new_snapshot,
            snapshots={new_id: new_snapshot},
            projects=[
                {"id": project_id, "name": "Watched", "permissions": {"can_read": True}},
                {"id": other_project_id, "name": "Ignored", "permissions": {"can_read": True}},
            ],
            project_threads={
                project_id: [
                    {"conversation_id": old_id, "title": "Existing", "project_id": project_id},
                ],
                other_project_id: [
                    {"conversation_id": other_id, "title": "Other", "project_id": other_project_id},
                ],
            },
        )
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
                projects_path=tmp_path / "projects.json",
                pre_send_confirmation_seconds=0,
            ),
            adapter_factory=lambda: fake,
            clock=lambda: 1000.0,
        )

        selected = await watchdog.select_project(project_id, name="Watched")
        assert selected["projects"]["count"] == 1
        assert selected["projects"]["items"][0]["seen_thread_count"] == 1
        entries, _ = watchdog.queue.entries()
        assert entries == []
        assert fake.project_thread_calls == [(project_id, None)]

        fake.project_threads[project_id] = [
            {"conversation_id": new_id, "title": "New running thread", "project_id": project_id},
            {"conversation_id": old_id, "title": "Existing", "project_id": project_id},
        ]
        result = await watchdog.scan_once(trigger="test")
        queued_ids = {entry["conversation_id"] for entry in result["queue"]["entries"]}
        assert queued_ids == {new_id}
        assert old_id not in queued_ids
        assert other_id not in queued_ids
        new_state = result["queue"]["entries"][0]["state"]
        assert new_state["task_source"] == f"project-watch:{project_id}"
        assert new_state["auto_project_id"] == project_id
        assert result["runtime"]["project_scan"]["selected_projects"] == 1
        assert result["runtime"]["project_scan"]["physical_list_requests"] == 1
        assert result["runtime"]["project_scan"]["new_threads_added"] == 1
        assert fake.project_thread_calls[-1] == (project_id, None)

        assert watchdog.remove_project(project_id) is True
        newer_id = "44444444-4444-4444-4444-444444444444"
        fake.project_threads[project_id].insert(
            0,
            {"conversation_id": newer_id, "title": "After removal", "project_id": project_id},
        )
        after_remove = await watchdog.scan_once(trigger="test")
        assert newer_id not in {
            entry["conversation_id"] for entry in after_remove["queue"]["entries"]
        }

    asyncio.run(run())


def test_existing_working_mode_enrolls_running_baselined_thread_with_one_inspection(tmp_path):
    async def run():
        selected = project_id("f")
        old_id = "66666666-6666-6666-6666-666666666666"
        running = canonical_snapshot(
            conversation_id=old_id,
            assistant_status="in_progress",
            assistant_end_turn=False,
            current_node="a-running",
            running=True,
            update_time=1001.0,
        )
        fake = FakeAdapter(
            running,
            snapshots={old_id: running},
            project_threads={
                selected: [
                    {
                        "conversation_id": old_id,
                        "title": "Existing running task",
                        "project_id": selected,
                        "current_node": "a-running",
                        "update_time": 1001.0,
                    }
                ]
            },
        )
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
                projects_path=tmp_path / "projects.json",
                pre_send_confirmation_seconds=0,
            ),
            adapter_factory=lambda: fake,
            clock=lambda: 1000.0,
        )

        await watchdog.select_project(selected, name="Existing work")
        switched = watchdog.set_project_mode(selected, "existing_working")
        assert switched["projects"]["items"][0]["watch_mode"] == "existing_working"
        assert switched["projects"]["items"][0]["existing_working_initialized"] is False

        result = await watchdog.scan_once(trigger="test")
        assert fake.inspections == 1
        assert fake.inspected_conversation_ids == [old_id]
        assert [item["conversation_id"] for item in result["queue"]["entries"]] == [old_id]
        state = result["queue"]["entries"][0]["state"]
        assert state["task_source"] == f"project-watch-existing:{selected}"
        assert state["auto_project_mode"] == "existing_working"
        assert result["projects"]["items"][0]["existing_working_initialized"] is True
        assert result["runtime"]["project_scan"]["physical_list_requests"] == 1
        assert result["runtime"]["project_scan"]["physical_existing_inspect_requests"] == 1
        assert result["runtime"]["project_scan"]["existing_working_threads_added"] == 1
        assert result["runtime"]["project_scan"]["new_threads_added"] == 0

    asyncio.run(run())


def test_existing_working_mode_skips_unchanged_stopped_thread_until_fingerprint_changes(tmp_path):
    async def run():
        selected = project_id("1")
        old_id = "77777777-7777-7777-7777-777777777777"
        stopped = canonical_snapshot(
            conversation_id=old_id,
            current_node="a-stopped",
            update_time=1000.0,
        )
        running = canonical_snapshot(
            conversation_id=old_id,
            assistant_status="in_progress",
            assistant_end_turn=False,
            current_node="a-new-work",
            running=True,
            update_time=1002.0,
        )
        fake = FakeAdapter(
            stopped,
            snapshots={old_id: stopped},
            project_threads={
                selected: [
                    {
                        "conversation_id": old_id,
                        "title": "Existing stopped task",
                        "project_id": selected,
                        "current_node": "a-stopped",
                        "update_time": 1000.0,
                    }
                ]
            },
        )
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
                projects_path=tmp_path / "projects.json",
                pre_send_confirmation_seconds=0,
            ),
            adapter_factory=lambda: fake,
            clock=lambda: 1000.0,
        )

        await watchdog.select_project(selected, name="Existing work")
        watchdog.set_project_mode(selected, "existing_working")

        first = await watchdog.scan_once(trigger="test")
        assert fake.inspections == 1
        assert first["queue"]["entries"] == []
        assert first["runtime"]["project_scan"]["physical_existing_inspect_requests"] == 1

        second = await watchdog.scan_once(trigger="test")
        assert fake.inspections == 1
        assert second["queue"]["entries"] == []
        assert second["runtime"]["project_scan"]["physical_existing_inspect_requests"] == 0

        fake.snapshots[old_id] = running
        fake.project_threads[selected][0]["current_node"] = "a-new-work"
        fake.project_threads[selected][0]["update_time"] = 1002.0

        third = await watchdog.scan_once(trigger="test")
        assert fake.inspections == 2
        assert [item["conversation_id"] for item in third["queue"]["entries"]] == [old_id]
        assert third["runtime"]["project_scan"]["physical_existing_inspect_requests"] == 1
        assert third["runtime"]["project_scan"]["existing_working_threads_added"] == 1

    asyncio.run(run())


def test_existing_working_mode_retries_only_failed_classification(tmp_path):
    async def run():
        selected = project_id("2")
        good_id = "88888888-8888-8888-8888-888888888888"
        failed_id = "99999999-9999-9999-9999-999999999999"
        stopped = canonical_snapshot(
            conversation_id=good_id,
            current_node="a-stopped",
            update_time=1000.0,
        )
        fake = FakeAdapter(
            stopped,
            snapshots={good_id: stopped},
            inspect_errors={failed_id},
            project_threads={
                selected: [
                    {
                        "conversation_id": good_id,
                        "title": "Classified stopped",
                        "project_id": selected,
                        "current_node": "a-stopped",
                        "update_time": 1000.0,
                    },
                    {
                        "conversation_id": failed_id,
                        "title": "Retry me",
                        "project_id": selected,
                        "current_node": "a-unknown",
                        "update_time": 1000.0,
                    },
                ]
            },
        )
        watchdog = ChatWatchdog(
            ChatWatchdogConfig(
                True,
                tmp_path / "threads.txt",
                tmp_path / "state.json",
                tmp_path / "done.jsonl",
                projects_path=tmp_path / "projects.json",
                pre_send_confirmation_seconds=0,
            ),
            adapter_factory=lambda: fake,
            clock=lambda: 1000.0,
        )

        await watchdog.select_project(selected, name="Existing work")
        watchdog.set_project_mode(selected, "existing_working")

        first = await watchdog.scan_once(trigger="test")
        assert fake.inspections == 2
        assert first["projects"]["items"][0]["existing_working_initialized"] is True
        assert "could not verify 1 existing thread" in first["projects"]["items"][0]["last_error"]

        second = await watchdog.scan_once(trigger="test")
        assert fake.inspections == 3
        assert fake.inspected_conversation_ids.count(good_id) == 1
        assert fake.inspected_conversation_ids.count(failed_id) == 2
        assert second["runtime"]["project_scan"]["physical_existing_inspect_requests"] == 1

    asyncio.run(run())


def test_watchdog_project_dashboard_selects_and_removes_projects(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_DASHBOARD_TOKEN", "secret")
    monkeypatch.setenv("TEST_WATCHDOG_PROJECT_HOME", str(tmp_path / "gpt-home"))
    store = GPTThreadStore(
        lambda: tmp_path / "workspace",
        home_env="TEST_WATCHDOG_PROJECT_HOME",
    )
    project_id = "g-p-" + "c" * 32
    existing_id = "55555555-5555-5555-5555-555555555555"
    fake = FakeAdapter(
        ThreadSnapshot(True, existing_id),
        projects=[
            {
                "id": project_id,
                "name": "Project C",
                "description": "Selected from dashboard",
                "permissions": {"can_read": True, "can_write": True},
            }
        ],
        project_threads={
            project_id: [
                {"conversation_id": existing_id, "title": "Existing", "project_id": project_id}
            ]
        },
    )
    watchdog = ChatWatchdog(
        ChatWatchdogConfig(
            False,
            tmp_path / "threads.txt",
            tmp_path / "state.json",
            tmp_path / "done.jsonl",
            projects_path=tmp_path / "projects.json",
        ),
        adapter_factory=lambda: fake,
    )
    app = Starlette()
    install_usage_dashboard(app, store, watchdog)
    auth = {"authorization": "Bearer secret"}
    mutation = {**auth, "x-mcp-dashboard-csrf": "chat-watchdog"}

    with TestClient(app) as client:
        available = client.get("/dashboard/watchdog/projects/available", headers=auth)
        assert available.status_code == 200
        assert available.json()["items"][0]["id"] == project_id
        assert available.json()["items"][0]["selected"] is False

        denied = client.post(
            "/dashboard/watchdog/projects",
            json={"project_id": project_id, "name": "Project C"},
            headers=auth,
        )
        assert denied.status_code == 403

        added = client.post(
            "/dashboard/watchdog/projects",
            json={"project_id": project_id, "name": "Project C"},
            headers=mutation,
        )
        assert added.status_code == 201
        assert added.json()["projects"]["items"][0]["project_id"] == project_id
        assert added.json()["projects"]["items"][0]["seen_thread_count"] == 1
        assert added.json()["projects"]["items"][0]["watch_mode"] == "new_threads_only"

        mode_denied = client.patch(
            f"/dashboard/watchdog/projects/{project_id}",
            json={"watch_mode": "existing_working"},
            headers=auth,
        )
        assert mode_denied.status_code == 403

        switched = client.patch(
            f"/dashboard/watchdog/projects/{project_id}",
            json={"watch_mode": "existing_working"},
            headers=mutation,
        )
        assert switched.status_code == 200
        assert switched.json()["projects"]["items"][0]["watch_mode"] == "existing_working"
        assert switched.json()["projects"]["items"][0]["existing_working_initialized"] is False

        invalid_mode = client.patch(
            f"/dashboard/watchdog/projects/{project_id}",
            json={"watch_mode": "everything"},
            headers=mutation,
        )
        assert invalid_mode.status_code == 400

        removed = client.delete(
            f"/dashboard/watchdog/projects/{project_id}",
            headers=mutation,
        )
        assert removed.status_code == 200
        assert removed.json()["removed"] is True
        assert removed.json()["projects"]["count"] == 0


def test_watchdog_config_keeps_legacy_positional_adapter_argument(tmp_path):
    config = ChatWatchdogConfig(
        True,
        tmp_path / "threads.txt",
        tmp_path / "state.json",
        tmp_path / "done.jsonl",
        "direct",
    )
    assert config.adapter_mode == "direct"
    assert config.projects_path is None
