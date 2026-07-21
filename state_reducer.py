"""Pure, canonical classification of a verified conversation snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable


class ReducedState(str, Enum):
    RUNNING = "running"
    AWAITING_ASSISTANT = "awaiting_assistant"
    STOPPED_INCOMPLETE = "stopped_incomplete"
    WAITING_FOR_PARENT = "waiting_for_parent"
    STALE = "stale"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class ActionType(str, Enum):
    RECORD_PROGRESS = "record_progress"
    RETRY_READ = "retry_read"
    DELIVER_COMMAND = "deliver_command"
    SEND_CONTINUATION = "send_continuation"
    NOTIFY_PARENT = "notify_parent"
    WAIT_FOR_PARENT = "wait_for_parent"
    MARK_COMPLETED = "mark_completed"
    MARK_FAILED = "mark_failed"
    MARK_STALE = "mark_stale"
    NO_ACTION = "no_action"


def _marker_present(text: str, marker: str) -> bool:
    return bool(marker) and any(
        line.strip() == marker for line in str(text or "").splitlines()
    )


@dataclass(frozen=True)
class Reduction:
    state: ReducedState
    actions: tuple[ActionType, ...]
    reason: str = ""

    def as_dicts(self) -> list[dict[str, str]]:
        return [{"action": action.value} for action in self.actions]


_RUNNING = {"in_progress", "streaming", "queued", "pending"}
_SUCCESS = {"finished_successfully"}
_FAILURE = {"finished_error", "failed", "cancelled", "canceled", "interrupted", "incomplete"}


def _commands(pending: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [command for command in pending if isinstance(command, dict)]


def _has(commands: list[dict[str, Any]], *purposes: str) -> bool:
    wanted = set(purposes)
    return any(str(c.get("purpose") or c.get("kind") or "") in wanted for c in commands)


def reduce_snapshot(
    task_state: dict[str, Any] | str,
    conversation_snapshot: dict[str, Any],
    pending_commands: Iterable[dict[str, Any]] = (),
) -> Reduction:
    """Classify from canonical snapshot details; task status is only context.

    The reducer never performs I/O.  Unverified or contradictory data is
    deliberately conservative and produces no write action.
    """
    task = task_state if isinstance(task_state, dict) else {"status": task_state}
    snapshot = conversation_snapshot if isinstance(conversation_snapshot, dict) else {}
    commands = _commands(pending_commands)
    if snapshot.get("transport_error") or snapshot.get("read_error"):
        return Reduction(ReducedState.UNKNOWN, (ActionType.RETRY_READ,), "runtime/read error")
    if snapshot.get("state_verified") is not True:
        return Reduction(ReducedState.UNKNOWN, (ActionType.RETRY_READ,), "canonical state is not verified")
    turns = snapshot.get("turns")
    if not isinstance(turns, (list, tuple)) or not turns:
        if str(task.get("status") or "") == "waiting_for_parent":
            return Reduction(ReducedState.WAITING_FOR_PARENT, (ActionType.WAIT_FOR_PARENT,), "waiting for parent")
        if snapshot.get("running") or snapshot.get("active_stream"):
            return Reduction(ReducedState.RUNNING, (ActionType.NO_ACTION,), "conversation is running")
        return Reduction(
            ReducedState.UNKNOWN,
            (ActionType.NO_ACTION,),
            "conversation has no verified turns; refusing an unsafe write",
        )
    current_node = str(snapshot.get("current_node") or "")
    latest = turns[-1] if isinstance(turns[-1], dict) else {}
    latest_key = str(latest.get("key") or latest.get("node_id") or latest.get("id") or "")
    role = str(latest.get("role") or "").casefold()
    status = str(latest.get("status") or "").casefold()
    end_turn = latest.get("end_turn")
    if snapshot.get("canonical") and (not current_node or (latest_key and latest_key != current_node)):
        return Reduction(ReducedState.UNKNOWN, (ActionType.NO_ACTION,), "canonical node and latest turn disagree")
    if (snapshot.get("stale") or str(task.get("status") or "") == "stale") and (
        snapshot.get("active_stream") or snapshot.get("running") or status in _RUNNING
    ):
        return Reduction(ReducedState.STALE, (ActionType.MARK_STALE,), "active generation is stale")
    if snapshot.get("active_stream") or snapshot.get("running"):
        return Reduction(ReducedState.RUNNING, (ActionType.NO_ACTION,), "conversation is running")
    if role == "user":
        return Reduction(ReducedState.AWAITING_ASSISTANT, (ActionType.NO_ACTION,), "latest turn is user")
    if role != "assistant" or not status or end_turn is None:
        return Reduction(ReducedState.UNKNOWN, (ActionType.NO_ACTION,), "missing canonical assistant fields")
    if status not in _RUNNING | _SUCCESS | _FAILURE:
        return Reduction(ReducedState.UNKNOWN, (ActionType.NO_ACTION,), "unknown assistant status")
    if (status in _RUNNING and end_turn is True) or (status in _SUCCESS | _FAILURE and end_turn is False):
        return Reduction(ReducedState.UNKNOWN, (ActionType.NO_ACTION,), "status and end_turn contradict")
    if status in _RUNNING:
        return Reduction(ReducedState.RUNNING, (ActionType.NO_ACTION,), "assistant generation is active")

    marker = str(task.get("completion_marker") or "")
    text = str(latest.get("text") or "")
    exact_marker = _marker_present(text, marker)
    if exact_marker and status == "finished_successfully" and end_turn is True:
        actions = [ActionType.MARK_COMPLETED]
        if _has(commands, "completion", "completed"):
            actions.append(ActionType.DELIVER_COMMAND)
        return Reduction(ReducedState.COMPLETED, tuple(actions), "verified terminal completion marker")
    if exact_marker:
        if status in {"cancelled", "canceled"}:
            return Reduction(ReducedState.CANCELLED, (ActionType.MARK_FAILED,), "verified cancelled terminal response")
        if status in {"finished_error", "failed", "interrupted", "incomplete"}:
            return Reduction(ReducedState.FAILED, (ActionType.MARK_FAILED,), "verified failed terminal response")
        return Reduction(ReducedState.UNKNOWN, (ActionType.NO_ACTION,), "marker is not a verified terminal success")
    if _has(commands, "instruction", "answer", "parent_instruction"):
        return Reduction(ReducedState.STOPPED_INCOMPLETE, (ActionType.DELIVER_COMMAND,), "parent command has priority")
    if str(task.get("status") or "") == "waiting_for_parent":
        return Reduction(ReducedState.WAITING_FOR_PARENT, (ActionType.WAIT_FOR_PARENT,), "waiting for parent")
    if _has(commands, "question"):
        return Reduction(ReducedState.STOPPED_INCOMPLETE, (ActionType.NOTIFY_PARENT,), "blocking question")
    if _has(commands, "completion", "completed"):
        return Reduction(ReducedState.STOPPED_INCOMPLETE, (ActionType.DELIVER_COMMAND,), "completion notification pending")
    return Reduction(ReducedState.STOPPED_INCOMPLETE, (ActionType.SEND_CONTINUATION,), "terminal response lacks marker")


def reduce_state(task_state: dict[str, Any] | str, conversation_snapshot: dict[str, Any], pending_commands: Iterable[dict[str, Any]] = ()) -> list[dict[str, str]]:
    return reduce_snapshot(task_state, conversation_snapshot, pending_commands).as_dicts()


reduce_actions = reduce_state

__all__ = ["ActionType", "ReducedState", "Reduction", "reduce_snapshot", "reduce_state", "reduce_actions"]
