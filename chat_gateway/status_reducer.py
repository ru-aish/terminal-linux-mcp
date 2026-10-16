from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Optional

from .models import AgentState, ThreadSnapshot, TurnSnapshot

_TERMINAL_SUCCESS = {"finished_successfully", "complete", "completed", "success"}
_TERMINAL_FAILURE = {"failed", "error", "finished_with_error"}
_TERMINAL_CANCELLED = {"cancelled", "canceled", "aborted"}
_IN_PROGRESS = {"in_progress", "running", "streaming", "pending"}


def _marker_present(text: str, marker: str) -> bool:
    return bool(marker) and any(
        line.strip() == marker for line in str(text or "").splitlines()
    )


@dataclass(frozen=True)
class Reduction:
    state: AgentState
    snapshot_hash: str
    terminal: bool
    marker_present: bool
    terminal_evidence: bool
    reason: str
    expected_message_present: Optional[bool] = None


def reduce_snapshot(
    snapshot: ThreadSnapshot,
    *,
    completion_marker: str,
    expected_message_id: Optional[str] = None,
) -> Reduction:
    fingerprint = snapshot_fingerprint(snapshot)
    expected_present = None
    if expected_message_id:
        expected_present = any(
            turn.message_id == expected_message_id for turn in snapshot.turns
        )

    if not snapshot.found:
        return Reduction(
            AgentState.UNKNOWN,
            fingerprint,
            False,
            False,
            False,
            "conversation not found canonically",
            expected_present,
        )

    assistant_turns = [turn for turn in snapshot.turns if turn.role == "assistant"]
    if not assistant_turns:
        return Reduction(
            AgentState.RUNNING if snapshot.running else AgentState.UNKNOWN,
            fingerprint,
            False,
            False,
            False,
            "no assistant turn exists yet",
            expected_present,
        )

    last_assistant = assistant_turns[-1]
    status = last_assistant.status.strip().lower()
    marker_present = _marker_present(last_assistant.text or "", completion_marker)
    later_in_progress = _has_later_in_progress(snapshot, last_assistant)

    if status in _TERMINAL_FAILURE and not later_in_progress:
        return Reduction(
            AgentState.FAILED,
            fingerprint,
            True,
            marker_present,
            True,
            "canonical terminal assistant failure",
            expected_present,
        )

    if status in _TERMINAL_CANCELLED and not later_in_progress:
        return Reduction(
            AgentState.CANCELLED,
            fingerprint,
            True,
            marker_present,
            True,
            "canonical terminal assistant cancellation",
            expected_present,
        )

    successful_terminal = (
        status in _TERMINAL_SUCCESS
        and last_assistant.end_turn is not False
        and marker_present
        and not later_in_progress
    )
    if successful_terminal:
        return Reduction(
            AgentState.COMPLETED,
            fingerprint,
            True,
            True,
            True,
            "canonical terminal assistant completion with marker",
            expected_present,
        )

    if snapshot.running or status in _IN_PROGRESS or later_in_progress:
        return Reduction(
            AgentState.RUNNING,
            fingerprint,
            False,
            marker_present,
            False,
            "canonical state contains active generation evidence",
            expected_present,
        )

    # Deliberately do not infer completion from running=false, missing handles,
    # elapsed time, or a superficially terminal-looking turn without the marker.
    return Reduction(
        AgentState.UNKNOWN,
        fingerprint,
        False,
        marker_present,
        False,
        "running=false without sufficient canonical terminal evidence",
        expected_present,
    )


def snapshot_fingerprint(snapshot: ThreadSnapshot) -> str:
    payload = {
        "conversation_id": snapshot.conversation_id,
        "found": snapshot.found,
        "running": snapshot.running,
        "current_node": snapshot.current_node,
        "turns": [
            {
                "id": turn.message_id,
                "role": turn.role,
                "status": turn.status,
                "end_turn": turn.end_turn,
                "text_hash": hashlib.sha256(
                    (turn.text or "").encode("utf-8")
                ).hexdigest(),
            }
            for turn in snapshot.turns
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _has_later_in_progress(
    snapshot: ThreadSnapshot,
    candidate: TurnSnapshot,
) -> bool:
    seen_candidate = False
    for turn in snapshot.turns:
        if turn.message_id == candidate.message_id:
            seen_candidate = True
            continue
        if not seen_candidate or turn.role != "assistant":
            continue
        if turn.status.strip().lower() in _IN_PROGRESS or turn.end_turn is False:
            return True
    return False
