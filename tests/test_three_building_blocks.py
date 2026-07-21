from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncio

from conversation_gateway import ConversationGateway, DeliveryState
from durable_ledger import DurableLedger
from state_reducer import ActionType, ReducedState, reduce_snapshot, reduce_state
from chat_internal_client import RuntimeNotReadyError, RuntimeProtocolError, RuntimeUnavailableError, normalize_conversation_payload
import pytest


def _message(message_id: str, role: str, text: str, *, status: str = "finished_successfully", end_turn=True):
    return {
        "id": message_id,
        "author": {"role": role},
        "content": {"parts": [text]},
        "status": status,
        "end_turn": end_turn,
        "metadata": {},
    }


def _canonical_snapshot(
    *,
    role="assistant",
    status="finished_successfully",
    end_turn=True,
    text="idle result",
    running=False,
    current_node="a",
):
    return {
        "state_verified": True,
        "canonical": True,
        "current_node": current_node,
        "running": running,
        "active_stream": False,
        "turns": [
            {
                "key": "a",
                "role": role,
                "status": status,
                "end_turn": end_turn,
                "text": text,
            }
        ],
    }


def test_normalized_snapshot_keeps_side_branch_ids_for_reconciliation():
    raw = {
        "id": "c",
        "current_node": "a2",
        "mapping": {
            "root": {"parent": None, "message": None},
            "u": {"parent": "root", "message": _message("u-msg", "user", "task", end_turn=None)},
            "a1": {"parent": "u", "message": _message("a1-msg", "assistant", "old")},
            "a2": {"parent": "u", "message": _message("a2-msg", "assistant", "new")},
            "side": {"parent": "u", "message": _message("generated-id", "user", "continue", end_turn=None)},
        },
    }
    snapshot = normalize_conversation_payload(raw, "c")
    assert [turn["key"] for turn in snapshot["turns"]] == ["u-msg", "a2-msg"]
    assert "generated-id" in snapshot["all_message_ids"]


def test_gateway_classifies_delivery_and_reconciles_side_branch():
    result = ConversationGateway.classify_send_result(
        {"sent": True, "observed": False, "user_message_id": "generated-id", "request_id": "r"}
    )
    assert result.state is DeliveryState.SENT_UNCONFIRMED
    reconciled = ConversationGateway.reconcile(
        result,
        {
            "found": True,
            "canonical": True,
            "state_verified": True,
            "all_message_ids": ["generated-id"],
        },
    )
    assert reconciled.state is DeliveryState.DELIVERED


def test_gateway_classification_keeps_transport_separate_from_task_failure():
    assert ConversationGateway.classify_send_result({"sent": False, "running": True}).state is DeliveryState.DEFERRED_RUNNING
    assert ConversationGateway.classify_send_result({"sent": False, "target_changed": True}).state is DeliveryState.TARGET_CHANGED
    assert ConversationGateway.classify_send_result({"sent": False, "permanent_failure": True}).state is DeliveryState.PERMANENT_FAILURE


def test_reducer_priority_completion_parent_waiting_and_read_error():
    completed = reduce_state(
        {"status": "stopped_incomplete", "completion_marker": "DONE"},
        {"state_verified": True, "turns": [{"role": "assistant", "status": "finished_successfully", "end_turn": True, "text": "DONE"}]},
        [{"purpose": "instruction"}],
    )
    assert completed == [{"action": "mark_completed"}]
    instruction = reduce_state(
        {"status": "stopped_incomplete"},
        _canonical_snapshot(),
        [{"purpose": "instruction"}],
    )
    assert instruction[0]["action"] == "deliver_command"
    assert reduce_state(
        {"status": "waiting_for_parent"},
        _canonical_snapshot(),
        [],
    )[0]["action"] == "wait_for_parent"
    assert reduce_state({"status": "stopped_incomplete"}, {"read_error": True}, [])[0]["action"] == "retry_read"


def test_reducer_terminal_without_marker_can_continue_and_unknown_fails_closed():
    assert reduce_state(
        {"status": "stopped_incomplete"},
        _canonical_snapshot(),
        [],
    )[0]["action"] == "send_continuation"
    assert reduce_state(
        {"status": "unknown"},
        {"state_verified": True, "turns": []},
        [],
    )[0]["action"] == "no_action"


@pytest.mark.parametrize("error", [RuntimeUnavailableError("app unavailable"), RuntimeNotReadyError("renderer not ready"), RuntimeProtocolError("JS evaluation failed"), TimeoutError("read timeout")])
def test_gateway_read_runtime_errors_are_retryable_without_task_failure(error):
    class Client:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return None
        async def get_thread(self, _conversation_id):
            raise error

    result = asyncio.run(ConversationGateway(lambda: Client()).read("conversation"))
    assert result["transport_error"] is True
    assert result["state_verified"] is False


def test_reducer_exact_marker_matrix_and_contradictions():
    marker = "DONE"
    def snap(role="assistant", status="finished_successfully", end=True, text=""):
        return {"state_verified": True, "canonical": True, "current_node": "a", "turns": [
            {"key": "a", "role": role, "status": status, "end_turn": end, "text": text}
        ]}

    assert reduce_snapshot({"completion_marker": marker}, snap(text="x\nDONE")).state is ReducedState.COMPLETED
    assert reduce_snapshot({"completion_marker": marker}, snap(role="user", text=marker)).state is ReducedState.AWAITING_ASSISTANT
    assert reduce_snapshot({"completion_marker": marker}, snap(status="in_progress", end=False, text=marker)).state is ReducedState.RUNNING
    assert reduce_snapshot({"completion_marker": marker}, snap(status="finished_successfully", end=True, text="partial")).actions == (ActionType.SEND_CONTINUATION,)
    assert reduce_snapshot({"completion_marker": marker}, {**snap(), "current_node": "other"}).state is ReducedState.UNKNOWN



def test_reducer_uses_visible_leaf_while_preserving_raw_current_node():
    snapshot = _canonical_snapshot(current_node="hidden-tail")
    snapshot["visible_current_node"] = "a"
    snapshot["turns"][0]["node_id"] = "a"
    snapshot["turns"][0]["key"] = "assistant-message-id"

    reduction = reduce_snapshot({"completion_marker": "DONE"}, snapshot)

    assert reduction.state is ReducedState.STOPPED_INCOMPLETE
    assert reduction.actions == (ActionType.SEND_CONTINUATION,)

    contradictory = {**snapshot, "visible_current_node": "other-visible-node"}
    assert reduce_snapshot(
        {"completion_marker": "DONE"}, contradictory
    ).state is ReducedState.UNKNOWN


def test_ledger_owns_schema_and_evidence_columns(tmp_path):
    ledger = DurableLedger(tmp_path / "ledger.sqlite")
    with ledger.connect() as db:
        columns = {row[1] for row in db.execute("PRAGMA table_info(commands)")}
        task_columns = {row[1] for row in db.execute("PRAGMA table_info(tasks)")}
        agent_columns = {row[1] for row in db.execute("PRAGMA table_info(agents)")}
        assert {
            "request_id",
            "user_message_id",
            "parent_message_id",
            "purpose",
            "next_attempt_at",
        } <= columns
        assert {"next_check_at", "gateway_operation_id"} <= task_columns
        assert {
            "creation_request_id",
            "creation_user_message_id",
            "gateway_agent_id",
            "gateway_control_operation_id",
        } <= agent_columns
        assert "gateway_operation_id" in columns
        assert db.execute("SELECT value FROM schema_meta WHERE key='version'").fetchone()[0] == "11"
        assert db.execute("PRAGMA foreign_keys").fetchone()[0] == 1


class _GatewayClient:
    def __init__(self, snapshot=None, send_result=None, send_error=None):
        self.snapshot = snapshot or _canonical_snapshot()
        self.send_result = send_result or {"sent": True, "observed": True}
        self.send_error = send_error
        self.continue_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def get_thread(self, _conversation_id):
        return self.snapshot

    async def continue_thread(
        self,
        conversation_id,
        message,
        *,
        expected_current_node,
        wait_for_completion,
        user_message_id=None,
    ):
        self.continue_calls.append(
            (conversation_id, message, expected_current_node, wait_for_completion)
        )
        if self.send_error is not None:
            raise self.send_error
        return dict(self.send_result)

    async def create_thread(self, prompt, *, project_id=None, title=None):
        return {"conversation_id": "created", "prompt": prompt}

    async def cancel(self, _conversation_id):
        return {"cancelled": True}


def test_gateway_send_checks_target_state_before_external_write():
    changed = _GatewayClient(snapshot=_canonical_snapshot(current_node="new"))
    result = asyncio.run(
        ConversationGateway(lambda: changed).send(
            "conversation", "message", expected_current_node="old"
        )
    )
    assert result.state is DeliveryState.TARGET_CHANGED
    assert changed.continue_calls == []

    running = _GatewayClient(
        snapshot=_canonical_snapshot(
            status="in_progress", end_turn=False, running=True
        )
    )
    result = asyncio.run(
        ConversationGateway(lambda: running).send(
            "conversation", "message", expected_current_node="a"
        )
    )
    assert result.state is DeliveryState.DEFERRED_RUNNING
    assert running.continue_calls == []


def test_gateway_send_classifies_uncertain_and_permanent_failures():
    uncertain = _GatewayClient(
        send_result={
            "sent": True,
            "observed": False,
            "request_id": "request",
            "user_message_id": "user-message",
            "parent_message_id": "a",
            "reason": "confirmation timed out",
        }
    )
    result = asyncio.run(
        ConversationGateway(lambda: uncertain).send(
            "conversation", "message", expected_current_node="a"
        )
    )
    assert result.state is DeliveryState.SENT_UNCONFIRMED
    assert result.message == "message"
    assert result.user_message_id == "user-message"

    invalid = _GatewayClient(send_error=ValueError("invalid request"))
    result = asyncio.run(
        ConversationGateway(lambda: invalid).send(
            "conversation", "message", expected_current_node="a"
        )
    )
    assert result.state is DeliveryState.PERMANENT_FAILURE


def test_reducer_stale_and_missing_transcript_are_fail_closed():
    stale = _canonical_snapshot(
        status="in_progress", end_turn=False, running=True
    )
    stale["stale"] = True
    reduction = reduce_snapshot({"status": "stale"}, stale)
    assert reduction.state is ReducedState.STALE
    assert reduction.actions == (ActionType.MARK_STALE,)

    no_transcript = reduce_snapshot(
        {"status": "stopped_incomplete"},
        {"state_verified": True, "turns": []},
        [{"purpose": "instruction"}],
    )
    assert no_transcript.state is ReducedState.UNKNOWN
    assert no_transcript.actions == (ActionType.NO_ACTION,)


def test_ledger_migrates_legacy_notification_purpose(tmp_path):
    path = tmp_path / "ledger.sqlite"
    ledger = DurableLedger(path)
    with ledger.transaction() as db:
        db.execute(
            "INSERT INTO orchestrations(orchestration_id,notification_policy,created_at) "
            "VALUES('orch','auto_resume',1)"
        )
        db.execute(
            "INSERT INTO agents(agent_id,orchestration_id,root_agent_id,title,status,"
            "notification_policy,created_at,updated_at) "
            "VALUES('parent','orch','parent','parent','running','notify_only',1,1)"
        )
        db.execute(
            "INSERT INTO agents(agent_id,orchestration_id,parent_agent_id,root_agent_id,"
            "title,status,notification_policy,created_at,updated_at) "
            "VALUES('child','orch','parent','parent','child','completed','auto_resume',1,1)"
        )
        db.execute(
            "INSERT INTO commands(command_id,from_agent_id,to_agent_id,sequence_no,message,"
            "interrupt_policy,status,ack_event_seq,purpose,created_at) "
            "VALUES('cmd','child','parent',1,?, 'queue','delivery_uncertain',9,'instruction',1)",
            ('[SUBAGENT UPDATE] events: [{"kind":"completed"}]',),
        )

    DurableLedger(path)
    with ledger.connect() as db:
        purpose = db.execute(
            "SELECT purpose FROM commands WHERE command_id='cmd'"
        ).fetchone()[0]
    assert purpose == "completion"


def test_gateway_reconciliation_rejects_unverified_conversation_evidence():
    result = ConversationGateway.classify_send_result(
        {
            "sent": True,
            "observed": False,
            "user_message_id": "generated-id",
        }
    )
    reconciled = ConversationGateway.reconcile(
        result,
        {
            "found": True,
            "canonical": True,
            "state_verified": False,
            "all_message_ids": ["generated-id"],
        },
    )
    assert reconciled.state is DeliveryState.SENT_UNCONFIRMED


def test_gateway_transport_failure_after_submission_is_sent_unconfirmed():
    client = _GatewayClient(
        send_error=RuntimeProtocolError("stream disconnected after dispatch")
    )
    result = asyncio.run(
        ConversationGateway(lambda: client).send(
            "conversation", "message", expected_current_node="a"
        )
    )
    assert result.state is DeliveryState.SENT_UNCONFIRMED
    assert result.message == "message"
    assert len(client.continue_calls) == 1


def test_domain_reducer_rejects_marker_embedded_in_other_text():
    marker = "DONE"
    snapshot = {
        "canonical": True,
        "state_verified": True,
        "current_node": "assistant",
        "running": False,
        "active_stream": False,
        "turns": [
            {
                "key": "assistant",
                "role": "assistant",
                "status": "finished_successfully",
                "end_turn": True,
                "text": "The word DONE appears inside a sentence.",
            }
        ],
    }
    reduction = reduce_snapshot({"completion_marker": marker}, snapshot)
    assert reduction.state is ReducedState.STOPPED_INCOMPLETE
    assert reduction.actions == (ActionType.SEND_CONTINUATION,)
