from __future__ import annotations

import asyncio
import inspect
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from chat_internal_client import (
    InternalChatClient,
    RuntimeNotReadyError,
    normalize_conversation_payload,
    sanitize_runtime_error,
    select_main_renderer_target,
)


def target(url: str, *, target_id: str = "target", title: str = "Codex") -> dict:
    return {
        "id": target_id,
        "type": "page",
        "title": title,
        "url": url,
        "webSocketDebuggerUrl": f"ws://127.0.0.1:9222/devtools/page/{target_id}",
    }


def test_renderer_selection_accepts_sandbox_devtools_and_prefers_plain_main():
    sandbox = target(
        "http://127.0.0.1:5175/?mcpAppSandboxDevtools=1",
        target_id="sandbox",
    )
    selected = select_main_renderer_target([sandbox])
    assert selected.target_id == "sandbox"

    plain = target("http://127.0.0.1:5175/", target_id="plain")
    selected = select_main_renderer_target([sandbox, plain])
    assert selected.target_id == "plain"


def test_renderer_selection_rejects_avatar_only_target():
    avatar = target(
        "http://127.0.0.1:5175/?initialRoute=%2Favatar-overlay",
        target_id="avatar",
    )
    with pytest.raises(RuntimeNotReadyError, match="auxiliary"):
        select_main_renderer_target([avatar])


def message(
    message_id: str,
    role: str,
    text: str,
    *,
    status: str = "finished_successfully",
    end_turn: bool | None = None,
    hidden: bool = False,
    model_slug: str = "",
) -> dict:
    metadata = {
        "is_visually_hidden_from_conversation": hidden,
        "model_slug": model_slug,
    }
    return {
        "id": message_id,
        "author": {"role": role},
        "content": {"content_type": "text", "parts": [text]},
        "status": status,
        "end_turn": end_turn,
        "create_time": 100.0,
        "metadata": metadata,
    }


def test_normalize_conversation_uses_only_active_visible_branch():
    raw = {
        "id": "conversation",
        "title": "Canonical thread",
        "current_node": "a-final-node",
        "update_time": 200.0,
        "mapping": {
            "root": {"parent": None, "message": None},
            "u-node": {
                "parent": "root",
                "message": message("u-message", "user", "task"),
            },
            "hidden-node": {
                "parent": "u-node",
                "message": message(
                    "hidden-message",
                    "assistant",
                    "not visible",
                    hidden=True,
                ),
            },
            "a-final-node": {
                "parent": "hidden-node",
                "message": message(
                    "a-final-message",
                    "assistant",
                    "done",
                    end_turn=True,
                    model_slug="gpt-5-6-thinking",
                ),
            },
            "off-branch": {
                "parent": "u-node",
                "message": message("off-message", "assistant", "wrong branch", end_turn=True),
            },
        },
    }
    payload = normalize_conversation_payload(raw, "conversation")
    assert payload["found"] is True
    assert payload["state_verified"] is True
    assert payload["current_node"] == "a-final-node"
    assert [turn["node_id"] for turn in payload["turns"]] == ["u-node", "a-final-node"]
    assert payload["turns"][-1]["model_slug"] == "gpt-5-6-thinking"
    assert payload["running"] is False


def test_normalize_conversation_reports_running_and_owned_stream():
    raw = {
        "current_node": "a-node",
        "mapping": {
            "u-node": {
                "parent": None,
                "message": message("u-message", "user", "task"),
            },
            "a-node": {
                "parent": "u-node",
                "message": message(
                    "a-message",
                    "assistant",
                    "partial",
                    status="in_progress",
                    end_turn=False,
                ),
            },
        },
    }
    payload = normalize_conversation_payload(raw, "conversation")
    assert payload["running"] is True
    assert payload["active_stream"] is False

    owned = normalize_conversation_payload(raw, "conversation", owned_stream=True)
    assert owned["running"] is True
    assert owned["active_stream"] is True


def test_normalize_conversation_fails_closed_for_missing_or_cyclic_branch():
    missing = normalize_conversation_payload(
        {"current_node": "missing", "mapping": {}},
        "conversation",
    )
    assert missing["found"] is True
    assert missing["state_verified"] is False
    assert "current_node" in missing["reason"]

    cyclic = normalize_conversation_payload(
        {
            "current_node": "a",
            "mapping": {
                "a": {
                    "parent": "b",
                    "message": message("a-message", "assistant", "a", end_turn=True),
                },
                "b": {
                    "parent": "a",
                    "message": message("b-message", "user", "b"),
                },
            },
        },
        "conversation",
    )
    assert cyclic["state_verified"] is False
    assert "cyclic" in cyclic["reason"]


def test_internal_operations_are_sequential_per_endpoint(monkeypatch):
    async def run():
        active = 0
        maximum = 0

        async def fake_evaluate_raw(self, _expression):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(0.02)
            active -= 1
            return {"ok": True}

        monkeypatch.setattr(InternalChatClient, "_evaluate_raw", fake_evaluate_raw)
        first = InternalChatClient("http://127.0.0.1:9999")
        second = InternalChatClient("http://127.0.0.1:9999")
        await asyncio.gather(
            first._evaluate("return {ok:true};"),
            second._evaluate("return {ok:true};"),
        )
        assert maximum == 1

    asyncio.run(run())


def test_evaluate_reconnects_once_after_transport_failure(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9998")
        attempts = 0
        reconnects = 0

        async def fake_evaluate_raw(_expression):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise OSError("renderer replaced")
            return {"ok": True}

        async def fake_reconnect():
            nonlocal reconnects
            reconnects += 1

        monkeypatch.setattr(client, "_evaluate_raw", fake_evaluate_raw)
        monkeypatch.setattr(client, "reconnect", fake_reconnect)
        assert await client._evaluate("return {ok:true};") == {"ok": True}
        assert attempts == 2
        assert reconnects == 1

    asyncio.run(run())


def test_continue_result_verifies_persisted_user_and_preserves_timeout(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9997")
        after_raw = {
            "current_node": "assistant-node",
            "mapping": {
                "user-node": {
                    "parent": None,
                    "message": message("user-message", "user", "continue"),
                },
                "assistant-node": {
                    "parent": "user-node",
                    "message": message(
                        "assistant-message",
                        "assistant",
                        "partial",
                        status="in_progress",
                        end_turn=False,
                    ),
                },
            },
        }

        async def fake_evaluate(_body):
            return {
                "sent": True,
                "observed": False,
                "running": True,
                "reason": "completion stream exceeded the configured timeout",
                "user_message_id": "user-message",
                "parent_message_id": "parent-node",
                "after_raw": after_raw,
            }

        monkeypatch.setattr(client, "_evaluate", fake_evaluate)
        result = await client.continue_thread(
            "conversation",
            "continue",
            expected_current_node="parent-node",
        )
        assert result["sent"] is True
        assert result["observed"] is True
        assert result["running"] is True
        assert "timeout" in result["reason"]
        assert result["final_message_id"] == "assistant-message"
        assert result["final_status"] == "in_progress"

    asyncio.run(run())


def test_bridge_cleans_owned_stream_after_late_completion():
    source = inspect.getsource(InternalChatClient.continue_thread)
    assert "const cleanup = () =>" in source
    assert "cleanup(); if (settled) return" in source
    assert "completion stream exceeded the configured timeout" in source


def test_stream_start_promise_is_not_treated_as_completion():
    source = inspect.getsource(InternalChatClient.continue_thread)
    assert "streamRequestId" in source
    assert "finish('promise')" not in source
    assert "if (!settled) handles.set(conversationId, handle)" in source


def test_runtime_errors_are_sanitized():
    text = sanitize_runtime_error(
        "Authorization: secret-token Cookie=session-value token=private Bearer abc.def"
    )
    assert "secret-token" not in text
    assert "session-value" not in text
    assert "private" not in text
    assert "abc.def" not in text
    assert "[redacted]" in text
