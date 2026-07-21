from __future__ import annotations

import asyncio
import json
import inspect
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest

from chat_internal_client import (
    CHAT_RENDERER_BRIDGE_JS,
    InternalChatClient,
    RuntimeNotReadyError,
    RuntimeProtocolError,
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


def test_renderer_selection_uses_configured_webview_port():
    alternate = target("http://127.0.0.1:5188/", target_id="alternate")
    selected = select_main_renderer_target(
        [alternate], expected_webview_port=5188
    )
    assert selected.target_id == "alternate"
    with pytest.raises(RuntimeNotReadyError, match="auxiliary"):
        select_main_renderer_target([alternate])


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

        async def fake_evaluate_raw(self, _expression, *, timeout=None):
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

        async def fake_evaluate_raw(_expression, *, timeout=None):
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

        async def fake_evaluate(_body, **_kwargs):
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
    assert "if (!streamFinished) handles.set(conversationId, handle)" in source
    assert "const waitForCompletion" in source
    assert "finishDispatch" in source


def test_runtime_errors_are_sanitized():
    text = sanitize_runtime_error(
        "Authorization: secret-token Cookie=session-value token=private Bearer abc.def"
    )
    assert "secret-token" not in text
    assert "session-value" not in text
    assert "private" not in text
    assert "abc.def" not in text
    assert "[redacted]" in text


def test_project_normalization_exposes_safe_selection_metadata():
    from chat_internal_client import normalize_project_list, normalize_project_threads

    raw = {
        "cursor": "next",
        "items": [
            {
                "gizmo": {
                    "gizmo": {
                        "id": "g-p-project",
                        "display": {"name": "PrivacyAI", "description": "Private project"},
                        "current_user_permission": {"can_read": True, "can_write": True},
                        "updated_at": "2026-07-19T00:00:00Z",
                    }
                },
                "conversations": {"items": [{"id": "chat-a"}]},
            }
        ],
    }
    normalized = normalize_project_list(raw)
    assert normalized["cursor"] == "next"
    assert normalized["items"] == [
        {
            "id": "g-p-project",
            "name": "PrivacyAI",
            "description": "Private project",
            "archived": False,
            "updated_at": "2026-07-19T00:00:00Z",
            "permissions": {"can_read": True, "can_write": True, "can_delete": False},
            "conversation_count": 1,
        }
    ]

    threads = normalize_project_threads(
        {"items": [{"id": "chat-a", "title": "Agent", "current_node": "node"}]},
        "g-p-project",
    )
    assert threads["items"][0]["project_id"] == "g-p-project"
    assert threads["items"][0]["conversation_id"] == "chat-a"


def test_context_digest_includes_tools_and_recap_but_not_hidden_reasoning():
    from chat_internal_client import build_thread_context, build_thread_tail

    def node(parent, item):
        return {"parent": parent, "message": item}

    def raw_message(
        mid,
        role,
        text,
        *,
        content_type="text",
        recipient="all",
        name="",
        hidden=False,
        preamble=False,
        end_turn=None,
    ):
        content = {"content_type": content_type}
        if content_type == "reasoning_recap":
            content["content"] = text
        else:
            content["parts"] = [text]
        return {
            "id": mid,
            "author": {"role": role, "name": name},
            "content": content,
            "recipient": recipient,
            "status": "finished_successfully",
            "end_turn": end_turn,
            "metadata": {
                "is_visually_hidden_from_conversation": hidden,
                "is_thinking_preamble_message": preamble,
            },
        }

    raw = {
        "id": "chat",
        "current_node": "final",
        "mapping": {
            "user": node(None, raw_message("user", "user", "do work")),
            "secret": node("user", raw_message("secret", "assistant", "private hidden thought", hidden=True, end_turn=False)),
            "progress": node("secret", raw_message("progress", "assistant", "Inspecting tests", hidden=True, preamble=True, end_turn=False)),
            "call": node("progress", raw_message("call", "assistant", '{"token":"secret"}', content_type="code", recipient="terminal.run")),
            "result": node(
                "call",
                raw_message(
                    "result",
                    "tool",
                    "Authorization: abc token=private session_id=s123 70 tests passed",
                    name="terminal.run",
                ),
            ),
            "recap": node("result", raw_message("recap", "assistant", "The stream contract is fixed.", content_type="reasoning_recap", end_turn=False)),
            "final": node("recap", raw_message("final", "assistant", "Completed", end_turn=True)),
        },
    }
    digest = build_thread_context(raw, "chat")
    kinds = [event["kind"] for event in digest["events"]]
    assert kinds == ["progress", "tool_call", "tool_result", "reasoning_recap", "final"]
    rendered = json.dumps(digest)
    assert "private hidden thought" not in rendered
    assert '"token":"secret"' not in rendered
    assert "abc" not in rendered
    assert "private" not in rendered
    assert "s123" not in rendered
    assert "[redacted]" in rendered

    cursor = digest["events"][1]["cursor"]
    delta = build_thread_context(raw, "chat", since_cursor=cursor)
    assert [event["kind"] for event in delta["events"]] == [
        "tool_result",
        "reasoning_recap",
        "final",
    ]
    tail = build_thread_tail(raw, "chat", lines=10)
    assert "private hidden thought" not in tail["text"]
    assert "tool_call: terminal.run" in tail["text"]
    assert "reasoning_recap: The stream contract is fixed." in tail["text"]
    assert "Completed" in tail["text"]


def test_create_thread_uses_project_mode_and_verifies_persistence(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9996")
        captured = ""
        after_raw = {
            "id": "conversation-new",
            "current_node": "assistant-node",
            "gizmo_id": "g-p-project",
            "mapping": {
                "user-node": {
                    "parent": None,
                    "message": message("user-message", "user", "start task"),
                },
                "assistant-node": {
                    "parent": "user-node",
                    "message": message(
                        "assistant-message",
                        "assistant",
                        "working",
                        status="in_progress",
                        end_turn=False,
                    ),
                },
            },
        }

        async def fake_evaluate(body, **_kwargs):
            nonlocal captured
            captured = body
            return {
                "sent": True,
                "observed": True,
                "running": True,
                "conversation_id": "conversation-new",
                "user_message_id": "user-message",
                "project_id": "g-p-project",
                "after_raw": after_raw,
            }

        monkeypatch.setattr(client, "_evaluate", fake_evaluate)
        result = await client.create_thread(
            "start task", project_id="g-p-project", title="Child"
        )
        assert "startCompletionStream" in captured
        assert "gizmo_interaction" in captured
        assert "conversation_mode" in captured
        assert "parent_message_id" not in captured
        assert result["observed"] is True
        assert result["conversation_id"] == "conversation-new"
        assert result["chat_url"].endswith("/g/g-p-project/c/conversation-new")
        assert result["current_node"] == "assistant-node"

    asyncio.run(run())


def test_new_thread_late_handle_cannot_restore_completed_stream_marker():
    source = inspect.getsource(InternalChatClient.create_thread)
    assert "streamFinished = false" in source
    assert "streamFinished = true; observe(value)" in source
    assert "conversationId && !streamFinished" in source
    assert "streams.delete(conversationId); handles.delete(conversationId)" in source
    assert "ownedStream = ownedStreamActive(conversationId) && !streamFinished" in source


def test_create_thread_reports_terminal_readback_without_owned_stream(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9995")
        after_raw = {
            "id": "conversation-done",
            "current_node": "assistant-node",
            "mapping": {
                "user-node": {
                    "parent": None,
                    "message": message("user-message", "user", "quick task"),
                },
                "assistant-node": {
                    "parent": "user-node",
                    "message": message(
                        "assistant-message",
                        "assistant",
                        "done",
                        status="finished_successfully",
                        end_turn=True,
                    ),
                },
            },
        }

        async def fake_evaluate(_body, **_kwargs):
            return {
                "sent": True,
                "observed": True,
                "running": True,
                "owned_stream": False,
                "conversation_id": "conversation-done",
                "user_message_id": "user-message",
                "after_raw": after_raw,
            }

        monkeypatch.setattr(client, "_evaluate", fake_evaluate)
        result = await client.create_thread("quick task")
        assert result["observed"] is True
        assert result["running"] is False
        assert result["current_node"] == "assistant-node"

    asyncio.run(run())


def test_orchestration_dispatch_mode_is_encoded_without_terminal_wait():
    source = inspect.getsource(InternalChatClient.continue_thread)
    assert "wait_for_completion: bool = True" in source
    assert "waitForCompletion ?" in source
    assert source.count("await client.get(conversationId)") == 2
    assert "completion stream did not start before the dispatch timeout" in source


def test_context_cursor_detects_same_node_progress_update():
    from chat_internal_client import build_thread_context

    def payload(text):
        return {
            "id": "chat",
            "current_node": "assistant-node",
            "mapping": {
                "user-node": {
                    "parent": None,
                    "message": message("user-message", "user", "task"),
                },
                "assistant-node": {
                    "parent": "user-node",
                    "message": {
                        **message(
                            "assistant-message",
                            "assistant",
                            text,
                            status="in_progress",
                            end_turn=False,
                        ),
                        "metadata": {
                            "is_thinking_preamble_message": True,
                            "is_visually_hidden_from_conversation": True,
                        },
                    },
                },
            },
        }

    first = build_thread_context(payload("working"), "chat")
    cursor = first["next_cursor"]
    second = build_thread_context(
        payload("working with more detail"),
        "chat",
        since_cursor=cursor,
    )
    assert second["cursor_reset"] is False
    assert second["cursor_updated"] is True
    assert len(second["events"]) == 1
    assert second["events"][0]["summary"] == "working with more detail"
    assert second["next_cursor"] != cursor


def test_truncated_context_event_cursor_does_not_repeat_forever():
    from chat_internal_client import build_thread_context

    raw = {
        "id": "chat",
        "current_node": "assistant-node",
        "mapping": {
            "user-node": {
                "parent": None,
                "message": message("user-message", "user", "task"),
            },
            "assistant-node": {
                "parent": "user-node",
                "message": {
                    **message(
                        "assistant-message",
                        "assistant",
                        "x" * 3000,
                        status="in_progress",
                        end_turn=False,
                    ),
                    "metadata": {
                        "is_thinking_preamble_message": True,
                        "is_visually_hidden_from_conversation": True,
                    },
                },
            },
        },
    }
    first = build_thread_context(raw, "chat", max_chars=300)
    assert len(first["events"]) == 1
    second = build_thread_context(
        raw,
        "chat",
        since_cursor=first["next_cursor"],
        max_chars=300,
    )
    assert second["events"] == []
    assert second["next_cursor"] == first["next_cursor"]


def test_continue_result_does_not_confirm_from_unverified_cyclic_state(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9994")
        after_raw = {
            "current_node": "user-node",
            "mapping": {
                "user-node": {
                    "parent": "user-node",
                    "message": message("user-message", "user", "continue"),
                }
            },
        }

        async def fake_evaluate(_body, **_kwargs):
            return {
                "sent": True,
                "observed": True,
                "running": False,
                "user_message_id": "user-message",
                "parent_message_id": "parent-node",
                "after_raw": after_raw,
            }

        monkeypatch.setattr(client, "_evaluate", fake_evaluate)
        result = await client.continue_thread(
            "conversation",
            "continue",
            expected_current_node="parent-node",
            wait_for_completion=False,
        )
        assert result["sent"] is True
        assert result["observed"] is False
        assert "not found" in result["reason"]

    asyncio.run(run())


def test_mutating_evaluation_is_never_replayed_after_transport_failure(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9993")
        attempts = 0
        reconnects = 0

        async def fake_evaluate_raw(_expression, *, timeout=None):
            nonlocal attempts
            attempts += 1
            raise asyncio.TimeoutError("mutation timed out")

        async def fake_reconnect():
            nonlocal reconnects
            reconnects += 1

        monkeypatch.setattr(client, "_evaluate_raw", fake_evaluate_raw)
        monkeypatch.setattr(client, "reconnect", fake_reconnect)

        with pytest.raises(asyncio.TimeoutError, match="mutation timed out"):
            await client._evaluate(
                "return {ok:true};",
                retry_on_transport=False,
                timeout=15,
            )

        assert attempts == 1
        assert reconnects == 0

    asyncio.run(run())


def test_javascript_exception_details_are_preserved_and_sanitized(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9992")

        async def fake_call(_method, _params=None, *, timeout=None):
            return {
                "result": {"type": "object", "subtype": "error"},
                "exceptionDetails": {
                    "text": "Uncaught ReferenceError: token=secret-value is not defined",
                    "lineNumber": 4,
                    "columnNumber": 8,
                },
            }

        monkeypatch.setattr(client, "_call", fake_call)

        with pytest.raises(RuntimeProtocolError) as error:
            await client._evaluate_raw("throw new Error('broken')")

        rendered = str(error.value)
        assert "ReferenceError" in rendered
        assert "line 5, column 9" in rendered
        assert "secret-value" not in rendered
        assert "token=[redacted]" in rendered

    asyncio.run(run())


def test_continuation_timeout_preserves_supplied_delivery_identity(monkeypatch):
    async def run():
        client = InternalChatClient("http://127.0.0.1:9991")
        calls = []

        async def fake_evaluate(body, **kwargs):
            calls.append((body, kwargs))
            raise asyncio.TimeoutError("dispatch timed out")

        monkeypatch.setattr(client, "_evaluate", fake_evaluate)

        result = await client.continue_thread(
            "conversation",
            "continue",
            expected_current_node="assistant-node",
            wait_for_completion=False,
            user_message_id="stable-user-message",
        )

        assert len(calls) == 1
        body, kwargs = calls[0]
        assert "stable-user-message" in body
        assert kwargs["retry_on_transport"] is False
        assert kwargs["timeout"] >= 15
        assert result["sent"] is True
        assert result["observed"] is False
        assert result["submission_uncertain"] is True
        assert result["user_message_id"] == "stable-user-message"
        assert result["parent_message_id"] == "assistant-node"

    asyncio.run(run())


def test_mutation_confirmation_uses_bounded_single_readback():
    create_source = inspect.getsource(InternalChatClient.create_thread)
    continue_source = inspect.getsource(InternalChatClient.continue_thread)

    assert create_source.count("await client.get(conversationId)") == 1
    assert continue_source.count("await client.get(conversationId)") == 2
    assert "attempt < 20" not in create_source
    assert "attempts = waitForCompletion" not in continue_source


def test_owned_stream_marker_is_age_bounded_but_cancel_handle_is_retained():
    source = CHAT_RENDERER_BRIDGE_JS + inspect.getsource(InternalChatClient)
    assert "Date.now() - startedAt <= 120000" in source
    assert "owned_stream:ownedStreamActive" in source
    assert "if (!streams.has(id)) return {cancelled:false" in source
