from __future__ import annotations

import asyncio

from chat_direct_client import DirectChatClient


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def request(self, method: str, **params):
        self.calls.append((method, params))
        if method == "get_thread":
            return {
                "id": params["conversation_id"],
                "title": "Direct",
                "current_node": "a1",
                "mapping": {
                    "u1": {
                        "parent": None,
                        "message": {
                            "id": "u1",
                            "author": {"role": "user"},
                            "content": {"content_type": "text", "parts": ["hello"]},
                            "status": "finished_successfully",
                            "end_turn": None,
                        },
                    },
                    "a1": {
                        "parent": "u1",
                        "message": {
                            "id": "a1",
                            "author": {"role": "assistant"},
                            "content": {"content_type": "text", "parts": ["world"]},
                            "status": "finished_successfully",
                            "end_turn": True,
                        },
                    },
                },
            }
        return {"ready": True}

    async def close(self) -> None:
        return None


def test_direct_client_normalizes_backend_thread_without_desktop_runtime() -> None:
    async def scenario() -> None:
        transport = FakeTransport()
        client = DirectChatClient(transport=transport)
        payload = await client.get_thread("thread-1")
        assert payload["found"] is True
        assert payload["current_node"] == "a1"
        assert [turn["text"] for turn in payload["turns"]] == ["hello", "world"]
        assert transport.calls == [("get_thread", {"conversation_id": "thread-1"})]

    asyncio.run(scenario())


def test_direct_client_continuation_uses_stable_delivery_id() -> None:
    async def scenario() -> None:
        transport = FakeTransport()
        client = DirectChatClient(transport=transport)
        await client.continue_thread(
            "thread-1",
            "continue",
            expected_current_node="a1",
            wait_for_completion=False,
            user_message_id="stable-message-id",
        )
        assert transport.calls[-1] == (
            "continue_thread",
            {
                "conversation_id": "thread-1",
                "message": "continue",
                "expected_current_node": "a1",
                "wait_for_completion": False,
                "user_message_id": "stable-message-id",
                "force": False,
                "preferred_model": "",
                "thinking_effort": "extended",
                "require_high_reasoning": True,
            },
        )

    asyncio.run(scenario())
