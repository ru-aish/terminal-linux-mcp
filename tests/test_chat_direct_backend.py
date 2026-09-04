from __future__ import annotations

import asyncio

from chat_direct_client import DirectChatClient
from chat_gateway.adapters.direct_chatgpt import DirectChatGPTBackend


class FakeTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def request(self, method: str, **params):
        self.calls.append((method, params))
        if method == "create_thread":
            return {
                "accepted": True,
                "conversation_id": "thread-1",
                "message_id": params["user_message_id"],
                "running": True,
            }
        return {"ready": True}

    async def close(self) -> None:
        return None


def test_backend_create_derives_repeatable_message_id_from_idempotency_key() -> None:
    async def scenario() -> None:
        transport = FakeTransport()
        backend = DirectChatGPTBackend(client=DirectChatClient(transport=transport))
        first = await backend.create_thread(
            project_id="project-1",
            prompt="work",
            title="title",
            idempotency_key="operation-1",
        )
        second = await backend.create_thread(
            project_id="project-1",
            prompt="work",
            title="title",
            idempotency_key="operation-1",
        )
        assert first.message_id == second.message_id
        assert transport.calls[0][1]["user_message_id"] == first.message_id

    asyncio.run(scenario())
