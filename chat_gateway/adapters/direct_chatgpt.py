from __future__ import annotations

import uuid
from typing import Any, Mapping, Sequence

from chat_direct_client import DirectChatClient

from ..errors import MalformedBackendResponse
from ..models import MutationResult, ThreadSnapshot, TurnSnapshot

DEFAULT_HIGH_REASONING_MODEL = "gpt-5-6-thinking"
_MESSAGE_NAMESPACE = uuid.UUID("c5c52224-90de-4b2c-a0e8-49ba3e945bee")


class DirectChatGPTBackend:
    def __init__(self, *, client: DirectChatClient | None = None, model_slug: str = DEFAULT_HIGH_REASONING_MODEL, thinking_effort: str = "extended", require_high_reasoning: bool = True) -> None:
        self.client = client or DirectChatClient(
            preferred_model=model_slug,
            thinking_effort=thinking_effort,
            require_high_reasoning=require_high_reasoning,
        )
        self._parents: dict[str, str] = {}

    async def health(self) -> Mapping[str, Any]:
        return await self.client.health()

    @staticmethod
    def _message_id(idempotency_key: str) -> str:
        return str(uuid.uuid5(_MESSAGE_NAMESPACE, idempotency_key))

    async def create_thread(self, *, project_id: str, prompt: str, title: str, idempotency_key: str) -> MutationResult:
        payload = await self.client.create_thread(
            prompt, project_id=project_id, title=title, wait_for_completion=False,
            user_message_id=self._message_id(idempotency_key),
        )
        if not isinstance(payload, Mapping):
            raise MalformedBackendResponse("direct creation returned a non-object")
        return _mutation(payload)

    async def get_thread(self, *, conversation_id: str) -> ThreadSnapshot:
        payload = await self.client.get_thread(conversation_id)
        current_node = str(payload.get("current_node") or "")
        if current_node:
            self._parents[conversation_id] = current_node
        turns = tuple(
            TurnSnapshot(
                message_id=str(turn.get("key") or turn.get("node_id") or ""),
                role=str(turn.get("role") or ""), status=str(turn.get("status") or ""),
                text=str(turn.get("text") or ""), end_turn=turn.get("end_turn"),
                created_at=turn.get("create_time"),
            )
            for turn in payload.get("turns", []) if isinstance(turn, Mapping)
        )
        return ThreadSnapshot(
            conversation_id=str(payload.get("conversation_id") or conversation_id),
            found=bool(payload.get("found")), running=bool(payload.get("running")), turns=turns,
            title=str(payload.get("title") or ""), current_node=current_node,
        )

    async def continue_thread(self, *, conversation_id: str, message: str, idempotency_key: str) -> MutationResult:
        parent = self._parents.get(conversation_id)
        if not parent:
            snapshot = await self.get_thread(conversation_id=conversation_id)
            parent = snapshot.current_node
        payload = await self.client.continue_thread(
            conversation_id, message, expected_current_node=parent,
            wait_for_completion=False, user_message_id=self._message_id(idempotency_key),
        )
        return _mutation(payload, conversation_id)

    async def cancel_thread(self, *, conversation_id: str) -> MutationResult:
        payload = await self.client.cancel(conversation_id)
        return MutationResult(bool(payload.get("cancelled")), conversation_id, running=False)

    async def delete_thread(self, *, conversation_id: str) -> MutationResult:
        payload = await self.client.delete_thread(conversation_id)
        self._parents.pop(conversation_id, None)
        return MutationResult(bool(payload.get("accepted")), conversation_id, running=False)

    async def list_project_threads(self, *, project_id: str) -> Sequence[ThreadSnapshot]:
        payload = await self.client.list_project_threads(project_id, limit=50)
        return [await self.get_thread(conversation_id=str(item["conversation_id"])) for item in payload.get("items", [])]


def _mutation(payload: Mapping[str, Any], conversation_id: str | None = None) -> MutationResult:
    return MutationResult(
        accepted=bool(payload.get("accepted", payload.get("sent"))),
        conversation_id=str(payload.get("conversation_id") or conversation_id or "") or None,
        message_id=str(payload.get("message_id") or payload.get("user_message_id") or "") or None,
        running=bool(payload.get("running", True)),
        metadata={key: value for key, value in payload.items() if key not in {"accepted", "sent", "conversation_id", "message_id", "user_message_id", "running"}},
    )


def make_backend() -> DirectChatGPTBackend:
    return DirectChatGPTBackend()
