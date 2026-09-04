from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from threading import Lock
from typing import Any, Mapping, Sequence

from chat_direct_client import (
    DirectBackendResponseError,
    DirectChatClient,
    DirectSubmissionUncertainError,
)
from chat_internal_client import RuntimeProtocolError, RuntimeUnavailableError

from ..errors import (
    BackendError,
    InfrastructureError,
    MalformedBackendResponse,
    RateLimitError,
    SubmissionUncertainError,
)
from ..models import MutationResult, ThreadSnapshot, TurnSnapshot

DEFAULT_HIGH_REASONING_MODEL = "gpt-5-6-thinking"
_MESSAGE_NAMESPACE = uuid.UUID("c5c52224-90de-4b2c-a0e8-49ba3e945bee")


class DirectChatGPTBackend:
    def __init__(self, *, client: DirectChatClient | None = None, model_slug: str = DEFAULT_HIGH_REASONING_MODEL, thinking_effort: str = "extended", require_high_reasoning: bool = True, journal_path: Path | None = None) -> None:
        self.client = client or DirectChatClient(
            preferred_model=model_slug,
            thinking_effort=thinking_effort,
            require_high_reasoning=require_high_reasoning,
        )
        self._parents: dict[str, str] = {}
        self._journal_path = journal_path or Path(
            os.environ.get(
                "MCP_CHAT_DIRECT_JOURNAL",
                "~/.GPT/direct-chat-idempotency.json",
            )
        ).expanduser()
        self._journal_lock = Lock()

    async def health(self) -> Mapping[str, Any]:
        return await self._call(self.client.health())

    @staticmethod
    def _message_id(idempotency_key: str) -> str:
        return str(uuid.uuid5(_MESSAGE_NAMESPACE, idempotency_key))

    async def create_thread(self, *, project_id: str, prompt: str, title: str, idempotency_key: str) -> MutationResult:
        message_id = self._message_id(idempotency_key)
        pending = self._journal_get(idempotency_key)
        if pending.get("conversation_id"):
            return MutationResult(
                True,
                str(pending["conversation_id"]),
                message_id,
                running=True,
                metadata={"reconciled": True},
            )
        if pending:
            found = await self._call(
                self.client.find_message(message_id, project_id=project_id)
            )
            if found:
                self._journal_put(
                    idempotency_key,
                    message_id=message_id,
                    project_id=project_id,
                    conversation_id=found,
                )
                return MutationResult(
                    True, found, message_id, running=True,
                    metadata={"reconciled": True},
                )
            raise SubmissionUncertainError(
                "pending new-thread submission is not visible yet; reconciliation will retry without POSTing"
            )
        else:
            self._journal_put(
                idempotency_key,
                message_id=message_id,
                project_id=project_id,
                conversation_id="",
            )
        try:
            payload = await self.client.create_thread(
                prompt, project_id=project_id, title=title, wait_for_completion=False,
                user_message_id=message_id,
            )
        except DirectSubmissionUncertainError as exc:
            raise SubmissionUncertainError(
                "new-thread submission became uncertain; it was not replayed"
            ) from exc
        except RuntimeUnavailableError as exc:
            self._journal_delete(idempotency_key)
            raise InfrastructureError(str(exc)) from exc
        except DirectBackendResponseError as exc:
            self._journal_delete(idempotency_key)
            raise _map_response_error(exc) from exc
        if not isinstance(payload, Mapping):
            raise MalformedBackendResponse("direct creation returned a non-object")
        conversation_id = str(payload.get("conversation_id") or "")
        if conversation_id:
            self._journal_put(
                idempotency_key,
                message_id=message_id,
                project_id=project_id,
                conversation_id=conversation_id,
            )
        return _mutation(payload)

    async def get_thread(self, *, conversation_id: str) -> ThreadSnapshot:
        payload = await self._call(self.client.get_thread(conversation_id))
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
        message_id = self._message_id(idempotency_key)
        try:
            payload = await self.client.continue_thread(
                conversation_id, message, expected_current_node=parent,
                wait_for_completion=False, user_message_id=message_id,
            )
        except DirectSubmissionUncertainError:
            # Verification by stable message id is safer than replaying an
            # already-accepted continuation.
            return MutationResult(
                True, conversation_id, message_id, running=None,
                metadata={"submission_uncertain": True},
            )
        except RuntimeUnavailableError as exc:
            raise InfrastructureError(str(exc)) from exc
        except DirectBackendResponseError as exc:
            raise _map_response_error(exc) from exc
        return _mutation(payload, conversation_id)

    async def cancel_thread(self, *, conversation_id: str) -> MutationResult:
        payload = await self._call(self.client.cancel(conversation_id))
        return MutationResult(bool(payload.get("cancelled")), conversation_id, running=False)

    async def delete_thread(self, *, conversation_id: str) -> MutationResult:
        payload = await self._call(self.client.delete_thread(conversation_id))
        self._parents.pop(conversation_id, None)
        return MutationResult(bool(payload.get("accepted")), conversation_id, running=False)

    async def list_project_threads(self, *, project_id: str) -> Sequence[ThreadSnapshot]:
        payload = await self._call(self.client.list_project_threads(project_id, limit=50))
        return [await self.get_thread(conversation_id=str(item["conversation_id"])) for item in payload.get("items", [])]

    async def _call(self, operation: Any) -> Any:
        try:
            return await operation
        except DirectBackendResponseError as exc:
            raise _map_response_error(exc) from exc
        except (RuntimeUnavailableError, RuntimeProtocolError) as exc:
            raise InfrastructureError(str(exc)) from exc

    def _journal_read(self) -> dict[str, dict[str, str]]:
        try:
            value = json.loads(self._journal_path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            return {}

    def _journal_write(self, value: dict[str, dict[str, str]]) -> None:
        self._journal_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self._journal_path.with_name(
            f".{self._journal_path.name}.{os.getpid()}.tmp"
        )
        temporary.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
        os.chmod(temporary, 0o600)
        os.replace(temporary, self._journal_path)

    def _journal_get(self, key: str) -> dict[str, str]:
        with self._journal_lock:
            value = self._journal_read().get(key)
            return dict(value) if isinstance(value, dict) else {}

    def _journal_put(self, key: str, **entry: str) -> None:
        with self._journal_lock:
            value = self._journal_read()
            value[key] = dict(entry)
            if len(value) > 2000:
                value = dict(list(value.items())[-2000:])
            self._journal_write(value)

    def _journal_delete(self, key: str) -> None:
        with self._journal_lock:
            value = self._journal_read()
            if value.pop(key, None) is not None:
                self._journal_write(value)


def _mutation(payload: Mapping[str, Any], conversation_id: str | None = None) -> MutationResult:
    return MutationResult(
        accepted=bool(payload.get("accepted", payload.get("sent"))),
        conversation_id=str(payload.get("conversation_id") or conversation_id or "") or None,
        message_id=str(payload.get("message_id") or payload.get("user_message_id") or "") or None,
        running=bool(payload.get("running", True)),
        metadata={key: value for key, value in payload.items() if key not in {"accepted", "sent", "conversation_id", "message_id", "user_message_id", "running"}},
    )


def _map_response_error(error: DirectBackendResponseError) -> BackendError:
    status = error.status_code
    if status == 429:
        return RateLimitError(str(error))
    if status is not None and 400 <= status < 500 and status not in {408, 425}:
        return BackendError(str(error), status_code=status, transient=False, permanent=True)
    return BackendError(str(error), status_code=status, transient=True, permanent=False)


def make_backend() -> DirectChatGPTBackend:
    return DirectChatGPTBackend()
