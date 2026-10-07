from __future__ import annotations

import inspect
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Mapping, Optional, Protocol, Sequence, TypeVar

from .errors import MalformedBackendResponse
from .models import MutationResult, ThreadSnapshot, TurnSnapshot


class BackendAdapter(Protocol):
    async def health(self) -> Mapping[str, Any]: ...

    async def create_thread(
        self,
        *,
        project_id: str,
        prompt: str,
        title: str,
        idempotency_key: str,
    ) -> MutationResult: ...

    async def get_thread(self, *, conversation_id: str) -> ThreadSnapshot: ...

    async def continue_thread(
        self,
        *,
        conversation_id: str,
        message: str,
        idempotency_key: str,
    ) -> MutationResult: ...

    async def cancel_thread(self, *, conversation_id: str) -> MutationResult: ...

    async def delete_thread(self, *, conversation_id: str) -> MutationResult: ...

    async def list_project_threads(
        self, *, project_id: str
    ) -> Sequence[ThreadSnapshot]: ...

    async def list_discovery_threads(
        self, *, project_id: str = "", cursor: str | None = None, offset: int = 0
    ) -> Mapping[str, Any]: ...


AsyncOrSyncCallable = Callable[..., Any]
T = TypeVar("T")


@dataclass
class CallableBackend:
    create: AsyncOrSyncCallable
    get: AsyncOrSyncCallable
    continue_: AsyncOrSyncCallable
    cancel: AsyncOrSyncCallable
    delete: AsyncOrSyncCallable
    list_project: AsyncOrSyncCallable
    health_: Optional[AsyncOrSyncCallable] = None

    async def health(self) -> Mapping[str, Any]:
        if self.health_ is None:
            return {"ready": True}
        value = await _invoke(self.health_)
        if not isinstance(value, Mapping):
            raise MalformedBackendResponse("health returned a non-mapping")
        return value

    async def create_thread(
        self,
        *,
        project_id: str,
        prompt: str,
        title: str,
        idempotency_key: str,
    ) -> MutationResult:
        value = await _invoke(
            self.create,
            project_id=project_id,
            prompt=prompt,
            title=title,
            idempotency_key=idempotency_key,
        )
        return _require(value, MutationResult, "create_thread")

    async def get_thread(self, *, conversation_id: str) -> ThreadSnapshot:
        value = await _invoke(self.get, conversation_id=conversation_id)
        return _require(value, ThreadSnapshot, "get_thread")

    async def continue_thread(
        self,
        *,
        conversation_id: str,
        message: str,
        idempotency_key: str,
    ) -> MutationResult:
        value = await _invoke(
            self.continue_,
            conversation_id=conversation_id,
            message=message,
            idempotency_key=idempotency_key,
        )
        return _require(value, MutationResult, "continue_thread")

    async def cancel_thread(self, *, conversation_id: str) -> MutationResult:
        value = await _invoke(self.cancel, conversation_id=conversation_id)
        return _require(value, MutationResult, "cancel_thread")

    async def delete_thread(self, *, conversation_id: str) -> MutationResult:
        value = await _invoke(self.delete, conversation_id=conversation_id)
        return _require(value, MutationResult, "delete_thread")

    async def list_project_threads(
        self, *, project_id: str
    ) -> Sequence[ThreadSnapshot]:
        value = await _invoke(self.list_project, project_id=project_id)
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise MalformedBackendResponse(
                "list_project_threads returned a non-sequence"
            )
        if not all(isinstance(item, ThreadSnapshot) for item in value):
            raise MalformedBackendResponse(
                "list_project_threads contained a non-ThreadSnapshot value"
            )
        return value


async def _invoke(function: AsyncOrSyncCallable, **kwargs: Any) -> Any:
    result = function(**kwargs)
    if inspect.isawaitable(result):
        return await result
    return result


def _require(value: Any, expected: type[T], method: str) -> T:
    if not isinstance(value, expected):
        raise MalformedBackendResponse(
            f"{method} returned {type(value).__name__}, expected {expected.__name__}"
        )
    return value


@dataclass(frozen=True)
class BackendCall:
    method: str
    arguments: Mapping[str, Any]


class FakeBackend:
    """Deterministic in-memory backend used by unit and integration tests."""

    def __init__(
        self,
        *,
        completion_marker: str = "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS",
    ) -> None:
        self.completion_marker = completion_marker
        self.calls: list[BackendCall] = []
        self.threads: dict[str, ThreadSnapshot] = {}
        self.projects: dict[str, list[str]] = defaultdict(list)
        self._idempotent_creates: dict[str, MutationResult] = {}
        self._idempotent_continues: dict[str, MutationResult] = {}
        self._scripted: dict[str, Deque[Any]] = defaultdict(deque)
        self._counter = 0
        self._read_counts: dict[str, int] = defaultdict(int)
        self.complete_after_reads: dict[str, int] = {}

    def queue(self, method: str, value_or_exception: Any) -> None:
        self._scripted[method].append(value_or_exception)

    def set_snapshot(self, snapshot: ThreadSnapshot, *, project_id: str = "") -> None:
        self.threads[snapshot.conversation_id] = snapshot
        if project_id and snapshot.conversation_id not in self.projects[project_id]:
            self.projects[project_id].append(snapshot.conversation_id)

    async def health(self) -> Mapping[str, Any]:
        scripted = self._record("health", {})
        if scripted is not None:
            if not isinstance(scripted, Mapping):
                raise MalformedBackendResponse("scripted health is not a mapping")
            return scripted
        return {"ready": True}

    def _record(self, method: str, arguments: Mapping[str, Any]) -> Optional[Any]:
        self.calls.append(BackendCall(method, dict(arguments)))
        queue = self._scripted[method]
        if not queue:
            return None
        value = queue.popleft()
        if isinstance(value, BaseException):
            raise value
        return value

    async def create_thread(
        self,
        *,
        project_id: str,
        prompt: str,
        title: str,
        idempotency_key: str,
    ) -> MutationResult:
        scripted = self._record(
            "create_thread",
            {
                "project_id": project_id,
                "title": title,
                "idempotency_key": idempotency_key,
                "prompt_length": len(prompt),
            },
        )
        if scripted is not None:
            return _require(scripted, MutationResult, "create_thread")
        existing = self._idempotent_creates.get(idempotency_key)
        if existing is not None:
            return existing
        self._counter += 1
        conversation_id = f"fake-conversation-{self._counter}"
        user_id = f"fake-user-{self._counter}"
        assistant_id = f"fake-assistant-{self._counter}"
        snapshot = ThreadSnapshot(
            conversation_id=conversation_id,
            found=True,
            running=True,
            title=title,
            current_node=assistant_id,
            turns=(
                TurnSnapshot(user_id, "user", "finished_successfully", prompt, True),
                TurnSnapshot(
                    assistant_id,
                    "assistant",
                    "in_progress",
                    "",
                    False,
                ),
            ),
        )
        self.set_snapshot(snapshot, project_id=project_id)
        result = MutationResult(
            accepted=True,
            conversation_id=conversation_id,
            message_id=user_id,
            running=True,
        )
        self._idempotent_creates[idempotency_key] = result
        return result

    async def get_thread(self, *, conversation_id: str) -> ThreadSnapshot:
        scripted = self._record("get_thread", {"conversation_id": conversation_id})
        if scripted is not None:
            return _require(scripted, ThreadSnapshot, "get_thread")
        self._read_counts[conversation_id] += 1
        snapshot = self.threads.get(conversation_id)
        if snapshot is None:
            return ThreadSnapshot(conversation_id, False, False, ())
        threshold = self.complete_after_reads.get(conversation_id)
        if threshold is not None and self._read_counts[conversation_id] >= threshold:
            snapshot = self.complete(conversation_id)
        return snapshot

    async def continue_thread(
        self,
        *,
        conversation_id: str,
        message: str,
        idempotency_key: str,
    ) -> MutationResult:
        scripted = self._record(
            "continue_thread",
            {
                "conversation_id": conversation_id,
                "idempotency_key": idempotency_key,
                "message_length": len(message),
            },
        )
        if scripted is not None:
            return _require(scripted, MutationResult, "continue_thread")
        existing = self._idempotent_continues.get(idempotency_key)
        if existing is not None:
            return existing
        snapshot = self.threads.get(conversation_id)
        if snapshot is None:
            raise MalformedBackendResponse(
                "cannot continue an unknown fake conversation"
            )
        self._counter += 1
        user_id = f"fake-user-{self._counter}"
        assistant_id = f"fake-assistant-{self._counter}"
        turns = (
            *snapshot.turns,
            TurnSnapshot(user_id, "user", "finished_successfully", message, True),
            TurnSnapshot(assistant_id, "assistant", "in_progress", "", False),
        )
        self.threads[conversation_id] = ThreadSnapshot(
            conversation_id=conversation_id,
            found=True,
            running=True,
            title=snapshot.title,
            current_node=assistant_id,
            turns=turns,
        )
        result = MutationResult(
            accepted=True,
            conversation_id=conversation_id,
            message_id=user_id,
            running=True,
        )
        self._idempotent_continues[idempotency_key] = result
        return result

    async def cancel_thread(self, *, conversation_id: str) -> MutationResult:
        scripted = self._record("cancel_thread", {"conversation_id": conversation_id})
        if scripted is not None:
            return _require(scripted, MutationResult, "cancel_thread")
        snapshot = self.threads.get(conversation_id)
        if snapshot is not None:
            turns = list(snapshot.turns)
            if turns and turns[-1].role == "assistant":
                last = turns[-1]
                turns[-1] = TurnSnapshot(
                    last.message_id,
                    last.role,
                    "cancelled",
                    last.text,
                    True,
                    last.created_at,
                )
            self.threads[conversation_id] = ThreadSnapshot(
                conversation_id,
                True,
                False,
                tuple(turns),
                snapshot.title,
                snapshot.current_node,
            )
        return MutationResult(True, conversation_id=conversation_id, running=False)

    async def delete_thread(self, *, conversation_id: str) -> MutationResult:
        scripted = self._record("delete_thread", {"conversation_id": conversation_id})
        if scripted is not None:
            return _require(scripted, MutationResult, "delete_thread")
        self.threads.pop(conversation_id, None)
        for values in self.projects.values():
            if conversation_id in values:
                values.remove(conversation_id)
        return MutationResult(True, conversation_id=conversation_id, running=False)

    async def list_project_threads(
        self, *, project_id: str
    ) -> Sequence[ThreadSnapshot]:
        scripted = self._record("list_project_threads", {"project_id": project_id})
        if scripted is not None:
            if not isinstance(scripted, Sequence):
                raise MalformedBackendResponse(
                    "scripted project list is not a sequence"
                )
            return scripted
        return tuple(
            self.threads[conversation_id]
            for conversation_id in self.projects.get(project_id, ())
            if conversation_id in self.threads
        )

    def complete(
        self,
        conversation_id: str,
        *,
        text: Optional[str] = None,
    ) -> ThreadSnapshot:
        snapshot = self.threads[conversation_id]
        turns = list(snapshot.turns)
        completion_text = text or f"completed\n{self.completion_marker}"
        if turns and turns[-1].role == "assistant":
            last = turns[-1]
            turns[-1] = TurnSnapshot(
                last.message_id,
                "assistant",
                "finished_successfully",
                completion_text,
                True,
                last.created_at,
            )
        else:
            self._counter += 1
            turns.append(
                TurnSnapshot(
                    f"fake-assistant-{self._counter}",
                    "assistant",
                    "finished_successfully",
                    completion_text,
                    True,
                )
            )
        completed = ThreadSnapshot(
            conversation_id,
            True,
            False,
            tuple(turns),
            snapshot.title,
            turns[-1].message_id,
        )
        self.threads[conversation_id] = completed
        return completed
