from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from chat_direct_client import (
    DirectBackendResponseError,
    DirectChatClient,
    DirectSubmissionUncertainError,
)
from chat_gateway.adapters.direct_chatgpt import DirectChatGPTBackend
from chat_gateway.errors import InfrastructureError, RateLimitError, SubmissionUncertainError
from chat_internal_client import RuntimeUnavailableError


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


def test_backend_create_derives_repeatable_message_id_from_idempotency_key(tmp_path: Path) -> None:
    async def scenario() -> None:
        transport = FakeTransport()
        backend = DirectChatGPTBackend(
            client=DirectChatClient(transport=transport),
            journal_path=tmp_path / "journal.json",
        )
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


def test_pending_create_reconciles_before_any_replay(tmp_path: Path) -> None:
    class Client:
        create_calls = 0

        async def find_message(self, message_id: str, *, project_id: str = "") -> str:
            assert message_id
            assert project_id == "project-1"
            return "already-created"

        async def create_thread(self, *_args, **_kwargs):
            self.create_calls += 1
            raise AssertionError("uncertain create must be reconciled, not replayed")

    async def scenario() -> None:
        client = Client()
        backend = DirectChatGPTBackend(
            client=client, journal_path=tmp_path / "journal.json"
        )
        key = "operation-pending"
        backend._journal_put(
            key,
            message_id=backend._message_id(key),
            project_id="project-1",
            conversation_id="",
        )
        result = await backend.create_thread(
            project_id="project-1", prompt="work", title="title", idempotency_key=key
        )
        assert result.conversation_id == "already-created"
        assert result.metadata == {"reconciled": True}
        assert client.create_calls == 0

    asyncio.run(scenario())


def test_uncertain_create_fails_closed_without_replay(tmp_path: Path) -> None:
    class Client:
        async def create_thread(self, *_args, **_kwargs):
            raise DirectSubmissionUncertainError("worker vanished after POST")

    async def scenario() -> None:
        backend = DirectChatGPTBackend(
            client=Client(), journal_path=tmp_path / "journal.json"
        )
        with pytest.raises(SubmissionUncertainError):
            await backend.create_thread(
                project_id="project-1",
                prompt="work",
                title="title",
                idempotency_key="uncertain-operation",
            )

    asyncio.run(scenario())


def test_pre_submission_create_failure_remains_retryable(tmp_path: Path) -> None:
    class Client:
        async def create_thread(self, *_args, **_kwargs):
            raise RuntimeUnavailableError("worker could not start")

    async def scenario() -> None:
        backend = DirectChatGPTBackend(
            client=Client(), journal_path=tmp_path / "journal.json"
        )
        with pytest.raises(InfrastructureError):
            await backend.create_thread(
                project_id="project-1", prompt="work", title="title",
                idempotency_key="pre-submit-failure",
            )
        assert backend._journal_get("pre-submit-failure") == {}

    asyncio.run(scenario())


def test_pending_create_miss_never_falls_through_to_post(tmp_path: Path) -> None:
    class Client:
        create_calls = 0

        async def find_message(self, *_args, **_kwargs):
            return ""

        async def create_thread(self, *_args, **_kwargs):
            self.create_calls += 1
            raise AssertionError("pending uncertain create must never be replayed")

    async def scenario() -> None:
        client = Client()
        backend = DirectChatGPTBackend(
            client=client, journal_path=tmp_path / "journal.json"
        )
        key = "still-pending"
        backend._journal_put(
            key, message_id=backend._message_id(key), project_id="", conversation_id=""
        )
        with pytest.raises(SubmissionUncertainError) as captured:
            await backend.create_thread(
                project_id="", prompt="must not post", title="", idempotency_key=key
            )
        assert captured.value.transient is True
        assert captured.value.permanent is False
        assert client.create_calls == 0

    asyncio.run(scenario())


def test_backend_preserves_rate_limit_classification(tmp_path: Path) -> None:
    class Client:
        async def health(self):
            raise DirectBackendResponseError("rate limited", status_code=429)

    async def scenario() -> None:
        backend = DirectChatGPTBackend(
            client=Client(), journal_path=tmp_path / "journal.json"
        )
        with pytest.raises(RateLimitError):
            await backend.health()

    asyncio.run(scenario())


def test_continuation_distinguishes_pre_and_post_submission_failure(tmp_path: Path) -> None:
    class Client:
        failure: Exception

        async def continue_thread(self, *_args, **_kwargs):
            raise self.failure

    async def scenario() -> None:
        client = Client()
        backend = DirectChatGPTBackend(
            client=client, journal_path=tmp_path / "journal.json"
        )
        backend._parents["thread-1"] = "parent-1"
        client.failure = RuntimeUnavailableError("worker never started")
        with pytest.raises(InfrastructureError):
            await backend.continue_thread(
                conversation_id="thread-1", message="continue", idempotency_key="pre"
            )
        client.failure = DirectSubmissionUncertainError("POST began")
        result = await backend.continue_thread(
            conversation_id="thread-1", message="continue", idempotency_key="post"
        )
        assert result.accepted is True
        assert result.metadata == {"submission_uncertain": True}

    asyncio.run(scenario())
