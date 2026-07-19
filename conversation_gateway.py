"""The narrow boundary between orchestration policy and the internal chat runtime."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Any, AsyncContextManager, Callable, Protocol

from chat_internal_client import (
    RuntimeNotReadyError,
    RuntimeProtocolError,
    RuntimeUnavailableError,
    normalize_conversation_payload,
    sanitize_runtime_error,
)


_TRANSPORT_ERRORS = (
    RuntimeUnavailableError,
    RuntimeNotReadyError,
    RuntimeProtocolError,
    OSError,
    TimeoutError,
    asyncio.TimeoutError,
)


class DeliveryState(str, Enum):
    DELIVERED = "delivered"
    NOT_SENT = "not_sent"
    DEFERRED_RUNNING = "deferred_running"
    TARGET_CHANGED = "target_changed"
    SENT_UNCONFIRMED = "sent_unconfirmed"
    TEMPORARILY_UNREADABLE = "temporarily_unreadable"
    PERMANENT_FAILURE = "permanent_failure"


@dataclass(frozen=True)
class DeliveryResult:
    state: DeliveryState
    reason: str = ""
    request_id: str = ""
    user_message_id: str = ""
    parent_message_id: str = ""
    final_message_id: str = ""
    final_status: str = ""
    message: str = ""
    running: bool = False
    snapshot: dict[str, Any] | None = None

    @property
    def delivered(self) -> bool:
        return self.state is DeliveryState.DELIVERED


class ChatClient(Protocol):
    async def __aenter__(self) -> Any: ...
    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> Any: ...


ClientFactory = Callable[[], AsyncContextManager[ChatClient]]


def _message_ids(snapshot: dict[str, Any] | None) -> set[str]:
    if not isinstance(snapshot, dict):
        return set()
    ids = snapshot.get("all_message_ids") or snapshot.get("branch_message_ids") or ()
    return {str(value) for value in ids if str(value)}


class ConversationGateway:
    """Small, policy-free gateway built on :class:`InternalChatClient`.

    ``client_factory`` is intentionally injectable for tests and for the existing
    runtime adapter.  Runtime exceptions are returned as transport/read outcomes;
    callers decide whether a task itself failed.
    """

    def __init__(self, client_factory: ClientFactory):
        self.client_factory = client_factory

    @staticmethod
    def _runtime_error(conversation_id: str, exc: BaseException) -> dict[str, Any]:
        return {
            "found": False,
            "conversation_id": conversation_id,
            "state_verified": False,
            "transport_error": True,
            "reason": sanitize_runtime_error(f"{type(exc).__name__}: {exc}"),
            "turns": [],
        }

    async def read(self, conversation_id: str) -> dict[str, Any]:
        try:
            async with self.client_factory() as client:
                return await client.get_thread(conversation_id)
        except _TRANSPORT_ERRORS as exc:
            return self._runtime_error(conversation_id, exc)

    async def create(self, prompt: str, *, project_id: str | None = None, title: str | None = None) -> dict[str, Any]:
        try:
            async with self.client_factory() as client:
                return await client.create_thread(prompt, project_id=project_id, title=title)
        except _TRANSPORT_ERRORS as exc:
            return self._runtime_error("", exc)
        except (ValueError, PermissionError) as exc:
            return {"created": False, "state_verified": False, "permanent_failure": True,
                    "reason": sanitize_runtime_error(f"{type(exc).__name__}: {exc}")}

    async def send(
        self,
        conversation_id: str,
        message: str,
        *,
        expected_current_node: str,
        wait_for_completion: bool = False,
    ) -> DeliveryResult:
        try:
            async with self.client_factory() as client:
                try:
                    before = await client.get_thread(conversation_id)
                except _TRANSPORT_ERRORS as exc:
                    return DeliveryResult(
                        state=DeliveryState.TEMPORARILY_UNREADABLE,
                        reason=sanitize_runtime_error(f"{type(exc).__name__}: {exc}"),
                        message=message,
                    )
                if not before.get("state_verified"):
                    return DeliveryResult(
                        state=DeliveryState.TEMPORARILY_UNREADABLE,
                        reason=str(before.get("reason") or "conversation is unreadable"),
                        message=message,
                    )
                if str(before.get("current_node") or "") != expected_current_node:
                    return DeliveryResult(
                        state=DeliveryState.TARGET_CHANGED,
                        reason="canonical current_node changed before send",
                        message=message,
                    )
                if before.get("running") or before.get("active_stream"):
                    return DeliveryResult(
                        state=DeliveryState.DEFERRED_RUNNING,
                        reason="conversation is running",
                        message=message,
                        running=True,
                    )
                try:
                    result = await client.continue_thread(
                        conversation_id,
                        message,
                        expected_current_node=expected_current_node,
                        wait_for_completion=wait_for_completion,
                    )
                except (ValueError, PermissionError) as exc:
                    return self.classify_send_exception(exc, message=message)
                except _TRANSPORT_ERRORS as exc:
                    # Once continue_thread is invoked, a transport failure can
                    # occur after ChatGPT accepted the message. Fail closed and
                    # reconcile later rather than risk a duplicate send.
                    return self.classify_send_exception(
                        exc,
                        message=message,
                        submission_started=True,
                    )
                return self.classify_send_result(result, message=message)
        except (ValueError, PermissionError) as exc:
            return self.classify_send_exception(exc, message=message)
        except _TRANSPORT_ERRORS as exc:
            return DeliveryResult(
                state=DeliveryState.TEMPORARILY_UNREADABLE,
                reason=sanitize_runtime_error(f"{type(exc).__name__}: {exc}"),
                message=message,
            )

    @staticmethod
    def classify_send_exception(
        exc: BaseException,
        *,
        message: str = "",
        submission_started: bool = False,
    ) -> DeliveryResult:
        reason = sanitize_runtime_error(f"{type(exc).__name__}: {exc}")
        if isinstance(exc, (ValueError, PermissionError)):
            state = DeliveryState.PERMANENT_FAILURE
        elif submission_started:
            state = DeliveryState.SENT_UNCONFIRMED
        else:
            state = DeliveryState.TEMPORARILY_UNREADABLE
        return DeliveryResult(state=state, reason=reason, message=message)

    @staticmethod
    def classify_send_result(result: dict[str, Any], *, message: str = "") -> DeliveryResult:
        sent = bool(result.get("sent"))
        observed = bool(result.get("observed"))
        reason = str(result.get("reason") or "")
        if result.get("target_changed") or "current_node changed" in reason:
            state = DeliveryState.TARGET_CHANGED
        elif result.get("running") and not sent:
            state = DeliveryState.DEFERRED_RUNNING
        elif sent and observed:
            state = DeliveryState.DELIVERED
        elif sent:
            state = DeliveryState.SENT_UNCONFIRMED
        elif result.get("permanent_failure"):
            state = DeliveryState.PERMANENT_FAILURE
        else:
            state = DeliveryState.NOT_SENT
        return DeliveryResult(
            state=state,
            reason=reason,
            request_id=str(result.get("request_id") or ""),
            user_message_id=str(result.get("user_message_id") or ""),
            parent_message_id=str(result.get("parent_message_id") or ""),
            final_message_id=str(result.get("final_message_id") or ""),
            final_status=str(result.get("final_status") or ""),
            message=message,
            running=bool(result.get("running")),
            snapshot=result.get("snapshot") if isinstance(result.get("snapshot"), dict) else None,
        )

    @staticmethod
    def reconcile(result: DeliveryResult, snapshot: dict[str, Any]) -> DeliveryResult:
        if not (
            snapshot.get("found") is True
            and snapshot.get("canonical") is True
            and snapshot.get("state_verified") is True
        ):
            if result.state is DeliveryState.SENT_UNCONFIRMED:
                return DeliveryResult(
                    state=DeliveryState.SENT_UNCONFIRMED,
                    reason=(
                        result.reason
                        or "conversation state is not verified for delivery reconciliation"
                    ),
                    request_id=result.request_id,
                    user_message_id=result.user_message_id,
                    parent_message_id=result.parent_message_id,
                    final_message_id=result.final_message_id,
                    final_status=result.final_status,
                    message=result.message,
                    running=result.running,
                    snapshot=snapshot,
                )
            return result
        delivered = bool(
            result.user_message_id
            and result.user_message_id in _message_ids(snapshot)
        )
        request_ids = {
            str(value) for value in snapshot.get("all_request_ids", ()) if str(value)
        }
        delivered = delivered or bool(
            result.request_id and result.request_id in request_ids
        )
        if not delivered and result.parent_message_id:
            for turn in snapshot.get("turns", ()):
                if not isinstance(turn, dict) or turn.get("role") != "user":
                    continue
                turn_parent = str(
                    turn.get("parent_id") or turn.get("parent_message_id") or ""
                )
                turn_key = str(
                    turn.get("key") or turn.get("node_id") or turn.get("id") or ""
                )
                if (
                    turn_parent == result.parent_message_id
                    and (not result.user_message_id or turn_key == result.user_message_id)
                    and (not result.message or str(turn.get("text") or "") == result.message)
                ):
                    delivered = True
                    break
        if delivered:
            return DeliveryResult(
                state=DeliveryState.DELIVERED,
                reason="generated user message found in conversation state",
                request_id=result.request_id,
                user_message_id=result.user_message_id,
                parent_message_id=result.parent_message_id,
                final_message_id=result.final_message_id,
                final_status=result.final_status,
                message=result.message,
                running=result.running,
                snapshot=snapshot,
            )
        if result.state is DeliveryState.SENT_UNCONFIRMED:
            return DeliveryResult(
                state=DeliveryState.SENT_UNCONFIRMED,
                reason=result.reason,
                request_id=result.request_id,
                user_message_id=result.user_message_id,
                parent_message_id=result.parent_message_id,
                final_message_id=result.final_message_id,
                final_status=result.final_status,
                message=result.message,
                running=result.running,
                snapshot=snapshot,
            )
        return result

    async def cancel(self, conversation_id: str) -> dict[str, Any]:
        try:
            async with self.client_factory() as client:
                return await client.cancel(conversation_id)
        except _TRANSPORT_ERRORS as exc:
            return self._runtime_error(conversation_id, exc)
        except (ValueError, PermissionError) as exc:
            return {"cancelled": False, "permanent_failure": True,
                    "reason": sanitize_runtime_error(f"{type(exc).__name__}: {exc}")}


__all__ = ["ConversationGateway", "DeliveryResult", "DeliveryState", "normalize_conversation_payload"]
