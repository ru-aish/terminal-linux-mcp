"""Private T3 control-plane routes for authorized ChatGPT-plan Responses API use.

There are no private ChatGPT backend endpoints or browser impersonation here.
The T3 daemon keeps its existing public MCP endpoint unchanged.
"""
from __future__ import annotations

import asyncio
import json
import re
import secrets
import uuid
from typing import Any, AsyncIterator

import httpx
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from .auth import AuthError, CredentialStore, RESOURCE


def _event(kind: str, value: dict[str, Any]) -> bytes:
    return ("event: " + kind + "\ndata: " + json.dumps(value, separators=(",", ":")) + "\n\n").encode()


def _validate_response_body(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict) or not isinstance(body.get("model"), str) or not body["model"] or len(body["model"]) > 160:
        raise ValueError("model must be a nonempty model slug")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", body["model"]):
        raise ValueError("Invalid model slug")
    messages = body.get("input")
    if not isinstance(messages, list) or not messages or len(messages) > 200:
        raise ValueError("input must be a nonempty array with at most 200 messages")
    # Direct SIWC usage does not accept system-role messages. Keep full history
    # client-owned; no previous_response_id or server-side ChatGPT chat access.
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in ("user", "assistant", "developer", "tool"):
            raise ValueError("input must contain supported messages, not system messages")
        if not isinstance(message.get("content"), (str, list)):
            raise ValueError("input message content must be text or a typed content array")
    allowed = {"model", "input", "instructions", "reasoning"}
    if set(body) - allowed:
        raise ValueError("Unsupported parameter for ChatGPT-plan inference")
    if "instructions" in body and not isinstance(body["instructions"], str):
        raise ValueError("instructions must be text")
    if "reasoning" in body and not isinstance(body["reasoning"], dict):
        raise ValueError("reasoning must be an object")
    return {**body, "store": False, "stream": True}


class SIWCGateway:
    def __init__(self, control_token: str, store: CredentialStore | None = None,
                 transport: httpx.AsyncBaseTransport | None = None):
        if not control_token:
            raise ValueError("The private T3 control token is required")
        self.control_token = control_token
        self.store = store or CredentialStore()
        self.transport = transport
        self.active: dict[str, tuple[asyncio.Event, asyncio.Task | None]] = {}

    def _authorized(self, request) -> bool:
        return secrets.compare_digest(request.headers.get("authorization", ""), "Bearer " + self.control_token)

    def routes(self) -> list[Route]:
        return [
            Route("/v1/siwc/models", self.models, methods=["GET"]),
            Route("/v1/siwc/responses", self.responses, methods=["POST"]),
            Route("/v1/siwc/cancel/{request_id}", self.cancel, methods=["POST"]),
        ]

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self.transport, timeout=httpx.Timeout(45, read=None))

    async def models(self, request):
        if not self._authorized(request):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        try:
            async with self.client() as client:
                token = await self.store.access_token(client)
                response = await client.get(RESOURCE + "/models", headers={"Authorization": "Bearer " + token})
                response.raise_for_status()
                payload = response.json()
            raw = payload.get("models", [])
            if not isinstance(raw, list):
                raise ValueError("Unexpected model catalog")
            return JSONResponse({"models": [
                {"slug": item["slug"], "display_name": item.get("display_name", item["slug"])}
                for item in raw if isinstance(item, dict) and item.get("visibility") == "list" and isinstance(item.get("slug"), str)
            ]})
        except AuthError as exc:
            return JSONResponse({"error": str(exc)}, status_code=409)
        except (httpx.HTTPError, ValueError, KeyError):
            return JSONResponse({"error": "ChatGPT model catalog unavailable"}, status_code=502)

    async def responses(self, request):
        if not self._authorized(request):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        try:
            if int(request.headers.get("content-length", "0")) > 2_000_000:
                raise ValueError("Request exceeds 2 MB")
            raw = await request.body()
            if len(raw) > 2_000_000:
                raise ValueError("Request exceeds 2 MB")
            body = _validate_response_body(json.loads(raw))
            async with self.client() as client:
                await self.store.access_token(client)  # Fail before opening a stream.
        except (ValueError, json.JSONDecodeError) as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        except AuthError as exc:
            return JSONResponse({"error": str(exc)}, status_code=409)
        except httpx.HTTPError:
            return JSONResponse({"error": "ChatGPT authentication unavailable"}, status_code=502)

        request_id = str(uuid.uuid4())
        cancelled = asyncio.Event()
        self.active[request_id] = (cancelled, None)

        async def output() -> AsyncIterator[bytes]:
            terminal = False
            self.active[request_id] = (cancelled, asyncio.current_task())
            try:
                if cancelled.is_set():
                    yield _event("gateway.interrupted", {"request_id": request_id})
                    return
                yield _event("gateway.started", {"request_id": request_id})
                async with self.client() as client:
                    token = await self.store.access_token(client)
                    async with client.stream("POST", RESOURCE + "/responses",
                                             headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
                                             json=body) as response:
                        if response.status_code != 200:
                            yield _event("gateway.failed", {"request_id": request_id, "upstream_status": response.status_code})
                            return
                        buffer: list[str] = []
                        async for line in response.aiter_lines():
                            if cancelled.is_set():
                                yield _event("gateway.interrupted", {"request_id": request_id})
                                return
                            if line:
                                buffer.append(line)
                                continue
                            if not buffer:
                                continue
                            frame = "\n".join(buffer)
                            buffer.clear()
                            data = next((part[5:].strip() for part in frame.splitlines() if part.startswith("data:")), None)
                            if not data or data == "[DONE]":
                                continue
                            try:
                                packet = json.loads(data)
                            except ValueError:
                                yield _event("gateway.failed", {"request_id": request_id, "reason": "malformed_upstream_event"})
                                return
                            kind = packet.get("type")
                            if not isinstance(kind, str):
                                yield _event("gateway.failed", {"request_id": request_id, "reason": "missing_event_type"})
                                return
                            yield _event(kind, packet)
                            if kind in ("response.completed", "response.failed", "response.incomplete", "error"):
                                terminal = True
                                return
                        if not terminal:
                            yield _event("gateway.failed", {"request_id": request_id, "reason": "stream_ended_without_completion"})
            except asyncio.CancelledError:
                # Cancellation always closes the upstream HTTP stream.
                if cancelled.is_set():
                    yield _event("gateway.interrupted", {"request_id": request_id})
                else:
                    raise
            except AuthError:
                yield _event("gateway.failed", {"request_id": request_id, "reason": "authentication_unavailable"})
            except (httpx.HTTPError, ValueError):
                yield _event("gateway.failed", {"request_id": request_id, "reason": "upstream_transport_error"})
            finally:
                self.active.pop(request_id, None)

        return StreamingResponse(output(), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-store", "X-Inference-Request-Id": request_id, "X-Accel-Buffering": "no"})

    async def cancel(self, request):
        if not self._authorized(request):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        request_id = request.path_params["request_id"]
        entry = self.active.get(request_id)
        if entry is None:
            return JSONResponse({"error": "Unknown or completed request"}, status_code=404)
        flag, task = entry
        flag.set()
        if task is not None:
            task.cancel()
        return JSONResponse({"cancellationRequested": True, "request_id": request_id})
