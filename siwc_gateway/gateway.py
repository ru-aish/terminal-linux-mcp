"""Private T3 control-plane routes for authorized ChatGPT-plan Responses API use.

There are no private ChatGPT backend endpoints or browser impersonation here.
The T3 daemon keeps its existing public MCP endpoint unchanged.
"""
from __future__ import annotations

import asyncio
import json
import re
import secrets
import time
import uuid
from typing import Any, AsyncIterator

import httpx
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route

from .auth import AuthError, CredentialStore, RESOURCE


def _event(kind: str, value: dict[str, Any]) -> bytes:
    return ("event: " + kind + "\ndata: " + json.dumps(value, separators=(",", ":")) + "\n\n").encode()


def _validate_response_body(body: Any) -> dict[str, Any]:
    """Validate the direct SIWC subset; reject unsupported API capabilities early.

    Keeping this strict avoids sending parameters reserved for paid API keys or
    private ChatGPT endpoints. The caller owns the full stateless input history.
    """
    if not isinstance(body, dict) or not isinstance(body.get("model"), str) or not body["model"] or len(body["model"]) > 160:
        raise ValueError("model must be a nonempty model slug")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", body["model"]):
        raise ValueError("Invalid model slug")
    messages = body.get("input")
    if not isinstance(messages, list) or not messages or len(messages) > 200:
        raise ValueError("input must be a nonempty array with at most 200 messages")
    # The public Responses API uses typed tool-call/output input items, not
    # role=tool chat-completion messages. Reasoning items must be replayed on
    # follow-up turns when provided by the model.
    for message in messages:
        if not isinstance(message, dict):
            raise ValueError("input items must be objects")
        if "role" in message:
            if message.get("type", "message") != "message" or message["role"] not in ("user", "assistant", "developer"):
                raise ValueError("Unsupported input message role or type")
            content = message.get("content")
            if not isinstance(content, (str, list)) or (isinstance(content, list) and
                    (not content or len(content) > 32 or any(
                        not isinstance(part, dict) or part.get("type") not in
                        ("input_text", "output_text", "input_image", "input_file") for part in content))):
                raise ValueError("message content must be text or supported text/image/file items")
        elif message.get("type") in ("function_call", "function_call_output", "custom_tool_call", "custom_tool_call_output"):
            if not isinstance(message.get("call_id"), str) or not message["call_id"]:
                raise ValueError("tool call items require call_id")
            if message["type"].endswith("_output") and not isinstance(message.get("output"), (str, list)):
                raise ValueError("tool output must be text or supported content")
        elif message.get("type") == "reasoning":
            # Model-returned reasoning items carry replay state; never modify.
            if not isinstance(message.get("id"), str):
                raise ValueError("reasoning replay items require an id")
        else:
            raise ValueError("Unsupported Responses input item")
    allowed = {"model", "input", "instructions", "reasoning", "tools", "tool_choice"}
    if set(body) - allowed:
        raise ValueError("Unsupported parameter for ChatGPT-plan inference")
    if "instructions" in body and not isinstance(body["instructions"], str):
        raise ValueError("instructions must be text")
    if "reasoning" in body and not isinstance(body["reasoning"], dict):
        raise ValueError("reasoning must be an object")
    tools = body.get("tools", [])
    if not isinstance(tools, list) or len(tools) > 25:
        raise ValueError("tools must be an array of at most 25 supported tool groups")
    for tool in tools:
        if not isinstance(tool, dict):
            raise ValueError("tools must be objects")
        if tool.get("type") == "namespace":
            nested = tool.get("tools")
            if not isinstance(tool.get("name"), str) or not tool["name"] or not isinstance(nested, list) or not nested or len(nested) > 50:
                raise ValueError("tool namespace requires a name and nonempty tools")
            if any(not isinstance(t, dict) or t.get("type") not in ("function", "custom") or not isinstance(t.get("name"), str) for t in nested):
                raise ValueError("namespaces may contain only function or custom tools")
        elif tool.get("type") not in ("web_search", "web_search_preview"):
            raise ValueError("Unsupported hosted tool; use local function/custom tools")
    if "tool_choice" in body and not isinstance(body["tool_choice"], (dict, str)):
        raise ValueError("tool_choice must be a string or supported tool choice object")
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
            Route("/v1/siwc/status", self.status, methods=["GET"]),
            Route("/v1/siwc/capabilities", self.capabilities, methods=["GET"]),
            Route("/v1/siwc/models", self.models, methods=["GET"]),
            Route("/v1/siwc/responses", self.responses, methods=["POST"]),
            Route("/v1/siwc/cancel/{request_id}", self.cancel, methods=["POST"]),
        ]

    async def status(self, request):
        if not self._authorized(request):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        try:
            record = self.store.load()
        except AuthError:
            return JSONResponse({"authenticated": False, "plan_usage_granted": False})
        # Never include identity, access/refresh tokens, or the on-disk path.
        granted = "chatgpt.tokens.use.direct" in record.get("scopes", [])
        expiry = float(record.get("saved_at", 0)) + int(record.get("expires_in", 0))
        return JSONResponse({"authenticated": bool(record.get("client_id")),
                             "plan_usage_granted": granted,
                             "token_refresh_needed": time.time() >= expiry - 300})

    async def capabilities(self, request):
        if not self._authorized(request):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        # Authenticated caller can feature-detect supported behavior without
        # probing undocumented ChatGPT endpoints or receiving credentials.
        return JSONResponse({
            "upstream": "public_responses_api",
            "methods": ["models", "responses_stream", "cancel_request"],
            "inputs": ["text", "images", "files", "function_call_output", "reasoning_replay"],
            "tools": ["namespace_function", "namespace_custom", "web_search_if_available"],
            "conversation_state": "client_owned_full_history",
            "not_available": ["chatgpt_conversation_history", "hosted_connectors", "native_computer_use", "file_upload_api", "audio_video"],
        })

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
