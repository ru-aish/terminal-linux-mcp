from __future__ import annotations

import json
import os
import uuid
from dataclasses import dataclass
from threading import Lock
from typing import Any, Mapping, Sequence
from urllib.request import urlopen

from ..renderer_bridge import CHAT_RENDERER_BRIDGE_JS
from ..renderer_target import RendererTargetError, select_main_renderer_target
from ..errors import (
    BackendError,
    InfrastructureError,
    MalformedBackendResponse,
    RateLimitError,
)
from ..models import MutationResult, ThreadSnapshot, TurnSnapshot

DEFAULT_HIGH_REASONING_MODEL = "gpt-5-6-thinking"


_BRIDGE = (
    CHAT_RENDERER_BRIDGE_JS
    + r"""
const gatewayRuntimeKey = Symbol.for('chat-backend-gateway.renderer.runtime.v2');
window[gatewayRuntimeKey] ||= {handles:new Map(), nodes:new Map()};
const gatewayRuntime = window[gatewayRuntimeKey];
"""
)


@dataclass(frozen=True)
class _Target:
    websocket_url: str
    title: str
    url: str


class CodexRendererBackend:
    """Optional adapter for the locally running ChatGPT/Codex desktop renderer.

    The adapter is self-contained and does not import, mutate, or communicate
    with Terminal MCP. One method invocation performs one app-client backend
    operation. The model slug is configured rather than fetched, preventing a
    hidden model-catalogue request during creation.
    """

    def __init__(
        self,
        *,
        cdp_endpoint: str = "http://127.0.0.1:9222",
        model_slug: str = DEFAULT_HIGH_REASONING_MODEL,
        stream_start_timeout: float = 30.0,
        cdp_timeout: float = 45.0,
        webview_port: int = 5175,
        thinking_effort: str = "extended",
        require_high_reasoning: bool = True,
    ) -> None:
        self.cdp_endpoint = cdp_endpoint.rstrip("/")
        self.model_slug = model_slug.strip()
        self.stream_start_timeout = stream_start_timeout
        self.cdp_timeout = cdp_timeout
        self.webview_port = int(webview_port)
        self.thinking_effort = thinking_effort.strip()
        self.require_high_reasoning = bool(require_high_reasoning)
        self._lock = Lock()
        if not self.model_slug:
            raise ValueError("model_slug cannot be empty")
        if not 1 <= self.webview_port <= 65535:
            raise ValueError("webview_port must be between 1 and 65535")
        if self.require_high_reasoning:
            if not self.thinking_effort:
                raise ValueError(
                    "thinking_effort is required when high reasoning is mandatory"
                )
            if self.thinking_effort.casefold() not in {"high", "extended"}:
                raise ValueError(
                    "thinking_effort must be high or extended when high reasoning is mandatory"
                )

    @classmethod
    def from_environment(cls) -> "CodexRendererBackend":
        return cls(
            cdp_endpoint=os.environ.get(
                "CHAT_GATEWAY_CDP_ENDPOINT", "http://127.0.0.1:9222"
            ),
            model_slug=os.environ.get("CHAT_GATEWAY_MODEL_SLUG", DEFAULT_HIGH_REASONING_MODEL),
            stream_start_timeout=float(
                os.environ.get("CHAT_GATEWAY_STREAM_START_TIMEOUT", "30")
            ),
            cdp_timeout=float(os.environ.get("CHAT_GATEWAY_CDP_TIMEOUT", "45")),
            webview_port=int(os.environ.get("CHAT_GATEWAY_WEBVIEW_PORT", "5175")),
            thinking_effort=os.environ.get(
                "CHAT_GATEWAY_THINKING_EFFORT",
                os.environ.get("MCP_CHAT_WATCHDOG_THINKING_EFFORT", "extended"),
            ),
            require_high_reasoning=os.environ.get(
                "CHAT_GATEWAY_REQUIRE_HIGH_REASONING",
                os.environ.get("MCP_CHAT_WATCHDOG_REQUIRE_HIGH", "1"),
            )
            .strip()
            .lower()
            not in {"0", "false", "no", "off"},
        )

    async def health(self) -> Mapping[str, Any]:
        expression = f"""
        (async () => {{
          {_BRIDGE}
          const resolved = await resolveClient();
          return plain({{ready:true, diagnostics:resolved.diagnostics || {{}}}});
        }})()
        """
        payload = self._evaluate(expression)
        if not isinstance(payload, Mapping) or payload.get("ready") is not True:
            raise InfrastructureError(
                "ChatGPT renderer client readiness returned false"
            )
        return payload

    async def create_thread(
        self,
        *,
        project_id: str,
        prompt: str,
        title: str,
        idempotency_key: str,
    ) -> MutationResult:
        del (
            idempotency_key
        )  # Provider request has a stable generated message ID instead.
        user_message_id = str(uuid.uuid4())
        expression = f"""
        (async () => {{
          {_BRIDGE}
          const resolved = await resolveClient();
          const client = resolved.client;
          const text = {json.dumps(prompt)};
          const projectId = {json.dumps(project_id)};
          const userMessageId = {json.dumps(user_message_id)};
          const request = {{
            action:'next',
            model:{json.dumps(self.model_slug)},
            messages:[{{
              id:userMessageId,
              author:{{role:'user'}},
              content:{{content_type:'text',parts:[text]}},
              create_time:Date.now()/1000,
              end_turn:null,
              metadata:{{}},
              recipient:'all',
              status:'finished_successfully',
              weight:1
            }}],
            supported_encodings:['v1'],
            timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,
            timezone_offset_min:new Date().getTimezoneOffset()
          }};
          const thinkingEffort = {json.dumps(self.thinking_effort)};
          if (thinkingEffort) request.thinking_effort = thinkingEffort;
          if (projectId) {{
            request.gizmo_id = projectId;
            request.conversation_mode = {{kind:'gizmo_interaction',gizmo_id:projectId}};
          }}
          let conversationId = '';
          let handle = null;
          let settled = false;
          const started = await new Promise((resolve, reject) => {{
            const timer = setTimeout(() => {{
              if (settled) return;
              settled = true;
              reject(new Error('conversation id was not observed before timeout'));
            }}, {int(self.stream_start_timeout * 1000)});
            const observe = (value) => {{
              const candidate = value && (value.message || value.data || value);
              const possibleId = value && (value.conversation_id || value.conversationId)
                || candidate && (candidate.conversation_id || candidate.conversationId);
              if (possibleId) conversationId = String(possibleId);
              if (conversationId) {{
                if (candidate && candidate.id) gatewayRuntime.nodes.set(conversationId, String(candidate.id));
                if (handle) gatewayRuntime.handles.set(conversationId, handle);
                if (!settled) {{ settled = true; clearTimeout(timer); resolve(true); }}
              }}
            }};
            const fail = (error) => {{
              if (!settled) {{ settled = true; clearTimeout(timer); reject(error); }}
            }};
            const complete = (value) => {{ observe(value); if (conversationId) gatewayRuntime.handles.delete(conversationId); }};
            try {{
              const pending = client.startCompletionStream({{
                request,
                onEvent:observe,
                onUpdate:observe,
                onComplete:complete,
                onError:fail,
                onRecoverableError:()=>{{}}
              }});
              Promise.resolve(pending).then((value) => {{
                handle = value;
                observe(value);
                if (conversationId) gatewayRuntime.handles.set(conversationId, value);
              }}, fail);
            }} catch (error) {{ fail(error); }}
          }});
          return plain({{
            accepted:!!started,
            conversation_id:conversationId,
            message_id:userMessageId,
            running:true,
            title_requested:{json.dumps(title)}
          }});
        }})()
        """
        payload = self._evaluate(expression, timeout=max(self.cdp_timeout, 35.0))
        if not isinstance(payload, Mapping):
            raise MalformedBackendResponse("renderer creation returned a non-object")
        return MutationResult(
            accepted=bool(payload.get("accepted")),
            conversation_id=_optional_string(payload.get("conversation_id")),
            message_id=_optional_string(payload.get("message_id")),
            running=bool(payload.get("running", True)),
        )

    async def get_thread(self, *, conversation_id: str) -> ThreadSnapshot:
        expression = f"""
        (async () => {{
          {_BRIDGE}
          const resolved = await resolveClient();
          const client = resolved.client;
          const id = {json.dumps(conversation_id)};
          const raw = await client.get(id);
          const currentNode = raw && (raw.current_node || raw.currentNode);
          if (currentNode) gatewayRuntime.nodes.set(id, String(currentNode));
          return plain(raw);
        }})()
        """
        raw = self._evaluate(expression)
        return _normalize_conversation(raw, conversation_id)

    async def continue_thread(
        self,
        *,
        conversation_id: str,
        message: str,
        idempotency_key: str,
    ) -> MutationResult:
        del idempotency_key
        user_message_id = str(uuid.uuid4())
        expression = f"""
        (async () => {{
          {_BRIDGE}
          const resolved = await resolveClient();
          const client = resolved.client;
          const id = {json.dumps(conversation_id)};
          const parent = gatewayRuntime.nodes.get(id);
          if (!parent) throw new Error('no cached canonical parent node; call get_thread before continue_thread');
          const userMessageId = {json.dumps(user_message_id)};
          const request = {{
            action:'next',
            conversation_id:id,
            parent_message_id:parent,
            model:{json.dumps(self.model_slug)},
            messages:[{{
              id:userMessageId,
              author:{{role:'user'}},
              content:{{content_type:'text',parts:[{json.dumps(message)}]}},
              create_time:Date.now()/1000,
              end_turn:null,
              metadata:{{}},recipient:'all',status:'finished_successfully',weight:1
            }}],
            supported_encodings:['v1'],
            timezone:Intl.DateTimeFormat().resolvedOptions().timeZone,
            timezone_offset_min:new Date().getTimezoneOffset()
          }};
          const thinkingEffort = {json.dumps(self.thinking_effort)};
          if (thinkingEffort) request.thinking_effort = thinkingEffort;
          let handle = null;
          const started = await new Promise((resolve,reject) => {{
            let settled=false;
            const timer=setTimeout(()=>{{if(!settled){{settled=true;reject(new Error('continuation did not start before timeout'));}}}}, {int(self.stream_start_timeout * 1000)});
            const observe=(value)=>{{
              const candidate=value&&(value.message||value.data||value);
              if(candidate&&candidate.id) gatewayRuntime.nodes.set(id,String(candidate.id));
              if(!settled){{settled=true;clearTimeout(timer);resolve(true);}}
            }};
            const fail=(error)=>{{if(!settled){{settled=true;clearTimeout(timer);reject(error);}}}};
            const complete=(value)=>{{observe(value);gatewayRuntime.handles.delete(id);}};
            try{{
              const pending=client.startCompletionStream({{request,onEvent:observe,onUpdate:observe,onComplete:complete,onError:fail,onRecoverableError:()=>{{}}}});
              Promise.resolve(pending).then((value)=>{{handle=value;gatewayRuntime.handles.set(id,value);observe(value);}},fail);
            }}catch(error){{fail(error);}}
          }});
          return plain({{accepted:!!started,conversation_id:id,message_id:userMessageId,running:true}});
        }})()
        """
        payload = self._evaluate(expression, timeout=max(self.cdp_timeout, 35.0))
        if not isinstance(payload, Mapping):
            raise MalformedBackendResponse(
                "renderer continuation returned a non-object"
            )
        return MutationResult(
            accepted=bool(payload.get("accepted")),
            conversation_id=conversation_id,
            message_id=_optional_string(payload.get("message_id")),
            running=bool(payload.get("running", True)),
        )

    async def cancel_thread(self, *, conversation_id: str) -> MutationResult:
        expression = f"""
        (async () => {{
          {_BRIDGE}
          const resolved = await resolveClient();
          const client = resolved.client;
          const id = {json.dumps(conversation_id)};
          const handle = gatewayRuntime.handles.get(id);
          if (handle && typeof handle.cancel === 'function') await handle.cancel();
          else if (typeof client.cancelStream === 'function') await client.cancelStream(id);
          else throw new Error('renderer cancellation is unavailable');
          gatewayRuntime.handles.delete(id);
          return {{accepted:true,conversation_id:id,running:false}};
        }})()
        """
        payload = self._evaluate(expression)
        return _mutation_from_payload(payload, conversation_id, "cancel")

    async def delete_thread(self, *, conversation_id: str) -> MutationResult:
        expression = f"""
        (async () => {{
          {_BRIDGE}
          const resolved = await resolveClient();
          const client = resolved.client;
          const id = {json.dumps(conversation_id)};
          const result = await client.delete(id);
          gatewayRuntime.handles.delete(id);
          gatewayRuntime.nodes.delete(id);
          return plain({{accepted:result?.success !== false,conversation_id:id,running:false,result}});
        }})()
        """
        payload = self._evaluate(expression)
        return _mutation_from_payload(payload, conversation_id, "delete")

    async def list_project_threads(
        self, *, project_id: str
    ) -> Sequence[ThreadSnapshot]:
        expression = f"""
        (async () => {{
          {_BRIDGE}
          const resolved = await resolveClient();
          const client = resolved.client;
          const result = await client.listProjectConversations({{
            projectId:{json.dumps(project_id)},limit:50,cursor:null,ownedOnly:true
          }});
          return plain(result);
        }})()
        """
        payload = self._evaluate(expression)
        if not isinstance(payload, Mapping):
            raise MalformedBackendResponse("project listing returned a non-object")
        items = payload.get("items")
        if not isinstance(items, list):
            raise MalformedBackendResponse("project listing items are missing")
        snapshots: list[ThreadSnapshot] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            conversation_id = _optional_string(
                item.get("id") or item.get("conversation_id")
            )
            if not conversation_id:
                continue
            snapshots.append(
                ThreadSnapshot(
                    conversation_id=conversation_id,
                    found=True,
                    running=False,
                    turns=(),
                    title=str(item.get("title") or ""),
                    current_node=str(item.get("current_node") or ""),
                )
            )
        return tuple(snapshots)

    def _target(self) -> _Target:
        try:
            with urlopen(f"{self.cdp_endpoint}/json/list", timeout=5.0) as response:
                targets = json.load(response)
        except Exception as error:
            raise InfrastructureError(
                f"ChatGPT CDP endpoint is unavailable: {type(error).__name__}: {error}"
            ) from error
        if not isinstance(targets, list):
            raise InfrastructureError("ChatGPT CDP target list was not a list")
        try:
            target = select_main_renderer_target(
                [dict(item) for item in targets if isinstance(item, Mapping)],
                expected_webview_port=self.webview_port,
            )
        except RendererTargetError as error:
            raise InfrastructureError(str(error)) from error
        return _Target(
            websocket_url=target.websocket_url,
            title=target.title,
            url=target.url,
        )

    def _evaluate(self, expression: str, *, timeout: float | None = None) -> Any:
        try:
            import websocket
        except ImportError as error:  # pragma: no cover - optional dependency
            raise RuntimeError(
                "CodexRendererBackend requires the 'live' extra: pip install .[live]"
            ) from error

        with self._lock:
            target = self._target()
            connection = websocket.create_connection(
                target.websocket_url,
                timeout=timeout or self.cdp_timeout,
                suppress_origin=True,
            )
            try:
                identifier = 1
                connection.send(
                    json.dumps(
                        {
                            "id": identifier,
                            "method": "Runtime.evaluate",
                            "params": {
                                "expression": expression,
                                "returnByValue": True,
                                "awaitPromise": True,
                                "userGesture": False,
                            },
                        }
                    )
                )
                while True:
                    message = json.loads(connection.recv())
                    if message.get("id") != identifier:
                        continue
                    if "error" in message:
                        raise InfrastructureError("CDP Runtime.evaluate failed")
                    result = message.get("result")
                    if not isinstance(result, Mapping):
                        raise MalformedBackendResponse("CDP result was not an object")
                    remote = result.get("result")
                    if not isinstance(remote, Mapping):
                        raise MalformedBackendResponse(
                            "CDP remote result was not an object"
                        )
                    exception = result.get("exceptionDetails")
                    if exception or remote.get("subtype") == "error":
                        description = str(
                            remote.get("description")
                            or _exception_text(exception)
                            or "renderer JavaScript failed"
                        )
                        if "Too many requests" in description:
                            raise RateLimitError(description)
                        if _is_infrastructure_message(description):
                            raise InfrastructureError(description)
                        raise BackendError(description, transient=True)
                    return remote.get("value")
            except RateLimitError:
                raise
            except InfrastructureError:
                raise
            except BackendError:
                raise
            except Exception as error:
                message = f"{type(error).__name__}: {error}"
                if "Too many requests" in message:
                    raise RateLimitError(message) from error
                raise InfrastructureError(message) from error
            finally:
                connection.close()


def _is_infrastructure_message(message: str) -> bool:
    normalized = message.lower()
    return any(
        marker in normalized
        for marker in (
            "normal-chat client unavailable",
            "renderer client was not discoverable",
            "renderer client readiness",
            "render frame was disposed",
            "websocket",
            "connection timed out",
            "target closed",
            "session closed",
        )
    )


def make_backend() -> CodexRendererBackend:
    """Factory used by ``--adapter chat_gateway.adapters.codex_renderer:make_backend``."""

    return CodexRendererBackend.from_environment()


def _mutation_from_payload(
    payload: Any, conversation_id: str, operation: str
) -> MutationResult:
    if not isinstance(payload, Mapping):
        raise MalformedBackendResponse(f"renderer {operation} returned a non-object")
    return MutationResult(
        accepted=bool(payload.get("accepted")),
        conversation_id=_optional_string(payload.get("conversation_id"))
        or conversation_id,
        message_id=_optional_string(payload.get("message_id")),
        running=bool(payload.get("running", False)),
    )


def _normalize_conversation(raw: Any, requested_id: str) -> ThreadSnapshot:
    if not isinstance(raw, Mapping):
        return ThreadSnapshot(requested_id, False, False, ())
    mapping = raw.get("mapping")
    if not isinstance(mapping, Mapping):
        return ThreadSnapshot(requested_id, False, False, ())
    current_node = str(raw.get("current_node") or raw.get("currentNode") or "")
    if not current_node or current_node not in mapping:
        return ThreadSnapshot(
            conversation_id=str(raw.get("id") or requested_id),
            found=True,
            running=False,
            turns=(),
            title=str(raw.get("title") or ""),
            current_node=current_node,
        )

    reverse: list[TurnSnapshot] = []
    cursor = current_node
    seen: set[str] = set()
    while cursor and cursor not in seen and len(reverse) < 10_000:
        seen.add(cursor)
        node = mapping.get(cursor)
        if not isinstance(node, Mapping):
            break
        message = node.get("message")
        if isinstance(message, Mapping):
            metadata = message.get("metadata")
            hidden = bool(
                isinstance(metadata, Mapping)
                and metadata.get("is_visually_hidden_from_conversation")
            )
            if not hidden:
                author = message.get("author")
                reverse.append(
                    TurnSnapshot(
                        message_id=str(message.get("id") or cursor),
                        role=str(
                            author.get("role") if isinstance(author, Mapping) else ""
                        ),
                        status=str(message.get("status") or ""),
                        text=_message_text(message),
                        end_turn=(
                            message.get("end_turn")
                            if isinstance(message.get("end_turn"), bool)
                            else None
                        ),
                        created_at=(
                            float(message["create_time"])
                            if isinstance(message.get("create_time"), (int, float))
                            else None
                        ),
                    )
                )
        cursor = str(node.get("parent") or "")
    turns = tuple(reversed(reverse))
    latest = turns[-1] if turns else None
    running = bool(
        latest
        and latest.role == "assistant"
        and (
            latest.status.casefold()
            in {"in_progress", "running", "streaming", "pending"}
            or latest.end_turn is False
        )
    )
    return ThreadSnapshot(
        conversation_id=str(raw.get("id") or requested_id),
        found=True,
        running=running,
        turns=turns,
        title=str(raw.get("title") or ""),
        current_node=current_node,
    )


def _message_text(message: Mapping[str, Any]) -> str:
    content = message.get("content")
    if not isinstance(content, Mapping):
        return ""
    parts = content.get("parts")
    if isinstance(parts, list):
        return "\n".join(str(part) for part in parts if isinstance(part, str))
    text = content.get("text")
    return str(text) if text is not None else ""


def _exception_text(value: Any) -> str:
    if not isinstance(value, Mapping):
        return ""
    exception = value.get("exception")
    if isinstance(exception, Mapping):
        return str(exception.get("description") or exception.get("value") or "")
    return str(value.get("text") or "")


def _optional_string(value: Any) -> str | None:
    return str(value) if value not in (None, "") else None
