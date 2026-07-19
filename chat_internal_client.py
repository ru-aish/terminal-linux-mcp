from __future__ import annotations

import asyncio
import contextlib
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import websockets


DEFAULT_CODEX_CDP_ENDPOINT = "http://127.0.0.1:9222"
_RUNNING_STATUSES = frozenset({"in_progress", "streaming", "queued", "pending"})


def sanitize_runtime_error(value: Any) -> str:
    text = str(value or "").replace("\n", " ")
    text = re.sub(r"Bearer\s+\S+", "Bearer [redacted]", text, flags=re.IGNORECASE)
    text = re.sub(
        r"(authorization|cookie|token)\s*[:=]\s*[^\s,;]+",
        r"\1=[redacted]",
        text,
        flags=re.IGNORECASE,
    )
    return re.sub(r"\s+", " ", text).strip()[:500]


class InternalRuntimeError(RuntimeError):
    pass


class RuntimeUnavailableError(InternalRuntimeError):
    pass


class RuntimeNotReadyError(InternalRuntimeError):
    pass


class RuntimeProtocolError(InternalRuntimeError):
    pass


@dataclass(frozen=True)
class RuntimeTarget:
    target_id: str
    url: str
    title: str
    websocket_url: str


@dataclass(frozen=True)
class RuntimeProbe:
    available: bool
    ready: bool
    reason: str = ""
    target: RuntimeTarget | None = None


def _is_auxiliary_target(url: str) -> bool:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    if "/avatar-overlay" in query.get("initialRoute", []):
        return True
    return "avatar-overlay-composition-surface" in parsed.path


def select_main_renderer_target(targets: list[dict[str, Any]]) -> RuntimeTarget:
    pages = [item for item in targets if item.get("type") == "page"]
    candidates: list[dict[str, Any]] = []
    for item in pages:
        url = str(item.get("url", ""))
        parsed = urlsplit(url)
        is_renderer = (
            parsed.scheme in {"http", "https"}
            and parsed.hostname in {"127.0.0.1", "localhost"}
            and parsed.port == 5175
        )
        if is_renderer and not _is_auxiliary_target(url):
            candidates.append(item)
    if not candidates:
        if pages:
            raise RuntimeNotReadyError(
                "Codex is running, but only auxiliary renderers are available; open the main app window"
            )
        raise RuntimeNotReadyError("Codex is running, but no renderer page is available")

    def rank(item: dict[str, Any]) -> tuple[int, int, str]:
        url = str(item.get("url", ""))
        query = parse_qs(urlsplit(url).query)
        return (
            1 if "mcpAppSandboxDevtools" in query else 0,
            1 if query.get("initialRoute") else 0,
            url,
        )

    selected = min(candidates, key=rank)
    websocket_url = str(selected.get("webSocketDebuggerUrl", ""))
    if not websocket_url:
        raise RuntimeNotReadyError("the main Codex renderer has no debugging websocket")
    return RuntimeTarget(
        target_id=str(selected.get("id", "")),
        url=str(selected.get("url", "")),
        title=str(selected.get("title", "")),
        websocket_url=websocket_url,
    )


async def probe_runtime(endpoint: str, *, timeout: float = 1.5) -> RuntimeProbe:
    try:
        async with httpx.AsyncClient(timeout=timeout, trust_env=False) as client:
            response = await client.get(f"{endpoint.rstrip('/')}/json/list")
            response.raise_for_status()
            payload = response.json()
    except (httpx.HTTPError, OSError, ValueError) as exc:
        return RuntimeProbe(False, False, f"{type(exc).__name__}: {exc}")
    if not isinstance(payload, list):
        return RuntimeProbe(True, False, "CDP target list is not an array")
    try:
        target = select_main_renderer_target(payload)
    except RuntimeNotReadyError as exc:
        return RuntimeProbe(True, False, str(exc))
    return RuntimeProbe(True, True, target=target)


def _plain_conversation(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    for key in ("conversation", "data"):
        nested = raw.get(key)
        if isinstance(nested, dict) and isinstance(nested.get("mapping"), dict):
            return nested
    return raw


def _part_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("text", "content", "value"):
            if isinstance(value.get(key), str):
                return value[key]
    return ""


def _message_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if not isinstance(content, dict):
        return ""
    parts = content.get("parts")
    if isinstance(parts, list):
        return "\n".join(text for text in (_part_text(part) for part in parts) if text)
    return content.get("text") if isinstance(content.get("text"), str) else ""


def normalize_conversation_payload(
    raw: Any,
    conversation_id: str,
    *,
    owned_stream: bool = False,
) -> dict[str, Any]:
    conversation = _plain_conversation(raw)
    mapping = conversation.get("mapping")
    if not isinstance(mapping, dict):
        return {
            "found": False,
            "conversation_id": conversation_id,
            "canonical": True,
            "state_verified": False,
            "reason": "canonical conversation mapping is missing",
            "turns": [],
        }

    current_node = str(conversation.get("current_node") or conversation.get("currentNode") or "")
    if not current_node or current_node not in mapping:
        return {
            "found": True,
            "conversation_id": str(conversation.get("id") or conversation_id),
            "title": str(conversation.get("title") or ""),
            "current_node": current_node,
            "canonical": True,
            "state_verified": False,
            "reason": "canonical current_node is missing from the mapping",
            "turns": [],
            "active_stream": owned_stream,
            "running": owned_stream,
        }

    reverse: list[dict[str, Any]] = []
    seen: set[str] = set()
    cursor = current_node
    valid = True
    while cursor:
        if cursor in seen or len(reverse) >= 10_000:
            valid = False
            break
        seen.add(cursor)
        node = mapping.get(cursor)
        if not isinstance(node, dict):
            valid = False
            break
        message = node.get("message")
        if isinstance(message, dict):
            author = message.get("author")
            metadata = message.get("metadata")
            hidden = bool(
                isinstance(metadata, dict)
                and metadata.get("is_visually_hidden_from_conversation")
            )
            finish_details = metadata.get("finish_details") if isinstance(metadata, dict) else None
            if not hidden:
                reverse.append(
                    {
                    "node_id": cursor,
                    "key": str(message.get("id") or cursor),
                    "role": str(author.get("role") if isinstance(author, dict) else ""),
                    "text": _message_text(message),
                    "parent_id": str(node.get("parent") or ""),
                    "status": str(message.get("status") or ""),
                    "end_turn": message.get("end_turn")
                    if isinstance(message.get("end_turn"), bool)
                    else None,
                    "create_time": float(message["create_time"])
                    if isinstance(message.get("create_time"), (int, float))
                    else None,
                    "model_slug": str(
                        metadata.get("model_slug")
                        or metadata.get("default_model_slug")
                        or ""
                    )
                    if isinstance(metadata, dict)
                    else "",
                    "finish_details": finish_details if isinstance(finish_details, dict) else None,
                }
            )
        cursor = str(node.get("parent") or "")

    active_branch = list(reversed(reverse))
    visible_turns = [turn for turn in active_branch if turn["role"] in {"user", "assistant"}]
    latest = active_branch[-1] if active_branch else None
    status = str(latest.get("status") or "").casefold() if latest else ""
    canonical_running = bool(
        latest
        and latest.get("role") == "assistant"
        and (status in _RUNNING_STATUSES or latest.get("end_turn") is False)
    )
    update_time = conversation.get("update_time")
    return {
        "found": True,
        "conversation_id": str(
            conversation.get("id") or conversation.get("conversation_id") or conversation_id
        ),
        "title": str(conversation.get("title") or ""),
        "current_node": current_node,
        "canonical": True,
        "state_verified": valid and bool(active_branch),
        "reason": "" if valid else "active branch is cyclic or incomplete",
        "update_time": float(update_time) if isinstance(update_time, (int, float)) else None,
        "active_stream": owned_stream,
        "running": owned_stream or canonical_running,
        "turns": visible_turns,
    }


_BRIDGE_JS = r"""
const clientKey = Symbol.for('terminal-mcp.chat-runtime.client.v4');
const streamsKey = Symbol.for('terminal-mcp.chat-runtime.streams.v4');
const handlesKey = Symbol.for('terminal-mcp.chat-runtime.handles.v4');
window[streamsKey] ||= new Map();
window[handlesKey] ||= new Map();
const streams = window[streamsKey];
const handles = window[handlesKey];
const objectLike = (value) => !!value && (typeof value === 'object' || typeof value === 'function');
const methodNames = (value) => {
  if (!objectLike(value)) return new Set();
  const names = new Set();
  try { for (const name of Object.getOwnPropertyNames(value)) names.add(name); } catch {}
  try {
    const prototype = Object.getPrototypeOf(value);
    if (prototype) for (const name of Object.getOwnPropertyNames(prototype)) names.add(name);
  } catch {}
  return names;
};
const clientShape = (value) => {
  if (!objectLike(value)) return false;
  const names = methodNames(value);
  return ['get', 'list', 'models', 'startCompletionStream'].every((name) => names.has(name))
    && typeof value.get === 'function'
    && typeof value.list === 'function'
    && typeof value.models === 'function'
    && typeof value.startCompletionStream === 'function';
};
const containerShape = (value) => objectLike(value)
  && typeof value.get === 'function'
  && typeof value.set === 'function'
  && !!value.queryClient
  && !!value.scope;
const roots = () => {
  const result = [];
  const hook = window.__REACT_DEVTOOLS_GLOBAL_HOOK__;
  if (hook && typeof hook.getFiberRoots === 'function') {
    const ids = hook.renderers && typeof hook.renderers.keys === 'function'
      ? [...hook.renderers.keys()] : Array.from({length:16}, (_, index) => index + 1);
    for (const id of ids) {
      try { for (const root of hook.getFiberRoots(id) || []) result.push(root.current || root); } catch {}
    }
  }
  const elements = document.querySelectorAll('*');
  for (let index = 0; index < elements.length && index < 1800; index += 1) {
    const element = elements[index];
    for (const key of Object.keys(element)) {
      if (key.startsWith('__reactFiber$') || key.startsWith('__reactContainer$')) result.push(element[key]);
    }
  }
  return result;
};
const discoverGraph = () => {
  const queue = roots();
  const seen = new WeakSet();
  const containers = [];
  let directClient = null;
  let visited = 0;
  while (queue.length && visited < 180000 && !directClient) {
    const value = queue.shift();
    if (!objectLike(value) || seen.has(value)) continue;
    seen.add(value);
    visited += 1;
    if (clientShape(value)) { directClient = value; break; }
    if (containerShape(value)) containers.push(value);
    let descriptors;
    try { descriptors = Object.getOwnPropertyDescriptors(value); } catch { continue; }
    for (const descriptor of Object.values(descriptors)) {
      const next = descriptor && descriptor.value;
      if (!objectLike(next) || next === window || next === document) continue;
      if (typeof Node !== 'undefined' && next instanceof Node) continue;
      queue.push(next);
    }
  }
  return {directClient, containers, visited};
};
const priority = (url) => {
  const value = String(url).toLowerCase();
  if (value.includes('app-initial')) return 0;
  if (value.includes('new-thread') || value.includes('conversation')) return 1;
  if (value.includes('app-main') || value.includes('artifact-tab')) return 2;
  return 3;
};
const resolveClient = async () => {
  if (clientShape(window[clientKey])) return {client:window[clientKey], diagnostics:{cached:true}};
  const graph = discoverGraph();
  if (graph.directClient) {
    Object.defineProperty(window, clientKey, {value:graph.directClient, configurable:true});
    return {client:graph.directClient, diagnostics:{direct:true, visited:graph.visited}};
  }
  const resources = [...new Set(performance.getEntriesByType('resource').map((entry) => entry.name)
    .filter((url) => {
      try { const parsed = new URL(url, location.href); return parsed.origin === location.origin && /\.js(?:\?|$)/.test(parsed.href); }
      catch { return false; }
    }))].sort((a, b) => priority(a) - priority(b)).slice(0, 320);
  let imported = 0;
  let candidates = 0;
  for (const url of resources) {
    let module;
    try { module = await import(url); imported += 1; } catch { continue; }
    for (const exported of Object.values(module)) {
      if (clientShape(exported)) {
        Object.defineProperty(window, clientKey, {value:exported, configurable:true});
        return {client:exported, diagnostics:{directExport:true, imported, visited:graph.visited}};
      }
      if (exported == null) continue;
      candidates += 1;
      for (const container of graph.containers) {
        try {
          const value = container.get(exported);
          if (!clientShape(value)) continue;
          Object.defineProperty(window, clientKey, {value, configurable:true});
          return {client:value, diagnostics:{imported, candidates, containers:graph.containers.length, visited:graph.visited}};
        } catch {}
      }
    }
  }
  throw new Error(`normal-chat client unavailable (modules=${imported}, candidates=${candidates}, containers=${graph.containers.length}, visited=${graph.visited})`);
};
const plain = (value) => JSON.parse(JSON.stringify(value));
const collectModels = (raw) => {
  const queue = [raw];
  const seen = new WeakSet();
  const bySlug = new Map();
  const defaultSlug = String(raw && (raw.default_model_slug || raw.defaultModelSlug || raw.default_model) || '');
  const addEffort = (entry, value) => {
    const effort = typeof value === 'string' ? value.trim() : '';
    if (effort) entry.efforts.add(effort);
  };
  while (queue.length && bySlug.size < 500) {
    const value = queue.shift();
    if (!value || typeof value !== 'object' || seen.has(value)) continue;
    seen.add(value);
    const slug = String(value.slug || value.id || value.model_slug || '').trim();
    const looksLikeModel = /^(gpt-|chatgpt-|o\d)/i.test(slug);
    if (looksLikeModel) {
      const available = !(value.disabled === true || value.enabled === false || value.available === false || value.is_available === false);
      let entry = bySlug.get(slug);
      if (!entry) {
        entry = {slug, is_default:false, is_available:false, efforts:new Set()};
        bySlug.set(slug, entry);
      }
      entry.is_default ||= !!(value.is_default || value.isDefault || value.default || slug === defaultSlug);
      entry.is_available ||= available;
      addEffort(entry, value.thinkingEffort);
      addEffort(entry, value.thinking_effort);
      for (const key of ['thinkingEfforts', 'thinking_efforts', 'supportedThinkingEfforts', 'supported_thinking_efforts']) {
        const efforts = value[key];
        if (Array.isArray(efforts)) for (const effort of efforts) addEffort(entry, effort);
      }
    }
    for (const child of Object.values(value)) if (child && typeof child === 'object') queue.push(child);
  }
  const models = [...bySlug.values()].map((entry) => {
    const thinkingEfforts = [...entry.efforts];
    return {
      slug:entry.slug,
      is_default:entry.is_default,
      is_available:entry.is_available,
      thinking_efforts:thinkingEfforts,
      supports_extended:thinkingEfforts.some((value) => value.toLowerCase() === 'extended'),
      supports_high:thinkingEfforts.some((value) => value.toLowerCase() === 'high'),
    };
  });
  return {models, default_slug:defaultSlug};
};
const chooseModel = (raw, preferred, latest, effort, requireHigh) => {
  const catalog = collectModels(raw);
  const available = catalog.models.filter((item) => item.slug && item.is_available !== false);
  const bySlug = new Map(available.map((item) => [item.slug, item]));
  const requestedInput = String(effort || '').trim();
  const supports = (item) => {
    const values = new Set((item?.thinking_efforts || []).map((value) => String(value).toLowerCase()));
    if (requestedInput) return values.has(requestedInput.toLowerCase());
    if (requireHigh) return values.has('extended') || values.has('high');
    return true;
  };
  let selected = null;
  if (preferred) {
    selected = bySlug.get(preferred) || null;
    if (!selected) throw new Error(`configured model is unavailable: ${preferred}`);
  } else if (latest && bySlug.has(latest) && supports(bySlug.get(latest))) {
    selected = bySlug.get(latest);
  } else if (requireHigh || requestedInput) {
    const catalogDefault = available.find((item) => item.is_default) || (catalog.default_slug && bySlug.get(catalog.default_slug));
    selected = (catalogDefault && supports(catalogDefault) ? catalogDefault : null)
      || available.find(supports)
      || null;
  } else {
    selected = available.find((item) => item.is_default)
      || (catalog.default_slug && bySlug.get(catalog.default_slug))
      || (latest && bySlug.get(latest))
      || available[0];
  }
  if (!selected) throw new Error('model catalogue contains no usable model with the required reasoning effort');
  const supported = new Map((selected.thinking_efforts || []).map((value) => [String(value).toLowerCase(), String(value)]));
  let requested = requestedInput;
  if (requireHigh && !requested) {
    requested = supported.get('extended') || supported.get('high') || '';
    if (!requested) throw new Error(`high reasoning is unavailable for ${selected.slug}`);
  }
  if (requested && supported.size && !supported.has(requested.toLowerCase())) {
    if (requireHigh) throw new Error(`configured reasoning effort is unavailable for ${selected.slug}`);
    requested = '';
  }
  if (requested && requireHigh && !supported.size) {
    throw new Error(`reasoning capabilities are unknown for ${selected.slug}`);
  }
  return {slug:selected.slug, effort:requested || null};
};
const summary = (raw) => {
  const conversation = raw && (raw.conversation || raw.data || raw) || {};
  const mapping = conversation.mapping || {};
  const currentNode = String(conversation.current_node || conversation.currentNode || '');
  const node = mapping[currentNode] || {};
  const latest = node.message || null;
  const status = String(latest && latest.status || '').toLowerCase();
  const role = String(latest && latest.author && latest.author.role || '');
  const running = role === 'assistant' && (['in_progress','streaming','queued','pending'].includes(status) || latest.end_turn === false);
  return {conversation, currentNode, latest, running, valid:!!currentNode && !!mapping[currentNode]};
};
"""


class InternalChatClient:
    """Sequential bridge to the normal-chat client already loaded by Codex desktop."""

    _locks: dict[tuple[str, int], asyncio.Lock] = {}

    def __init__(
        self,
        endpoint: str = DEFAULT_CODEX_CDP_ENDPOINT,
        *,
        timeout: float = 10.0,
        stream_timeout_seconds: int = 3600,
        preferred_model: str = "",
        thinking_effort: str = "extended",
        require_high_reasoning: bool = True,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = max(1.0, timeout)
        self.stream_timeout_seconds = max(30, stream_timeout_seconds)
        self.preferred_model = preferred_model.strip()
        self.thinking_effort = thinking_effort.strip()
        self.require_high_reasoning = require_high_reasoning
        self._socket: Any = None
        self._sequence = 0
        self._target: RuntimeTarget | None = None

    @classmethod
    def _lock_for(cls, endpoint: str) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        return cls._locks.setdefault((endpoint, id(loop)), asyncio.Lock())

    async def __aenter__(self) -> "InternalChatClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        await self.close()

    async def connect(self) -> None:
        probe = await probe_runtime(self.endpoint, timeout=self.timeout)
        if not probe.available:
            raise RuntimeUnavailableError(probe.reason or "Codex CDP endpoint is unavailable")
        if not probe.ready or probe.target is None:
            raise RuntimeNotReadyError(probe.reason or "Codex main renderer is not ready")
        self._target = probe.target
        try:
            self._socket = await websockets.connect(
                probe.target.websocket_url,
                origin=None,
                open_timeout=self.timeout,
                close_timeout=1,
                max_size=16 * 1024 * 1024,
            )
            await self._call("Runtime.enable")
        except Exception as exc:
            await self.close()
            raise RuntimeUnavailableError(
                f"could not attach to Codex renderer: {sanitize_runtime_error(exc)}"
            ) from exc

    async def close(self) -> None:
        if self._socket is not None:
            with contextlib.suppress(Exception):
                await self._socket.close()
        self._socket = None
        self._target = None

    async def reconnect(self) -> None:
        await self.close()
        await self.connect()

    async def _call(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if self._socket is None:
            raise RuntimeUnavailableError("Codex renderer is not connected")
        self._sequence += 1
        identifier = self._sequence
        await self._socket.send(json.dumps({"id": identifier, "method": method, "params": params or {}}))
        while True:
            raw = await asyncio.wait_for(self._socket.recv(), timeout=self.timeout)
            message = json.loads(raw)
            if message.get("id") != identifier:
                continue
            if "error" in message:
                raise RuntimeProtocolError(f"CDP {method} failed")
            return message.get("result", {})

    async def _evaluate_raw(self, expression: str) -> Any:
        result = await self._call("Runtime.evaluate", {"expression": expression, "returnByValue": True, "awaitPromise": True, "userGesture": False})
        remote = result.get("result", {})
        if remote.get("subtype") == "error" or result.get("exceptionDetails"):
            description = remote.get("description") or "JavaScript evaluation failed"
            raise RuntimeProtocolError(sanitize_runtime_error(description))
        return remote.get("value")

    async def _evaluate(self, body: str) -> Any:
        expression = f"(async () => {{ {_BRIDGE_JS} {body} }})()"
        async with self._lock_for(self.endpoint):
            try:
                return await self._evaluate_raw(expression)
            except (ConnectionError, OSError, asyncio.TimeoutError, websockets.ConnectionClosed):
                await self.reconnect()
                return await self._evaluate_raw(expression)

    async def health(self) -> dict[str, Any]:
        payload = await self._evaluate("const resolved = await resolveClient(); const catalog = collectModels(await resolved.client.models()); return {ready:true, model_count:catalog.models.length, diagnostics:resolved.diagnostics};")
        if not isinstance(payload, dict) or not payload.get("ready"):
            raise RuntimeProtocolError("internal client readiness returned an invalid result")
        return {
            "ready": True,
            "model_count": int(payload.get("model_count", 0) or 0),
            "target_id": self._target.target_id if self._target else "",
            "target_url": self._target.url if self._target else "",
            "diagnostics": payload.get("diagnostics") if isinstance(payload.get("diagnostics"), dict) else {},
        }

    async def models(self) -> dict[str, Any]:
        payload = await self._evaluate("const resolved = await resolveClient(); return collectModels(await resolved.client.models());")
        if not isinstance(payload, dict):
            raise RuntimeProtocolError("model catalogue returned an invalid result")
        return {"models": payload.get("models") if isinstance(payload.get("models"), list) else [], "default_slug": str(payload.get("default_slug") or "")}

    async def get_thread(self, conversation_id: str) -> dict[str, Any]:
        payload = await self._evaluate(
            f"const resolved = await resolveClient(); const raw = await resolved.client.get({json.dumps(conversation_id)}); return {{raw:plain(raw), owned_stream:streams.has({json.dumps(conversation_id)})}};"
        )
        if not isinstance(payload, dict):
            raise RuntimeProtocolError("conversation read returned an invalid result")
        return normalize_conversation_payload(payload.get("raw"), conversation_id, owned_stream=bool(payload.get("owned_stream")))

    async def continue_thread(
        self,
        conversation_id: str,
        message: str,
        *,
        expected_current_node: str,
    ) -> dict[str, Any]:
        if not expected_current_node:
            raise ValueError("expected_current_node is required")
        body = f"""
        const resolved = await resolveClient();
        const client = resolved.client;
        const conversationId = {json.dumps(conversation_id)};
        const expectedNode = {json.dumps(expected_current_node)};
        const text = {json.dumps(message)};
        const beforeRaw = await client.get(conversationId);
        const before = summary(beforeRaw);
        if (!before.valid || before.currentNode !== expectedNode) return {{sent:false, running:false, reason:'canonical current_node changed before send'}};
        if (streams.has(conversationId) || before.running) return {{sent:false, running:true, reason:'thread is running'}};
        const latest = before.latest;
        const status = String(latest && latest.status || '').toLowerCase();
        const terminal = new Set(['finished_successfully','finished_error','failed','cancelled','canceled','interrupted','incomplete']);
        if (!latest || String(latest.author && latest.author.role || '') !== 'assistant') return {{sent:false, running:false, reason:'latest canonical message is not an assistant response'}};
        if (!terminal.has(status) || latest.end_turn !== true) return {{sent:false, running:false, reason:'latest assistant response is not terminal'}};
        const metadata = latest.metadata || {{}};
        const model = chooseModel(await client.models(), {json.dumps(self.preferred_model)}, String(metadata.model_slug || metadata.default_model_slug || ''), {json.dumps(self.thinking_effort)}, {json.dumps(self.require_high_reasoning)});
        const userMessageId = crypto.randomUUID();
        const request = {{
          action:'next', conversation_id:conversationId, parent_message_id:expectedNode, model:model.slug,
          messages:[{{id:userMessageId, author:{{role:'user'}}, content:{{content_type:'text', parts:[text]}}, create_time:Date.now()/1000, end_turn:null, metadata:{{}}, recipient:'all', status:'finished_successfully', weight:1}}],
          supported_encodings:['v1'], timezone:Intl.DateTimeFormat().resolvedOptions().timeZone, timezone_offset_min:new Date().getTimezoneOffset(),
        }};
        if (model.effort) request.thinking_effort = model.effort;
        if (before.conversation.gizmo_id) request.gizmo_id = before.conversation.gizmo_id;
        let requestId = '', finalMessageId = '', finalStatus = '', terminalEvent = '';
        let timedOut = false;
        streams.set(conversationId, {{started_at:Date.now(), user_message_id:userMessageId}});
        await new Promise((resolvePromise, rejectPromise) => {{
          let settled = false;
          const cleanup = () => {{ streams.delete(conversationId); handles.delete(conversationId); clearTimeout(timer); }};
          const finish = (eventType='') => {{ cleanup(); if (settled) return; settled = true; terminalEvent = eventType || terminalEvent; resolvePromise(); }};
          const fail = (error) => {{ cleanup(); if (settled) return; settled = true; rejectPromise(error instanceof Error ? error : new Error(String(error))); }};
          const timer = setTimeout(() => {{ if (settled) return; settled = true; timedOut = true; resolvePromise(); }}, {self.stream_timeout_seconds * 1000});
          const observe = (value) => {{
            const candidate = value && (value.message || value.data || value);
            if (candidate && candidate.id) finalMessageId = String(candidate.id);
            if (candidate && candidate.status) finalStatus = String(candidate.status);
            if (value && value.request_id) requestId = String(value.request_id);
            if (value && value.streamRequestId) requestId = String(value.streamRequestId);
            const eventType = String(value && (value.type || value.event_type || value.event) || '');
            if (eventType === 'message_stream_complete') finish(eventType);
          }};
          try {{
            const starting = client.startCompletionStream({{request, onEvent:observe, onUpdate:observe, onComplete:(value) => {{ observe(value); finish('onComplete'); }}, onError:fail, onRecoverableError:() => {{}}}});
            handles.set(conversationId, starting);
            if (starting && typeof starting.then === 'function') {{
              starting.then((handle) => {{ observe(handle); if (!settled) handles.set(conversationId, handle); }}, fail);
            }} else {{
              observe(starting);
              if (!settled) handles.set(conversationId, starting);
            }}
          }} catch (error) {{ fail(error); }}
        }});
        if (timedOut) {{
          let afterRaw = null;
          try {{ afterRaw = await client.get(conversationId); }} catch {{}}
          return {{sent:true, observed:false, running:true, reason:'completion stream exceeded the configured timeout', request_id:requestId, user_message_id:userMessageId, final_message_id:finalMessageId, final_status:finalStatus, parent_message_id:expectedNode, terminal_event:terminalEvent, after_raw:afterRaw ? plain(afterRaw) : null}};
        }}
        const afterRaw = await client.get(conversationId);
        return {{sent:true, observed:true, running:false, reason:'', request_id:requestId, user_message_id:userMessageId, final_message_id:finalMessageId, final_status:finalStatus, parent_message_id:expectedNode, terminal_event:terminalEvent, after_raw:plain(afterRaw)}};
        """
        payload = await self._evaluate(body)
        if not isinstance(payload, dict):
            raise RuntimeProtocolError("continuation returned an invalid result")
        if payload.get("after_raw") is not None:
            after = normalize_conversation_payload(payload.pop("after_raw"), conversation_id)
            user_id = str(payload.get("user_message_id") or "")
            persisted = any(turn.get("key") == user_id for turn in after.get("turns", []))
            payload["observed"] = persisted
            payload["running"] = bool(payload.get("running")) or bool(after.get("running"))
            if not persisted and not payload.get("reason"):
                payload["reason"] = "submitted user message was not found in canonical conversation state"
            turns = after.get("turns", [])
            final = next(
                (turn for turn in reversed(turns) if turn.get("role") == "assistant"),
                None,
            )
            if final is not None:
                payload["final_message_id"] = str(
                    final.get("key") or payload.get("final_message_id") or ""
                )
                payload["final_status"] = str(
                    final.get("status") or payload.get("final_status") or ""
                )
        return payload

    async def cancel(self, conversation_id: str) -> dict[str, Any]:
        payload = await self._evaluate(
            f"const resolved = await resolveClient(); const id = {json.dumps(conversation_id)};"
            "if (!streams.has(id)) return {cancelled:false, reason:'no watchdog-owned stream'};"
            "const handle = handles.get(id);"
            "try {"
            " if (handle && typeof handle.cancel === 'function') await handle.cancel();"
            " else if (typeof resolved.client.cancelStream === 'function') await resolved.client.cancelStream(id);"
            " else return {cancelled:false, reason:'cancel is unavailable'};"
            " streams.delete(id); handles.delete(id); return {cancelled:true, reason:''};"
            "} catch { return {cancelled:false, reason:'cancel failed'}; }"
        )
        return payload if isinstance(payload, dict) else {"cancelled": False, "reason": "invalid result"}
