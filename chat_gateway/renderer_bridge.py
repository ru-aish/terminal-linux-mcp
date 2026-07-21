from __future__ import annotations

"""Shared renderer-side normal-chat client discovery bridge."""

CHAT_RENDERER_BRIDGE_JS = r"""
const clientKey = Symbol.for('terminal-mcp.chat-runtime.client.v4');
const streamsKey = Symbol.for('terminal-mcp.chat-runtime.streams.v4');
const handlesKey = Symbol.for('terminal-mcp.chat-runtime.handles.v4');
window[streamsKey] ||= new Map();
window[handlesKey] ||= new Map();
const streams = window[streamsKey];
const handles = window[handlesKey];
const ownedStreamActive = (conversationId) => {
  const state = streams.get(conversationId);
  if (!state) return false;
  const startedAt = Number(state.started_at || 0);
  return !startedAt || Date.now() - startedAt <= 120000;
};
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
