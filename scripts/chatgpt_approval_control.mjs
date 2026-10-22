/** Scoped approval control through the existing authenticated ChatGPT client. */
import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';

export function pendingApproval(conversation) {
  const message = conversation.mapping?.[conversation.current_node]?.message;
  const server = message?.metadata?.jit_plugin_data?.from_server
    ?? message?.metadata?.elicitation_metadata?.plugin_data?.from_server;
  if (message?.author?.role !== 'tool') return null;
  if (server?.type !== 'confirm_action' || server.body?.codex_mcp_elicitation?.kind !== 'mcpToolCall') return null;
  if (server.body.codex_mcp_elicitation.status !== 'pending') return null;
  const body = server.body;
  if (![conversation.conversation_id, message.id, body.connector_id, body.link_id]
    .every(value => typeof value === 'string' && value.length > 0 && value.length <= 512)) return null;
  const levels = [];
  for (const action of body.actions ?? []) {
    if (action.allow?.target_message_id === message.id) levels.push('once');
    if (action.deny?.target_message_id === message.id) levels.push('deny');
    if (action.always_allow?.target_message_id === message.id) levels.push('always');
    for (const option of action.split_action_options ?? []) {
      if (option.action?.type === 'allow' && option.action.remember_answer === true
          && option.action.target_message_id === message.id) levels.push('conversation');
    }
  }
  return {
    conversationId: conversation.conversation_id,
    messageId: message.id,
    connectorId: body.connector_id,
    linkId: body.link_id,
    connectorName: body.connector_name,
    toolPath: body.params?.path,
    levels: [...new Set(levels)],
  };
}

export function buildApprovalRequest(conversation, choice, messageId, now = Date.now()) {
  if (![choice.conversationId, choice.messageId, choice.connectorId, choice.linkId, messageId]
    .every(value => typeof value === 'string' && value.length > 0 && value.length <= 512)) {
    throw new Error('Approval requires explicit conversation, message, connector, and link identities');
  }
  const approval = pendingApproval(conversation);
  if (!approval || approval.conversationId !== choice.conversationId || approval.messageId !== choice.messageId) throw new Error('Pending approval changed; inspect again');
  if (approval.connectorId !== choice.connectorId || approval.linkId !== choice.linkId) throw new Error('Connector/link identity mismatch');
  if (!approval.levels.includes(choice.level)) throw new Error('Approval level is not offered by this request');
  const action = choice.level === 'always' ? 'always_allow' : choice.level === 'deny' ? 'deny' : 'allow';
  const fromClient = { target_message_id: approval.messageId, type: action };
  if (action === 'allow' || action === 'deny') fromClient.remember_answer = choice.level === 'conversation';
  const source = conversation.mapping[approval.messageId].message;
  const request = {
    action: 'next',
    conversation_id: conversation.conversation_id,
    parent_message_id: approval.messageId,
    model: conversation.default_model_slug,
    ...(source.metadata?.thinking_effort ? { thinking_effort: source.metadata.thinking_effort } : {}),
    supported_encodings: ['v1'],
    ...(conversation.gizmo_id ? {
      gizmo_id: conversation.gizmo_id,
      conversation_mode: { kind: 'gizmo_interaction', gizmo_id: conversation.gizmo_id },
    } : {}),
    messages: [{
      id: messageId,
      author: { role: 'tool', name: 'api_tool.call_tool', metadata: {} },
      channel: null,
      content: { content_type: 'text', parts: [''] },
      create_time: now / 1000,
      end_turn: null,
      metadata: { jit_plugin_data: { from_client: fromClient } },
      recipient: 'all', status: 'finished_successfully', update_time: null, weight: 1,
    }],
  };
  if (!request.model) throw new Error('Conversation model is unavailable');
  return request;
}

async function submitApproval(client, conversation, args, jobs) {
  const jobKey = `${args.conversationId}:${args.messageId}`;
  const prior = jobs.get(jobKey);
  if (prior) {
    if (prior.level !== args.level || prior.connectorId !== args.connectorId || prior.linkId !== args.linkId) {
      throw new Error('A different decision was already attempted for this request');
    }
    return { alreadyAttempted: true, job: prior };
  }
  if (jobs.size >= 200) throw new Error('Approval audit limit reached; inspect existing decisions first');
  const request = buildApprovalRequest(conversation, args, args.responseMessageId);
  request.timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  request.timezone_offset_min = -new Date().getTimezoneOffset();
  const job = {
    conversationId: args.conversationId, messageId: args.messageId,
    responseMessageId: args.responseMessageId, connectorId: args.connectorId,
    linkId: args.linkId, level: args.level, startedAt: Date.now(), state: 'submitting', updates: 0,
  };
  jobs.set(jobKey, job);
  try {
    const handle = await client.startCompletionStream({
      request,
      onUpdate: () => { job.updates++; job.updatedAt = Date.now(); },
      onComplete: () => { job.state = 'stream_complete'; },
      onError: () => { job.state = 'stream_error'; },
    });
    job.streamRequestId = handle?.streamRequestId;
    if (job.state === 'submitting') job.state = 'submitted';
    return { alreadyAttempted: false, job };
  } catch (error) {
    job.state = 'submission_error';
    throw error;
  }
}

// These three functions run in the same authenticated renderer as T3's API client.
export async function nativeOperation(operation, args) {
  const client = window[Symbol.for('terminal-mcp.chat-runtime.client.v5')];
  if (typeof client?.get !== 'function' || typeof client?.startCompletionStream !== 'function') {
    throw new Error('Existing ChatGPT API client is unavailable; initialize the normal connection first');
  }
  const key = Symbol.for('terminal-mcp.chatgpt-approval-control.v1');
  const jobs = window[key] ?? new Map();
  window[key] = jobs;
  const raw = await client.get(args.conversationId);
  const conversation = raw.data ?? raw;
  if (conversation.conversation_id !== args.conversationId) throw new Error('Conversation identity mismatch');
  if (operation === 'inspect') return { pending: pendingApproval(conversation) };
  const watcherKey = Symbol.for('terminal-mcp.chatgpt-approval-watchers.v1');
  const watchers = window[watcherKey] ?? new Map();
  if (operation === 'watch') {
    const previous = watchers.get(args.conversationId);
    if (previous) return { alreadyWatching: true, watcher: previous };
    if (!Number.isInteger(args.intervalMs) || args.intervalMs < 1000 || args.intervalMs > 60000) {
      throw new Error('Watch interval must be between 1000 and 60000 milliseconds');
    }
    const watcher = {
      conversationId: args.conversationId, level: 'always', intervalMs: args.intervalMs,
      state: 'watching', startedAt: Date.now(), approvals: 0,
    };
    watchers.set(args.conversationId, watcher);
    window[watcherKey] = watchers;
    const poll = async currentConversation => {
      if (watcher.state !== 'watching' || watcher.inFlight) return;
      watcher.inFlight = true;
      try {
        const current = currentConversation ?? (await client.get(args.conversationId));
        const latest = current.data ?? current;
        if (latest.conversation_id !== args.conversationId) throw new Error('Conversation identity mismatch');
        const pending = pendingApproval(latest);
        if (pending && pending.messageId !== watcher.lastMessageId) {
          const result = await submitApproval(client, latest, {
            ...pending, level: 'always', responseMessageId: crypto.randomUUID(),
          }, jobs);
          watcher.lastMessageId = pending.messageId;
          if (!result.alreadyAttempted) {
            watcher.approvals++;
            watcher.lastApprovalAt = Date.now();
            watcher.lastApproval = { messageId: pending.messageId, connectorId: pending.connectorId, linkId: pending.linkId };
          }
        }
        watcher.lastCheckedAt = Date.now();
        watcher.lastError = null;
      } catch (error) {
        watcher.lastCheckedAt = Date.now();
        watcher.lastError = String(error?.message ?? error);
      } finally {
        watcher.inFlight = false;
        if (watcher.state === 'watching') watcher.timer = setTimeout(() => { void poll(); }, watcher.intervalMs);
      }
    };
    void poll(conversation);
    return { alreadyWatching: false, watcher };
  }
  if (operation === 'unwatch') {
    const watcher = watchers.get(args.conversationId);
    if (watcher) {
      watcher.state = 'stopped';
      clearTimeout(watcher.timer);
      watchers.delete(args.conversationId);
    }
    window[watcherKey] = watchers;
    return { stopped: !!watcher, watcher: watcher ?? null };
  }
  const jobKey = `${args.conversationId}:${args.messageId}`;
  if (operation === 'status') {
    const responses = Object.values(conversation.mapping ?? {}).flatMap(node => {
      const message = node?.message;
      const response = message?.metadata?.jit_plugin_data?.from_client;
      return response?.target_message_id === args.messageId ? [{ messageId: message.id, decision: response }] : [];
    });
    return {
      job: args.messageId ? jobs.get(jobKey) ?? null : null,
      watcher: watchers.get(args.conversationId) ?? null,
      pending: pendingApproval(conversation), recordedResponses: responses,
    };
  }
  if (operation !== 'respond') throw new Error('Unsupported operation');
  window[key] = jobs;
  return submitApproval(client, conversation, args, jobs);
}

export async function control(operation, args, endpoint = 'http://127.0.0.1:9234') {
  const url = new URL(endpoint);
  if (!['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname) || url.protocol !== 'http:') {
    throw new Error('The existing ChatGPT client endpoint must be loopback HTTP');
  }
  const targets = await (await fetch(new URL('/json/list', url), { signal: AbortSignal.timeout(5000) })).json();
  const target = targets.find(t => t.type === 'page' && t.url === 'app://-/index.html');
  if (!target?.webSocketDebuggerUrl) throw new Error('Existing ChatGPT renderer is unavailable');
  const socket = new WebSocket(target.webSocketDebuggerUrl);
  let timer;
  try {
    const connected = new Promise((resolve, reject) => {
      socket.addEventListener('open', resolve, { once: true });
      socket.addEventListener('error', () => reject(new Error('Cannot connect to the existing client')), { once: true });
    });
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('Approval outcome is uncertain; inspect status before retrying')), 60000);
    });
    return await Promise.race([timeout, (async () => {
      await connected;
      const response = new Promise((resolve, reject) => {
        socket.addEventListener('message', event => {
          const result = JSON.parse(String(event.data));
          if (result.id === 1) result.error ? reject(new Error(result.error.message)) : resolve(result.result);
        });
        socket.addEventListener('close', () => reject(new Error('Connection closed; inspect status before retrying')), { once: true });
      });
      const expression = `(async()=>{
        const pendingApproval=${pendingApproval.toString()};
        const buildApprovalRequest=${buildApprovalRequest.toString()};
        const submitApproval=${submitApproval.toString()};
        return (${nativeOperation.toString()})(${JSON.stringify(operation)},${JSON.stringify({ ...args, responseMessageId: args.responseMessageId ?? randomUUID() })});
      })()`;
      socket.send(JSON.stringify({ id: 1, method: 'Runtime.evaluate', params: { expression, awaitPromise: true, returnByValue: true } }));
      const result = await response;
      if (result.exceptionDetails) throw new Error(result.exceptionDetails.exception?.description ?? result.exceptionDetails.text);
      if (result.result?.value == null) throw new Error('Unknown native approval response format');
      return result.result.value;
    })()]);
  } finally {
    clearTimeout(timer);
    socket.close();
  }
}

async function main() {
  const { values, positionals } = parseArgs({ allowPositionals: true, options: {
    'thread-id': { type: 'string' }, 'conversation-id': { type: 'string' },
    'cache-dir': { type: 'string', default: join(homedir(), '.t3/userdata/chatgpt/chatgpt') },
    'cdp-endpoint': { type: 'string', default: 'http://127.0.0.1:9234' },
    'message-id': { type: 'string' }, 'connector-id': { type: 'string' },
    'link-id': { type: 'string' }, level: { type: 'string' },
    'interval-ms': { type: 'string', default: '3000' }, help: { type: 'boolean' },
  } });
  if (values.help) {
    console.log('Usage: node scripts/chatgpt_approval_control.mjs inspect|respond|status|watch|unwatch --thread-id T3_ID [--message-id ID --connector-id ID --link-id ID --level once|conversation|always|deny]');
    return;
  }
  const operation = positionals[0];
  if (!['inspect', 'respond', 'status', 'watch', 'unwatch'].includes(operation)) throw new Error('Choose inspect, respond, status, watch, or unwatch');
  if (!!values['thread-id'] === !!values['conversation-id']) throw new Error('Provide exactly one thread-id or conversation-id');
  let conversationId = values['conversation-id'];
  if (values['thread-id']) {
    const file = createHash('sha256').update(values['thread-id']).digest('hex') + '.json';
    const cache = JSON.parse(await readFile(join(values['cache-dir'], file), 'utf8'));
    conversationId = cache.conversationId;
  }
  if (!conversationId) throw new Error('The thread has no native conversation');
  if (operation === 'respond' && !values['message-id']) throw new Error('Provide message-id from inspect');
  if (operation === 'respond' && (!values['connector-id'] || !values['link-id'] || !values.level)) {
    throw new Error('Respond requires connector-id, link-id, and level from inspect');
  }
  const intervalMs = Number(values['interval-ms']);
  if (operation === 'watch' && (!Number.isInteger(intervalMs) || intervalMs < 1000 || intervalMs > 60000)) {
    throw new Error('--interval-ms must be between 1000 and 60000');
  }
  console.log(JSON.stringify(await control(operation, {
    conversationId, messageId: values['message-id'], connectorId: values['connector-id'],
    linkId: values['link-id'], level: values.level, intervalMs,
  }, values['cdp-endpoint']), null, 2));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => { console.error(error.message); process.exitCode = 1; });
}
