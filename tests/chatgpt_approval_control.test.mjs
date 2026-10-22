import { afterEach, test } from 'node:test';
import assert from 'node:assert/strict';
import { pendingApproval, buildApprovalRequest, nativeOperation, nativeResult } from '../scripts/chatgpt_approval_control.mjs';

function fixture() {
  return {
    conversation_id: 'native-one', current_node: 'approval-one',
    default_model_slug: 'gpt-6-thinking', gizmo_id: 'project-one',
    mapping: { 'approval-one': { message: {
      id: 'approval-one', author: { role: 'tool' }, metadata: {
        thinking_effort: 'extended', jit_plugin_data: { from_server: {
          type: 'confirm_action', body: {
            connector_id: 'app-one', link_id: 'link-one', connector_name: 'fedora_t3code',
            params: { path: '/app-one/link-one/bootstrap_thread' },
            codex_mcp_elicitation: { kind: 'mcpToolCall', status: 'pending' },
            actions: [
              { type: 'always_allow', always_allow: { target_message_id: 'approval-one' } },
              { type: 'deny', deny: { target_message_id: 'approval-one' } },
              { type: 'allow', allow: { target_message_id: 'approval-one' }, split_action_options: [{
                action: { type: 'allow', target_message_id: 'approval-one', remember_answer: true },
              }] },
            ],
          },
        } },
      },
    } } },
  };
}
const choice = (level) => ({
  conversationId: 'native-one', messageId: 'approval-one',
  connectorId: 'app-one', linkId: 'link-one', level, responseMessageId: 'response-one',
});

test('inspect identifies an empty approval card and its actual scopes', () => {
  const pending = pendingApproval(fixture());
  assert.deepEqual(pending.levels, ['always', 'deny', 'once', 'conversation']);
  assert.equal(pending.connectorId, 'app-one');
  assert.equal(pending.linkId, 'link-one');
});

for (const [level, type, remember] of [
  ['once', 'allow', false], ['conversation', 'allow', true],
  ['always', 'always_allow', undefined], ['deny', 'deny', false],
]) {
  test(`${level} sends a native tool response while preserving the conversation`, () => {
    const request = buildApprovalRequest(fixture(), choice(level), 'response-one', 1000);
    assert.equal(request.parent_message_id, 'approval-one');
    assert.equal(request.conversation_id, 'native-one');
    assert.equal(request.model, 'gpt-6-thinking');
    assert.equal(request.gizmo_id, 'project-one');
    assert.equal(request.messages[0].author.role, 'tool');
    assert.deepEqual(request.messages[0].metadata.jit_plugin_data.from_client, {
      target_message_id: 'approval-one', type,
      ...(remember === undefined ? {} : { remember_answer: remember }),
    });
  });
}

test('refuses stale requests, connector substitutions, and unoffered scopes', () => {
  const conversation = fixture();
  for (const override of [{ messageId: 'old-request' }, { connectorId: 'other-app' }, { linkId: 'other-link' }, { level: 'full-access' }, { conversationId: 'other-conversation' }, { linkId: undefined }]) {
    assert.throws(() => buildApprovalRequest(conversation, { ...choice('once'), ...override }, 'response-one'));
  }
  conversation.mapping['approval-one'].message.metadata.jit_plugin_data.from_server.body.actions.shift();
  assert.throws(() => buildApprovalRequest(conversation, choice('always'), 'response-one'), /not offered/);
});

test('does not turn authentication challenges or completed requests into approvals', () => {
  const c = fixture();
  const server = c.mapping['approval-one'].message.metadata.jit_plugin_data.from_server;
  c.mapping['approval-one'].message.author.role = 'assistant';
  assert.equal(pendingApproval(c), null);
  c.mapping['approval-one'].message.author.role = 'tool';
  server.type = 'oauth_required';
  assert.equal(pendingApproval(c), null);
  server.type = 'confirm_action';
  server.body.codex_mcp_elicitation.status = 'accepted';
  assert.equal(pendingApproval(c), null);
  c.current_node = 'new-leaf';
  assert.equal(pendingApproval(c), null);
});

const originalWindow = globalThis.window;
afterEach(() => { globalThis.window = originalWindow; });

test('an inspection cannot submit a response and repeated decisions submit only once', async () => {
  const c = fixture();
  let calls = 0;
  globalThis.window = { [Symbol.for('terminal-mcp.chat-runtime.client.v5')]: {
    get: async () => c,
    startCompletionStream: async ({ request, onComplete }) => {
      calls++;
      c.current_node = 'response-one';
      c.mapping['response-one'] = { message: request.messages[0] };
      onComplete();
      return { streamRequestId: 'stream-one' };
    },
  } };
  assert.ok((await nativeOperation('inspect', choice('always'))).pending);
  assert.equal(calls, 0);
  const first = await nativeOperation('respond', choice('always'));
  assert.equal(first.job.state, 'stream_complete');
  assert.equal((await nativeOperation('respond', choice('always'))).alreadyAttempted, true);
  assert.equal(calls, 1);
  const status = await nativeOperation('status', choice('always'));
  assert.equal(status.pending, null);
  assert.equal(status.recordedResponses[0].decision.type, 'always_allow');
  await assert.rejects(nativeOperation('respond', choice('deny')), /already attempted/);
  await assert.rejects(nativeOperation('respond', { ...choice('always'), linkId: 'different-link' }), /already attempted/);
});

test('an uncertain submission is retained and never automatically repeated', async () => {
  let calls = 0;
  globalThis.window = { [Symbol.for('terminal-mcp.chat-runtime.client.v5')]: {
    get: async () => fixture(),
    startCompletionStream: async () => { calls++; throw new Error('Lost transport'); },
  } };
  await assert.rejects(nativeOperation('respond', choice('once')), /Lost transport/);
  const result = await nativeOperation('respond', choice('once'));
  assert.equal(result.alreadyAttempted, true);
  assert.equal(result.job.state, 'submission_error');
  assert.equal(calls, 1);
});

test('cyclic native rate-limit errors remain serializable across CDP', async () => {
  const error = { message: '{"detail":"Too many requests"}' };
  error.cause = error;
  globalThis.window = { [Symbol.for('terminal-mcp.chat-runtime.client.v5')]: {
    get: async () => { throw error; },
    startCompletionStream: async () => assert.fail('Read-only inspection must not submit'),
  } };
  const result = await nativeResult('inspect', choice('always'));
  assert.deepEqual(JSON.parse(JSON.stringify(result)), {
    ok: false, error: '{"detail":"Too many requests"}',
  });
});

test('watcher status and stopping do not fetch ChatGPT conversation history', async () => {
  globalThis.window = { [Symbol.for('terminal-mcp.chat-runtime.client.v5')]: {
    get: async () => assert.fail('Local watcher management must not use the remote API'),
    startCompletionStream: async () => assert.fail('Watcher management must not submit'),
  } };
  const status = await nativeOperation('status', { conversationId: 'native-one' });
  assert.equal(status.conversationChecked, false);
  assert.equal(status.watcher, null);
  assert.equal((await nativeOperation('unwatch', { conversationId: 'native-one' })).stopped, false);
});
