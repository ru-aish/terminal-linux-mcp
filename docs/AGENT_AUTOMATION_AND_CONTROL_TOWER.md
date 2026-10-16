# Agent automation and control tower

This feature adds a durable local automation layer above the existing one-request ChatGPT gateway. It does not create a second provider scheduler.

## Ownership

```text
Terminal MCP domain ledger
├── agent tree, tasks, commands, events
├── wake timers
└── completion-marker gates
        │ materialize one ordinary command
        ▼
ChatGateway
├── one physical request per tick
├── rate windows and circuits
├── canonical reads
└── staged create / continue verification
```

A timer becoming due or a marker gate becoming true is local SQLite work. It does not itself consume a ChatGPT request. The resulting prompt enters the same ordered command mailbox and provider rail as `agent_send`.

## Public tools

### `agent_schedule_wakeup`

Inputs:

- `thread_id`: durable agent ID or ChatGPT conversation ID.
- `wake_at`: Unix seconds or ISO-8601 with an explicit timezone.
- `prompt`: optional wake instruction. A safe orchestration-review prompt is used when omitted.
- `idempotency_key`: optional replay key scoped to target and automation kind.

When the timer is due, Terminal MCP queues one continuation. If the target lacks a cached canonical snapshot, a read occurs first. If the target is still generating, delivery remains queued until a safe canonical terminal turn exists.

### `agent_queue_after_completion`

Inputs:

- `source_thread_id`: agent or conversation whose completion is the gate.
- `target_thread_id`: agent or conversation that receives the queued prompt.
- `prompt`: instruction to send after the gate opens.
- `completion_marker`: optional exact marker; defaults to the source task marker, or the standard Terminal MCP marker for a registered root.
- `idempotency_key`: optional replay key.

The gate opens only when the latest canonical assistant turn:

1. has a terminal status;
2. has `end_turn=true`; and
3. contains the configured marker as an exact standalone line.

A stopped thread, missing stream handle, elapsed time, or terminal-looking response without that line does not activate the prompt and remains eligible for later continuation. A source that is failed, cancelled, or externally terminalized without the exact evidence fails the automation visibly.

### `agent_automation`

- `action=list`: lists timers and completion gates, optionally filtered by agent/conversation.
- `action=cancel`: cancels a scheduled, waiting, or safely queued automation.

Delivery already in flight or canonically uncertain is not cancelled because doing so could create duplicate provider writes.

## Reasoning contract

The durable renderer adapter puts `thinking_effort` on every `CREATE` and `CONTINUE` request. Wake-up and completion-triggered prompts become `CONTINUE` operations and inherit the same setting.

Defaults:

```text
CHAT_GATEWAY_THINKING_EFFORT=extended
CHAT_GATEWAY_REQUIRE_HIGH_REASONING=1
```

When high reasoning is required, only `high` or `extended` are accepted during backend construction. No extra model-catalogue provider call is introduced to select the effort.

## Dashboard

`/dashboard/agents` now reports:

- DFS parent/child structure and depth;
- agent/task/gateway/mailbox state;
- synchronization-loop state and latest physical request count;
- configured model and reasoning effort;
- wake timers and exact-marker gates;
- provider operation rail;
- circuits, request history, capacity, and next eligibility.

The page is observational. Mutation remains available only through authenticated MCP tools.

## Timing model

- A newly scheduled timer wakes the background service directly.
- Near timers use a one-second local wait floor instead of the normal 30-second provider-poll floor.
- Canonical reads and continuations remain separate ticks.
- Rate limits, runtime circuits, and provider spacing can delay actual prompt delivery after local activation.

## Verification map

1. **Persistence:** migrate a schema-10 online backup to schema 11; preserve all agents and pass integrity/FK checks.
2. **Timer:** prove no command or provider operation exists before due time; at due time queue once; inspect if needed; continue through the gateway; mark delivered.
3. **Completion gate:** terminal response without marker stays waiting; embedded marker fails; exact standalone marker queues once.
4. **Cancellation:** scheduled cancellation is idempotent and never materializes later.
5. **Reasoning:** capture create/continue JavaScript and assert the configured effort is attached without `client.models()`.
6. **Synchronization:** verify near timers bypass the 30-second polling floor while provider calls remain 0/1 per tick.
7. **Dashboard:** render populated data at 390×844 and desktop width; confirm no horizontal overflow and visible nested depth.
8. **Regression:** run the complete Terminal MCP suite and the original standalone gateway contract.
