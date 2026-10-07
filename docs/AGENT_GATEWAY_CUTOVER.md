# Agent gateway cutover and rollback

## Runtime layout

Terminal MCP keeps two separate SQLite databases:

- `MCP_CHAT_AGENT_DB` — orchestration relationships, tasks, mailboxes, subscriptions, events, and cursors.
- `MCP_CHAT_GATEWAY_DB` — provider operations, canonical agent lifecycle, request events, claims, rate windows, caches, and circuit breakers.

The first database is migrated in place from schema version 9 to 10. The second database may start empty. Existing conversations in the domain database are adopted into gateway records during coordinator initialization; their ChatGPT conversations are not recreated.

## Pre-cutover checks

1. Confirm no managed child is actively generating or waiting on an uncertain mutation.
2. Record the running process PID, command line, repository commit, and service ownership.
3. Run `PRAGMA integrity_check` and `PRAGMA foreign_key_check` on the domain database.
4. Back up the live repository and copy the domain database plus `-wal` and `-shm` files while the process is stopped, or use SQLite’s online backup API before stopping.
5. Preserve the prior environment/configuration and startup command.
6. Build and test the candidate outside the live repository.

## Cutover

1. Stop the old Terminal MCP process cleanly.
2. Take a final domain database backup after shutdown.
3. Copy the validated candidate files into the live repository.
4. Set or confirm:

```text
MCP_CHAT_AGENT_DB=~/.GPT/chat-agent-orchestrator.db
MCP_CHAT_GATEWAY_DB=~/.GPT/chat-agent-gateway.db
MCP_CHAT_AGENT_MAX_ACTIVE_CHILDREN=5
CHAT_GATEWAY_CDP_ENDPOINT=http://127.0.0.1:9222
CHAT_GATEWAY_MODEL_SLUG=gpt-5-6-thinking
```

5. Start Terminal MCP once. Do not run the old and new coordinators concurrently against the same domain database.
6. Verify the process, port, MCP endpoint, `/dashboard`, `/dashboard/agents`, and `/dashboard/agents/api`.
7. Load context using `get_thread_context` with a server-assigned ID and confirm the core behavior appears before `.GPT/AGENTS.md` and the `terminal-mcp-subagents` skill is listed.
8. Call `agent_sync` while idle and confirm `physical_requests` is `0`.
9. Run one bounded child canary. Confirm each sync result reports `physical_requests` as `0` or `1`, create and verification occur in separate ticks, and the dashboard records the request events.

## Expected first-start behavior

- The domain schema advances to version 10 and adds gateway mapping columns.
- Existing domain agents with conversation IDs receive gateway records locally.
- Existing active nonterminal children receive paced canonical inspections; they are not recreated.
- Existing terminal agents remain terminal.
- The new gateway database initializes its own schema and circuit records on demand.
- `running=false` without the completion marker remains nonterminal `UNKNOWN`.

## Rollback

1. Stop the new Terminal MCP process.
2. Preserve the new domain and gateway databases for diagnosis.
3. Restore the pre-cutover live repository.
4. Restore the final pre-cutover domain database backup, including its WAL state when applicable.
5. Remove or move the new gateway database out of the configured path. It is not read by the old coordinator.
6. Restore the previous environment and startup command.
7. Start the old version and verify its MCP endpoint and agent database integrity.

Do not point the old code at a schema-10 domain database as the rollback strategy. Restore the schema-9 backup so rollback is deterministic.

## Incident handling

### 429 / open circuit

Do not restart or delete the database to bypass a circuit. Preserve the durable queue. The gateway waits 120 seconds for the first probe, then uses 6, 12, 24, 48, and 60-minute failed-probe delays before half-open and paced recovery.

### Creation or continuation uncertainty

Do not resend immediately. Preserve operation, request, conversation, and generated-message identities. Reconcile after the configured cooldown. Retry only after canonical evidence proves the prior submission is absent.

### Dashboard unavailable

The dashboard is observational. Agent scheduling remains in SQLite. Check authentication, `/dashboard/agents/api`, process logs, database integrity, and asset routes before changing orchestration state.

### Adapter protocol change

Stop automatic managed-agent synchronization, preserve both databases, and update only the renderer adapter. The gateway scheduler and domain coordinator should not require redesign when the internal ChatGPT client payload changes.
