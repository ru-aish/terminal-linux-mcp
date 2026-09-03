# Direct ChatGPT Transport Cutover Design

## Purpose

Replace every automatic dependency on the Codex/ChatGPT desktop GUI, Electron
renderer, webview port 5175, and CDP port 9222 with a lightweight direct
ChatGPT transport. Terminal MCP must continue to operate across suspend/resume,
temporary power loss, DNS failures, network switching, token expiry, helper
process death, and long offline periods without duplicating mutations or
opening the desktop application.

The direct transport targets normal ChatGPT UI conversations identified by
ordinary UUIDs. It does not substitute the official API Conversations resource,
whose identifiers and storage are separate.

## Decisions

1. The direct transport is the default for the gateway, watchdog, agent
   coordinator, and runtime tools.
2. The desktop GUI is never launched automatically for authentication,
   recovery, health checks, or conversation operations.
3. Normal authentication reads the existing mode-0600 Codex credential file at
   `~/.codex/auth.json` and keeps the access token only in process memory.
4. `codex app-server` is an on-demand refresh helper, not a resident service. It
   is spawned only when the cached/stored token is near expiry, a request
   returns `401`, or the credential file lacks a usable access token. It is
   terminated immediately after the refresh response.
5. A completely invalid login is reported as `login_required`. Terminal MCP
   does not open either desktop application. The operator performs a one-time
   Codex login separately.
6. The renderer implementation remains available only through an explicit
   emergency configuration override. There is no automatic renderer fallback.
7. The private backend and Sentinel implementation are version-sensitive.
   Compatibility failures fail closed and surface actionable health data.

## Architecture

```text
Terminal MCP Python services
  |
  +-- DirectChatGPTClient / DirectChatGPTBackend
        |
        +-- persistent-on-demand Node JSON-lines worker
              |
              +-- AuthManager
              |     +-- in-memory token cache
              |     +-- ~/.codex/auth.json
              |     +-- short-lived codex app-server refresh fallback
              |
              +-- SentinelManager
              |     +-- requirements key
              |     +-- proof of work
              |     +-- exact VM implementation
              |
              +-- ChatGPT backend HTTP transport
                    +-- read/list/project operations
                    +-- create/continue/cancel/delete operations
```

The Python parent owns the worker lifecycle. It starts one worker on the first
request, multiplexes requests by opaque request ID, and shuts it down after a
configurable idle interval. The worker is shared by the gateway, watchdog, and
agent coordinator within one Terminal MCP process. Secrets never appear in
command-line arguments, environment variables, logs, or JSON responses to MCP
clients.

## Components

### Node direct-transport worker

The worker receives JSON-lines requests on stdin and writes JSON-lines results
on stdout. Protocol messages contain `id`, `method`, and `params`; results
contain the same `id` and either `result` or a structured `error`. Supported
methods are:

- `health`
- `models`
- `list_projects`
- `get_project`
- `list_project_threads`
- `create_thread`
- `get_thread`
- `continue_thread`
- `cancel_thread`
- `delete_thread`
- `shutdown`

The worker serializes Sentinel preparation and write submission because the
first-party VM has shared execution state. Independent reads may run
concurrently within a small fixed limit. It never binds a listening socket.

### Authentication manager

The manager validates the credential file owner and rejects files writable by
group or others. It decodes only the JWT payload needed for expiry and account
selection; it does not treat unverified claims as authorization beyond sending
the signed token back to ChatGPT.

Token selection follows this sequence:

1. Reuse the in-memory token when its JWT expiry is more than five minutes away.
2. Reload `~/.codex/auth.json` when its modification time changes or the memory
   token is near expiry.
3. If the stored token is still unusable, enter a single-flight refresh lock and
   spawn `codex app-server`.
4. Send `initialize`, then `getAuthStatus` with token refresh enabled.
5. Validate the returned token and account ID, cache it in memory, and terminate
   the helper.
6. On an HTTP `401`, invalidate the cache, perform one forced refresh, and retry
   the request exactly once.

Concurrent requests wait for the same refresh promise, so a reconnect storm
cannot spawn multiple auth helpers. Refresh has a bounded startup and response
timeout; timeout or malformed output kills the helper and returns a redacted
authentication error.

### Python adapter boundary

`DirectChatGPTBackend` implements the existing `BackendAdapter` protocol and
returns the existing `MutationResult`, `ThreadSnapshot`, and `TurnSnapshot`
models. `DirectChatGPTClient` implements the async context-manager interface
currently consumed by `ConversationGateway`, preserving watchdog and agent
coordinator policy while replacing their transport.

The Python worker manager detects EOF, invalid JSON, request timeout, and child
exit. It restarts the worker automatically for read-only requests. Mutation
failures after submission begins are returned as `sent_unconfirmed`; callers
reconcile by message ID before another send rather than replaying blindly.

## Network, Suspend, and Power Behavior

### Connection state machine

The transport exposes these states:

- `idle`: no active operation; worker may be stopped after its idle timeout.
- `ready`: credential and compatibility health checks passed.
- `offline`: DNS, route, connection, or TLS establishment failed.
- `backoff`: waiting before a retry.
- `auth_refresh`: one refresh helper is active.
- `degraded`: private protocol or Sentinel compatibility check failed.
- `login_required`: no refreshable Codex login exists.
- `paused`: explicitly stopped through runtime control.

### Retry policy

DNS failures, connection resets, unreachable routes, TLS handshake interruption,
and HTTP `408`, `425`, `429`, `500`, `502`, `503`, and `504` are retryable.
Backoff uses full jitter with delays bounded from one second to five minutes and
honors `Retry-After` when present. Non-retryable 4xx responses fail immediately,
except one `401` refresh attempt.

Read-only requests can retry automatically. Writes are retried only before any
response headers are received. Once submission may have reached ChatGPT, the
operation becomes `sent_unconfirmed` and is reconciled through canonical thread
state and the stable user message ID.

### Suspend and resume

Deadlines use a monotonic clock. After a long monotonic jump, the worker discards
stale keep-alive assumptions, invalidates expired tokens, and runs a lightweight
health probe before releasing queued work. Scheduled watchdog work is coalesced:
resume performs one scan rather than replaying every missed interval.

No retry loop runs while there is no queued work. This prevents battery drain
during offline or low-power periods. A network failure opens the existing
gateway circuit, so repeated watchdog and agent requests do not create a retry
storm.

### Worker or parent process death

The Python parent lazily respawns the Node worker. The durable SQLite gateway
ledger remains the source of truth for queued work, claims, retries, and
idempotency. On restart, unfinished mutations are reconciled before dispatch.
The auth helper is always a child of the Node worker and receives a bounded
termination sequence if the worker exits.

## Runtime Tool Semantics

Existing MCP tool names remain stable, but their meaning changes:

- `chat_runtime_health` reports worker, credential, network, Sentinel, and
  backend health; it no longer probes webview/CDP.
- `chat_runtime_status` reports worker PID/state, auth expiry distance, last
  network success/failure, backoff, and gateway circuits. It never returns token
  or account values.
- `chat_runtime_control(start)` clears the inhibit flag and performs lazy direct
  transport initialization.
- `chat_runtime_control(stop)` pauses requests and terminates the Node worker.
- `chat_runtime_control(restart)` replaces the Node worker without launching a
  GUI.
- `chat_runtime_control(recover)` resets stale network connections, refreshes
  authentication only if necessary, and probes the backend.
- `chat_runtime_logs` reads bounded, redacted direct-transport logs.

The obsolete desktop runtime supervisor service is disabled during deployment.
Installation templates and environment examples stop enabling or referencing
the GUI runtime by default.

## Compatibility and Rollback

`MCP_CHAT_TRANSPORT=direct` is the default. Setting
`MCP_CHAT_TRANSPORT=renderer` explicitly selects the legacy implementation for
manual emergency rollback. Direct transport never changes this setting or falls
back on its own.

The Sentinel VM source carries a compatibility fingerprint derived from the
installed first-party bundle version. Health reports `degraded` if response
shape or VM execution becomes incompatible. Updating the VM requires a new
fixture and live disposable smoke validation.

## Testing

Automated tests cover:

1. Credential permission validation, JWT expiry handling, file reload, refresh
   single-flight, helper timeout, and secret redaction.
2. JSON-lines framing, request multiplexing, worker exit, lazy restart, idle
   shutdown, and cancellation.
3. Retry classification, full-jitter bounds, `Retry-After`, offline recovery,
   monotonic suspend jumps, and coalesced watchdog scans.
4. Mutation uncertainty: pre-submit retry, post-submit reconciliation, stable
   message IDs, and duplicate prevention.
5. Every `BackendAdapter` and `ConversationGateway` operation against a fake
   HTTP server.
6. Watchdog and Terminal MCP composition proving that direct mode never calls a
   desktop launcher, CDP endpoint, or webview endpoint.
7. Runtime tool response compatibility and complete secret redaction.
8. A gated live smoke test that creates, reads, continues, cancels when
   applicable, and deletes a disposable ChatGPT thread.

## Rollout and Success Criteria

The cutover is complete when:

- all unit and integration tests pass;
- direct mode is the installed default;
- the desktop runtime service is disabled and remains stopped during a live
  create/read/continue/delete cycle;
- no process connects to ports 5175 or 9222 during that cycle;
- the auth helper is absent when a valid stored token is available;
- forcing token refresh creates exactly one temporary helper and leaves none
  running afterward;
- offline-to-online and suspend/resume simulations recover without duplicate
  messages;
- unrelated existing ledger changes remain untouched.
