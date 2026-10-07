# Permanent conversation identity

Each observed ChatGPT conversation has one durable Terminal MCP thread ID. New IDs
are UUIDv5 values derived from the conversation ID. SQLite enforces uniqueness of
both sides of the binding. Existing managed conversations retain a legacy ID only
when the gateway supplies the corresponding durable agent ID and that ID has
already completed bootstrap.

The managed creation sequence is:

1. Create one conversation with instructions to wait for identity handoff.
2. Observe the provider's actual conversation ID and create the binding locally.
3. Queue one identity message with a durable idempotency key.
4. Verify creation before dispatching the identity continuation.
5. Bootstrap once with the assigned ID; use it for subsequent tools and context reloads.

A successful initial bootstrap is committed with a conditional SQLite update.
Concurrent requests cannot both consume that bootstrap. Truncated responses do not
consume it and direct the caller to retry bootstrap with a larger context limit.

## Required transport contract

Every external tool call carrying a thread/session ID must have host-owned
`x-codex-turn-metadata` in its MCP request metadata. The identity can be supplied as
`conversation_id`, `chat_id`, or the host's `thread_id`. It must identify the real
calling conversation; it must not come from model-controlled tool arguments.

The server rejects missing metadata and mismatched IDs before executing a tool.
A known thread ID by itself proves no ownership. Metadata marked
`thread_source=terminal_mcp` is a downstream fallback and is not accepted as an
independent conversation identity. Direct server-owned Python calls are internal
and do not constitute an external transport authentication mechanism.

The host must authenticate clients and attach this metadata at a trusted boundary.
If a connector cannot supply conversation identity, strict enforcement is not
supported on that connector. Assigning an ID in the chat prompt alone cannot close
that gap. Do not deploy this change to such a connector until its gateway can
supply the required identity.

## Verification on 2026-10-07

Automated coverage includes gateway creation/handoff idempotency, deterministic
bindings, legacy migration, HTTP bootstrap and context reload, rejection before
command execution, missing metadata, cross-conversation attempts, concurrent
bootstrap commits, and an actual candidate-server stop/start using the same database.

The complete repository suite passed: 255 tests. Compile checks and
`git diff --check` passed. An earlier run had one timing-dependent failure in the
unchanged zombie-process test; that test passed on its own and the final complete
run passed.

The real provider canary used isolated domain/gateway databases and a stable spawn
key. It read the referenced parent conversation once and attempted integrity
preparation twice, at the gateway's heavy-operation spacing. ChatGPT required a
CAPTCHA before completion submission. No `/f/conversation` POST was sent, no new
chat was created, and no identity continuation was sent. The runner was stopped;
its pending local operation was cancelled. Including an earlier direct health
check, this used four ChatGPT backend requests.

This is a blocked provider test, not a live end-to-end pass. Native model calls to
the candidate connector, the real wait/handoff sequence, and provider-level
continuation still require a working authenticated ChatGPT runtime and the trusted
metadata path. The running Terminal MCP service was not upgraded or restarted.
