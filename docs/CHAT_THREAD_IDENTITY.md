# Permanent conversation identity

The server assigns one immutable Terminal MCP ID to each bound ChatGPT conversation.
New IDs are UUIDv5 values derived from the real conversation ID. SQLite makes both
sides unique. Managed legacy chats retain their old agent ID only when durable
records prove that ID already loaded context; model arguments cannot claim it.

## Ordinary chats

An unassigned chat calls `bootstrap_thread(exact_user_message=...)`, copying the
latest user request verbatim. `thread_id` is omitted. An optional `project_id` must
be in the configured discovery scopes. The response includes a server-issued
`discovery_event_id`; retries reuse this token. Tokens cannot change project or cwd.

A cached connector definition may still require the `thread_id` property. In that
case, pass `thread_id=""` with `exact_user_message`; the empty string means
unassigned. An unbound supplied ID is discarded and cannot gain access or become
a persisted identity. Missing message text returns explicit retry guidance.
Registered parents without a chat ID do not represent pending child creation.

1. Look for an exact or sufficiently long prefix match in the durable preview cache.
2. On a cache miss, request desktop listings through the existing paced gateway.
   Listings preserve conversation ID, title, snippet, timestamps, and pagination.
3. Exclude managed agents. Pending managed creations defer assignment until their
   real IDs are known. Import the existing watchdog's project baselines locally.
4. A first complete listing establishes an old-chat baseline. Partial/failed scans
   never establish a baseline or erase cached records. Successful watchdog prefix
   scans can enrich an already initialized cache without extra provider requests.
5. When previews are insufficient, read at most eight new candidates once. Freeze
   the pending bootstrap batch after the last candidate read, then use one signed-in
   `codex exec --model gpt-6-luna` job with a JSON schema in a temporary directory.
   No authentication token is extracted and no custom inference harness is used.
6. Validate matcher IDs, confidence, exact-message evidence, project scope, and
   one-to-one correspondence. Duplicate/very short requests remain unmatched.
   Events arriving during inference remain pending for the next batch.
7. Claim the matched conversation durably, persist its permanent binding, stop
   only that conversation, and confirm it is stopped with the same latest user
   request. Send one identity continuation through the desktop gateway.
8. Verify the identity message canonically. This delivery does not start ongoing
   gateway polling for the ordinary task; normal watchdog policy remains unchanged.

No new eligible chat means no stop or message. An older match returns its saved ID
without a handoff; an older chat with no saved binding is not silently allocated a
replacement ID. No match asks for the exact latest user request or an existing ID.

All ChatGPT listing/read/stop/send operations use `CodexRendererBackend` and the
configured desktop CDP endpoint (`CHAT_GATEWAY_CDP_ENDPOINT`, falling back to
`MCP_CHAT_WATCHDOG_CDP`). They share the gateway's durable request pacing and circuit
handling. The matcher uses the normal Codex harness separately. There is no direct
ChatGPT backend adapter in discovery.

At startup, discovery warms uninitialized baselines. Scans are paginated, bounded
at 20 pages, and complete-list cache reuse has a 60-second cooldown. The tool waits
up to 45 seconds for mapping feedback; longer paced scans return a pending token.
Discovery state survives restarts and HTTP reconnections. An identity submission
has one attempt: a lost response or expired claim never causes automatic resend or
a replacement ID. Uncertain delivery retains the binding for reconciliation.

`MCP_IDENTITY_PROJECT_IDS` overrides selected project scopes; ordinary non-project
chats are also listed. `MCP_CHAT_IDENTITY_DISCOVERY_ENABLED=0` disables discovery.
The background lifecycle is installed for HTTP/SSE; a valid stdio bootstrap starts
its worker lazily. The process must stay alive for a pending handoff to finish.
An explicitly paused gateway remains paused across deployments. Inspect its runtime
circuit and pending operations before resuming; stale identity messages to older
chats should not be replayed merely to recover a new chat.

## Context loading and identity checks

After assignment, **bootstrap is forbidden**, including before the first context
load. Call `get_thread_context(thread_id=assigned_id, cwd=...)` initially and later.
The historical `bootstrap_count` column records the first successful context load.
A conditional SQLite update commits it once; a truncated response does not consume
it and tells the caller to retry `get_thread_context` with a larger limit.

Host-owned `x-codex-turn-metadata` (`conversation_id`, `chat_id`, or `thread_id`)
is authoritative whenever present. A mismatched ID is rejected before a tool runs.
Metadata marked `thread_source=terminal_mcp` is a downstream fallback, not independent
caller identity. Without native metadata, issued `desktop_preview`/`agent_gateway`
IDs are accepted; arbitrary/unbound IDs cannot initialize or execute gated work.

**Preview matching is correlation, not authentication.** Without trusted calling-
conversation metadata the server cannot distinguish the rightful chat from another
caller supplying an already-issued ID. It guarantees durable one-to-one bindings
and rejects unissued IDs, but cannot enforce caller ownership in this fallback.
The host must authenticate clients and attach trusted identity metadata for that
stronger guarantee.

## Verification on 2026-10-08

The complete isolated repository suite passes **277 tests**. A separate 20-round
first-start stress test passed 160 concurrent binding requests. Compile checks and
`git diff --check` are clean. Tests isolate home, databases, and background-service
defaults before imports; the instruction line touched by earlier imports was
restored without changing custom instructions.

Automated tests cover cache/listing discovery, old/no-new matches, duplicate events,
ambiguous messages, managed exclusions, pagination failures, partial-cache reuse,
batch cutoffs, changed user requests, restart recovery, uncertain submissions,
initial context loading, truncation retries, HTTP transport, and native metadata.

A live canary used the existing desktop app at `127.0.0.1:9234`, one new chat, one
identity handoff, and isolated SQLite state. The real model waited, received the ID,
and echoed it. An isolated candidate HTTP client loaded context with that observed
ID, verified bootstrap rejection and wrong-ID blocking, and refreshed context with
one initial-load count. Retrying the discovery event used no extra desktop requests.

There were nine paced desktop operations: create, listing, creation verification,
stop, stopped-chat read, identity continuation, handoff verification, then a final
completion-marker continuation and verification so the normal watchdog leaves the
canary finished. Matching inference used the requested GPT-6 Luna through Codex
and returned a valid structured positive match.

**The live model did not call the candidate connector directly.** Its tools still
point to the unchanged production service. Candidate tool calls were made through
an isolated HTTP driver. A native model-to-candidate test requires routing the live
connector to the reviewed build (and preserving this canary binding there); no live
service was switched or restarted. This is a desktop delivery pass plus an HTTP
integration pass, not a claim of that final connector-level test.

The earlier direct-backend attempt required CAPTCHA and sent no creation request.
It was an unsuitable test route and is not used by this implementation or canary.
