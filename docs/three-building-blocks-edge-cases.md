# Three-Building-Block Reliability Plan

This document is the implementation contract for the orchestration refactor. The system is intentionally limited to three reusable building blocks:

1. **Conversation Gateway** — external ChatGPT conversation I/O.
2. **Durable Ledger** — persistent actors, monitored task generations, commands, cursors, and delivery evidence.
3. **State Reducer** — a pure deterministic function that converts durable state plus a conversation snapshot into actions.

The watchdog, parent/child workflow, continuation loop, questions, completion notifications, and batching are thin policies composed from these blocks.

## Non-negotiable invariants

- One logical command must never produce two ChatGPT user messages.
- No write may target an unverified or changed canonical parent node.
- A transport/read failure must never be treated as task failure.
- A verified terminal response without the completion marker may be continued.
- A verified terminal response with the completion marker must never be continued.
- An uncertain earlier progress delivery must never block a later final-completion delivery.
- One conversation task generation has one active monitor record.
- Parent/child links and pending commands survive process and app restarts.
- Unknown or contradictory state fails closed: record it and perform no unsafe write.
- Completed tasks leave the active monitoring set after their final result is durably captured.

---

# Block 1 — Conversation Gateway

## Contract

The gateway exposes only external conversation operations:

- create a conversation;
- read and normalize canonical conversation state;
- send a message against an expected canonical parent node;
- reconcile an uncertain send;
- cancel a generation owned by this runtime;
- project discovery used for explicit placement.

It does not understand parent/child policy, continuation markers, task priority, batching, or database ownership.

## Snapshot requirements

A normalized snapshot must include:

- conversation ID and title;
- canonical current node;
- active branch turns;
- all relevant branch message IDs needed for delivery reconciliation;
- running and owned-stream state;
- latest user/assistant role, status, `end_turn`, and text;
- transcript and structural-progress fingerprints;
- state verification result and explicit reason when unverifiable.

## Edge-case checklist

### Runtime and application lifecycle

- [ ] Desktop app already open and internal client ready.
- [ ] Desktop app closed; managed runtime starts it and waits for readiness.
- [ ] App process exists but primary renderer is missing; reopen/warm-start once.
- [ ] App crashes before a read.
- [ ] App crashes during a read.
- [ ] App crashes before message submission.
- [ ] App crashes after submission but before confirmation.
- [ ] App restarts while an owned stream is active.
- [ ] CDP endpoint is unavailable.
- [ ] CDP endpoint accepts connection but JavaScript evaluation fails.
- [ ] Runtime target list contains only auxiliary/avatar/devtools pages.
- [ ] Multiple renderer candidates exist; select the supported main renderer deterministically.
- [ ] Authentication/session is missing or access is denied.

### Conversation reads

- [ ] Conversation exists with a valid active branch.
- [ ] Conversation does not exist.
- [ ] Mapping is missing or malformed.
- [ ] `current_node` is missing.
- [ ] `current_node` points to a missing node.
- [ ] Active branch contains a cycle.
- [ ] Latest message is a user turn.
- [ ] Latest assistant message is running.
- [ ] Latest assistant message is terminal success.
- [ ] Latest assistant message is terminal failure/interruption.
- [ ] Status and `end_turn` contradict each other.
- [ ] Message text is empty but tool activity exists.
- [ ] Same node gains additional streamed text.
- [ ] Conversation branches after a previous read.
- [ ] Active branch changes while preserving the previously sent message on another branch.
- [ ] Very large transcript is bounded without losing current-node verification.
- [ ] Hidden chain-of-thought is excluded while safe progress, recap, tools, and final output remain available.

### Message submission

- [ ] Target is idle and canonical node matches.
- [ ] Target becomes running before send.
- [ ] Canonical node changes before send.
- [ ] Transcript changes during pre-send confirmation.
- [ ] Message submission returns synchronously.
- [ ] Submission returns a promise/stream handle.
- [ ] Stream starts and finishes normally.
- [ ] Stream starts but callback delivery is delayed.
- [ ] Stream exceeds configured timeout.
- [ ] Submission throws before request dispatch.
- [ ] Submission may have reached ChatGPT before transport failure.
- [ ] User message is visible on active branch after send.
- [ ] User message exists only on a non-active branch after branching.
- [ ] Generated user-message ID is absent, but request ID and content evidence exist.
- [ ] Duplicate call with the same logical command is reconciled rather than resent.
- [ ] Existing identical text from another command is not mistaken for this command.

### Cancellation

- [ ] Owned stream exists and cancellation succeeds.
- [ ] No owned stream exists.
- [ ] Cancellation request is sent but result is uncertain.
- [ ] Target becomes terminal before cancellation.
- [ ] Cancellation is not repeated indefinitely.

## Gateway delivery result states

The gateway must distinguish:

- `delivered` — generated message is canonically observed on any relevant branch;
- `not_sent` — precondition failed before submission;
- `deferred_running` — target is active and queue policy applies;
- `target_changed` — expected parent node no longer matches;
- `sent_unconfirmed` — submission occurred but persistence cannot yet be proven;
- `temporarily_unreadable` — runtime/read transport failure;
- `permanent_failure` — validated access/protocol failure that retry cannot solve automatically.

`sent_unconfirmed` is a reconciliation state, not a terminal dead end.

---

# Block 2 — Durable Ledger

## Contract

The ledger is the only source of truth. It stores three record families:

- **Actors** — parent/child identity and conversation binding.
- **Tasks** — one monitored task generation per conversation.
- **Commands** — every intended message, regardless of direction or purpose.

Events and subscriptions may be represented as append-only audit records or derived views, but they must not become separate competing workflow ledgers.

## Required uniqueness

- One actor per stable agent ID.
- One actor per bound conversation ID.
- One parent/child relationship per pair.
- One active task per `(conversation_id, task_generation)`.
- One command per `(source, target, idempotency_key)` when an idempotency key is supplied.
- Ordered command sequence per target conversation.

## Edge-case checklist

### Database lifecycle

- [ ] New empty database creation.
- [ ] Existing database migration from the live schema.
- [ ] Migration interrupted before commit.
- [ ] Process crashes during actor creation.
- [ ] Process crashes during child reservation.
- [ ] Process crashes during task creation.
- [ ] Process crashes after command persistence but before external send.
- [ ] Process crashes after external send but before delivery evidence persistence.
- [ ] WAL recovery after abrupt process termination.
- [ ] Concurrent readers during writes.
- [ ] Concurrent spawn retries with the same idempotency key.
- [ ] Concurrent send retries with the same idempotency key.
- [ ] Database busy/locked timeout is surfaced without corrupting state.
- [ ] Foreign-key enforcement is enabled for every connection.

### Actor and relationship integrity

- [ ] Register a root parent once.
- [ ] Re-register the same parent conversation idempotently.
- [ ] Reject one conversation bound to two different actors.
- [ ] Create a child with correct parent, root, orchestration, project, and working directory.
- [ ] Prevent cross-orchestration messaging.
- [ ] Preserve relationships across restart.
- [ ] Cancelling a parent safely handles descendants and queued child notifications.

### Task monitoring

- [ ] Spawn automatically creates a monitored task.
- [ ] Explicitly monitored ordinary URL creates the same task abstraction.
- [ ] Re-adding a completed conversation creates a new task generation.
- [ ] Duplicate registration updates activity instead of creating another active monitor.
- [ ] Running tasks are queryable as active.
- [ ] Completed/failed/cancelled tasks leave the active set.
- [ ] Historical records remain available without keeping the scheduler awake.
- [ ] Last successful read and last activity are separate timestamps.
- [ ] Structural progress updates the progress cursor even when current node is unchanged.
- [ ] Waiting-for-parent is represented explicitly and is not classified as incomplete stoppage.

### Command lifecycle

- [ ] Queue parent-to-child instruction.
- [ ] Queue child-to-parent question.
- [ ] Queue child completion notification.
- [ ] Queue generic continuation.
- [ ] Queue while target is running.
- [ ] Deliver in target sequence order.
- [ ] Persist `delivery_in_flight` before external send.
- [ ] Recover `delivery_in_flight` after restart as `sent_unconfirmed`.
- [ ] Reconcile uncertain delivery later.
- [ ] Mark definitely absent uncertain command safe for retry only after evidence.
- [ ] Never automatically retry still-ambiguous submission.
- [ ] One uncertain command does not starve unrelated targets.
- [ ] One uncertain progress command does not block later completion for the same child.
- [ ] Completion supersedes or merges unsent progress.
- [ ] Parent instruction outranks generic continuation.
- [ ] Queued instruction is cancelled if target completed before delivery.
- [ ] A manually acknowledged delivery advances the correct cursor.
- [ ] Delivery cursors only advance after confirmed inclusion or explicit acknowledgement.

### Scheduling and leases

- [ ] Multiple scheduler processes cannot claim the same task simultaneously.
- [ ] Expired leases are recoverable after worker crash.
- [ ] Lease renewal does not extend dead work forever.
- [ ] One unreadable conversation does not starve others.
- [ ] Fair ordering prevents one noisy child from monopolizing writes.
- [ ] At most the configured number of external writes occurs per cycle.
- [ ] The scheduler sleeps when no active task, command, or reconciliation exists.

---

# Block 3 — State Reducer

## Contract

The reducer is pure:

```text
reduce(task_state, conversation_snapshot, pending_commands) -> actions
```

It performs no I/O and no database writes. The same input always produces the same ordered action list.

## Reducer state vocabulary

Task states:

- `running`
- `awaiting_assistant`
- `stopped_incomplete`
- `waiting_for_parent`
- `completed`
- `failed`
- `cancelled`
- `stale`
- `unknown`

Actions:

- `record_progress`
- `retry_read`
- `deliver_command`
- `send_continuation`
- `notify_parent`
- `wait_for_parent`
- `mark_completed`
- `mark_failed`
- `mark_stale`
- `no_action`

## Priority order

1. Verified completion.
2. Explicit cancellation/failure handling.
3. Parent instruction or parent answer.
4. Child blocking question / waiting-for-parent.
5. Final completion notification and unsynced final context.
6. Critical update.
7. Generic continuation for verified stopped-incomplete state.
8. Progress batching.
9. No action.

## Edge-case checklist

### Canonical state matrix

- [ ] Running assistant turn -> no write.
- [ ] Owned running stream -> no duplicate write.
- [ ] Latest user turn -> await assistant.
- [ ] Verified terminal success with exact completion marker -> mark completed.
- [ ] Verified terminal failure with exact completion marker -> mark completed only if contract permits completed terminal status; otherwise explicit failure.
- [ ] Marker in old assistant turn -> not complete.
- [ ] Marker in user turn -> not complete.
- [ ] Marker embedded in a sentence -> not complete.
- [ ] Marker on non-terminal assistant turn -> unknown/no write.
- [ ] Verified terminal success without marker -> stopped incomplete, continuation eligible.
- [ ] Verified terminal failure/interruption without marker -> stopped incomplete, continuation eligible.
- [ ] Contradictory status/`end_turn` -> unknown/no write.
- [ ] Missing current node -> unknown/no write.
- [ ] Branch changed during decision -> no write; reread.
- [ ] Runtime/read error -> retry read, never continuation.
- [ ] Stale in-progress turn -> mark stale/blocked, never blindly continue until terminal state is verified.

### Pending command interactions

- [ ] Parent instruction pending while child is running -> queue/defer.
- [ ] Parent instruction pending when child becomes terminal -> deliver instruction before generic continuation.
- [ ] Interrupt instruction -> request cancel once, then deliver after terminal state.
- [ ] Child waiting for parent -> do not send generic continuation.
- [ ] Parent answer pending -> deliver and leave waiting-for-parent.
- [ ] Child completes while parent instruction is queued -> cancel obsolete instruction.
- [ ] Completion occurs with unsent progress -> one final envelope includes unsynced progress/final output.
- [ ] Earlier progress is sent-unconfirmed -> final completion still becomes deliverable.
- [ ] Multiple children complete together -> aggregate by parent without losing per-child cursors.
- [ ] Duplicate completion observation -> no duplicate parent notification.

### Attempts and limits

- [ ] Continuation attempts increment only after actual submission.
- [ ] Unconfirmed submission does not trigger immediate retry.
- [ ] Unchanged transcript after confirmed continuation waits.
- [ ] Transcript change permits a new decision.
- [ ] Maximum continuation attempts produces durable failure/blocked state.
- [ ] Retryable transport errors do not consume continuation attempts.

---

# Integration workflows

## Spawn

1. Gateway creates the child conversation.
2. Ledger atomically binds child actor, relationship, task generation, and monitoring state.
3. Scheduler immediately wakes.

## Parent instruction

1. `agent_send` persists a command with an idempotency key.
2. Reducer decides whether to defer, cancel-then-send, or deliver.
3. Gateway performs one safe send.
4. Ledger records evidence and later reconciliation if needed.

## Child question

1. Child persists a structured question command to parent.
2. Ledger marks the child `waiting_for_parent` when the question is blocking.
3. Reducer prevents generic continuation.
4. Parent answer is another command to the child.

## Child completion

1. Gateway reads a verified terminal marker.
2. Reducer emits `mark_completed` and `notify_parent`.
3. Ledger captures final context before removing the task from active monitoring.
4. Completion notification supersedes unsent progress and remains deliverable even if an earlier progress write is uncertain.
5. Parent delivery is reconciled branch-safely.

---

# Validation gates

The refactor is not complete until all gates pass:

- [x] Pure reducer decision matrix tests.
- [x] Ledger migration, concurrency, crash-recovery, ordering, and idempotency tests.
- [x] Gateway target-selection, app lifecycle, branch-aware confirmation, uncertain reconciliation, and cancellation tests.
- [x] Existing watchdog and agent public-tool behavior remains compatible unless intentionally documented.
- [x] Full test suite passes (146 tests).
- [x] Static syntax/compile checks pass.
- [x] `git diff --check` passes.
- [x] Independent code review finds no high-confidence correctness defects.
- [ ] Live canary: create/read/send/follow-up on a disposable conversation.
- [ ] Live canary: app closed/reopened recovery.
- [ ] Live canary: parent sends a follow-up to a child.
- [ ] Live canary: child completion automatically reaches parent.
- [ ] Live canary: branch change after submission reconciles without duplicate send.

## Implementation verification status

Automated verification completed in the isolated worktree:

- **Conversation Gateway:** read/create/send/cancel classification, target-node changes, active streams, app/runtime unavailable states, JavaScript protocol failures, timeout boundaries, post-dispatch uncertainty, verified branch-wide reconciliation, and invalid-input failures.
- **Durable Ledger:** schema versions 1–7, legacy notification-purpose migration, legacy watchdog JSON import, shared watchdog/agent database coexistence, WAL/foreign-key configuration, crash recovery, send-evidence persistence, strict same-target ordering, idempotency, completion supersession, and active-task removal.
- **State Reducer:** exact completion-marker matrix, terminal failure/incomplete states, contradictory canonical metadata, missing transcripts, stale generation handling, parent-instruction priority, blocking question/answer behavior, and fail-closed read errors.
- **Integrated workflows:** parent-to-child delivery, child questions, parent answers, restart recovery, interrupt cancellation recovery, multi-child fairness, uncertain progress followed by completion, oversized event histories, duplicate-final prevention, terminal-parent handling, and background reconciliation liveness.

The five live canaries below remain intentionally unchecked. They require deploying/restarting this branch against the authenticated desktop runtime; unit and integration tests do not substitute for that deployment validation. The existing live Terminal MCP service was not restarted or modified during this work.

Live tests that require the authenticated desktop runtime must report environmental blockers honestly; they must not be replaced by claims based only on fakes.
