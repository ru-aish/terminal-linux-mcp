# Single-Thread Validation Contract

This document is mandatory for all further work on `refactor/three-block-orchestration-core`.

## Isolation

- Work only in `/home/coder/mcp_workspace/terminal-linux-mcp-three-block-core`.
- Never edit the original checkout used by the existing MCP daemon.
- Never restart, stop, reconfigure, deploy into, or otherwise touch the existing MCP daemon or its service.
- Never add service overrides or change its environment.

## Tool and thread discipline

- Use only `terminal_mcp` for terminal work.
- Reuse terminal thread/session ID `live-queued-prompts-status` for every terminal call.
- Earlier `terminal_mcp2` validation sessions were abandoned after connector or execution-worker failures. Do not return to them or create another replacement session.
- Do not create or use sub-agents.
- Codex may be used only when necessary; prefer direct inspection and implementation first.
- Do not use another terminal connector as a fallback.

## Real ChatGPT validation discipline

- Use exactly one dedicated ChatGPT test conversation for the complete live validation sequence.
- Record its conversation ID here before the first real test:
  - Test conversation ID: `UNASSIGNED`
- Do not create repeated conversations for retries.
- If the single test conversation becomes unusable, abandon/delete it once, record the reason, replace the ID above, and use exactly one replacement conversation.
- Never return to an abandoned test conversation.

## Request budget

- Avoid recursive or fixed-interval full scans.
- Reuse locally stored snapshots and cursors.
- Perform one canonical conversation read per due check.
- Fetch deeper context only after a structural change, terminal transition, explicit user request, or suspicious/stale state.
- Do not poll while a retry/cooldown deadline is in the future.
- Do not scan parent conversations unless a due command or notification requires it.
- Completed, failed, cancelled, and waiting-for-parent tasks must not be routinely scanned.
- Record the number and purpose of remote ChatGPT requests for every live validation section.

## Verification order

Validate in the same single ChatGPT test conversation, progressing only after the previous section is evidenced:

1. Conversation Gateway read and canonical normalization.
2. Durable Ledger persistence and restart-safe evidence.
3. State Reducer decisions from the retrieved snapshot.
4. Parent-to-child instruction delivery.
5. Child-to-parent progress and completion notification.
6. Blocking question and parent answer flow.
7. Uncertain-send reconciliation without duplication.
8. App/runtime unavailable and recovery behavior.
9. Minimal-request scheduler behavior and request counts.
10. End-to-end completion in the single test conversation.

## Reporting

- Distinguish automated validation from real ChatGPT validation.
- Never claim a live path was validated unless the single test conversation provides evidence.
- Document anything unsafe or impractical to force rather than creating additional conversations or touching production.
