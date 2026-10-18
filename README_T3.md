# T3-specific Terminal MCP

This deployment is an isolated copy of the running Terminal MCP, extended by `t3_terminal_server.py` and `t3_thread_state.py`. The previous service is not modified.

- Public connector: https://t3-terminal.r7code.space/mcp (no connector authentication).
- Public readiness: https://t3-terminal.r7code.space/healthz.
- Usage dashboard: https://t3-terminal.r7code.space/dashboard. It uses the legacy usage-ledger UI and live accounting stream, but omits watchdog/project scanning and agent controls that are not part of the T3 flavor.
- Dashboard authentication: set `MCP_DASHBOARD_TOKEN` (the deployed service reuses `~/.config/terminal-mcp/dashboard.env`) so the public tunnel does not expose thread paths or usage data.
- Private T3 control: http://127.0.0.1:8022/v1; bearer token in `~/.config/terminal-mcp-t3/control-token`.
- State: `~/.local/share/terminal-mcp-t3/state.sqlite`; separate context/usage state in its `gpt` directory.
- Services: `terminal-mcp-t3.service`, `terminal-cloudflared-t3.service` (user units, enabled at login).
- Dedicated tunnel: `109593bc-5d8a-48a5-beaf-ebcf458e1809`; config `~/.cloudflared/t3-terminal-mcp.yml`.

T3 privately registers its permanent thread ID before sending to ChatGPT and finalizes its immutable ChatGPT conversation binding after creation. Every public tool requires a registered ID. The connector is shared and unauthenticated by request: this is an ID allowlist, not a cryptographic assertion of which conversation supplied a known ID, and it is not filesystem isolation.

T3 owns prompts, streaming, steering, and backend thread reads. Terminal MCP owns task status, tools/processes, and continuation decisions. A task stays working until `thread_task(action="complete", evidence=[...])` is accepted with no outstanding operation. `thread_task(action="wait", reason="...")` pauses automatic recovery. User Stop also pauses recovery without falsely marking the task done.

Tool starts cancel that thread's deadline. The final tool/process exit arms a five-minute deadline. Expiry emits a durable continuation request to T3, which claims the current event, stops an unfinished generation if necessary, and sends one continuation. New activity invalidates stale recovery requests. There are no project/conversation discovery loops, transcript polls, model-health polls, or old watchdog/admission schedules in this flavor. Managed process exits use process wait/pidfd events.

For explicit self-continuation, use `schedule_continue(session_id=..., after_seconds=...)` or `schedule_continue(session_id=..., wake_at=...)`, with exactly one positive delay or absolute Unix timestamp/timezone-aware ISO-8601 time. Use `continue_after_process(process_id=..., session_id=...)` to continue after a managed process exits, regardless of exit code. Both target only the registered session's thread and persist in T3 SQLite across restarts. A due continuation waits until the task is working, unpaused, and has no active tools/processes. Tool activity/revision changes do not invalidate it; pause, completion, or a new activation epoch does. An optional nonempty `idempotency_key` returns the same automation for that thread/tool, including after delivery or invalidation; use a new key for a new automation. Each automation produces at most one effective continuation. If tools/processes become active before its event is claimed, that delivery attempt is retired and retried with a new event once idle. If the T3 consumer restarts after claiming but before acknowledging a continuation, it can reclaim the same event ID so provider dispatch deduplication can safely reconcile the delivery. Generic `agent_*` tools remain unavailable.

Private registration can override `inactivitySeconds` (1–3600) for an individual thread; production defaults remain 300. This allows short recovery tests without changing other threads.

Run focused tests using the existing deployment Python environment:

```sh
/home/coder/mcp_workspace/terminal-linux-mcp-gpt-bootstrap-agent-orchestrator/.venv/bin/python -m pytest tests/test_t3_thread_state.py -q
```

Run all T3 tests with `tests/test_t3_*.py` in place of the focused test file.
