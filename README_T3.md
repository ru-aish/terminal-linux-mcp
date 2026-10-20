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

T3 owns prompts, streaming, steering, and backend thread reads. Terminal MCP owns task status, tools/processes, and continuation decisions. A task stays working until `thread_task(action="complete", evidence=[...])` is accepted. Active non-process tools still block explicit completion, but intentionally long-lived background processes are detached from thread-idleness accounting when completion is accepted and remain controllable through the process tools. If explicit completion is never called, active processes continue to block inactivity recovery exactly as before. `thread_task(action="wait", reason="...")` pauses automatic recovery. User Stop also pauses recovery without falsely marking the task done.

Tool starts cancel that thread's deadline. The final tool/process exit arms a five-minute deadline. Expiry emits a durable continuation request to T3, which claims the current event, stops an unfinished generation if necessary, and sends one continuation. New activity invalidates stale recovery requests. There are no project/conversation discovery loops, transcript polls, model-health polls, or old watchdog/admission schedules in this flavor. Managed process exits use process wait/pidfd events.

For explicit self-continuation, use `schedule_continue(session_id=..., after_seconds=...)` or `schedule_continue(session_id=..., wake_at=...)`, with exactly one positive delay or absolute Unix timestamp/timezone-aware ISO-8601 time. Use `continue_after_process(process_id=..., session_id=...)` to continue after a managed process exits, regardless of exit code. Both target only the registered session's thread and persist in T3 SQLite across restarts. A due continuation waits until the task is working, unpaused, and has no blocking tools/processes. Tool activity/revision changes do not invalidate it; pause, completion, or a new activation epoch does. An optional nonempty `idempotency_key` returns the same automation for that thread/tool, including after delivery or invalidation; use a new key for a new automation. Each automation produces at most one effective continuation. If tools/processes become active before its event is claimed, that delivery attempt is retired and retried with a new event once idle. If the T3 consumer restarts after claiming but before acknowledging a continuation, it can reclaim the same event ID so provider dispatch deduplication can safely reconcile the delivery. Generic `agent_*` tools remain unavailable.

Private registration can override `inactivitySeconds` (1–3600) for an individual thread; production defaults remain 300. This allows short recovery tests without changing other threads.

Run focused tests using the existing deployment Python environment:

```sh
/home/coder/mcp_workspace/terminal-linux-mcp-gpt-bootstrap-agent-orchestrator/.venv/bin/python -m pytest tests/test_t3_thread_state.py -q
```

Run all T3 tests with `tests/test_t3_*.py` in place of the focused test file.


## Durable T3 commands and 180-second presentation detachment

In the T3 flavor only, ordinary `run_command` launches through
`DurableProcessSupervisor` from the start. Short commands return their real
exit code, stdout and stderr normally. If still running at **180 seconds**, the
MCP response returns the stable `Process ID` and private log paths without
stopping the process. The command's optional `timeout` is a **separate,
supervisor-enforced hard execution limit** and survives an MCP restart. No
hard timeout is assumed when omitted. Supply an `idempotency_key` for retries:
the same thread/key/parameters identify one command forever; a reused key with
different parameters is rejected. Without it, each tool invocation receives a
new identity (automatic replay deduplication across separate requests is not
guaranteed).

The `durable_process_status`, `durable_process_output`,
`list_durable_processes`, and `stop_durable_process` tools also work for
commands detached by `run_command`. Jobs have separately fsynced stdout/stderr
and an atomic `result.json` receipt. A restarted Terminal MCP discovers only
its own job registry, never rescans project processes or re-executes commands.
SQLite `operations` rows are associated with the persistent job ID.
A detached process's terminal result publishes exactly one durable
`process.completed` event (including after the parent task is `done`).
Long native T3 IDs containing percent-encoded separators are preserved without
128-character truncation.

For production, jobs default to independent **systemd user services**. Use
`MCP_T3_DURABLE_LAUNCH_MODE=process` only on hosts/tests without a working
user systemd manager; direct-process jobs cannot survive a service-manager
cgroup kill. `MCP_T3_DURABLE_JOBS_DIR` overrides the private on-disk registry
(default beside the T3 SQLite database). Session environment and per-call
environment overrides are combined using the regular `core._build_env` behavior.
Systemd-launched workers receive the complete intended environment over a
short-lived, same-UID authenticated Unix socket—not systemd command-line
`--setenv` flags or an on-disk secrets file. A lost handoff fails closed and
never launches a command with an incomplete environment. Normal shell output
and session cwd markers are persisted separately in the private job directory;
quick foreground commands restore session cwd when it remains within the
bound workspace. Changing into a new GPT project root can still require
`bootstrap_thread` or `get_thread_context` before the next command.

On explicit hard timeout or stop, the supervisor sends SIGTERM to the complete
child process group, then escalates to SIGKILL after a 0.5-second grace period;
this also handles a shell that exits while a descendant ignores TERM. Its
leader stays unreaped until descendant cleanup has been checked, preventing
process-group-ID reuse. The receipt records whether the group is verified
stopped and distinguishes `cancelled`, `descendants_stopped`,
`stop_not_effective`, and `termination_unverified`. Direct-process mode cannot
contain descendants that deliberately escape their process group with
`setsid` or another cgroup; systemd uses `Delegate=yes` to isolate each
child in a workload cgroup and confirms `populated=0` (with `cgroup.kill`
escalation when necessary) before writing a terminal receipt. The worker stays
outside that subgroup. `KillMode=control-group` for unit
cleanup. Launched commands must stay inside their thread's bound workspace. Never treat `outcome_unknown` (supervisor
crash/reboot before its exit receipt) as success, and never retry execution by
reusing a new key to resolve an uncertain outcome. Workspace checkpoint rollback
must still be coordinated externally with any running job modifying that tree.
The T3 control-plane consumer must separately render/handle the
`process.completed` event if it wants an in-app notification.
