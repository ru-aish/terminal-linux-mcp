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

For explicit self-continuation, use `schedule_continue(session_id=..., after_seconds=...)` or `schedule_continue(session_id=..., wake_at=...)`, with exactly one positive delay or absolute Unix timestamp/timezone-aware ISO-8601 time. Use `continue_after_process(process_id=..., session_id=...)` to continue after a managed process exits, regardless of exit code. Both target only the registered session's thread and persist in T3 SQLite across restarts. A due continuation waits until the task is working, unpaused, and has no blocking tools/processes. Tool activity/revision changes do not invalidate it; pause, completion, or a new activation epoch does. An optional nonempty `idempotency_key` returns the same automation for that thread/tool, including after delivery or invalidation; use a new key for a new automation. Each automation produces at most one effective continuation. If tools/processes become active before its event is claimed, that delivery attempt is retired and retried with a new event once idle. If the T3 consumer restarts after claiming but before acknowledging a continuation, it can reclaim the same event ID so provider dispatch deduplication can safely reconcile the delivery. Legacy Terminal MCP `agent_*` tools remain unavailable; T3 app-owned agent controls are exposed as `agents_available`, `spawn_agent`, `agent_status`, `send_input`, `close_agent`, and `list_agents`. These proxy authenticated T3 orchestration (not Terminal MCP native agent registration) using private per-thread loopback MCP credentials. A completed child resumes its parent through T3.

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


## Versioned runtime releases

Commit every runtime change before deploying. The `T3 runtime contract and
release` GitHub workflow checks tool-event persistence, failures, cancellation,
durable execution, and release integrity before uploading a commit-named archive
and its SHA-256 checksum. The archive contains `RELEASE.json` with the Git commit,
tree, and every source file's hash and executable mode. A successful workflow
builds an artifact; it does not restart a production service.

From a clean checkout, stage a release and generate its systemd drop-in:

```sh
python scripts/t3_release.py stage --repo . --ref HEAD --root /absolute/runtime/releases
python scripts/t3_release.py systemd-unit --repo /absolute/git/repository \
  --release /absolute/runtime/releases/FULL_COMMIT_SHA \
  --python /absolute/runtime/python > /tmp/40-versioned-runtime.conf
```

The staging command rejects tracked or untracked edits and takes files only from
Git objects. Generated files and directories are read-only. The drop-in sets
`PYTHONDONTWRITEBYTECODE=1` and verifies the release against the recorded Git commit
before every start. Verification rejects overlays, missing files, symlinks,
writable source files, changed content, and incorrect provenance. Do not edit a
release or copy local patches into it: commit, test, and stage a new revision.
Keep the Git repository and its referenced commit available for startup checks.
The Python environment must have the dependencies from `requirements.txt`.

After checking that no foreground tools are running, back up the installed
`terminal-mcp-t3.service.d/40-versioned-runtime.conf`, atomically install the
generated drop-in, run `systemctl --user daemon-reload`, and restart only
`terminal-mcp-t3.service`. This interrupts MCP connections; do not restart the
T3 app or the tunnel. Confirm `/healthz` reports the expected `releaseCommit` and
`tool-events-v1` capability, then test a registered thread's tool call and private
event stream. If readiness fails, restore the previous drop-in, reload systemd,
and restart the same service. Keep previous release directories for rollback.

Tool calls persist correlated `tool_started` and `tool_completed` events through
the same private control stream as task state. Tool input and result previews
use the accounting sanitizer and size cap; environment overrides are redacted.
Persistence errors are logged rather than silently discarded.

## Inspect and answer native ChatGPT tool approvals

An empty tool message can contain a pending approval in
`metadata.jit_plugin_data.from_server`, including a `confirm_action` and a pending
`codex_mcp_elicitation`. The public Terminal MCP request has not run at that point.
A T3 permission mode and a ChatGPT connector grant are separate decisions.

`scripts/chatgpt_approval_control.mjs` provides standalone JSON controls through
T3's existing authenticated ChatGPT API client. It does not open browser tabs,
modify T3, restart services, or extract login credentials. Use Node 24 or later
while the normal connection is active:

```sh
node scripts/chatgpt_approval_control.mjs inspect --thread-id T3_THREAD_ID
node scripts/chatgpt_approval_control.mjs respond --thread-id T3_THREAD_ID \
  --message-id PENDING_MESSAGE_ID --connector-id CONNECTOR_ID --link-id LINK_ID \
  --level conversation
node scripts/chatgpt_approval_control.mjs status --thread-id T3_THREAD_ID \
  --message-id PENDING_MESSAGE_ID
node scripts/chatgpt_approval_control.mjs watch --thread-id T3_THREAD_ID
node scripts/chatgpt_approval_control.mjs status --thread-id T3_THREAD_ID
node scripts/chatgpt_approval_control.mjs unwatch --thread-id T3_THREAD_ID
```

`respond` accepts `once`, `conversation`, `always`, or `deny`. Use `inspect` without
responding to keep manual approval. Supply the exact message, connector, and link
IDs returned by inspection; a friendly name does not identify a grant. `fedora`
and `fedora_t3code` are separate connectors. Only levels offered by the pending
request are accepted. Changed requests, authentication challenges, and unknown
approval formats are rejected. Repeated calls in the same renderer return the
existing attempt instead of submitting twice. After a connection failure, inspect
status before retrying; restarting the desktop app clears that local attempt
record. A recorded response confirms submission, not that the resumed task passed.

For programmatic use, import `control` from the module. Its interface is
`control(operation, {conversationId, messageId, connectorId, linkId, level}, endpoint)`;
operations are `inspect`, `respond`, and `status`. The default endpoint is the
existing loopback renderer on port 9234. The CLI can also take a native
`--conversation-id` instead of a T3 thread ID, or override `--cache-dir` and
`--cdp-endpoint`.

These are private native-client APIs, not a documented public ChatGPT approval
endpoint. No general account-wide permission-level setting is assumed. An
`always` response requests the connector-scoped behavior offered by ChatGPT;
verify subsequent calls rather than assuming it applies to other connectors,
accounts, or future API versions. Contract tests check the currently observed
format without making live approvals in CI.

`watch` keeps a watcher in the existing ChatGPT renderer for that one
conversation. It checks the conversation every three seconds and submits the
offered `always_allow` choice when a pending MCP tool approval appears. Set
`--interval-ms` between 1000 and 60000 to tune the check rate. The watcher
continues after the CLI exits while the ChatGPT renderer stays open; use
`unwatch` to stop it. `status` reports the watcher and approved requests. It
does not approve authentication prompts, non-MCP prompts, or approvals in other
conversations. Restarting ChatGPT clears its watchers, so start `watch` again.
