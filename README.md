# Terminal GPT Experimental MCP

An isolated experimental copy of the Terminal Linux MCP that bootstraps every model thread with GPT-specific instructions, skills, tool manifests, downstream MCP discovery, and persistent token-usage accounting.

The server can run locally over stdio, SSE, or Streamable HTTP. `start.sh` starts the Streamable HTTP server and an ngrok tunnel together, prints the final MCP endpoint, and shuts both processes down cleanly.

> [!CAUTION]
> This server can execute shell commands and modify files on the host. Treat it like remote shell access. Run it as a dedicated low-privilege user, require authentication, keep tool approvals enabled, and never expose it anonymously on the public internet.

## Highlights

- Native command execution with per-session working directories and environment variables.
- Foreground and background process management.
- Direct file read/write/edit/copy/move operations.
- Native local image inspection through `watch_image`, returning MCP `ImageContent` instead of a text path or JSON-wrapped base64 blob.
- Optional persistent per-thread goals with fixed finish conditions, evidence-gated completion, and 15-minute reminder injection.
- Mandatory per-thread bootstrap gate using `~/.GPT/AGENTS.md` and project `.GPT/AGENTS.md` files; Codex `~/.codex/AGENTS.md` is intentionally ignored.
- MCP initialization instructions include the GPT rules, every discoverable skill, every public Terminal tool, and configured nested MCP server names.
- SQLite tracking for host-reported model input/output/cached-input tokens plus separately labeled server estimates for returned bootstrap context.
- A mobile-first live usage ledger at `/dashboard`, mounted in the same HTTP process and updated through Server-Sent Events.
- Discovery of local `SKILL.md` files.
- Discovery and proxying of configured MCP servers.
- Persistent downstream MCP connections keyed by `(session_id, server_name)`.
- Per-server serialization, one reconnect attempt after a crashed downstream server, idle cleanup, and explicit reset actions.
- Exclusive ownership for configured browser profile directories, preventing two owners from opening the same profile concurrently.
- Optional bearer authentication for HTTP transports.
- Safe launcher that refuses occupied ports and refuses an unauthenticated public tunnel unless explicitly overridden.

## Thread bootstrap architecture

This branch is designed to run beside the normal Terminal MCP rather than replace it. Its defaults are isolated:

```text
Normal Terminal MCP       Experimental Terminal GPT MCP
port 8000                 port 8011
~/.codex/AGENTS.md        ~/.GPT/AGENTS.md
normal server process     separate worktree/process
```

On MCP initialization, the server sends a startup instruction document containing:

1. The full global `~/.GPT/AGENTS.md` instruction content.
2. Every discovered `SKILL.md` name, description, source, and path.
3. Every public Terminal GPT tool and description.
4. Every configured nested MCP server name and public endpoint summary.
5. The recovery and token-accounting tools.

Project-specific `.GPT/AGENTS.md` files are intentionally not injected until `bootstrap_thread` receives the target `cwd`, preventing one project’s rules from leaking into another project’s initialization. The global startup payload is rebuilt for every MCP initialization, so changes to global rules or discovered skills are visible to newly initialized clients without restarting the server.

A model thread must then call:

```json
{
  "thread_id": "a-unique-stable-id-for-this-model-thread",
  "cwd": "/absolute/project/path"
}
```

through `bootstrap_thread`. The same value must be reused as `session_id` for later tools. The shared value `default` is rejected for gated execution or modification, preventing one chat from inheriting another chat's loaded-context fingerprint.

If the model loses context after compaction or a long conversation, it calls `get_thread_context`. If any applicable `.GPT/AGENTS.md` changes, the fingerprint gate blocks further gated work until the thread reloads its context.

### GPT instruction hierarchy

The experimental server never reads Codex's instruction tree. It resolves GPT instructions in this order:

```text
~/.GPT/AGENTS.override.md       # overrides ~/.GPT/AGENTS.md
~/.GPT/AGENTS.md
<repo>/.GPT/AGENTS.override.md  # overrides that directory's AGENTS.md
<repo>/.GPT/AGENTS.md
<repo>/<subdir>/.GPT/AGENTS.md
...
```

`MCP_GPT_HOME` changes the global GPT directory. The default is `~/.GPT`.

### Persistent thread goals

`thread_goal` stores one optional goal per bootstrapped thread. A goal contains an objective and fixed finish conditions; it remains active until `complete` receives exactly one non-empty evidence entry per condition, or `technical_error` records a genuine blocking failure. `resume` reactivates a technical-error goal and `clear` removes it.

When the goal has not been returned to the model for `MCP_GOAL_REMINDER_SECONDS` (15 minutes by default), the first later tool result atomically receives a compact `<goal_context>` block. The reminder is appended without flattening native image content or changing an existing MCP error flag. `bootstrap_thread`, `get_thread_context`, and `thread_goal(action="get")` also return the goal and reset the reminder timer.

```text
thread_goal(
    action="set",
    session_id="the-bootstrapped-thread-id",
    objective="Finish and verify the requested implementation",
    finish_conditions=[
        "The requested behavior exists",
        "Relevant success and failure tests pass",
    ],
)
```

The server cannot start a new model turn by itself. It persists and re-injects the goal so a compatible host or the next user/model turn can continue from the full objective instead of treating partial progress as completion.

### Native image inspection

`watch_image` accepts an absolute or working-directory-relative local path and returns the original image bytes as native MCP `ImageContent`. This lets an MCP-capable model receive the image as visual input instead of receiving only the filename or a JSON string.

Supported formats are PNG, JPEG, WEBP, and non-animated GIF. The server validates the file signature rather than trusting the extension, rejects malformed or animated GIFs, and reads at most `MCP_WATCH_IMAGE_MAX_BYTES` bytes. The default limit is 20 MiB before base64 expansion.

```text
watch_image(
    path="/absolute/path/to/screenshot.png",
    session_id="the-bootstrapped-thread-id",
    cwd="/absolute/project/path",
)
```

The MCP image content carries base64 data plus its MIME type. It preserves the original file bytes; image-detail selection remains a responsibility of the MCP host when it maps the content into a model request.

### Token usage database

The database is stored at:

```text
~/.GPT/thread_usage.db
```

It stores a thread record and an append-only usage event stream with:

- input tokens
- output tokens
- cached input tokens
- model and provider request ID
- exact-versus-estimated classification
- context fingerprints and bootstrap counts

`record_token_usage` is for exact usage reported by the model host/provider. An MCP server cannot independently observe the ChatGPT host's complete model prompt or response, so exact accounting requires the wrapper/runtime to report those values.

The proxy additionally records a clearly separate `proxy_estimate` event for every non-default thread tool call. It uses `tiktoken`'s `o200k_base` encoding: the tool-call name and JSON arguments are estimated model output, while the textual tool result is estimated model input available on the next turn. It deliberately excludes image/audio payloads and cannot calculate provider prompt-cache hits. Bootstrap text remains a `server_estimate` input event. These estimates must not be added to exact provider totals, because they describe overlapping portions of the same model turns.

`get_token_usage` returns global or per-thread totals, grouped thread summaries, and recent events. Reusing the same non-empty provider request ID is idempotent and does not double-count a retried report.

### Live usage dashboard

For SSE or Streamable HTTP transports, the same server process exposes:

```text
/dashboard          interactive usage ledger
/dashboard/api      current JSON snapshot
/dashboard/events   live Server-Sent Events stream
```

The dashboard shows exact provider-reported tokens, proxy-estimated MCP text, tool-call counts and ranking, active/recent threads, time-window charts, and the newest accounting events. Exact and estimated figures stay visually and numerically separate because they can overlap.

Set `MCP_DASHBOARD_TOKEN` to require the dashboard login form. The MCP bearer middleware deliberately leaves `/dashboard` to this cookie-based browser flow; `/mcp` continues to use `MCP_BEARER_TOKEN` independently. When the dashboard token is unset, the dashboard inherits the reachability of the HTTP server and any tunnel in front of it, so do not expose it publicly without another access policy.

## Requirements

- Linux
- Optional agent CLIs used by delegated tools: Codex and/or Antigravity

`store.sh` installs the remaining prerequisites. It supports Debian/Ubuntu,
Fedora/RHEL, Arch, openSUSE, and Alpine package managers. When the operating
system does not provide Python 3.11 or newer, it installs `uv` in user space and
uses a managed Python instead.

## Quick start

```bash
git clone https://github.com/ru-aish/terminal-linux-mcp.git
cd terminal-linux-mcp
./store.sh --with-ngrok
cp .env.example .env
```

`install.sh` is a conventional alias for the same installer:

```bash
./install.sh --with-ngrok
```

Useful installer modes:

```bash
./store.sh                         # runtime dependencies only
./store.sh --dev --test            # development dependencies and test suite
./store.sh --skip-system-packages  # user-space setup without sudo/doas
./store.sh --check                 # readiness report without modifications
```

For a local-only server, ngrok is not required:

```bash
./store.sh
./start.sh --local-only
```

`setup.sh` is the lower-level, idempotent Python environment installer. It can
repair an existing `.venv`, or rebuild it with backup-and-rollback protection:

```bash
./setup.sh
./setup.sh --recreate
./setup.sh --dev --test
```

Generate a strong bearer token:

```bash
openssl rand -hex 32
```

Put the generated value in `.env`:

```dotenv
MCP_BEARER_TOKEN=replace-with-the-generated-value
```

Authenticate ngrok once:

```bash
ngrok config add-authtoken YOUR_NGROK_AUTHTOKEN
```

Start the MCP server and tunnel:

```bash
./start.sh
```

The launcher prints both endpoints:

```text
Local MCP endpoint: http://127.0.0.1:8011/mcp
Public MCP endpoint: https://example.ngrok.app/mcp
Authentication: Authorization: Bearer <MCP_BEARER_TOKEN>
```

`Ctrl+C` stops the server and the tunnel.

### Local-only mode

```bash
./start.sh --local-only
# Equivalent: MCP_SKIP_NGROK=1 ./start.sh
```

### Verify the endpoint

```bash
MCP_BEARER_TOKEN='your-token' \
  python scripts/smoke_test.py https://example.ngrok.app/mcp
```

## OpenAI setup

OpenAI products connect to a **remote** MCP URL; ChatGPT does not connect directly to a local MCP process. The ngrok URL printed by `start.sh` is a remote Streamable HTTP endpoint. OpenAI also provides a Secure MCP Tunnel for private or on-premises servers.

Official references:

- ChatGPT developer mode and MCP apps: <https://help.openai.com/en/articles/12584461-developer-mode-and-full-mcp-connectors-in-chatgpt-beta>
- MCP and Connectors in the Responses API: <https://developers.openai.com/api/docs/guides/tools-connectors-mcp>
- Secure MCP Tunnel: <https://developers.openai.com/api/docs/guides/secure-mcp-tunnels>

### ChatGPT custom app

Plan availability and the UI can change. Follow the current OpenAI documentation for your workspace. As of July 2026, OpenAI documents full MCP support in ChatGPT for Business, Enterprise, and Edu workspaces, while Pro developer mode is limited to read/fetch capabilities. The current setup flow is:

1. Enable developer mode for your account or workspace.
2. Open **Settings → Apps → Create**, or the equivalent workspace-admin path.
3. Enter the public endpoint printed by `start.sh`, including `/mcp`.
4. Configure authentication.
5. Select **Scan Tools** and review every discovered tool.
6. Create the draft app and test it in a new chat.

This repository supports a static bearer token. The Responses API can pass that token directly. ChatGPT custom apps commonly use OAuth for authenticated servers; place this MCP behind an OAuth-capable gateway or use OpenAI's Secure MCP Tunnel when the ChatGPT setup does not offer a static bearer-token option.

Do not select unauthenticated access for a public terminal endpoint. The launcher requires one of these before starting ngrok:

- `MCP_BEARER_TOKEN`
- `NGROK_TRAFFIC_POLICY_FILE`
- the explicit and dangerous `MCP_ALLOW_UNAUTHENTICATED_PUBLIC=1` override

### Responses API

The Responses API supports remote MCP servers over Streamable HTTP or HTTP/SSE. Pass the MCP URL as `server_url` and the terminal bearer token as `authorization`.

```python
import os
from openai import OpenAI

client = OpenAI()

response = client.responses.create(
    model=os.environ.get("OPENAI_MODEL", "gpt-5.6"),
    input="List the files in the configured workspace.",
    tools=[
        {
            "type": "mcp",
            "server_label": "terminal_linux",
            "server_description": "A controlled Linux development terminal.",
            "server_url": os.environ["TERMINAL_MCP_URL"],
            "authorization": os.environ["MCP_BEARER_TOKEN"],
            "require_approval": "always",
            "allowed_tools": [
                "bootstrap_thread",
                "get_thread_context",
                "context_manifest",
                "thread_goal",
                "record_token_usage",
                "get_token_usage",
                "list_dir",
                "stat_path",
                "read_file",
                "watch_image",
            ],
        }
    ],
)

print(response.output_text)
```

OpenAI does not store the MCP `authorization` value in the Response object, so provide it on every Responses API request that uses the server.

## Public tools

| Area | Tools |
| --- | --- |
| Thread bootstrap and recovery | `bootstrap_thread`, `get_thread_context`, `context_manifest`, `refresh_startup_context`, `project_context` |
| Persistent goal | `thread_goal` |
| Token accounting | `record_token_usage`, `get_token_usage` |
| Skills and MCP discovery | `local_skills`, `local_mcp` |
| Commands | `run_command`, `start_process`, `poll_process`, `stop_process` |
| Session environment | `set_session_env` |
| Files and images | `read_file`, `watch_image`, `write_file`, `replace_in_file`, `apply_patch`, `list_dir`, `stat_path`, `make_dir`, `copy_path`, `move_path` |
| Delegated agents | `run_codex_yolo`, `start_codex_yolo`, `run_agy_yolo`, `start_agy_yolo` |
| ChatGPT agent orchestration | `agent_projects_list`, `agent_project_get`, `agent_project_threads`, `agent_register_parent`, `agent_spawn`, `agent_status`, `agent_context`, `agent_tail`, `agent_send`, `agent_schedule_wakeup`, `agent_queue_after_completion`, `agent_automation`, `agent_wait`, `agent_sync`, `agent_children`, `agent_subscribe`, `agent_ack`, `agent_cancel` |

## Persistent downstream MCP proxy

`local_mcp` keeps downstream MCP transports alive instead of spawning one child process per tool call. This is essential for stateful servers such as browsers.

A connection is identified by:

```text
(session_id, server_name)
```

Use the same `session_id` for a sequence that must share state:

```text
local_mcp(action="call", server="stealth-browser", tool="browser_navigate", session_id="research")
local_mcp(action="call", server="stealth-browser", tool="browser_type", session_id="research")
```

Supported actions:

- `list`: list configured servers and show `connected` for the current terminal session.
- `tools`: inspect a downstream server's tool schemas.
- `call`: invoke a downstream tool.
- `reset`: close one cached downstream server for the current `session_id`.
- `reset-all`: close every cached downstream server for the current `session_id`.

Behavior:

- Calls to the same cached server are serialized.
- A failed child connection is discarded and reconnected once.
- Changed MCP configuration causes a fresh connection.
- Idle connections close after `MCP_PROXY_IDLE_TIMEOUT` seconds; `0` disables idle cleanup.
- Environment values such as `SAB_USER_DATA_DIR`, `BROWSER_PROFILE_DIR`, and similar profile-path settings are treated as exclusive resources. Another `session_id` cannot claim the same profile while it is active.
- Server shutdown closes cached downstream clients and their child processes.

The persistent proxy preserves live tabs, DOM state, JavaScript state, and aria references while the downstream browser process stays alive. Cookie persistence across a process restart still depends on whether the downstream browser MCP genuinely uses a persistent browser context.

## Downstream MCP configuration

The server discovers MCP configuration from:

- `~/.codex/config.toml`
- `~/.gemini/config/mcp_config.json`
- `~/.config/Claude/claude_desktop_config.json`
- `<project>/.codex/config.toml`
- `<project>/.mcp.json`
- `<project>/mcp.json`
- `<project>/.vscode/mcp.json`

Example project `.mcp.json`:

```json
{
  "mcpServers": {
    "stealth-browser": {
      "command": "node",
      "args": ["/absolute/path/to/stealth-agent-browser-mcp/dist/index.js"],
      "env": {
        "SAB_HEADLESS": "1",
        "SAB_USER_DATA_DIR": "/absolute/path/to/browser-profile"
      }
    }
  }
}
```

Secrets in downstream MCP configuration are used to launch the child but are not included in `local_mcp(action="list")` output.

## Configuration

Copy `.env.example` to `.env`. Important values:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MCP_HOST` | `127.0.0.1` | HTTP listener address |
| `MCP_PORT` | `8000` | HTTP listener port |
| `MCP_TRANSPORT` | `streamable-http` | `stdio`, `sse`, or `streamable-http` |
| `MCP_WORKSPACE` | `~/mcp_workspace` | Default terminal workspace |
| `MCP_LOG_DIR` | `.run/logs` through `start.sh` | Runtime logs |
| `MCP_BEARER_TOKEN` | empty | Required bearer token for HTTP requests |
| `MCP_PROXY_IDLE_TIMEOUT` | `1800` | Downstream MCP idle timeout in seconds |
| `MCP_SKIP_NGROK` | `0` | Set `1` for local-only mode |
| `MCP_AUTO_SETUP` | `1` | Run `setup.sh` automatically when the Python runtime is missing or incomplete |
| `MCP_AUTO_INSTALL_NGROK` | `0` | Let `start.sh` invoke `store.sh --with-ngrok` when ngrok is missing |
| `MCP_VENV_DIR` | `.venv` | Override the virtual-environment directory |
| `NGROK_URL` | random URL | Reserved ngrok URL/domain |
| `NGROK_TRAFFIC_POLICY_FILE` | empty | Optional ngrok Traffic Policy |
| `MCP_ALLOW_UNAUTHENTICATED_PUBLIC` | `0` | Dangerous public-tunnel override |
| `MCP_CHAT_WATCHDOG_ENABLED` | `1` | Enable the five-minute ChatGPT task watchdog for SSE/Streamable HTTP |
| `MCP_CHAT_WATCHDOG_ADAPTER` | `codex-internal` | Use the Codex desktop app's internal normal-chat client; no UI fallback is enabled |
| `MCP_CHAT_WATCHDOG_QUEUE` | `~/.GPT/chat-watchdog/threads.txt` | Editable one-active-task-URL-per-line input queue; task state is reconciled into the durable ledger |
| `MCP_CHAT_WATCHDOG_STATE` | `~/.GPT/chat-watchdog/state.json` | Compatibility mirror and one-time import source for watchdog task state |
| `MCP_CHAT_WATCHDOG_COMPLETED` | `~/.GPT/chat-watchdog/completed.jsonl` | Append-only task completion history |
| `MCP_CHAT_WATCHDOG_PROJECTS` | `~/.GPT/chat-watchdog/projects.json` | Selected Project allowlist plus baselined/seen conversation IDs for new-thread discovery |
| `MCP_CHAT_WATCHDOG_CDP` | `http://127.0.0.1:9222` | Codex desktop CDP endpoint |
| `MCP_CHAT_WATCHDOG_INTERVAL_SECONDS` | `300` | Scheduled scan interval |
| `MCP_CHAT_WATCHDOG_RETRY_SECONDS` | `300` | Duplicate-send cooldown |
| `MCP_CHAT_WATCHDOG_DRY_RUN` | `0` | Inspect and report without sending |
| `MCP_CHAT_WATCHDOG_AUTO_START_APP` | `1` | Start the desktop host when offline, or reopen its primary renderer when CDP is up but the internal client is not ready |
| `MCP_CHAT_WATCHDOG_APP_COMMAND` | `/usr/bin/codex-desktop` | Installed Codex desktop launch command |
| `MCP_CHAT_WATCHDOG_APP_START_TIMEOUT_SECONDS` | `30` | Maximum time to wait for a usable primary renderer after starting or reopening the desktop host |
| `MCP_CHAT_WATCHDOG_INTERNAL_TIMEOUT_SECONDS` | `10` | Timeout for CDP attachment and internal reads |
| `MCP_CHAT_WATCHDOG_REQUIRE_HIGH` | `1` | Refuse to silently lower the requested reasoning level |
| `MCP_CHAT_WATCHDOG_MODEL` | empty | Preferred live model slug; otherwise reuse the thread model or live default |
| `MCP_CHAT_WATCHDOG_THINKING_EFFORT` | `extended` | Requested live reasoning effort |
| `CHAT_GATEWAY_THINKING_EFFORT` | falls back to watchdog effort | Reasoning effort attached to every durable agent create/continue request |
| `CHAT_GATEWAY_REQUIRE_HIGH_REASONING` | falls back to watchdog requirement | Accept only high or extended effort for durable agent requests |
| `MCP_CHAT_WATCHDOG_STREAM_TIMEOUT_SECONDS` | `3600` | Maximum time to wait for a watchdog-owned completion stream |
| `MCP_CHAT_WATCHDOG_STALE_GENERATION_SECONDS` | `600` | Age after which an in-progress canonical state is reported as stale and never auto-continued |
| `MCP_CHAT_WATCHDOG_MAX_CONTINUE_ATTEMPTS` | `20` | Hard per-task continuation-attempt ceiling |
| `MCP_CHAT_WATCHDOG_PRE_SEND_CONFIRM_SECONDS` | `1.25` | Delay before the canonical pre-send refetch |
| `MCP_CHAT_AGENT_ENABLED` | `1` | Enable the restart-safe parent/child orchestration sync loop for SSE/Streamable HTTP |
| `MCP_CHAT_AGENT_DB` | `~/.GPT/chat-agent-orchestrator.db` | Shared SQLite ledger for watchdog task generations plus agent registry, tasks, commands, events, subscriptions, and cursors |
| `MCP_CHAT_AGENT_SYNC_SECONDS` | `15` | Background orchestration sync interval |
| `MCP_CHAT_AGENT_CONTEXT_MAX_EVENTS` | `60` | Maximum compact execution events returned or journaled per delta |
| `MCP_CHAT_AGENT_CONTEXT_MAX_CHARS` | `12000` | Maximum serialized compact execution context |
| `MCP_CHAT_AGENT_TAIL_LINES` | `60` | Default recent visible/tool-summary lines |
| `MCP_CHAT_AGENT_TAIL_MAX_CHARS` | `12000` | Maximum recent-tail text size |
| `MCP_CHAT_AGENT_MAX_CONTINUE_ATTEMPTS` | `20` | Hard ceiling for automatic child-task continuation attempts |
| `MCP_CHAT_AGENT_STALE_SECONDS` | `600` | Unchanged canonical-node age before an active/waiting child is marked stale and its parent is notified |

The launcher refuses to start when the selected port is already occupied. It never kills or replaces an existing service.

### ChatGPT task watchdog

When enabled, the SSE and Streamable HTTP app scans every five minutes. Each URL line is one active task instance in an ordinary saved ChatGPT conversation, not a permanent thread watch or Work task. Completion removes that task URL; adding the same URL again creates a fresh task ID and increments its task generation, resetting hashes, cooldowns, attempts, completion state, and user-turn baseline. Direct file removal and later reappearance are reconciled the same way.

The default `codex-internal` adapter attaches to the installed desktop host at `127.0.0.1:9222` and calls its app-owned client for ordinary saved ChatGPT conversations directly. It does not launch Chrome, navigate conversations, read rendered transcript DOM, focus controls, type, or click, and there is no UI fallback. The desktop host must already be signed in once with the ChatGPT account that owns the queued conversations. If it is closed, the watchdog may start `/usr/bin/codex-desktop`. If CDP is up but no usable primary renderer exists, the watchdog sends the launcher's supported `--new-chat` warm-start action and waits for the internal runtime to become ready; failure remains non-destructive and sends nothing. The current build's `mcpAppSandboxDevtools=1` primary renderer is supported.

Task completion is turn-bounded. A re-added task uses the prior generation's recorded completion turn as its boundary and waits for a later user turn, so an old `DONE_I_HAVE_COMPLETED_ALL_THE_STEPS` marker cannot complete a new task. Completion is accepted only when the latest meaningful turn is an assistant turn after the current task's user baseline and contains the marker as an exact standalone line. The durable completion record includes task ID, generation, baseline turn, and completion turn before the URL is removed.

A stopped incomplete task receives the configured continuation only after a second canonical `get` immediately before sending. A new completion marker, a changed transcript or `current_node`, an active stream, an existing continuation as the latest user turn, a duplicate parent node, cooldown, or the attempt ceiling cancels the send. The app's live model catalogue merges duplicate picker entries by backend slug. It uses an explicitly configured model strictly, otherwise reuses the thread model only when it satisfies the requested effort, and falls back to another verified high-reasoning model rather than silently downgrading. Watchdog operations share one global scan lock and one endpoint-wide internal-client lock, so only one watchdog operation or completion stream is initiated at a time while normal app use remains independent.

The authenticated dashboard exposes `/dashboard/watchdog` plus task edit/add/remove and manual-scan controls. It also has a Project discovery section: `Find projects` performs an explicit Project catalogue read, and the user can add only chosen Projects to the persistent allowlist. Adding a Project baselines all currently listed conversations without enqueueing them. On each later watchdog timer, each selected Project normally costs one conversation-list request; unseen non-archived conversation IDs are added to the ordinary watchdog queue before task inspection. If a full page contains only unseen IDs, discovery follows the cursor until it reaches a previously seen ID so bursts larger than one page are not truncated. General chats and unselected Projects are never auto-added. The textarea edits the same `threads.txt` file used by the daemon, and active rows show their task generation. Saves include an optimistic file version, so a browser tab cannot overwrite URLs changed directly on disk. Mutating requests require same-origin context and the `x-mcp-dashboard-csrf: chat-watchdog` header; the existing dashboard token/cookie boundary still applies.

### Persistent ChatGPT sub-agent orchestration

Terminal MCP exposes the existing `agent_*` tools while using two durable layers internally. `durable_ledger.py` stores parent/child identity, tasks, ordered mailboxes, subscriptions, events, working directories, and delivery cursors. The vendored `chat_gateway` package exclusively schedules automatic managed-agent provider work: creation, canonical inspection, continuation, cancellation, deletion, verification, request claims, rate windows, and circuit recovery. `gateway_agent_orchestrator.py` maps those layers without changing public tool schemas.

One `agent_sync` or background scheduler tick performs arbitrary local SQLite work and **zero or one physical ChatGPT backend request**. Create and continue are staged: the mutation uses one tick and canonical verification happens in a later tick. There are no same-tick retries, immediate readback, or per-agent polling loops. More agents increase queue latency rather than request rate. The default managed-child limit is five.

Default request limits are conservative: global and canonical reads use a 20-second minimum, 15 requests per five minutes, and 120 per hour; heavy generation operations use a 60-second minimum, five per ten minutes, and 20 per hour; metadata and cleanup have separate lanes. A 429 opens a durable shared circuit. Recovery starts after 120 seconds, then uses 6/12/24/48/60-minute failed-probe delays, half-open probes, and a paced recovery window. The gateway database survives restarts and serializes claims across processes.

Canonical completion is fail-closed. `running=false`, elapsed time, or an expired renderer stream record does not complete an agent. The gateway reports `UNKNOWN` until a canonical assistant terminal turn contains the configured completion marker, or verified failure/cancellation evidence exists. This prevents long-running agents from flipping through false terminal states.

`agent_register_parent` is a one-time, user-invoked canonical registration read used to discover the parent’s existing project. Project discovery tools are also explicit user calls. Automatic synchronization, creation, continuation, steering, verification, and cancellation are all routed through the gateway scheduler and accounted as physical request operations.

Every child receives exact orchestration IDs, project and working-directory placement, the Terminal MCP bootstrap/session contract, its assigned task, and an exact completion marker. Sub-agent operating guidance is provided by the machine skill `terminal-mcp-subagents`; it is discovered in startup metadata and loaded only when delegation work requires it. Nested delegation remains disabled by default unless the parent explicitly authorizes it.

The Claude-style core behavior prompt is stored at `prompts/terminal_mcp_core_behavior.md` and is inserted before global/project `.GPT/AGENTS.md` content for startup, bootstrap, and context reload. It contains no sub-agent policy or proprietary Claude prompt text. UI tasks are instructed to load the `frontend-design` and `user-html-ui-preference` skills.

The authenticated agent control room is available at `/dashboard/agents`. Its API at `/dashboard/agents/api` exposes the durable parent/child tree, synchronization state, reasoning configuration, wake timers, exact-marker completion gates, managed capacity, mailboxes, queued gateway operations, request history, circuits, errors, and next request eligibility. The page is mobile-first and linked from the existing `/dashboard` usage ledger.

Wake timers and completion-triggered prompts are local durable automations. When activated, they materialize one ordinary ordered command and use the same one-request gateway, rate limits, circuits, and staged verification as `agent_send`. See `docs/AGENT_AUTOMATION_AND_CONTROL_TOWER.md` for the exact completion contract, timing model, tools, and verification map.

Operational migration and rollback instructions are in `docs/AGENT_GATEWAY_CUTOVER.md`. Existing orchestration records are preserved in the domain database and mapped to gateway records on startup. Always back up the domain database before first deployment; the gateway database may start clean.

## Security guidance

1. Run under a dedicated, unprivileged Linux user.
2. Limit `MCP_WORKSPACE` and filesystem permissions to only the directories the agent needs.
3. Set `MCP_BEARER_TOKEN` to a unique random value and rotate it if exposed.
4. Keep OpenAI tool approvals set to `always` until the workflow is thoroughly reviewed.
5. Use `allowed_tools` to expose the smallest possible tool set.
6. Do not place `.env`, browser profiles, logs, SSH keys, or MCP config files containing secrets in Git.
7. Prefer a private network or OpenAI Secure MCP Tunnel over a public endpoint.
8. Review downstream MCP servers: they run with the same operating-system privileges as this process.

Bearer authentication protects the endpoint from anonymous requests, but it does not sandbox commands. The bearer token grants the holder the capabilities exposed by this server.

## Development

Install development dependencies:

```bash
./store.sh --dev
# Or, when Linux prerequisites are already installed:
./setup.sh --dev
```

Run checks:

```bash
bash -n start.sh setup.sh store.sh install.sh
.venv/bin/python -m py_compile terminal_mcp.py scripts/smoke_test.py
.venv/bin/pytest
```

The regression suite verifies:

- exact public tool discovery
- project-context gating
- skill discovery
- persistent downstream MCP process reuse
- reconnect after a child crash
- explicit reset behavior
- exclusive browser-profile ownership
- filesystem and background-process lifecycle
- bearer authentication

## Dedicated ChatGPT runtime

For multi-day subagent workloads, run ChatGPT Desktop under its own supervised
user service rather than as a child of Terminal MCP. The runtime circuit, MCP
control tools, installation order, fault-injection tests, and rollback procedure
are documented in [`docs/chat-runtime-supervisor.md`](docs/chat-runtime-supervisor.md).

## Troubleshooting

### Fresh Linux machine or missing `venv`/`pip`

Run the full bootstrap installer:

```bash
./store.sh
```

If sudo access is unavailable, use the user-space path:

```bash
./store.sh --skip-system-packages
```

The installer downloads `uv` only when no usable Python 3.11+ interpreter is
available. Downloads use HTTPS, retry transient failures, and support optional
`UV_INSTALLER_SHA256` and `NGROK_SHA256` verification overrides.

### Repair a broken environment

```bash
./setup.sh --recreate
./start.sh --check --local-only
```

### `Port ... is already in use`

Choose another port:

```bash
MCP_PORT=8010 ./start.sh
```

The launcher intentionally does not stop the existing process.

### `401 unauthorized`

Pass the same token configured as `MCP_BEARER_TOKEN`:

```text
Authorization: Bearer your-token
```

### Browser returns `Stale aria-ref` or resets to `about:blank`

- Use the same `session_id` for all calls in the sequence.
- Check `local_mcp(action="list", session_id="...")`; the server should show `connected`.
- Reset a broken owner with `local_mcp(action="reset", server="stealth-browser", session_id="...")`.
- Ensure another session is not trying to use the same browser profile.
- Confirm the downstream browser MCP itself uses a persistent context if cookies must survive process restarts.

### ngrok starts but no public URL is printed

Check the ngrok log path printed by the launcher. Verify the ngrok account is authenticated and the requested reserved URL is available.

## License

MIT
