# Terminal Linux MCP

A native Linux terminal MCP server for files, commands, long-running processes, project instructions, local skills, and downstream MCP servers.

The server can run locally over stdio, SSE, or Streamable HTTP. `start.sh` starts the Streamable HTTP server and an ngrok tunnel together, prints the final MCP endpoint, and shuts both processes down cleanly.

> [!CAUTION]
> This server can execute shell commands and modify files on the host. Treat it like remote shell access. Run it as a dedicated low-privilege user, require authentication, keep tool approvals enabled, and never expose it anonymously on the public internet.

## Highlights

- Native command execution with per-session working directories and environment variables.
- Foreground and background process management.
- Direct file read/write/edit/copy/move operations.
- Mandatory `AGENTS.md` project-context gate before executing or modifying work.
- Discovery of local `SKILL.md` files.
- Discovery and proxying of configured MCP servers.
- Persistent downstream MCP connections keyed by `(session_id, server_name)`.
- Per-server serialization, one reconnect attempt after a crashed downstream server, idle cleanup, and explicit reset actions.
- Exclusive ownership for configured browser profile directories, preventing two owners from opening the same profile concurrently.
- Optional bearer authentication for HTTP transports.
- Safe launcher that refuses occupied ports and refuses an unauthenticated public tunnel unless explicitly overridden.

## Requirements

- Linux
- Python 3.11 or newer
- `ngrok` for a public tunnel
- Optional agent CLIs used by the delegated tools: Codex and/or Antigravity

## Quick start

```bash
git clone https://github.com/ru-aish/terminal-linux-mcp.git
cd terminal-linux-mcp
./setup.sh
cp .env.example .env
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
Local MCP endpoint: http://127.0.0.1:8000/mcp
Public MCP endpoint: https://example.ngrok.app/mcp
Authentication: Authorization: Bearer <MCP_BEARER_TOKEN>
```

`Ctrl+C` stops the server and the tunnel.

### Local-only mode

```bash
MCP_SKIP_NGROK=1 ./start.sh
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
                "project_context",
                "list_dir",
                "stat_path",
                "read_file",
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
| Project instructions | `project_context` |
| Skills and MCP discovery | `local_skills`, `local_mcp` |
| Commands | `run_command`, `start_process`, `poll_process`, `stop_process` |
| Session environment | `set_session_env` |
| Files | `read_file`, `write_file`, `replace_in_file`, `apply_patch`, `list_dir`, `stat_path`, `make_dir`, `copy_path`, `move_path` |
| Delegated agents | `run_codex_yolo`, `start_codex_yolo`, `run_agy_yolo`, `start_agy_yolo` |

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
| `NGROK_URL` | random URL | Reserved ngrok URL/domain |
| `NGROK_TRAFFIC_POLICY_FILE` | empty | Optional ngrok Traffic Policy |
| `MCP_ALLOW_UNAUTHENTICATED_PUBLIC` | `0` | Dangerous public-tunnel override |

The launcher refuses to start when the selected port is already occupied. It never kills or replaces an existing service.

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
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
```

Run checks:

```bash
bash -n start.sh setup.sh
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

## Troubleshooting

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
