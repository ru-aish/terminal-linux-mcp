# Dedicated ChatGPT runtime supervisor

## Ownership boundary

The supported production layout is:

```text
terminal-mcp-local.service
└── Terminal MCP, durable queue, request pacing, runtime circuit, MCP tools

codex-desktop-runtime.service
└── supervisor
    └── ChatGPT launcher
        ├── packaged webview server
        ├── Electron main process
        ├── renderer/GPU/network children
        └── normal-chat app server
```

Terminal MCP never launches individual ChatGPT windows or owns part of the
ChatGPT process tree. The services communicate only through loopback webview/CDP
health checks and `systemctl --user` lifecycle operations.

`KDE_APPLICATIONS_AS_SCOPE=0` is set both in the unit and by the supervisor. It
prevents KDE from moving Electron into an `app-codex-desktop-*.scope`. The
supervisor also stops matching generated scopes during recovery as a defensive
fallback.

## Health and recovery

A runtime is ready only when all layers pass:

1. The packaged webview returns the expected ChatGPT/Codex HTML markers.
2. CDP exposes the configured main renderer, not an auxiliary overlay.
3. CDP JavaScript evaluation succeeds.
4. The normal-chat client is discoverable and reports ready.

After the startup grace period, the supervisor requires several consecutive
failed probes before replacing a generation. Recovery terminates the launcher's
entire process group, stops any matching generated application scope, removes
validated stale PID/socket markers, waits for a bounded backoff, and starts one
new generation.

The gateway has a separate global `runtime` circuit. Infrastructure failures do
not spend an agent's retry budget and do not terminalize the agent. The original
operation remains pending while health-only recovery probes drive the circuit:

```text
CLOSED → OPEN → HALF_OPEN → PACED → CLOSED
```

Manual pause leaves the circuit OPEN without a retry time. Only an explicit
resume after a passing health check releases requests.

## MCP control tools

Terminal MCP exposes:

- `chat_runtime_status` — service properties, layered health, and circuits.
- `chat_runtime_health` — read-only webview/CDP/client probe.
- `chat_runtime_control` — `start`, `stop`, `restart`, or `recover`.
- `chat_runtime_logs` — bounded journal output.
- `chat_runtime_circuit` — `status`, `pause`, or health-gated `resume`.

The lifecycle tools use a fixed, validated user-service name. They do not accept
arbitrary commands or units.

## Installation

Do not deploy the Terminal MCP source change before the dedicated service exists.
From the validated repository checkout:

```bash
CHAT_RUNTIME_PYTHON=/path/to/terminal-mcp-venv/bin/python \
  ./scripts/install_chat_runtime_service.sh
```

Review:

```text
~/.config/systemd/user/codex-desktop-runtime.service
~/.config/terminal-mcp/chat-runtime.env
```

The installer performs `daemon-reload` but does not start anything unless
`--enable` is supplied. For the first migration:

```bash
systemctl --user stop terminal-mcp-local.service
# Close the old ChatGPT process tree completely and confirm 5175/9222 are free.
systemctl --user enable --now codex-desktop-runtime.service
systemctl --user status codex-desktop-runtime.service --no-pager
curl --noproxy '*' http://127.0.0.1:5175/index.html
curl --noproxy '*' http://127.0.0.1:9222/json/list
# Deploy/restart Terminal MCP only after layered runtime health passes.
systemctl --user start terminal-mcp-local.service
```

The Terminal MCP unit must also read `~/.config/terminal-mcp/chat-runtime.env`
when non-default service names or endpoints are used. Add this drop-in:

```ini
[Service]
EnvironmentFile=-%h/.config/terminal-mcp/chat-runtime.env
```

## Validation

Focused source tests:

```bash
python -m pytest -q \
  tests/test_chat_runtime_supervisor.py \
  tests/test_gateway_agent_orchestrator.py \
  tests/test_chat_internal_client.py
```

Isolated systemd fault injection uses ports 5187 and 9234 and never starts the
real ChatGPT binary:

```bash
PYTHON_BIN=/path/to/terminal-mcp-venv/bin/python \
  ./tests/run_runtime_fault_injection.sh
```

It kills the isolated webview, renderer, and Electron-like processes one at a
time, then restarts the whole service. The test requires each generation to
recover, verifies every test PID remains in the isolated unit cgroup, and checks
that the live 5175/9222 PIDs are unchanged.

The Codex launcher repository has a separate smoke test:

```bash
./tests/launcher_recovery_smoke.sh
```

It reproduces Electron's combined `argv[0]`, proves pidfd termination succeeds
without `CODEX_*` environment variables, and confirms helper-role processes are
rejected.

## Rollback

1. Pause the runtime circuit:
   `chat_runtime_circuit(action="pause")`.
2. Stop Terminal MCP.
3. Disable the dedicated service:
   `systemctl --user disable --now codex-desktop-runtime.service`.
4. Restore the previous Terminal MCP commit and unit definition.
5. Remove or rename the runtime drop-in and run `systemctl --user daemon-reload`.
6. Start the former ChatGPT launch path and verify 5175/9222.
7. Start Terminal MCP.
8. Resume the circuit only after `chat_runtime_health` reports ready.

Durable orchestration and gateway SQLite databases are not modified by service
installation or rollback. Pending work remains recoverable.
