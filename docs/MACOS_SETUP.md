# macOS setup

Terminal MCP supports both Apple Silicon (`arm64`) and Intel (`x86_64`) macOS.

## Prerequisites

Install Homebrew from https://brew.sh/ if it is not already installed. Then:

```bash
brew install python@3.11 tmux
```

For public HTTP access through ngrok:

```bash
brew install ngrok
ngrok config add-authtoken "YOUR_TOKEN"
```

## Bootstrap

```bash
cd terminal-mac_mcp
./store.sh --dev
./doctor.sh
cp .env.example .env
./start.sh --local-only
```

`store.sh` uses Homebrew on macOS. It does not download a Linux Python or Linux ngrok binary. Homebrew's prefix is discovered dynamically, so the same scripts work with the normal `/opt/homebrew` Apple Silicon installation and `/usr/local` Intel installation.

## Shell and workload execution

The terminal runner uses `MCP_SHELL` when set and otherwise uses the logged-in `$SHELL`; if `$SHELL` is unset on macOS, `/bin/zsh` is used.

Linux `systemd-run` workload scopes are not required on macOS. macOS commands run in a dedicated POSIX process group and are terminated as a group on timeout or cleanup. Linux keeps the existing `systemd-run` path when available.

Resource settings named `MCP_WORKLOAD_MEMORY_MAX`, `MCP_WORKLOAD_CPU_QUOTA`, and `MCP_WORKLOAD_TASKS_MAX` are Linux/systemd controls. They are intentionally not translated into pretend macOS equivalents.

## ChatGPT/Codex watchdog

The watchdog is configured through `MCP_CHAT_WATCHDOG_APP_COMMAND`. On macOS, when that variable is unset, the default launch command is:

```text
/usr/bin/open -a "Codex" --args
```

When the watchdog needs to reopen a running app whose renderer is unavailable, it appends `--new-chat`, producing the equivalent of:

```text
/usr/bin/open -a "Codex" --args --new-chat
```

Override the command when your installed desktop app uses a different application name or launcher.

macOS watchdog and gateway state defaults to the user's native application support area under `~/Library/Application Support/terminal-mcp/`. You can continue to override every path with the existing `MCP_CHAT_WATCHDOG_*`, `MCP_CHAT_AGENT_DB`, and `MCP_CHAT_GATEWAY_DB` variables.

## Development checks

```bash
bash -n start.sh setup.sh store.sh install.sh doctor.sh
python3 -m py_compile platform_support.py terminal_mcp.py chat_watchdog.py
./doctor.sh
.venv/bin/pytest -q
```

The repository's GitHub Actions workflow also runs the test matrix on both Linux and macOS.

## Optional launchd service

Foreground startup remains the supported development path. A `launchd` service is intentionally not installed by the default bootstrap so that debugging remains transparent. Once local startup is verified, a dedicated `~/Library/LaunchAgents/*.plist` can be added for automatic login-time startup.
