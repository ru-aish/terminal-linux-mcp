#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python_bin="${CHAT_RUNTIME_PYTHON:-$repo_dir/.venv/bin/python}"
unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
env_dir="${XDG_CONFIG_HOME:-$HOME/.config}/terminal-mcp"
unit_path="$unit_dir/codex-desktop-runtime.service"
terminal_dropin_dir="$unit_dir/terminal-mcp-local.service.d"
terminal_dropin="$terminal_dropin_dir/50-chat-runtime.conf"
env_path="$env_dir/chat-runtime.env"

enable=0
if [[ "${1:-}" == "--enable" ]]; then
  enable=1
elif [[ $# -gt 0 ]]; then
  echo "usage: $0 [--enable]" >&2
  exit 2
fi

[[ -x "$python_bin" ]] || {
  echo "Python environment not found: $python_bin" >&2
  exit 1
}

mkdir -p "$unit_dir" "$env_dir" "$terminal_dropin_dir"
sed \
  -e "s|@REPOSITORY@|$repo_dir|g" \
  -e "s|@PYTHON@|$python_bin|g" \
  "$repo_dir/systemd/codex-desktop-runtime.service.in" > "$unit_path"
cp "$repo_dir/systemd/terminal-mcp-chat-runtime.conf" "$terminal_dropin"

if [[ ! -e "$env_path" ]]; then
  cat > "$env_path" <<ENV
CHAT_RUNTIME_COMMAND=/usr/bin/codex-desktop
CHAT_RUNTIME_APP_ID=codex-desktop
CHAT_RUNTIME_WEBVIEW_URL=http://127.0.0.1:5175/index.html
CHAT_RUNTIME_CDP_ENDPOINT=http://127.0.0.1:9222
CHAT_RUNTIME_STATE_DIR=$HOME/.local/state/codex-desktop
CHAT_RUNTIME_SOCKET_DIR=${XDG_RUNTIME_DIR:-/run/user/$(id -u)}/codex-desktop
CHAT_RUNTIME_STATUS_PATH=$HOME/.local/state/codex-desktop/supervisor-status.json
CHAT_RUNTIME_STARTUP_GRACE=45
CHAT_RUNTIME_PROBE_INTERVAL=10
CHAT_RUNTIME_FAILURE_THRESHOLD=3
CHAT_RUNTIME_RESTART_BACKOFF=5
CHAT_RUNTIME_STOP_TIMEOUT=20
MCP_CHAT_RUNTIME_SERVICE=codex-desktop-runtime.service
MCP_CHAT_RUNTIME_WEBVIEW_URL=http://127.0.0.1:5175/index.html
MCP_CHAT_RUNTIME_RECOVERY_TIMEOUT=90
MCP_CHAT_RUNTIME_POLL_INTERVAL=1
ENV
  chmod 600 "$env_path"
fi

systemctl --user daemon-reload
printf 'Installed %s\nTerminal MCP drop-in: %s\nConfiguration: %s\n' \
  "$unit_path" "$terminal_dropin" "$env_path"
if [[ "$enable" -eq 1 ]]; then
  systemctl --user enable --now codex-desktop-runtime.service
else
  echo "Service was not started. Validate, then run: systemctl --user enable --now codex-desktop-runtime.service"
fi
