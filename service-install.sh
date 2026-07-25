#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
MODE=auto
SERVICE_NAME="${TERMINAL_MCP_SERVICE_NAME:-terminal-mcp.service}"
SYSTEM_ROOT="${TERMINAL_MCP_SYSTEM_ROOT:-}"
SYSTEMCTL="${TERMINAL_MCP_SYSTEMCTL:-systemctl}"
SUDO="${TERMINAL_MCP_SUDO:-sudo}"
PRIVILEGE_BIN=""
rendered=""
INVOKING_USER="${TERMINAL_MCP_INVOKING_USER:-${SUDO_USER:-${USER:-$(id -un)}}}"
INVOKING_GROUP="${TERMINAL_MCP_INVOKING_GROUP:-$(id -gn "$INVOKING_USER" 2>/dev/null || id -gn)}"
if command -v getent >/dev/null 2>&1; then
  resolved_home="$(getent passwd "$INVOKING_USER" 2>/dev/null | cut -d: -f6 || true)"
else
  resolved_home=""
fi
INVOKING_HOME="${TERMINAL_MCP_INVOKING_HOME:-${resolved_home:-$HOME}}"

usage() {
  cat <<'EOF'
Usage: ./service-install.sh [--service auto|system|user|none]

Install, enable, start, and verify a loopback-only Terminal MCP systemd service.
Auto prefers system mode when an operational system manager and root/sudo/doas
are available, then falls back to the current user's systemd manager.
EOF
}

die() { printf 'service-install.sh: %s\n' "$*" >&2; exit 1; }
log() { printf '[service] %s\n' "$*"; }
warn() { printf '[service] WARNING: %s\n' "$*" >&2; }
cleanup() { [[ -z "$rendered" ]] || rm -f -- "$rendered"; }
trap cleanup EXIT

while (($#)); do
  case "$1" in
    --service)
      shift
      (($#)) || die '--service requires a value'
      MODE=$1
      ;;
    --service=*) MODE=${1#*=} ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
  shift
done
case "$MODE" in auto|system|user|none) ;; *) die "invalid service mode: $MODE" ;; esac
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9@_.:-]*\.service$ ]] || die "invalid service name: $SERVICE_NAME"
[[ "$MODE" != none ]] || { log 'service installation disabled'; exit 0; }

have_privilege() {
  if [[ ${TERMINAL_MCP_TEST_EUID:-$EUID} == 0 ]]; then
    PRIVILEGE_BIN=""
    return 0
  fi
  if command -v "$SUDO" >/dev/null 2>&1 && "$SUDO" -v; then
    PRIVILEGE_BIN="$SUDO"
    return 0
  fi
  if command -v doas >/dev/null 2>&1 && doas true; then
    PRIVILEGE_BIN=doas
    return 0
  fi
  return 1
}

run_system() {
  if [[ ${TERMINAL_MCP_TEST_EUID:-$EUID} == 0 ]]; then
    "$@"
  else
    "$PRIVILEGE_BIN" "$@"
  fi
}

system_systemd_available() {
  command -v "$SYSTEMCTL" >/dev/null 2>&1 || return 1
  run_system "$SYSTEMCTL" show-environment >/dev/null 2>&1
}

user_systemd_available() {
  command -v "$SYSTEMCTL" >/dev/null 2>&1 || return 1
  "$SYSTEMCTL" --user show-environment >/dev/null 2>&1
}

if [[ "$MODE" == auto ]]; then
  if have_privilege && system_systemd_available; then
    MODE=system
  elif user_systemd_available; then
    MODE=user
  else
    die 'an operational system or user systemd manager is unavailable; use --service none for CI/development'
  fi
elif [[ "$MODE" == system ]]; then
  have_privilege || die 'system mode requested but root/sudo/doas is unavailable'
  system_systemd_available || die 'system mode requested but the systemd system manager is unavailable'
elif [[ "$MODE" == user ]] && ! user_systemd_available; then
  die 'user mode requested but the user systemd manager is unavailable'
fi
[[ "$INVOKING_USER" != root ]] || die 'refusing to run a remote terminal service as root; run the installer as a non-root user and let it use sudo/doas'

metadata_value() {
  local file=$1 key=$2
  awk -v key="$key" '$0 ~ "^" key "=" {sub("^[^=]*=", ""); print; exit}' "$file"
}

previous_metadata="$ROOT_DIR/.service-mode"
if [[ -f "$previous_metadata" ]]; then
  previous_mode="$(metadata_value "$previous_metadata" SERVICE_MODE)"
  previous_name="$(metadata_value "$previous_metadata" SERVICE_NAME)"
  [[ "$previous_name" =~ ^[A-Za-z0-9][A-Za-z0-9@_.:-]*\.service$ ]] || die "invalid previous service name in $previous_metadata"
  if [[ "$previous_mode" != "$MODE" || "$previous_name" != "$SERVICE_NAME" ]]; then
    case "$previous_mode" in
      system)
        have_privilege || die "cannot replace the previous system service without root/sudo/doas: $previous_name"
        system_systemd_available || die 'cannot replace the previous system service because the systemd system manager is unavailable'
        run_system "$SYSTEMCTL" disable --now "$previous_name"
        ;;
      user)
        user_systemd_available || die 'cannot replace the previous user service because the user systemd manager is unavailable'
        "$SYSTEMCTL" --user disable --now "$previous_name"
        ;;
      *)
        die "invalid previous service metadata in $previous_metadata"
        ;;
    esac
    log "disabled previous $previous_mode service $previous_name"
  fi
fi

PYTHON="${MCP_VENV_DIR:-$ROOT_DIR/.venv}/bin/python"
[[ -x "$PYTHON" ]] || die "Python environment is missing at $PYTHON"
if [[ "$MODE" == system ]]; then
  ENV_FILE="${MCP_ENV_FILE:-$SYSTEM_ROOT/etc/terminal-mcp/terminal-mcp.env}"
  STATE_DIR="${MCP_STATE_DIR:-$SYSTEM_ROOT/var/lib/terminal-mcp}"
  LOG_DIR="${MCP_LOG_DIR:-$SYSTEM_ROOT/var/log/terminal-mcp}"
  RUNTIME_DIR="${MCP_RUNTIME_DIR:-$STATE_DIR/run}"
  UNIT_DIR="${TERMINAL_MCP_SYSTEM_UNIT_DIR:-$SYSTEM_ROOT/etc/systemd/system}"
  COMMAND_DIR="${TERMINAL_MCP_SYSTEM_BIN_DIR:-$SYSTEM_ROOT/usr/local/bin}"
  TEMPLATE="$ROOT_DIR/systemd/terminal-mcp.service.in"
else
  config_home="${XDG_CONFIG_HOME:-$INVOKING_HOME/.config}"
  state_home="${XDG_STATE_HOME:-$INVOKING_HOME/.local/state}"
  ENV_FILE="${MCP_ENV_FILE:-$config_home/terminal-mcp/terminal-mcp.env}"
  STATE_DIR="${MCP_STATE_DIR:-$state_home/terminal-mcp}"
  LOG_DIR="${MCP_LOG_DIR:-$state_home/terminal-mcp/logs}"
  if [[ -n "${XDG_RUNTIME_DIR:-}" ]]; then
    default_runtime="$XDG_RUNTIME_DIR/terminal-mcp"
  else
    default_runtime="$state_home/terminal-mcp/run"
  fi
  RUNTIME_DIR="${MCP_RUNTIME_DIR:-$default_runtime}"
  UNIT_DIR="${TERMINAL_MCP_USER_UNIT_DIR:-$config_home/systemd/user}"
  COMMAND_DIR="${INSTALL_BIN_DIR:-$INVOKING_HOME/.local/bin}"
  TEMPLATE="$ROOT_DIR/systemd/terminal-mcp-user.service.in"
fi
UNIT_FILE="$UNIT_DIR/$SERVICE_NAME"
COMMAND_PATH="$COMMAND_DIR/terminal-mcp"
WORKSPACE="${MCP_WORKSPACE:-$INVOKING_HOME/mcp_workspace}"
GPT_HOME="${MCP_GPT_HOME:-$INVOKING_HOME/.GPT}"
dirs=("$UNIT_DIR" "$COMMAND_DIR" "$(dirname -- "$ENV_FILE")" "$STATE_DIR" "$LOG_DIR" "$RUNTIME_DIR" "$WORKSPACE" "$GPT_HOME")
if [[ "$MODE" == system ]]; then
  run_system mkdir -p "${dirs[@]}"
else
  mkdir -p "${dirs[@]}"
fi

seed_env="${TERMINAL_MCP_SEED_ENV_FILE:-$ROOT_DIR/.env}"
if [[ ! -f "$ENV_FILE" ]]; then
  [[ -f "$seed_env" ]] || die "authentication seed is missing at $seed_env"
  if [[ "$MODE" == system ]]; then
    run_system install -m 600 -o "$INVOKING_USER" -g "$INVOKING_GROUP" "$seed_env" "$ENV_FILE"
  else
    install -m 600 "$seed_env" "$ENV_FILE"
  fi
else
  log "preserving existing configuration and secrets in $ENV_FILE"
fi
if [[ "$MODE" == system ]]; then
  run_system chown "$INVOKING_USER:$INVOKING_GROUP" "$ENV_FILE"
  run_system chmod 600 "$ENV_FILE"
else
  chmod 600 "$ENV_FILE"
fi

upsert_setting() {
  local key=$1 value=$2 temporary
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || die "$key contains a newline"
  temporary="$(mktemp)"
  awk -v key="$key" -v replacement="$key=$value" '
    BEGIN { written = 0 }
    {
      normalized = $0
      sub("^[[:space:]]*#[[:space:]]*", "", normalized)
      if (normalized ~ "^" key "[[:space:]]*=") {
        if (!written) { print replacement; written = 1 }
        next
      }
      print
    }
    END { if (!written) print replacement }
  ' "$ENV_FILE" >"$temporary"
  if [[ "$MODE" == system ]]; then
    run_system install -m 600 -o "$INVOKING_USER" -g "$INVOKING_GROUP" "$temporary" "$ENV_FILE"
  else
    chmod 600 "$temporary"
    mv -f -- "$temporary" "$ENV_FILE"
  fi
  rm -f -- "$temporary"
}

SERVICE_PORT="${MCP_SERVICE_PORT:-8011}"
SERVICE_PATH="${MCP_SERVICE_PATH:-$INVOKING_HOME/.local/bin:$INVOKING_HOME/.cargo/bin:$INVOKING_HOME/.npm-global/bin:$INVOKING_HOME/bin:/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin}"
[[ "$SERVICE_PORT" =~ ^[0-9]+$ ]] && (( SERVICE_PORT >= 1 && SERVICE_PORT <= 65535 )) \
  || die "MCP_SERVICE_PORT must be an integer between 1 and 65535: $SERVICE_PORT"
upsert_setting HOME "$INVOKING_HOME"
upsert_setting USER "$INVOKING_USER"
upsert_setting LOGNAME "$INVOKING_USER"
upsert_setting PATH "$SERVICE_PATH"
upsert_setting MCP_HOST 127.0.0.1
upsert_setting MCP_PORT "$SERVICE_PORT"
upsert_setting MCP_PATH /mcp
upsert_setting MCP_WORKSPACE "$WORKSPACE"
upsert_setting MCP_LOG_DIR "$LOG_DIR"
upsert_setting MCP_RUNTIME_DIR "$RUNTIME_DIR"
upsert_setting MCP_GPT_HOME "$GPT_HOME"

[[ "$INVOKING_USER" =~ ^[A-Za-z0-9_.-]+$ ]] || die "unsupported service user name: $INVOKING_USER"
[[ "$INVOKING_GROUP" =~ ^[A-Za-z0-9_.-]+$ ]] || die "unsupported service group name: $INVOKING_GROUP"
rendered="$(mktemp)"
"$PYTHON" - "$TEMPLATE" "$rendered" "$INVOKING_USER" "$INVOKING_GROUP" \
  "$ROOT_DIR" "$ENV_FILE" "$PYTHON" "$ROOT_DIR/terminal_mcp.py" \
  "$WORKSPACE" "$STATE_DIR" "$LOG_DIR" "$RUNTIME_DIR" "$GPT_HOME" <<'PY'
from pathlib import Path
import re
import sys

template, output, user, group, project, env_file, python, script, *writable = sys.argv[1:]

def quote(value: str) -> str:
    if "\n" in value or "\r" in value:
        raise SystemExit("systemd paths must not contain newlines")
    safe = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/._-:@"
    encoded: list[str] = []
    for byte in value.encode("utf-8"):
        if byte == ord("%"):
            encoded.append("%%")
        elif byte in safe:
            encoded.append(chr(byte))
        else:
            encoded.append(f"\\x{byte:02x}")
    return "".join(encoded)

text = Path(template).read_text(encoding="utf-8")
replacements = {
    "@SERVICE_USER@": user,
    "@SERVICE_GROUP@": group,
    "@PROJECT_DIR_Q@": quote(project),
    "@ENV_FILE_Q@": quote(env_file),
    "@PYTHON_Q@": quote(python),
    "@SCRIPT_Q@": quote(script),
    "@WRITABLE_PATHS_Q@": " ".join(quote(path) for path in writable),
}
for marker, value in replacements.items():
    text = text.replace(marker, value)
if re.search(r"@[A-Z0-9_]+@", text):
    raise SystemExit("unresolved systemd template marker")
Path(output).write_text(text, encoding="utf-8")
PY
if [[ "$MODE" == system ]]; then
  run_system install -m 644 "$rendered" "$UNIT_FILE"
  run_system ln -sfn "$ROOT_DIR/terminal-mcp-service" "$COMMAND_PATH"
  run_system chown -R "$INVOKING_USER:$INVOKING_GROUP" "$STATE_DIR" "$LOG_DIR" "$RUNTIME_DIR"
  run_system chown "$INVOKING_USER:$INVOKING_GROUP" "$WORKSPACE" "$GPT_HOME"
  run_system "$SYSTEMCTL" daemon-reload
  run_system "$SYSTEMCTL" enable "$SERVICE_NAME"
  run_system "$SYSTEMCTL" restart "$SERVICE_NAME"
else
  install -m 644 "$rendered" "$UNIT_FILE"
  ln -sfn "$ROOT_DIR/terminal-mcp-service" "$COMMAND_PATH"
  "$SYSTEMCTL" --user daemon-reload
  "$SYSTEMCTL" --user enable "$SERVICE_NAME"
  "$SYSTEMCTL" --user restart "$SERVICE_NAME"
fi
rm -f "$rendered"
rendered=""

metadata="$(mktemp)"
cat >"$metadata" <<EOF
SERVICE_MODE=$MODE
SERVICE_NAME=$SERVICE_NAME
SERVICE_ENV_FILE=$ENV_FILE
SERVICE_COMMAND=$COMMAND_PATH
EOF
chmod 600 "$metadata"
mv -f -- "$metadata" "$ROOT_DIR/.service-mode"
"$ROOT_DIR/terminal-mcp-service" verify
if [[ "$MODE" == user ]]; then
  warn "user services start after login unless lingering is enabled; for boot startup run: loginctl enable-linger $INVOKING_USER"
fi
log "$MODE service installed, enabled, started, and verified"
