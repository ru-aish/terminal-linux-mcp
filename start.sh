#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CLI_LOCAL_ONLY=0
CLI_SETUP_ONLY=0
CLI_CHECK_ONLY=0

usage() {
  cat <<'EOF'
Usage: ./start.sh [options]

Options:
  --local-only   Start without ngrok.
  --setup-only   Install or repair dependencies, then exit.
  --check        Validate dependencies and configuration, then exit.
  -h, --help     Show this help.

Missing Python packages trigger ./setup.sh automatically by default. Set
MCP_AUTO_SETUP=0 to disable that behavior. Set MCP_AUTO_INSTALL_NGROK=1 to let
the launcher invoke ./store.sh --with-ngrok when ngrok is missing.
EOF
}

die() {
  printf 'start.sh: %s\n' "$*" >&2
  exit 1
}

while (($#)); do
  case "$1" in
    --local-only) CLI_LOCAL_ONLY=1 ;;
    --setup-only) CLI_SETUP_ONLY=1 ;;
    --check) CLI_CHECK_ONLY=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
  shift
done

ENV_FILE="${MCP_ENV_FILE:-$ROOT_DIR/.env}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

MCP_HOST="${MCP_HOST:-127.0.0.1}"
MCP_PORT="${MCP_PORT:-8011}"
MCP_TRANSPORT="${MCP_TRANSPORT:-streamable-http}"
MCP_PATH="${MCP_PATH:-/mcp}"
MCP_WORKSPACE="${MCP_WORKSPACE:-$HOME/mcp_workspace}"
MCP_VENV_DIR="${MCP_VENV_DIR:-$ROOT_DIR/.venv}"
MCP_RUNTIME_DIR="${MCP_RUNTIME_DIR:-$ROOT_DIR/.run}"
MCP_LOG_DIR="${MCP_LOG_DIR:-$MCP_RUNTIME_DIR/logs}"
MCP_GPT_HOME="${MCP_GPT_HOME:-$HOME/.GPT}"
MCP_BOOTSTRAP_MAX_CHARS="${MCP_BOOTSTRAP_MAX_CHARS:-100000}"
MCP_GOAL_REMINDER_SECONDS="${MCP_GOAL_REMINDER_SECONDS:-900}"
MCP_WATCH_IMAGE_MAX_BYTES="${MCP_WATCH_IMAGE_MAX_BYTES:-20971520}"
MCP_START_TIMEOUT="${MCP_START_TIMEOUT:-30}"
MCP_SKIP_NGROK="${MCP_SKIP_NGROK:-0}"
MCP_AUTO_SETUP="${MCP_AUTO_SETUP:-1}"
MCP_AUTO_INSTALL_NGROK="${MCP_AUTO_INSTALL_NGROK:-0}"
MCP_BEARER_TOKEN="${MCP_BEARER_TOKEN:-}"
MCP_DASHBOARD_TOKEN="${MCP_DASHBOARD_TOKEN:-$MCP_BEARER_TOKEN}"
MCP_ALLOW_UNAUTHENTICATED="${MCP_ALLOW_UNAUTHENTICATED:-${MCP_ALLOW_UNAUTHENTICATED_PUBLIC:-0}}"
NGROK_BIN="${NGROK_BIN:-ngrok}"
NGROK_UPSTREAM="${NGROK_UPSTREAM:-http://127.0.0.1:$MCP_PORT}"

[[ "$CLI_LOCAL_ONLY" == 0 ]] || MCP_SKIP_NGROK=1
[[ "$MCP_PORT" =~ ^[0-9]+$ ]] || die "MCP_PORT must be an integer: $MCP_PORT"
(( MCP_PORT >= 1 && MCP_PORT <= 65535 )) || die 'MCP_PORT must be between 1 and 65535'
[[ "$MCP_START_TIMEOUT" =~ ^[0-9]+$ ]] || die 'MCP_START_TIMEOUT must be a non-negative integer'
[[ "$MCP_GOAL_REMINDER_SECONDS" =~ ^[1-9][0-9]*$ ]] || die 'MCP_GOAL_REMINDER_SECONDS must be a positive integer'
[[ "$MCP_WATCH_IMAGE_MAX_BYTES" =~ ^[1-9][0-9]*$ ]] || die 'MCP_WATCH_IMAGE_MAX_BYTES must be a positive integer'
[[ "$MCP_PATH" == /* ]] || die 'MCP_PATH must begin with /'
case "$MCP_TRANSPORT" in
  streamable-http|sse) ;;
  stdio) die 'start.sh manages an HTTP listener; run terminal_mcp.py directly for stdio' ;;
  *) die "unsupported MCP_TRANSPORT: $MCP_TRANSPORT" ;;
esac

export MCP_WORKSPACE MCP_LOG_DIR MCP_GPT_HOME MCP_BOOTSTRAP_MAX_CHARS MCP_GOAL_REMINDER_SECONDS MCP_WATCH_IMAGE_MAX_BYTES MCP_BEARER_TOKEN MCP_DASHBOARD_TOKEN MCP_ALLOW_UNAUTHENTICATED
mkdir -p "$MCP_RUNTIME_DIR" "$MCP_LOG_DIR"

select_python() {
  if [[ -n "${MCP_PYTHON:-}" ]]; then
    printf '%s\n' "$MCP_PYTHON"
  elif [[ -x "$MCP_VENV_DIR/bin/python" ]]; then
    printf '%s\n' "$MCP_VENV_DIR/bin/python"
  else
    command -v python3 || true
  fi
}

runtime_ready() {
  local candidate=$1
  [[ -n "$candidate" && -x "$candidate" ]] || return 1
  "$candidate" - <<'PY' >/dev/null 2>&1
import importlib
import sys
if sys.version_info < (3, 11):
    raise SystemExit(1)
for name in ("mcp", "uvicorn", "starlette", "httpx"):
    importlib.import_module(name)
PY
}

if [[ "$CLI_SETUP_ONLY" == 1 ]]; then
  "$ROOT_DIR/setup.sh"
  echo "Setup complete: $MCP_VENV_DIR/bin/python"
  exit 0
fi

if [[ -z "$MCP_BEARER_TOKEN" && "$MCP_ALLOW_UNAUTHENTICATED" != 1 ]]; then
  cat >&2 <<'EOF'
Refusing to start an HTTP terminal MCP without authentication.
Run ./install.sh --configure-only to generate private bearer tokens, or explicitly
set MCP_ALLOW_UNAUTHENTICATED=1 only after accepting the development risk.
EOF
  exit 1
fi

if [[ "$CLI_CHECK_ONLY" == 1 ]]; then
  "$ROOT_DIR/setup.sh" --check
  if [[ "$MCP_SKIP_NGROK" != 1 ]]; then
    command -v "$NGROK_BIN" >/dev/null 2>&1 || die 'ngrok check failed'
  fi
  echo 'Preflight check passed.'
  exit 0
fi

PYTHON="$(select_python)"
if ! runtime_ready "$PYTHON"; then
  if [[ "$MCP_AUTO_SETUP" == 1 ]]; then
    echo 'Python runtime is missing or incomplete; running setup.sh...' >&2
    "$ROOT_DIR/setup.sh"
    PYTHON="$MCP_VENV_DIR/bin/python"
  else
    die 'Python 3.11+ or required packages are missing. Run ./store.sh or ./setup.sh.'
  fi
fi
runtime_ready "$PYTHON" || die 'runtime validation failed after setup'

if [[ "$MCP_SKIP_NGROK" != 1 ]] && ! command -v "$NGROK_BIN" >/dev/null 2>&1; then
  if [[ "$MCP_AUTO_INSTALL_NGROK" == 1 ]]; then
    echo 'ngrok is missing; installing it with store.sh...' >&2
    "$ROOT_DIR/store.sh" --skip-system-packages --with-ngrok
    export PATH="$HOME/.local/bin:$PATH"
    hash -r
  fi
fi
if [[ "$MCP_SKIP_NGROK" != 1 ]] && ! command -v "$NGROK_BIN" >/dev/null 2>&1; then
  die 'ngrok was not found. Run ./store.sh --with-ngrok, set NGROK_BIN, or use --local-only.'
fi

if command -v flock >/dev/null 2>&1; then
  exec 8>"$MCP_RUNTIME_DIR/start.lock"
  flock -n 8 || die 'another start.sh instance is already running'
fi

SERVER_LOG="$MCP_LOG_DIR/server.log"
NGROK_LOG="$MCP_LOG_DIR/ngrok.log"
SERVER_PID=""
NGROK_PID=""

cleanup() {
  local status=$?
  trap - EXIT INT TERM HUP
  if [[ -n "$NGROK_PID" ]] && kill -0 "$NGROK_PID" 2>/dev/null; then
    kill "$NGROK_PID" 2>/dev/null || true
    wait "$NGROK_PID" 2>/dev/null || true
  fi
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -f "$MCP_RUNTIME_DIR/server.pid" "$MCP_RUNTIME_DIR/ngrok.pid"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM HUP

probe_host="$MCP_HOST"
if [[ "$probe_host" == 0.0.0.0 ]]; then
  probe_host=127.0.0.1
elif [[ "$probe_host" == :: ]]; then
  probe_host=::1
fi

if ! "$PYTHON" - "$MCP_HOST" "$MCP_PORT" <<'PY'
import socket
import sys
host = sys.argv[1]
port = int(sys.argv[2])
family = socket.AF_INET6 if ":" in host else socket.AF_INET
sock = socket.socket(family, socket.SOCK_STREAM)
try:
    sock.bind((host, port))
except OSError as exc:
    print(exc, file=sys.stderr)
    raise SystemExit(1)
finally:
    sock.close()
PY
then
  die "port $MCP_HOST:$MCP_PORT is already in use; refusing to disturb the existing service"
fi

: >"$SERVER_LOG"
echo "Starting Terminal MCP on http://$MCP_HOST:$MCP_PORT$MCP_PATH" >&2
"$PYTHON" "$ROOT_DIR/terminal_mcp.py" \
  --transport "$MCP_TRANSPORT" \
  --host "$MCP_HOST" \
  --port "$MCP_PORT" \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
printf '%s\n' "$SERVER_PID" >"$MCP_RUNTIME_DIR/server.pid"

server_ready=0
deadline=$((SECONDS + MCP_START_TIMEOUT))
while (( SECONDS < deadline )); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo 'Terminal MCP exited during startup.' >&2
    tail -n 80 "$SERVER_LOG" >&2 || true
    exit 1
  fi
  if "$PYTHON" - "$probe_host" "$MCP_PORT" <<'PY' >/dev/null 2>&1
import socket
import sys
with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=0.5):
    pass
PY
  then
    server_ready=1
    break
  fi
  sleep 0.25
done

if [[ "$server_ready" != 1 ]]; then
  echo "Terminal MCP did not become ready within ${MCP_START_TIMEOUT}s." >&2
  tail -n 80 "$SERVER_LOG" >&2 || true
  exit 1
fi

local_endpoint="http://$probe_host:$MCP_PORT$MCP_PATH"
echo "Local MCP endpoint: $local_endpoint"
echo "Server log: $SERVER_LOG"
if [[ -n "$MCP_BEARER_TOKEN" ]]; then
  echo 'Authentication: Authorization: Bearer <MCP_BEARER_TOKEN>'
else
  echo 'WARNING: authentication is disabled by explicit override' >&2
fi

if [[ "$MCP_SKIP_NGROK" == 1 ]]; then
  echo 'ngrok disabled'
  wait "$SERVER_PID"
  exit $?
fi

ngrok_args=(http "$NGROK_UPSTREAM" --log=stdout --log-format=json --inspect=false)
[[ -z "${NGROK_AUTHTOKEN:-}" ]] || ngrok_args+=(--authtoken "$NGROK_AUTHTOKEN")
[[ -z "${NGROK_URL:-}" ]] || ngrok_args+=(--url "$NGROK_URL")
[[ -z "${NGROK_TRAFFIC_POLICY_FILE:-}" ]] || ngrok_args+=(--traffic-policy-file "$NGROK_TRAFFIC_POLICY_FILE")

: >"$NGROK_LOG"
"$NGROK_BIN" "${ngrok_args[@]}" >"$NGROK_LOG" 2>&1 &
NGROK_PID=$!
printf '%s\n' "$NGROK_PID" >"$MCP_RUNTIME_DIR/ngrok.pid"

extract_public_url() {
  "$PYTHON" - "$NGROK_LOG" <<'PY'
import json
import re
import sys
from pathlib import Path
path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(1)
candidates = []
for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
    try:
        item = json.loads(raw)
    except json.JSONDecodeError:
        item = None
    if isinstance(item, dict) and isinstance(item.get("url"), str):
        candidates.append(item["url"])
    candidates.extend(re.findall(r"https://[^\s\"']+", raw))
for value in reversed(candidates):
    if value.startswith("https://"):
        print(value.rstrip("/"))
        raise SystemExit(0)
raise SystemExit(1)
PY
}

public_url=""
deadline=$((SECONDS + MCP_START_TIMEOUT))
while (( SECONDS < deadline )); do
  if ! kill -0 "$NGROK_PID" 2>/dev/null; then
    echo 'ngrok exited during startup.' >&2
    tail -n 80 "$NGROK_LOG" >&2 || true
    exit 1
  fi
  if public_url="$(extract_public_url 2>/dev/null)"; then
    break
  fi
  sleep 0.25
done

if [[ -z "$public_url" ]]; then
  echo "ngrok did not publish an HTTPS URL within ${MCP_START_TIMEOUT}s." >&2
  tail -n 80 "$NGROK_LOG" >&2 || true
  exit 1
fi

printf '\nPublic MCP endpoint: %s%s\n' "$public_url" "$MCP_PATH"
if [[ -n "$MCP_BEARER_TOKEN" ]]; then
  echo 'Authentication: Authorization: Bearer <MCP_BEARER_TOKEN>'
elif [[ -n "${NGROK_TRAFFIC_POLICY_FILE:-}" ]]; then
  echo 'Authentication: ngrok Traffic Policy only; MCP bearer auth disabled by explicit override'
else
  echo 'WARNING: public endpoint is unauthenticated by explicit override' >&2
fi
echo "ngrok log: $NGROK_LOG"
echo 'Press Ctrl+C to stop both processes.'

while kill -0 "$SERVER_PID" 2>/dev/null && kill -0 "$NGROK_PID" 2>/dev/null; do
  sleep 1
done

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo 'Terminal MCP stopped unexpectedly.' >&2
  tail -n 80 "$SERVER_LOG" >&2 || true
  exit 1
fi

echo 'ngrok stopped unexpectedly.' >&2
tail -n 80 "$NGROK_LOG" >&2 || true
exit 1
