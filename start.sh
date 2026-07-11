#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

MCP_HOST="${MCP_HOST:-127.0.0.1}"
MCP_PORT="${MCP_PORT:-8000}"
MCP_TRANSPORT="${MCP_TRANSPORT:-streamable-http}"
MCP_PATH="${MCP_PATH:-/mcp}"
MCP_WORKSPACE="${MCP_WORKSPACE:-$HOME/mcp_workspace}"
MCP_RUNTIME_DIR="${MCP_RUNTIME_DIR:-$ROOT_DIR/.run}"
MCP_LOG_DIR="${MCP_LOG_DIR:-$MCP_RUNTIME_DIR/logs}"
MCP_START_TIMEOUT="${MCP_START_TIMEOUT:-30}"
MCP_SKIP_NGROK="${MCP_SKIP_NGROK:-0}"
MCP_BEARER_TOKEN="${MCP_BEARER_TOKEN:-}"
MCP_ALLOW_UNAUTHENTICATED_PUBLIC="${MCP_ALLOW_UNAUTHENTICATED_PUBLIC:-0}"
NGROK_BIN="${NGROK_BIN:-ngrok}"
NGROK_UPSTREAM="${NGROK_UPSTREAM:-http://127.0.0.1:$MCP_PORT}"

export MCP_WORKSPACE MCP_LOG_DIR MCP_BEARER_TOKEN
mkdir -p "$MCP_RUNTIME_DIR" "$MCP_LOG_DIR"

if [[ -n "${MCP_PYTHON:-}" ]]; then
  PYTHON="$MCP_PYTHON"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON="$ROOT_DIR/.venv/bin/python"
else
  PYTHON="$(command -v python3 || true)"
fi

if [[ -z "$PYTHON" || ! -x "$PYTHON" ]]; then
  echo "Python 3 was not found. Run ./setup.sh first." >&2
  exit 1
fi

if ! "$PYTHON" -c 'import mcp, uvicorn, starlette' >/dev/null 2>&1; then
  echo "Required Python packages are missing for $PYTHON. Run ./setup.sh first." >&2
  exit 1
fi

if [[ "$MCP_SKIP_NGROK" != "1" ]] && ! command -v "$NGROK_BIN" >/dev/null 2>&1; then
  echo "ngrok was not found. Install it, set NGROK_BIN, or use MCP_SKIP_NGROK=1." >&2
  exit 1
fi

if [[ "$MCP_SKIP_NGROK" != "1" \
      && -z "$MCP_BEARER_TOKEN" \
      && -z "${NGROK_TRAFFIC_POLICY_FILE:-}" \
      && "$MCP_ALLOW_UNAUTHENTICATED_PUBLIC" != "1" ]]; then
  cat >&2 <<'EOF'
Refusing to expose a full terminal MCP without authentication.
Set MCP_BEARER_TOKEN, configure NGROK_TRAFFIC_POLICY_FILE, or explicitly set
MCP_ALLOW_UNAUTHENTICATED_PUBLIC=1 after accepting the risk.
EOF
  exit 1
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
if [[ "$probe_host" == "0.0.0.0" ]]; then
  probe_host="127.0.0.1"
elif [[ "$probe_host" == "::" ]]; then
  probe_host="::1"
fi

if "$PYTHON" - "$MCP_HOST" "$MCP_PORT" <<'PY'
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
  :
else
  echo "Port $MCP_HOST:$MCP_PORT is already in use. Refusing to disturb the existing service." >&2
  exit 1
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
    echo "Terminal MCP exited during startup." >&2
    tail -n 80 "$SERVER_LOG" >&2 || true
    exit 1
  fi
  if "$PYTHON" - "$probe_host" "$MCP_PORT" <<'PY' >/dev/null 2>&1
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
with socket.create_connection((host, port), timeout=0.5):
    pass
PY
  then
    server_ready=1
    break
  fi
  sleep 0.25
done

if [[ "$server_ready" != "1" ]]; then
  echo "Terminal MCP did not become ready within ${MCP_START_TIMEOUT}s." >&2
  tail -n 80 "$SERVER_LOG" >&2 || true
  exit 1
fi

local_endpoint="http://$probe_host:$MCP_PORT$MCP_PATH"
echo "Local MCP endpoint: $local_endpoint"
echo "Server log: $SERVER_LOG"

if [[ "$MCP_SKIP_NGROK" == "1" ]]; then
  echo "ngrok disabled by MCP_SKIP_NGROK=1"
  wait "$SERVER_PID"
  exit $?
fi

ngrok_args=(http "$NGROK_UPSTREAM" --log=stdout --log-format=json --inspect=false)
if [[ -n "${NGROK_AUTHTOKEN:-}" ]]; then
  ngrok_args+=(--authtoken "$NGROK_AUTHTOKEN")
fi
if [[ -n "${NGROK_URL:-}" ]]; then
  ngrok_args+=(--url "$NGROK_URL")
fi
if [[ -n "${NGROK_TRAFFIC_POLICY_FILE:-}" ]]; then
  ngrok_args+=(--traffic-policy-file "$NGROK_TRAFFIC_POLICY_FILE")
fi

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
    if isinstance(item, dict):
        value = item.get("url")
        if isinstance(value, str):
            candidates.append(value)
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
    echo "ngrok exited during startup." >&2
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
  echo "Authentication: Authorization: Bearer <MCP_BEARER_TOKEN>"
elif [[ -n "${NGROK_TRAFFIC_POLICY_FILE:-}" ]]; then
  echo "Authentication: delegated to ngrok Traffic Policy"
else
  echo "WARNING: public endpoint is unauthenticated by explicit override" >&2
fi
echo "ngrok log: $NGROK_LOG"
echo "Press Ctrl+C to stop both processes."

while kill -0 "$SERVER_PID" 2>/dev/null && kill -0 "$NGROK_PID" 2>/dev/null; do
  sleep 1
done

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
  echo "Terminal MCP stopped unexpectedly." >&2
  tail -n 80 "$SERVER_LOG" >&2 || true
  exit 1
fi

echo "ngrok stopped unexpectedly." >&2
tail -n 80 "$NGROK_LOG" >&2 || true
exit 1
