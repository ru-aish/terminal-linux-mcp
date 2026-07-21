#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
python_bin="${PYTHON_BIN:-$repo_dir/.venv/bin/python}"
workspace="$(mktemp -d)"
unit="chat-runtime-isolated-test-$$.service"
state="$workspace/runtime.json"
generation_file="$workspace/generation.txt"
supervisor_status="$workspace/supervisor.json"
webview_port=5187
cdp_port=9234
report="$repo_dir/.run/chat-runtime-fault-report.json"
mkdir -p "$repo_dir/.run"

cleanup() {
  systemctl --user stop "$unit" >/dev/null 2>&1 || true
  rm -rf "$workspace"
}
trap cleanup EXIT

port_pid() {
  local port="$1"
  ss -ltnp "sport = :$port" 2>/dev/null \
    | grep -oE 'pid=[0-9]+' | cut -d= -f2 | sort -nu | paste -sd, - || true
}

live_webview_before="$(port_pid 5175)"
live_cdp_before="$(port_pid 9222)"
[[ -z "$(port_pid "$webview_port")" && -z "$(port_pid "$cdp_port")" ]] || {
  echo "isolated fault-test ports are occupied" >&2
  exit 1
}

runtime_command="$python_bin $repo_dir/tests/fake_chat_runtime.py launcher --state $state --generation $generation_file --webview-port $webview_port --cdp-port $cdp_port"

systemd-run --user \
  --unit="$unit" \
  --collect \
  --property=KillMode=control-group \
  --property="WorkingDirectory=$repo_dir" \
  --property=TimeoutStopSec=10s \
  --setenv="CHAT_RUNTIME_COMMAND=$runtime_command" \
  --setenv="CHAT_RUNTIME_APP_ID=codex-runtime-isolated-test" \
  --setenv="CHAT_RUNTIME_STATE_DIR=$workspace/supervisor-state" \
  --setenv="CHAT_RUNTIME_SOCKET_DIR=$workspace/supervisor-socket" \
  --setenv="CHAT_RUNTIME_STATUS_PATH=$supervisor_status" \
  --setenv="CHAT_RUNTIME_WEBVIEW_URL=http://127.0.0.1:$webview_port/index.html" \
  --setenv="CHAT_RUNTIME_CDP_ENDPOINT=http://127.0.0.1:$cdp_port" \
  --setenv="CHAT_RUNTIME_STARTUP_GRACE=1" \
  --setenv="CHAT_RUNTIME_PROBE_INTERVAL=0.5" \
  --setenv="CHAT_RUNTIME_FAILURE_THRESHOLD=2" \
  --setenv="CHAT_RUNTIME_RESTART_BACKOFF=0.5" \
  --setenv="CHAT_RUNTIME_STOP_TIMEOUT=5" \
  --setenv="FAKE_RUNTIME_STATE=$state" \
  --setenv="PYTHONPATH=$repo_dir" \
  "$python_bin" "$repo_dir/tests/runtime_supervisor_harness.py" >/dev/null

json_value() {
  local file="$1" key="$2"
  "$python_bin" - "$file" "$key" <<'PY'
import json, sys
print(json.load(open(sys.argv[1], encoding="utf-8"))[sys.argv[2]])
PY
}

wait_ready() {
  local minimum_generation="$1" deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if [[ -s "$state" && -s "$supervisor_status" ]]; then
      local generation supervisor_state
      generation="$(json_value "$state" generation 2>/dev/null || echo 0)"
      supervisor_state="$(json_value "$supervisor_status" state 2>/dev/null || true)"
      if [[ "$generation" -ge "$minimum_generation" && "$supervisor_state" == "ready" ]]; then
        return 0
      fi
    fi
    sleep 0.2
  done
  systemctl --user status "$unit" --no-pager >&2 || true
  [[ -s "$supervisor_status" ]] && cat "$supervisor_status" >&2 || true
  return 1
}

assert_generation_cgroup() {
  local cgroup pid key
  cgroup="$(systemctl --user show "$unit" -p ControlGroup --value)"
  for key in launcher_pid electron_pid renderer_pid webview_pid cdp_pid; do
    pid="$(json_value "$state" "$key")"
    grep -Fq "$cgroup" "/proc/$pid/cgroup" || {
      echo "$key pid=$pid escaped test service cgroup $cgroup" >&2
      cat "/proc/$pid/cgroup" >&2
      exit 1
    }
  done
}

inject_layer_failure() {
  local key="$1" label="$2" before pid after
  before="$(json_value "$state" generation)"
  pid="$(json_value "$state" "$key")"
  kill -KILL "$pid"
  wait_ready "$((before + 1))"
  after="$(json_value "$state" generation)"
  assert_generation_cgroup
  printf '%s:%s:%s\n' "$label" "$before" "$after" >> "$workspace/results.txt"
}

wait_ready 1
assert_generation_cgroup
inject_layer_failure webview_pid webview
inject_layer_failure renderer_pid renderer
inject_layer_failure electron_pid electron

before_restart="$(json_value "$state" generation)"
systemctl --user restart "$unit"
wait_ready "$((before_restart + 1))"
after_restart="$(json_value "$state" generation)"
assert_generation_cgroup
printf 'service:%s:%s\n' "$before_restart" "$after_restart" >> "$workspace/results.txt"

live_webview_after="$(port_pid 5175)"
live_cdp_after="$(port_pid 9222)"
[[ "$live_webview_after" == "$live_webview_before" ]] || {
  echo "live webview PID changed during isolated test: $live_webview_before -> $live_webview_after" >&2
  exit 1
}
[[ "$live_cdp_after" == "$live_cdp_before" ]] || {
  echo "live CDP PID changed during isolated test: $live_cdp_before -> $live_cdp_after" >&2
  exit 1
}

"$python_bin" - "$workspace/results.txt" "$report" "$unit" "$live_webview_before" "$live_cdp_before" <<'PY'
import json, sys, time
rows = []
for line in open(sys.argv[1], encoding="utf-8"):
    layer, before, after = line.strip().split(":")
    rows.append({"layer": layer, "generation_before": int(before), "generation_after": int(after), "recovered": int(after) > int(before)})
payload = {
    "tested_at": time.time(),
    "unit": sys.argv[3],
    "faults": rows,
    "live_webview_pid": sys.argv[4],
    "live_cdp_pid": sys.argv[5],
    "live_runtime_present": bool(sys.argv[4] or sys.argv[5]),
    "live_runtime_unchanged": True,
}
open(sys.argv[2], "w", encoding="utf-8").write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print(json.dumps(payload, indent=2, sort_keys=True))
PY
