#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "$TEMP_DIR"; rm -f -- "$ROOT_DIR/.service-mode"' EXIT

fail() { printf 'test_service_workflow.sh: %s\n' "$*" >&2; exit 1; }

make_fakes() {
  local bin=$1 log=$2
  mkdir -p "$bin"
  cat >"$bin/systemctl" <<'EOF'
#!/usr/bin/env bash
printf 'systemctl %s\n' "$*" >>"$FAKE_COMMAND_LOG"
exit 0
EOF
  cat >"$bin/sudo" <<'EOF'
#!/usr/bin/env bash
printf 'sudo %s\n' "$*" >>"$FAKE_COMMAND_LOG"
[[ "${1:-}" == -v ]] && exit 0
exec "$@"
EOF
  cat >"$bin/journalctl" <<'EOF'
#!/usr/bin/env bash
printf 'journalctl %s\n' "$*" >>"$FAKE_COMMAND_LOG"
exit 0
EOF
  cat >"$bin/curl" <<'EOF'
#!/usr/bin/env bash
if [[ "${FAKE_CURL_ALWAYS_FAIL:-0}" == 1 ]]; then
  exit 7
fi
if [[ -n "${FAKE_CURL_FAIL_FILE:-}" && "${FAKE_CURL_FAIL_UNTIL:-0}" =~ ^[0-9]+$ ]]; then
  count=0
  [[ ! -f "$FAKE_CURL_FAIL_FILE" ]] || count="$(cat "$FAKE_CURL_FAIL_FILE")"
  if (( count < FAKE_CURL_FAIL_UNTIL )); then
    printf '%s\n' "$((count + 1))" >"$FAKE_CURL_FAIL_FILE"
    exit 7
  fi
fi
args=" $* "
case "$args" in
  *"/mcp"*) [[ "$args" == *"Authorization:"* ]] && printf 200 || printf 401 ;;
  *"/healthz"*) [[ "$args" == *"Authorization:"* ]] && printf 200 || printf 401 ;;
  *"/dashboard"*) [[ "$args" == *"Authorization:"* ]] && printf 200 || printf 401 ;;
  *) printf 404 ;;
esac
EOF
  chmod +x "$bin"/*
  : >"$log"
}

fake_venv="$TEMP_DIR/venv"
mkdir -p "$fake_venv/bin"
printf '#!/usr/bin/env bash\nexec python3 "$@"\n' >"$fake_venv/bin/python"
chmod +x "$fake_venv/bin/python"
seed="$TEMP_DIR/seed.env"
cat >"$seed" <<EOF
MCP_HOST=0.0.0.0
MCP_PORT=9999
MCP_PATH=/unsafe
MCP_BEARER_TOKEN=$(printf '%064d' 1)
MCP_DASHBOARD_TOKEN=$(printf '%064d' 2)
EOF
chmod 600 "$seed"

system_bin="$TEMP_DIR/system-bin"
system_log="$TEMP_DIR/system.log"
make_fakes "$system_bin" "$system_log"
system_root="$TEMP_DIR/system-root"
system_home="$TEMP_DIR/system-home"
PATH="$system_bin:$PATH" FAKE_COMMAND_LOG="$system_log" TERMINAL_MCP_TEST_EUID=1000 \
  TERMINAL_MCP_SYSTEM_ROOT="$system_root" TERMINAL_MCP_SEED_ENV_FILE="$seed" \
  TERMINAL_MCP_INVOKING_HOME="$system_home" MCP_VENV_DIR="$fake_venv" \
  MCP_WORKSPACE="$TEMP_DIR/system%workspace" MCP_GPT_HOME="$TEMP_DIR/system-gpt" \
  "$ROOT_DIR/service-install.sh" --service system >"$TEMP_DIR/system.out"

unit="$system_root/etc/systemd/system/terminal-mcp.service"
env_file="$system_root/etc/terminal-mcp/terminal-mcp.env"
system_command="$system_root/usr/local/bin/terminal-mcp"
[[ -f "$unit" && -f "$env_file" ]] || fail 'system mode did not render unit and config'
[[ -L "$system_command" ]] || fail 'system mode did not install terminal-mcp command'
grep -Fq 'User=' "$unit" || fail 'system unit lacks User'
grep -Fq 'ProtectSystem=strict' "$unit" || fail 'system unit lacks hardening'
grep -Fq 'UMask=0077' "$unit" || fail 'system unit lacks restrictive umask'
! grep -Fq 'PrivateDevices=true' "$unit" || fail 'system unit blocks device-backed tools'
! grep -Fq 'ProtectHome=read-only' "$unit" || fail 'system unit blocks required user configuration'
grep -Fq 'system%%workspace' "$unit" || fail 'systemd specifier characters were not escaped in paths'
grep -Fxq 'MCP_HOST=127.0.0.1' "$env_file" || fail 'system listener is not forced to loopback'
grep -Fxq 'MCP_PORT=8011' "$env_file" || fail 'system service port was not normalized'
grep -Fxq 'MCP_PATH=/mcp' "$env_file" || fail 'system MCP path was not normalized'
[[ "$(stat -c '%a' "$env_file")" == 600 ]] || fail 'system config is not mode 600'
grep -Fq 'sudo systemctl show-environment' "$system_log" || fail 'system manager availability was not checked'
grep -Fq 'sudo systemctl enable terminal-mcp.service' "$system_log" || fail 'system enable was not invoked through sudo'
grep -Fq 'sudo systemctl restart terminal-mcp.service' "$system_log" || fail 'system restart was not invoked through sudo'
! grep -qi ngrok "$unit" || fail 'daemon unit references ngrok'
if command -v systemd-analyze >/dev/null 2>&1; then
  systemd-analyze verify "$unit" >/dev/null || fail 'rendered system unit failed systemd-analyze verify'
fi

sed -i 's/^MCP_BEARER_TOKEN=.*/MCP_BEARER_TOKEN=preserved-system-secret-1234567890/' "$env_file"
PATH="$system_bin:$PATH" FAKE_COMMAND_LOG="$system_log" TERMINAL_MCP_TEST_EUID=1000 \
  TERMINAL_MCP_SYSTEM_ROOT="$system_root" TERMINAL_MCP_SEED_ENV_FILE="$seed" \
  TERMINAL_MCP_INVOKING_HOME="$system_home" MCP_VENV_DIR="$fake_venv" \
  MCP_WORKSPACE="$TEMP_DIR/system%workspace" MCP_GPT_HOME="$TEMP_DIR/system-gpt" \
  "$ROOT_DIR/service-install.sh" --service system >/dev/null
grep -Fq 'MCP_BEARER_TOKEN=preserved-system-secret-1234567890' "$env_file" || fail 'system reinstall replaced existing secret'

user_bin="$TEMP_DIR/user-bin"
user_log="$TEMP_DIR/user.log"
make_fakes "$user_bin" "$user_log"
user_home="$TEMP_DIR/user-home"
config_home="$TEMP_DIR/xdg-config"
state_home="$TEMP_DIR/xdg-state"
PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" TERMINAL_MCP_TEST_EUID=1000 \
  TERMINAL_MCP_INVOKING_HOME="$user_home" XDG_CONFIG_HOME="$config_home" XDG_STATE_HOME="$state_home" \
  TERMINAL_MCP_SEED_ENV_FILE="$seed" MCP_VENV_DIR="$fake_venv" \
  MCP_WORKSPACE="$TEMP_DIR/user-workspace" MCP_GPT_HOME="$TEMP_DIR/user-gpt" \
  "$ROOT_DIR/service-install.sh" --service user >"$TEMP_DIR/user.out" 2>"$TEMP_DIR/user.err"

user_unit="$config_home/systemd/user/terminal-mcp.service"
user_env="$config_home/terminal-mcp/terminal-mcp.env"
user_command="$user_home/.local/bin/terminal-mcp"
[[ -f "$user_unit" && -f "$user_env" ]] || fail 'user mode did not render unit and config'
[[ -L "$user_command" ]] || fail 'user mode did not install terminal-mcp command'
! grep -q '^User=' "$user_unit" || fail 'user unit must not set User'
! grep -Fq 'ProtectSystem=strict' "$user_unit" || fail 'user unit uses mount namespace hardening that may be unavailable'
grep -Fq 'UMask=0077' "$user_unit" || fail 'user unit lacks restrictive umask'
grep -Fq 'sudo systemctl disable --now terminal-mcp.service' "$user_log" || fail 'switching modes did not stop the previous system service'
grep -Fq 'systemctl --user enable terminal-mcp.service' "$user_log" || fail 'user enable was not invoked'
grep -Fq 'systemctl --user restart terminal-mcp.service' "$user_log" || fail 'user restart was not invoked'
grep -Fq 'Service and authenticated HTTP checks passed.' "$TEMP_DIR/user.out" || fail 'verification success missing'
grep -Fq 'loginctl enable-linger' "$TEMP_DIR/user.err" || fail 'user-mode startup limitation was not reported'
if command -v systemd-analyze >/dev/null 2>&1; then
  systemd-analyze verify "$user_unit" >/dev/null || fail 'rendered user unit failed systemd-analyze verify'
fi

PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" TERMINAL_MCP_TEST_EUID=1000 \
  TERMINAL_MCP_SUDO=missing-sudo TERMINAL_MCP_INVOKING_HOME="$user_home" \
  XDG_CONFIG_HOME="$config_home" XDG_STATE_HOME="$state_home" \
  TERMINAL_MCP_SEED_ENV_FILE="$seed" MCP_VENV_DIR="$fake_venv" \
  MCP_WORKSPACE="$TEMP_DIR/user-workspace" MCP_GPT_HOME="$TEMP_DIR/user-gpt" \
  "$ROOT_DIR/service-install.sh" --service auto >/dev/null 2>/dev/null
grep -Fq 'SERVICE_MODE=user' "$ROOT_DIR/.service-mode" || fail 'auto mode did not fall back to user systemd'

dashboard="$(PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" "$user_command" dashboard)"
[[ "$dashboard" == http://127.0.0.1:8011/dashboard ]] || fail 'dashboard command returned wrong URL'
credentials="$(PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" "$user_command" credentials)"
[[ "$credentials" != *MCP_BEARER_TOKEN=* ]] || fail 'credentials printed a secret by default'
shown="$(PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" "$user_command" credentials --show)"
grep -Fq 'MCP_BEARER_TOKEN=' <<<"$shown" || fail 'credentials --show omitted bearer token'
PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" "$user_command" start
PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" "$user_command" stop
PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" "$user_command" restart
grep -Fq 'systemctl --user restart terminal-mcp.service' "$user_log" || fail 'manager did not use user service mode'

retry_file="$TEMP_DIR/curl-retries"
PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" FAKE_CURL_FAIL_FILE="$retry_file" FAKE_CURL_FAIL_UNTIL=2 \
  MCP_SERVICE_VERIFY_TIMEOUT=5 "$user_command" verify >/dev/null
[[ "$(cat "$retry_file")" == 2 ]] || fail 'verification did not retry transient startup failures'
if PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" FAKE_CURL_ALWAYS_FAIL=1 \
  MCP_SERVICE_VERIFY_TIMEOUT=0 "$user_command" verify >"$TEMP_DIR/fail.out" 2>"$TEMP_DIR/fail.err"; then
  fail 'verification claimed success when HTTP was unavailable'
fi
grep -Fq 'HTTP verification failed' "$TEMP_DIR/fail.err" || fail 'verification failure diagnostic is missing'

if PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" TERMINAL_MCP_TEST_EUID=1000 \
  TERMINAL_MCP_INVOKING_USER=root TERMINAL_MCP_INVOKING_GROUP=root TERMINAL_MCP_INVOKING_HOME=/root \
  TERMINAL_MCP_SEED_ENV_FILE="$seed" MCP_VENV_DIR="$fake_venv" \
  "$ROOT_DIR/service-install.sh" --service user >"$TEMP_DIR/root.out" 2>"$TEMP_DIR/root.err"; then
  fail 'installer allowed the remote terminal daemon to run as root'
fi
grep -Fq 'refusing to run a remote terminal service as root' "$TEMP_DIR/root.err" || fail 'root-service refusal diagnostic is missing'

none_output="$("$ROOT_DIR/service-install.sh" --service none)"
grep -Fq 'service installation disabled' <<<"$none_output" || fail 'none mode output missing'

install_copy="$TEMP_DIR/install-copy"
mkdir -p "$install_copy/systemd" "$install_copy/.venv/bin"
cp "$ROOT_DIR/install.sh" "$ROOT_DIR/service-install.sh" "$ROOT_DIR/terminal-mcp-service" \
  "$ROOT_DIR/terminal_mcp.py" "$ROOT_DIR/.env.example" "$install_copy/"
cp "$ROOT_DIR/systemd/"*.in "$install_copy/systemd/"
cp "$fake_venv/bin/python" "$install_copy/.venv/bin/python"
cat >"$install_copy/store.sh" <<'EOF'
#!/usr/bin/env bash
printf '[store] fake dependency installation\n'
EOF
chmod +x "$install_copy/"*.sh "$install_copy/terminal-mcp-service"
install_home="$TEMP_DIR/install-home"
install_output="$(
  PATH="$user_bin:$PATH" FAKE_COMMAND_LOG="$user_log" TERMINAL_MCP_TEST_EUID=1000 \
    TERMINAL_MCP_INVOKING_HOME="$install_home" XDG_CONFIG_HOME="$TEMP_DIR/install-config" \
    XDG_STATE_HOME="$TEMP_DIR/install-state" MCP_WORKSPACE="$TEMP_DIR/install-workspace" \
    MCP_GPT_HOME="$TEMP_DIR/install-gpt" "$install_copy/install.sh" --service user 2>"$TEMP_DIR/install.err"
)"
grep -Fq 'MCP URL:       http://127.0.0.1:8011/mcp' <<<"$install_output" || fail 'installer final MCP URL missing'
grep -Fq 'Dashboard URL: http://127.0.0.1:8011/dashboard' <<<"$install_output" || fail 'installer final dashboard URL missing'
grep -Fq "$install_home/.local/bin/terminal-mcp status" <<<"$install_output" || fail 'installer did not print installed management command'
grep -Fq 'ngrok is never' <<<"$install_output" || fail 'installer did not explain daemon tunnel separation'
[[ "$install_output" != *MCP_BEARER_TOKEN=* ]] || fail 'installer final output disclosed a secret'
grep -Fq 'loginctl enable-linger' <<<"$install_output" || fail 'installer omitted user-service boot guidance'

printf 'system and user service workflows passed\n'
