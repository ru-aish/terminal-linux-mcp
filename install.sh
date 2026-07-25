#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${MCP_ENV_FILE:-$ROOT_DIR/.env}"
VENV_DIR="${MCP_VENV_DIR:-$ROOT_DIR/.venv}"
CONFIGURE_ONLY=0
SHOW_SECRETS=0
CHECK_ONLY=0
SERVICE_MODE=auto
STORE_ARGS=()

usage() {
  cat <<'EOF'
Usage: ./install.sh [options]

Install Terminal MCP and create a private .env with generated bearer tokens.
The installer is idempotent: existing non-placeholder secrets are preserved.

Options:
  --configure-only          Create or repair authentication config without installing packages.
  --service MODE            Service mode: auto, system, user, or none (default: auto).
  --no-service              Alias for --service none; safe for CI/development.
  --show-secrets            Print the configured MCP and dashboard tokens, then exit.
  --dev                     Install development/test dependencies.
  --test                    Install development dependencies and run tests.
  --with-ngrok              Install ngrok into ~/.local/bin when missing.
  --skip-system-packages    Do not invoke the system package manager.
  --check                   Validate dependencies and authentication configuration.
  -h, --help                Show this help.

Environment:
  MCP_ENV_FILE              Configuration file path (default: ./.env)
  MCP_VENV_DIR              Virtual environment path (default: ./.venv)
EOF
}

die() {
  printf 'install.sh: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[install] %s\n' "$*"
}

while (($#)); do
  case "$1" in
    --configure-only) CONFIGURE_ONLY=1; SERVICE_MODE=none ;;
    --service)
      shift
      (($#)) || die '--service requires auto, system, user, or none'
      SERVICE_MODE=$1
      ;;
    --service=*) SERVICE_MODE=${1#*=} ;;
    --no-service) SERVICE_MODE=none ;;
    --show-secrets) SHOW_SECRETS=1 ;;
    --check) CHECK_ONLY=1; STORE_ARGS+=("$1") ;;
    --dev|--test|--with-ngrok|--skip-system-packages|--no-system-packages)
      STORE_ARGS+=("$1")
      ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
  shift
done
case "$SERVICE_MODE" in auto|system|user|none) ;; *) die "invalid service mode: $SERVICE_MODE" ;; esac
[[ "$CONFIGURE_ONLY" == 0 ]] || SERVICE_MODE=none

read_env_value() {
  local key=$1 source_file=${2:-$ENV_FILE}
  [[ -f "$source_file" ]] || return 0
  awk -v key="$key" '
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      value = $0
      sub("^[^=]*=", "", value)
      sub("^[[:space:]]+", "", value)
      sub("[[:space:]]+$", "", value)
      print value
      exit
    }
  ' "$source_file"
}

usable_secret() {
  local value=${1:-}
  [[ -n "$value" ]] || return 1
  case "$value" in
    replace-with-*|REPLACE_WITH_*|changeme|CHANGE_ME|change-me) return 1 ;;
  esac
  [[ ${#value} -ge 32 ]]
}

select_token_generator() {
  local candidate
  for candidate in "$VENV_DIR/bin/python" python3 python; do
    if [[ -x "$candidate" ]] || command -v "$candidate" >/dev/null 2>&1; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done
  return 1
}

generate_token() {
  local python_bin
  python_bin="$(select_token_generator || true)"
  if [[ -n "$python_bin" ]]; then
    "$python_bin" - <<'PY'
import secrets
print(secrets.token_hex(32))
PY
    return
  fi
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
    return
  fi
  if command -v od >/dev/null 2>&1 && [[ -r /dev/urandom ]]; then
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
    printf '\n'
    return
  fi
  die 'cannot generate an authentication token; Python 3, OpenSSL, or od with /dev/urandom is required'
}

upsert_secret() {
  local key=$1
  local current token temporary
  current="$(read_env_value "$key")"
  if usable_secret "$current"; then
    return 1
  fi

  token="$(generate_token)"
  [[ "$token" =~ ^[0-9a-fA-F]{64}$ ]] || die "generated $key has an unexpected format"
  temporary="$(mktemp "${ENV_FILE}.tmp.XXXXXX")"
  awk -v key="$key" -v replacement="$key=$token" '
    BEGIN { written = 0 }
    {
      normalized = $0
      sub("^[[:space:]]*#[[:space:]]*", "", normalized)
      if (normalized ~ "^" key "[[:space:]]*=") {
        if (!written) {
          print replacement
          written = 1
        }
        next
      }
      print
    }
    END {
      if (!written) {
        print replacement
      }
    }
  ' "$ENV_FILE" >"$temporary"
  chmod 600 "$temporary"
  mv -f -- "$temporary" "$ENV_FILE"
  return 0
}

configure_auth() {
  local created=0
  umask 077
  mkdir -p "$(dirname -- "$ENV_FILE")"
  if [[ ! -e "$ENV_FILE" ]]; then
    if [[ -f "$ROOT_DIR/.env.example" ]]; then
      cp -- "$ROOT_DIR/.env.example" "$ENV_FILE"
    else
      : >"$ENV_FILE"
    fi
    created=1
  elif [[ ! -f "$ENV_FILE" ]]; then
    die "$ENV_FILE exists but is not a regular file"
  fi
  chmod 600 "$ENV_FILE"

  local bearer_created=0 dashboard_created=0
  if upsert_secret MCP_BEARER_TOKEN; then
    bearer_created=1
  fi
  if upsert_secret MCP_DASHBOARD_TOKEN; then
    dashboard_created=1
  fi

  if (( created || bearer_created || dashboard_created )); then
    log "secured configuration at $ENV_FILE"
  else
    log "preserved existing authentication secrets in $ENV_FILE"
  fi
}

validate_auth() {
  local source_file=${1:-$ENV_FILE} bearer dashboard mode
  [[ -f "$source_file" ]] || die "authentication configuration is missing at $source_file; run ./install.sh --configure-only"
  bearer="$(read_env_value MCP_BEARER_TOKEN "$source_file")"
  dashboard="$(read_env_value MCP_DASHBOARD_TOKEN "$source_file")"
  usable_secret "$bearer" || die 'MCP_BEARER_TOKEN is missing, placeholder, or shorter than 32 characters'
  usable_secret "$dashboard" || die 'MCP_DASHBOARD_TOKEN is missing, placeholder, or shorter than 32 characters'
  mode="$(stat -c '%a' "$source_file" 2>/dev/null || stat -f '%Lp' "$source_file" 2>/dev/null || true)"
  [[ "$mode" == 600 ]] || die "$source_file must have mode 600 (found ${mode:-unknown})"
}

show_secrets() {
  validate_auth
  printf 'MCP_BEARER_TOKEN=%s\n' "$(read_env_value MCP_BEARER_TOKEN)"
  printf 'MCP_DASHBOARD_TOKEN=%s\n' "$(read_env_value MCP_DASHBOARD_TOKEN)"
}

if [[ "$SHOW_SECRETS" == 1 ]]; then
  show_secrets
  exit 0
fi

if [[ "$CONFIGURE_ONLY" != 1 ]]; then
  "$ROOT_DIR/store.sh" "${STORE_ARGS[@]}"
fi

if [[ "$CHECK_ONLY" == 1 ]]; then
  validate_auth
  log 'dependencies and authentication configuration are ready'
  exit 0
fi

configure_auth
validate_auth

if [[ "$CONFIGURE_ONLY" != 1 && "$SERVICE_MODE" != none ]]; then
  "$ROOT_DIR/service-install.sh" --service "$SERVICE_MODE"
fi

if [[ "$SERVICE_MODE" == none ]]; then
cat <<EOF

Terminal MCP dependencies and authentication are configured.
No service was installed (--service none).

Start locally:
  cd "$ROOT_DIR" && ./start.sh --local-only

Show the credentials when configuring a client:
  cd "$ROOT_DIR" && ./install.sh --show-secrets

The secrets are stored with mode 600 in:
  $ENV_FILE
EOF
else
  service_env="$(awk -F= '$1 == "SERVICE_ENV_FILE" {sub("^[^=]*=", ""); print; exit}' "$ROOT_DIR/.service-mode")"
  service_command="$(awk -F= '$1 == "SERVICE_COMMAND" {sub("^[^=]*=", ""); print; exit}' "$ROOT_DIR/.service-mode")"
  service_mode="$(awk -F= '$1 == "SERVICE_MODE" {sub("^[^=]*=", ""); print; exit}' "$ROOT_DIR/.service-mode")"
  host="$(read_env_value MCP_HOST "$service_env")"
  port="$(read_env_value MCP_PORT "$service_env")"
  host="${host:-127.0.0.1}"
  port="${port:-8011}"
  service_command="${service_command:-$ROOT_DIR/terminal-mcp-service}"
  cat <<EOF

Terminal MCP is installed as a persistent systemd service.

MCP URL:       http://$host:$port/mcp
Dashboard URL: http://$host:$port/dashboard

Management:
  $service_command status
  $service_command restart
  $service_command logs
  $service_command verify
  $service_command credentials
  $service_command credentials --show  # explicitly reveal tokens

Credentials are stored with mode 600 in:
  $service_env

The listener is loopback-only. Public tunnel setup is separate; ngrok is never
started by the daemon.
EOF
  if [[ "$service_mode" == user ]]; then
    cat <<EOF

This is a user service. It starts after login. To allow startup before login,
run as an administrator:
  loginctl enable-linger ${TERMINAL_MCP_INVOKING_USER:-${SUDO_USER:-${USER:-$(id -un)}}}
EOF
  fi
fi
