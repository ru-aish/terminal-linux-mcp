#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${MCP_ENV_FILE:-$ROOT_DIR/.env}"
VENV_DIR="${MCP_VENV_DIR:-$ROOT_DIR/.venv}"
CONFIGURE_ONLY=0
SHOW_SECRETS=0
CHECK_ONLY=0
STORE_ARGS=()

usage() {
  cat <<'EOF'
Usage: ./install.sh [options]

Install Terminal MCP and create a private .env with generated bearer tokens.
The installer is idempotent: existing non-placeholder secrets are preserved.

Options:
  --configure-only          Create or repair authentication config without installing packages.
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
    --configure-only) CONFIGURE_ONLY=1 ;;
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

read_env_value() {
  local key=$1
  [[ -f "$ENV_FILE" ]] || return 0
  awk -v key="$key" '
    $0 ~ "^[[:space:]]*" key "[[:space:]]*=" {
      value = $0
      sub("^[^=]*=", "", value)
      sub("^[[:space:]]+", "", value)
      sub("[[:space:]]+$", "", value)
      print value
      exit
    }
  ' "$ENV_FILE"
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
  local bearer dashboard
  [[ -f "$ENV_FILE" ]] || die "authentication configuration is missing at $ENV_FILE; run ./install.sh --configure-only"
  bearer="$(read_env_value MCP_BEARER_TOKEN)"
  dashboard="$(read_env_value MCP_DASHBOARD_TOKEN)"
  usable_secret "$bearer" || die 'MCP_BEARER_TOKEN is missing, placeholder, or shorter than 32 characters'
  usable_secret "$dashboard" || die 'MCP_DASHBOARD_TOKEN is missing, placeholder, or shorter than 32 characters'
  local mode
  mode="$(stat -c '%a' "$ENV_FILE" 2>/dev/null || stat -f '%Lp' "$ENV_FILE" 2>/dev/null || true)"
  [[ "$mode" == 600 ]] || die "$ENV_FILE must have mode 600 (found ${mode:-unknown})"
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

cat <<EOF

Terminal MCP is installed and authentication is enabled.

Start locally:
  cd "$ROOT_DIR" && ./start.sh --local-only

Show the credentials when configuring a client:
  cd "$ROOT_DIR" && ./install.sh --show-secrets

The secrets are stored with mode 600 in:
  $ENV_FILE
EOF
