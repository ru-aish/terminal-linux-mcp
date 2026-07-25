#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
TEMP_DIR="$(mktemp -d)"
trap 'rm -rf -- "$TEMP_DIR"' EXIT

fail() {
  printf 'test_install_workflow.sh: %s\n' "$*" >&2
  exit 1
}

read_value() {
  local file=$1 key=$2
  awk -F= -v key="$key" '$1 == key {sub("^[^=]*=", ""); print; exit}' "$file"
}

archive_root="$TEMP_DIR/archive/terminal-linux-mcp-test"
mkdir -p "$archive_root"
tar \
  --exclude=.git \
  --exclude=.venv \
  --exclude=.env \
  --exclude=.run \
  --exclude='*.pyc' \
  -cf - -C "$ROOT_DIR" . | tar -xf - -C "$archive_root"
tar -czf "$TEMP_DIR/source.tar.gz" -C "$TEMP_DIR/archive" terminal-linux-mcp-test

install_dir="$TEMP_DIR/install"
TERMINAL_MCP_BOOTSTRAP_ARCHIVE="$TEMP_DIR/source.tar.gz" \
TERMINAL_MCP_INSTALL_DIR="$install_dir" \
  bash "$ROOT_DIR/bootstrap.sh" --configure-only >/dev/null

env_file="$install_dir/.env"
[[ -f "$env_file" ]] || fail 'bootstrap did not create .env'
mode="$(stat -c '%a' "$env_file")"
[[ "$mode" == 600 ]] || fail ".env mode is $mode, expected 600"

bearer="$(read_value "$env_file" MCP_BEARER_TOKEN)"
dashboard="$(read_value "$env_file" MCP_DASHBOARD_TOKEN)"
[[ "$bearer" =~ ^[0-9a-f]{64}$ ]] || fail 'bearer token is not 64 lowercase hex characters'
[[ "$dashboard" =~ ^[0-9a-f]{64}$ ]] || fail 'dashboard token is not 64 lowercase hex characters'
[[ "$bearer" != "$dashboard" ]] || fail 'MCP and dashboard tokens must be distinct'

TERMINAL_MCP_BOOTSTRAP_ARCHIVE="$TEMP_DIR/source.tar.gz" \
TERMINAL_MCP_INSTALL_DIR="$install_dir" \
  bash "$ROOT_DIR/bootstrap.sh" --configure-only >/dev/null

[[ "$(read_value "$env_file" MCP_BEARER_TOKEN)" == "$bearer" ]] || fail 'reinstall replaced the bearer token'
[[ "$(read_value "$env_file" MCP_DASHBOARD_TOKEN)" == "$dashboard" ]] || fail 'reinstall replaced the dashboard token'

shown="$($install_dir/install.sh --show-secrets)"
grep -Fxq "MCP_BEARER_TOKEN=$bearer" <<<"$shown" || fail '--show-secrets omitted bearer token'
grep -Fxq "MCP_DASHBOARD_TOKEN=$dashboard" <<<"$shown" || fail '--show-secrets omitted dashboard token'

custom_env="$TEMP_DIR/custom.env"
MCP_ENV_FILE="$custom_env" "$ROOT_DIR/install.sh" --configure-only >/dev/null
[[ -f "$custom_env" ]] || fail 'MCP_ENV_FILE override was not created'
[[ "$(stat -c '%a' "$custom_env")" == 600 ]] || fail 'custom environment file is not mode 600'

printf 'secure install workflow passed\n'
