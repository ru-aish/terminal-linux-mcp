#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_SLUG="${TERMINAL_MCP_PROJECT:-ru-aish/terminal-linux-mcp}"
INSTALL_DIR="${TERMINAL_MCP_INSTALL_DIR:-$HOME/.local/share/terminal-gpt-mcp}"
REF="${TERMINAL_MCP_REF:-main}"
ARCHIVE_FILE="${TERMINAL_MCP_BOOTSTRAP_ARCHIVE:-}"
INSTALL_ARGS=()
TEMP_DIR=""

usage() {
  cat <<'EOF'
Usage: bootstrap.sh [bootstrap options] [install options]

Download Terminal MCP into a user-owned directory and run its secure installer.
Designed for private repositories:
  gh api -H 'Accept: application/vnd.github.raw+json' \
    repos/ru-aish/terminal-linux-mcp/contents/bootstrap.sh | bash

Bootstrap options:
  --install-dir PATH    Installation directory (default: ~/.local/share/terminal-gpt-mcp)
  --ref REF             Git branch, tag, or commit to download (default: main)
  -h, --help            Show this help.

All other options are passed to install.sh, including --with-ngrok, --test,
--skip-system-packages, and --configure-only.
EOF
}

die() {
  printf 'bootstrap.sh: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[bootstrap] %s\n' "$*"
}

cleanup() {
  [[ -z "$TEMP_DIR" ]] || rm -rf -- "$TEMP_DIR"
}
trap cleanup EXIT

while (($#)); do
  case "$1" in
    --install-dir)
      shift
      (($#)) || die '--install-dir requires a path'
      INSTALL_DIR="$1"
      ;;
    --ref)
      shift
      (($#)) || die '--ref requires a branch, tag, or commit'
      REF="$1"
      ;;
    -h|--help) usage; exit 0 ;;
    *) INSTALL_ARGS+=("$1") ;;
  esac
  shift
done

case "$INSTALL_DIR" in
  /*) ;;
  *) INSTALL_DIR="$PWD/$INSTALL_DIR" ;;
esac

if [[ -x "$INSTALL_DIR/install.sh" ]]; then
  log "reusing existing installation at $INSTALL_DIR"
  exec "$INSTALL_DIR/install.sh" "${INSTALL_ARGS[@]}"
fi
if [[ -e "$INSTALL_DIR" ]]; then
  die "$INSTALL_DIR already exists but is not a Terminal MCP installation"
fi

command -v tar >/dev/null 2>&1 || die 'tar is required'
parent="$(dirname -- "$INSTALL_DIR")"
mkdir -p "$parent"
TEMP_DIR="$(mktemp -d "$parent/.terminal-mcp-bootstrap.XXXXXX")"
archive="$TEMP_DIR/source.tar.gz"

if [[ -n "$ARCHIVE_FILE" ]]; then
  [[ -f "$ARCHIVE_FILE" ]] || die "local bootstrap archive not found: $ARCHIVE_FILE"
  cp -- "$ARCHIVE_FILE" "$archive"
else
  encoded_ref="${REF//\//%2F}"
  api_path="repos/$PROJECT_SLUG/tarball/$encoded_ref"
  api_url="https://api.github.com/$api_path"
  log "downloading $PROJECT_SLUG at $REF"
  downloaded=0

  if command -v gh >/dev/null 2>&1; then
    if gh api -H 'Accept: application/vnd.github+json' "$api_path" >"$archive" 2>/dev/null; then
      downloaded=1
    fi
  fi

  if [[ "$downloaded" == 0 ]] && command -v curl >/dev/null 2>&1; then
    curl_args=(
      --fail --location --silent --show-error --retry 5 --retry-delay 2
      --connect-timeout 20 --max-time 600
      -H 'Accept: application/vnd.github+json'
    )
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
      curl_args+=(-H "Authorization: Bearer $GITHUB_TOKEN")
    elif [[ -n "${GH_TOKEN:-}" ]]; then
      curl_args+=(-H "Authorization: Bearer $GH_TOKEN")
    fi
    if curl "${curl_args[@]}" "$api_url" -o "$archive"; then
      downloaded=1
    fi
  fi

  if [[ "$downloaded" == 0 ]] && command -v wget >/dev/null 2>&1 \
      && [[ -z "${GITHUB_TOKEN:-}${GH_TOKEN:-}" ]]; then
    if wget --https-only --tries=5 --timeout=30 -O "$archive" "$api_url"; then
      downloaded=1
    fi
  fi

  [[ "$downloaded" == 1 ]] || die \
    'source download failed; private repositories require an authenticated gh CLI or GITHUB_TOKEN/GH_TOKEN'
fi

entries="$(tar -tzf "$archive")" || die 'downloaded archive is not a valid gzip-compressed tar file'
[[ -n "$entries" ]] || die 'downloaded archive is empty'
if printf '%s\n' "$entries" | grep -Eq '(^/|(^|/)\.\.(/|$))'; then
  die 'archive contains an unsafe path'
fi
root_name="$(printf '%s\n' "$entries" | awk -F/ 'NF {print $1}' | sort -u)"
[[ -n "$root_name" && "$root_name" != *$'\n'* ]] || die 'archive must contain exactly one top-level directory'

tar -xzf "$archive" -C "$TEMP_DIR"
source_dir="$TEMP_DIR/$root_name"
[[ -x "$source_dir/install.sh" ]] || die 'archive does not contain an executable install.sh'

mv -- "$source_dir" "$INSTALL_DIR"
log "installed source at $INSTALL_DIR"
rm -rf -- "$TEMP_DIR"
TEMP_DIR=""
exec "$INSTALL_DIR/install.sh" "${INSTALL_ARGS[@]}"
