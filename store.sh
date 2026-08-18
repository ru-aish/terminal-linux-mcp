#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DEV=0
RUN_TESTS=0
WITH_NGROK=0
SKIP_SYSTEM_PACKAGES=0
CHECK_ONLY=0
TEMP_PATHS=()

cleanup_temp_paths() {
  local path
  for path in "${TEMP_PATHS[@]}"; do
    [[ -z "$path" ]] || rm -rf -- "$path"
  done
}
trap cleanup_temp_paths EXIT

usage() {
  cat <<'EOF'
Usage: ./store.sh [options]

Bootstrap Terminal MCP on Linux or macOS. This script can install OS
packages, install a user-local managed Python through uv when Python 3.11+ is
missing, build the project virtual environment, and optionally install ngrok.

Options:
  --dev                    Install development/test dependencies.
  --test                   Install development dependencies and run tests.
  --with-ngrok             Install ngrok into ~/.local/bin when missing.
  --skip-system-packages   Do not install OS-level packages.
  --check                  Report installation readiness without changing files.
  -h, --help               Show this help.

Useful environment overrides:
  UV_INSTALLER_URL         uv installer URL (default: https://astral.sh/uv/install.sh)
  UV_INSTALLER_SHA256      Optional expected SHA-256 for the uv installer script
  NGROK_DOWNLOAD_URL       Override an ngrok archive URL on Linux
  NGROK_SHA256             Optional expected SHA-256 for a downloaded ngrok archive
  INSTALL_BIN_DIR          User binary directory (default: ~/.local/bin)

On macOS, Homebrew is used for Python 3.11, tmux, and optional ngrok.
EOF
}

die() {
  printf 'store.sh: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[store] %s\n' "$*"
}

warn() {
  printf '[store] WARNING: %s\n' "$*" >&2
}

find_brew() {
  if command -v brew >/dev/null 2>&1; then
    command -v brew
  elif [[ -x /opt/homebrew/bin/brew ]]; then
    printf '%s\n' /opt/homebrew/bin/brew
  elif [[ -x /usr/local/bin/brew ]]; then
    printf '%s\n' /usr/local/bin/brew
  else
    return 1
  fi
}

brew_install() {
  local brew_bin
  brew_bin="$(find_brew || true)"
  [[ -n "$brew_bin" ]] || die 'Homebrew is required on macOS for automatic dependency installation. Install Homebrew from https://brew.sh/ and rerun this script.'
  log "installing macOS prerequisites with Homebrew"
  "$brew_bin" install python@3.11 tmux
}

while (($#)); do
  case "$1" in
    --dev) INSTALL_DEV=1 ;;
    --test) INSTALL_DEV=1; RUN_TESTS=1 ;;
    --with-ngrok) WITH_NGROK=1 ;;
    --skip-system-packages|--no-system-packages) SKIP_SYSTEM_PACKAGES=1 ;;
    --check) CHECK_ONLY=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
  shift
done

OS_NAME="$(uname -s)"
case "$OS_NAME" in
  Linux|Darwin) ;;
  *) die "unsupported operating system: $OS_NAME" ;;
esac

INSTALL_BIN_DIR="${INSTALL_BIN_DIR:-$HOME/.local/bin}"
VENV_DIR="${MCP_VENV_DIR:-$ROOT_DIR/.venv}"
BREW_BIN="$(find_brew || true)"
if [[ "$OS_NAME" == "Darwin" && -n "$BREW_BIN" ]]; then
  BREW_PREFIX="$($BREW_BIN --prefix)"
  export PATH="$BREW_PREFIX/bin:$INSTALL_BIN_DIR:$HOME/.cargo/bin:$PATH"
  if "$BREW_BIN" --prefix python@3.11 >/dev/null 2>&1; then
    export PATH="$($BREW_BIN --prefix python@3.11)/bin:$PATH"
  fi
else
  export PATH="$INSTALL_BIN_DIR:$HOME/.cargo/bin:$PATH"
fi

python_ok() {
  local candidate=$1
  command -v "$candidate" >/dev/null 2>&1 || return 1
  "$candidate" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
}

find_python() {
  local candidate
  for candidate in python3.11 python3.12 python3.13 python3.14 python3 python; do
    if python_ok "$candidate"; then
      command -v "$candidate"
      return
    fi
  done
  return 1
}

find_uv() {
  if command -v uv >/dev/null 2>&1; then
    command -v uv
  elif [[ -x "$INSTALL_BIN_DIR/uv" ]]; then
    printf '%s\n' "$INSTALL_BIN_DIR/uv"
  else
    return 1
  fi
}

find_ngrok() {
  if command -v ngrok >/dev/null 2>&1; then
    command -v ngrok
  elif [[ -x "$INSTALL_BIN_DIR/ngrok" ]]; then
    printf '%s\n' "$INSTALL_BIN_DIR/ngrok"
  else
    return 1
  fi
}

package_manager() {
  [[ "$OS_NAME" == "Linux" ]] || return 1
  local manager
  for manager in apt-get dnf yum pacman zypper apk; do
    if command -v "$manager" >/dev/null 2>&1; then
      printf '%s\n' "$manager"
      return
    fi
  done
  return 1
}

privilege_prefix() {
  if [[ "$EUID" == 0 ]]; then
    return
  elif command -v sudo >/dev/null 2>&1; then
    printf '%s\n' sudo
  elif command -v doas >/dev/null 2>&1; then
    printf '%s\n' doas
  else
    return 1
  fi
}

run_privileged() {
  local prefix
  prefix="$(privilege_prefix || true)"
  if [[ "$EUID" != 0 && -z "$prefix" ]]; then
    return 126
  fi
  if [[ -n "$prefix" ]]; then
    "$prefix" "$@"
  else
    "$@"
  fi
}

install_system_packages() {
  [[ "$SKIP_SYSTEM_PACKAGES" == 0 ]] || { log 'skipping system package installation'; return; }
  if [[ "$OS_NAME" == "Darwin" ]]; then
    brew_install
    return
  fi
  local manager
  manager="$(package_manager || true)"
  [[ -n "$manager" ]] || { warn 'no supported package manager found; continuing with user-space installation'; return; }
  if [[ "$EUID" != 0 ]] && ! privilege_prefix >/dev/null 2>&1; then
    warn 'sudo/doas is unavailable; skipping system packages and using user-space fallbacks'
    return
  fi

  log "installing Linux prerequisites with $manager"
  case "$manager" in
    apt-get)
      run_privileged apt-get update
      run_privileged env DEBIAN_FRONTEND=noninteractive apt-get install -y \
        ca-certificates curl git tar gzip build-essential pkg-config tmux \
        libffi-dev libssl-dev python3 python3-venv python3-pip python3-dev
      ;;
    dnf)
      run_privileged dnf install -y \
        ca-certificates curl git tar gzip tmux gcc gcc-c++ make pkgconf-pkg-config \
        libffi-devel openssl-devel python3 python3-pip python3-devel
      ;;
    yum)
      run_privileged yum install -y \
        ca-certificates curl git tar gzip tmux gcc gcc-c++ make pkgconfig \
        libffi-devel openssl-devel python3 python3-pip python3-devel
      ;;
    pacman)
      run_privileged pacman -S --needed --noconfirm \
        ca-certificates curl git tar gzip tmux base-devel pkgconf libffi openssl python python-pip
      ;;
    zypper)
      run_privileged zypper --non-interactive install -y \
        ca-certificates curl git tar gzip tmux gcc gcc-c++ make pkg-config \
        libffi-devel libopenssl-devel python3 python3-pip python3-devel
      ;;
    apk)
      run_privileged apk add --no-cache \
        ca-certificates curl git tar gzip tmux build-base pkgconf \
        libffi-dev openssl-dev python3 py3-pip py3-virtualenv python3-dev
      ;;
  esac
}

download() {
  local url=$1
  local output=$2
  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --silent --show-error --retry 5 --retry-delay 2 \
      --connect-timeout 20 --max-time 600 "$url" -o "$output"
  elif command -v wget >/dev/null 2>&1; then
    wget --https-only --tries=5 --timeout=30 -O "$output" "$url"
  else
    die 'curl or wget is required to download user-space dependencies'
  fi
}

verify_sha256() {
  local file=$1
  local expected=${2:-}
  [[ -n "$expected" ]] || return 0
  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s  %s\n' "$expected" "$file" | sha256sum --check --status || die "checksum verification failed for $file"
  elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$file" | awk '{print $1}')"
    [[ "$actual" == "$expected" ]] || die "checksum verification failed for $file"
  else
    die 'sha256sum or shasum is required for checksum verification'
  fi
}

ensure_uv_and_python() {
  local python_bin uv_bin installer tmp
  python_bin="$(find_python || true)"
  if [[ -n "$python_bin" ]]; then
    log "using $($python_bin -c 'import sys; print(sys.executable, sys.version.split()[0])')"
    return
  fi

  uv_bin="$(find_uv || true)"
  if [[ -z "$uv_bin" ]]; then
    installer="${UV_INSTALLER_URL:-https://astral.sh/uv/install.sh}"
    tmp="$(mktemp)"
    TEMP_PATHS+=("$tmp")
    log 'Python 3.11+ is unavailable; installing uv in user space'
    download "$installer" "$tmp"
    verify_sha256 "$tmp" "${UV_INSTALLER_SHA256:-}"
    mkdir -p "$INSTALL_BIN_DIR"
    UV_INSTALL_DIR="$INSTALL_BIN_DIR" sh "$tmp"
    rm -f "$tmp"
    uv_bin="$(find_uv || true)"
  fi
  [[ -n "$uv_bin" ]] || die 'uv installation did not produce an executable'
  log 'installing a managed Python 3.11 with uv'
  "$uv_bin" python install 3.11
}

ngrok_url_for_arch() {
  local arch
  arch="$(uname -m)"
  case "$arch" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    armv7l|armv7) arch=arm ;;
    i386|i686) arch=386 ;;
    *) die "unsupported ngrok architecture: $arch" ;;
  esac
  if [[ "$OS_NAME" == "Darwin" ]]; then
    printf 'https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-darwin-%s.tgz\n' "$arch"
  else
    printf 'https://bin.equinox.io/c/bNyj1mQVY4c/ngrok-v3-stable-linux-%s.tgz\n' "$arch"
  fi
}

ensure_ngrok() {
  local ngrok_bin url temp_dir archive brew_bin
  ngrok_bin="$(find_ngrok || true)"
  if [[ -n "$ngrok_bin" ]]; then
    log "ngrok already available: $($ngrok_bin version 2>/dev/null | head -1 || printf '%s' "$ngrok_bin")"
    return
  fi

  if [[ "$OS_NAME" == "Darwin" ]]; then
    brew_bin="$(find_brew || true)"
    [[ -n "$brew_bin" ]] || die 'Homebrew is required to install ngrok on macOS. Install Homebrew or set NGROK_BIN to an existing ngrok executable.'
    log 'installing ngrok with Homebrew'
    "$brew_bin" install ngrok
    ngrok_bin="$(find_ngrok || true)"
    [[ -n "$ngrok_bin" ]] || die 'Homebrew reported success but ngrok could not be found on PATH'
    log "ngrok installed at $ngrok_bin"
    return
  fi

  url="${NGROK_DOWNLOAD_URL:-$(ngrok_url_for_arch)}"
  temp_dir="$(mktemp -d)"
  TEMP_PATHS+=("$temp_dir")
  archive="$temp_dir/ngrok.tgz"
  log 'downloading ngrok into the user-local bin directory'
  download "$url" "$archive"
  verify_sha256 "$archive" "${NGROK_SHA256:-}"
  tar -xzf "$archive" -C "$temp_dir"
  [[ -f "$temp_dir/ngrok" ]] || die 'ngrok archive did not contain the ngrok executable'
  mkdir -p "$INSTALL_BIN_DIR"
  install -m 0755 "$temp_dir/ngrok" "$INSTALL_BIN_DIR/ngrok"
  rm -rf "$temp_dir"
  "$INSTALL_BIN_DIR/ngrok" version >/dev/null
  log "installed ngrok at $INSTALL_BIN_DIR/ngrok"
}


check_readiness() {
  local failed=0 python_bin uv_bin ngrok_bin
  if [[ "$OS_NAME" == "Darwin" ]]; then
    printf 'macOS: %s\n' "$(sw_vers -productVersion 2>/dev/null || uname -sr)"
    printf 'Homebrew: %s\n' "$(find_brew || printf 'not detected')"
  else
    printf 'Linux: %s\n' "$(. /etc/os-release 2>/dev/null && printf '%s %s' "${NAME:-unknown}" "${VERSION_ID:-}" || uname -sr)"
    printf 'Package manager: %s\n' "$(package_manager || printf 'not detected')"
  fi
  python_bin="$(find_python || true)"
  uv_bin="$(find_uv || true)"
  ngrok_bin="$(find_ngrok || true)"
  printf 'Python 3.11+: %s\n' "${python_bin:-not found}"
  printf 'uv: %s\n' "${uv_bin:-not found}"
  printf 'ngrok: %s\n' "${ngrok_bin:-not found}"
  if [[ -x "$VENV_DIR/bin/python" ]]; then
    if "$ROOT_DIR/setup.sh" --check; then
      printf 'Project environment: ready\n'
    else
      printf 'Project environment: invalid\n'
      failed=1
    fi
  else
    printf 'Project environment: not installed\n'
    failed=1
  fi
  if [[ "$WITH_NGROK" == 1 && -z "$ngrok_bin" ]]; then
    failed=1
  fi
  return "$failed"
}

if [[ "$CHECK_ONLY" == 1 ]]; then
  if check_readiness; then
    exit 0
  fi
  exit 1
fi

install_system_packages
ensure_uv_and_python
[[ "$WITH_NGROK" == 0 ]] || ensure_ngrok

setup_args=()
[[ "$INSTALL_DEV" == 0 ]] || setup_args+=(--dev)
[[ "$RUN_TESTS" == 0 ]] || setup_args+=(--test)
"$ROOT_DIR/setup.sh" "${setup_args[@]}"

log 'bootstrap completed successfully'
log "ensure $INSTALL_BIN_DIR is in PATH"
if [[ "$WITH_NGROK" == 1 ]]; then
  log 'configure ngrok authentication before starting a public tunnel'
fi
