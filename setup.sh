#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${MCP_VENV_DIR:-$ROOT_DIR/.venv}"
VENV_PARENT="$(dirname -- "$VENV_DIR")"
RUNTIME_DIR="${MCP_RUNTIME_DIR:-$ROOT_DIR/.run}"
PYTHON_REQUEST="${PYTHON_BIN:-}"
INSTALL_DEV=0
RUN_TESTS=0
RECREATE=0
CHECK_ONLY=0
UPGRADE_PIP=1
BACKUP_VENV=""
SETUP_SUCCEEDED=0

usage() {
  cat <<'EOF'
Usage: ./setup.sh [options]

Create or repair the project virtual environment and install Python packages.

Options:
  --dev                Install development/test dependencies.
  --test               Install development dependencies and run the test suite.
  --recreate           Rebuild the virtual environment at its final path. The
                       previous environment is backed up and restored on failure.
  --check              Validate the existing environment without changing it.
  --python PATH        Prefer this Python executable (must be Python 3.11+).
  --no-pip-upgrade     Do not upgrade pip/setuptools/wheel first.
  -h, --help           Show this help.

Environment:
  MCP_VENV_DIR         Virtual environment path (default: ./.venv)
  PIP_RETRIES          Download retry count (default: 5)
  PIP_TIMEOUT          Per-request timeout in seconds (default: 45)
EOF
}

die() {
  printf 'setup.sh: %s\n' "$*" >&2
  exit 1
}

log() {
  printf '[setup] %s\n' "$*"
}

while (($#)); do
  case "$1" in
    --dev) INSTALL_DEV=1 ;;
    --test) INSTALL_DEV=1; RUN_TESTS=1 ;;
    --recreate) RECREATE=1 ;;
    --check) CHECK_ONLY=1 ;;
    --python)
      shift
      (($#)) || die '--python requires a path'
      PYTHON_REQUEST="$1"
      ;;
    --no-pip-upgrade) UPGRADE_PIP=0 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
  shift
done

mkdir -p "$RUNTIME_DIR" "$VENV_PARENT"
if command -v flock >/dev/null 2>&1; then
  exec 9>"$RUNTIME_DIR/setup.lock"
  flock -w "${MCP_SETUP_LOCK_TIMEOUT:-300}" 9 || die 'another setup process is already running'
fi

python_ok() {
  local candidate=$1
  [[ -n "$candidate" ]] || return 1
  command -v "$candidate" >/dev/null 2>&1 || [[ -x "$candidate" ]] || return 1
  "$candidate" - <<'PY' >/dev/null 2>&1
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
}

find_python() {
  local candidate
  if [[ -n "$PYTHON_REQUEST" ]]; then
    python_ok "$PYTHON_REQUEST" || die "$PYTHON_REQUEST is not a usable Python 3.11+ executable"
    command -v "$PYTHON_REQUEST" 2>/dev/null || printf '%s\n' "$PYTHON_REQUEST"
    return
  fi
  for candidate in python3.14 python3.13 python3.12 python3.11 python3 python; do
    if python_ok "$candidate"; then
      command -v "$candidate"
      return
    fi
  done
  return 1
}

find_uv() {
  local candidate
  for candidate in "${UV_BIN:-}" uv "$HOME/.local/bin/uv" "$HOME/.cargo/bin/uv"; do
    [[ -n "$candidate" ]] || continue
    if command -v "$candidate" >/dev/null 2>&1; then
      command -v "$candidate"
      return
    elif [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return
    fi
  done
  return 1
}

create_venv() {
  local target=$1
  local python_bin=${2:-}
  local uv_bin=${3:-}
  rm -rf "$target"

  if [[ -n "$python_bin" ]]; then
    if "$python_bin" -m venv "$target" >/dev/null 2>&1; then
      return
    fi
    log "Python's venv module is unavailable; trying uv instead"
  fi

  [[ -n "$uv_bin" ]] || die 'cannot create a virtual environment. Run ./store.sh to install Linux prerequisites and a managed Python.'
  "$uv_bin" python install 3.11
  "$uv_bin" venv --seed --python 3.11 "$target"
}

validate_venv() {
  local python_bin=$1
  [[ -x "$python_bin" ]] || return 1
  "$python_bin" - <<'PY'
import importlib
import sys

if sys.version_info < (3, 11):
    raise SystemExit(f"Python 3.11+ required; found {sys.version.split()[0]}")
for name in ("mcp", "uvicorn", "starlette", "httpx"):
    importlib.import_module(name)
print(sys.version.split()[0])
PY
  "$python_bin" -m pip check
  "$python_bin" -m py_compile "$ROOT_DIR/terminal_mcp.py" "$ROOT_DIR/scripts/smoke_test.py"
}

if [[ "$CHECK_ONLY" == 1 ]]; then
  [[ -x "$VENV_DIR/bin/python" ]] || die "virtual environment is missing at $VENV_DIR; run ./setup.sh or ./store.sh"
  version="$(validate_venv "$VENV_DIR/bin/python")" || die 'virtual environment validation failed; rerun ./setup.sh --recreate'
  log "environment is ready (Python ${version%%$'\n'*})"
  exit 0
fi

SYSTEM_PYTHON="$(find_python || true)"
UV="$(find_uv || true)"
if [[ -z "$SYSTEM_PYTHON" && -z "$UV" ]]; then
  die 'Python 3.11+ was not found. Run ./store.sh; it can install system prerequisites and a managed Python on Linux.'
fi

needs_rebuild=$RECREATE
if [[ ! -x "$VENV_DIR/bin/python" ]] || ! python_ok "$VENV_DIR/bin/python"; then
  needs_rebuild=1
fi

rollback_rebuild() {
  local status=$?
  trap - EXIT
  if [[ "$SETUP_SUCCEEDED" != 1 && "$needs_rebuild" == 1 ]]; then
    rm -rf "$VENV_DIR"
    if [[ -n "$BACKUP_VENV" && -e "$BACKUP_VENV" ]]; then
      mv "$BACKUP_VENV" "$VENV_DIR"
      printf '[setup] restored the previous virtual environment after failure\n' >&2
    fi
  fi
  exit "$status"
}

if [[ "$needs_rebuild" == 1 ]]; then
  trap rollback_rebuild EXIT
  if [[ -e "$VENV_DIR" ]]; then
    BACKUP_VENV="$VENV_PARENT/.terminal-mcp-venv.backup.$(date +%Y%m%d-%H%M%S).$$"
    mv "$VENV_DIR" "$BACKUP_VENV"
  fi
  log "creating a fresh virtual environment at $VENV_DIR"
  create_venv "$VENV_DIR" "$SYSTEM_PYTHON" "$UV"
else
  log "repairing existing virtual environment at $VENV_DIR"
fi

ACTIVE_VENV="$VENV_DIR"

VENV_PYTHON="$ACTIVE_VENV/bin/python"
[[ -x "$VENV_PYTHON" ]] || die "virtual environment did not create $VENV_PYTHON"

if ! "$VENV_PYTHON" -m pip --version >/dev/null 2>&1; then
  "$VENV_PYTHON" -m ensurepip --upgrade || die 'pip is unavailable in the virtual environment'
fi

pip_args=(
  --disable-pip-version-check
  --retries "${PIP_RETRIES:-5}"
  --timeout "${PIP_TIMEOUT:-45}"
)

if [[ "$UPGRADE_PIP" == 1 ]]; then
  log 'upgrading pip build tooling'
  "$VENV_PYTHON" -m pip install "${pip_args[@]}" --upgrade pip setuptools wheel
fi

requirements="$ROOT_DIR/requirements.txt"
if [[ "$INSTALL_DEV" == 1 ]]; then
  requirements="$ROOT_DIR/requirements-dev.txt"
fi

log "installing dependencies from $(basename "$requirements")"
"$VENV_PYTHON" -m pip install "${pip_args[@]}" -r "$requirements"

log 'validating installed environment'
validate_venv "$VENV_PYTHON" >/dev/null

if [[ "$RUN_TESTS" == 1 ]]; then
  log 'running tests'
  "$VENV_PYTHON" -m pytest -q "$ROOT_DIR/tests"
fi

SETUP_SUCCEEDED=1
if [[ -n "$BACKUP_VENV" && -e "$BACKUP_VENV" ]]; then
  rm -rf "$BACKUP_VENV"
fi
trap - EXIT

version="$($VENV_DIR/bin/python -c 'import sys; print(sys.version.split()[0])')"
log "setup complete with Python $version"
log 'start locally with: ./start.sh --local-only'
log 'start with ngrok with: ./start.sh'
