#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${MCP_VENV_DIR:-$ROOT_DIR/.venv}"

ok=0
fail=0

report() {
  local label=$1 value=$2 status=$3
  printf '%-24s %-50s %s\n' "$label" "$value" "$status"
  if [[ "$status" == "OK" || "$status" == "INFO" ]]; then
    ok=$((ok+1))
  else
    fail=$((fail+1))
  fi
}

printf '%s\n' 'Terminal MCP environment doctor'
printf '%s\n\n' '================================'

OS_NAME="$(uname -s 2>/dev/null || printf unknown)"
ARCH="$(uname -m 2>/dev/null || printf unknown)"
report 'Operating system' "$OS_NAME" INFO
report 'Architecture' "$ARCH" INFO
report 'Shell' "${SHELL:-unknown}" INFO

if [[ "$OS_NAME" == "Darwin" ]]; then
  if command -v brew >/dev/null 2>&1; then
    report 'Homebrew' "$(brew --prefix)" OK
  else
    report 'Homebrew' 'not found — install Homebrew before store.sh' FAIL
  fi
else
  report 'Package manager' "$(command -v apt-get || command -v dnf || command -v pacman || printf unknown)" INFO
fi

python_bin="${MCP_PYTHON:-}"
if [[ -z "$python_bin" && -x "$VENV_DIR/bin/python" ]]; then
  python_bin="$VENV_DIR/bin/python"
elif [[ -z "$python_bin" ]]; then
  python_bin="$(command -v python3 || true)"
fi
if [[ -n "$python_bin" && -x "$python_bin" ]]; then
  if python_version="$($python_bin -c 'import sys; print(sys.version.split()[0])' 2>/dev/null)" && "$python_bin" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)' >/dev/null 2>&1; then
    report 'Python 3.11+' "$python_version ($python_bin)" OK
  else
    report 'Python 3.11+' "$python_bin is too old or broken" FAIL
  fi
else
  report 'Python 3.11+' 'not found' FAIL
fi

if [[ -x "$VENV_DIR/bin/python" ]] && "$VENV_DIR/bin/python" -c 'import mcp,uvicorn,starlette,httpx; print("ok")' >/dev/null 2>&1; then
  report 'Virtual environment' "$VENV_DIR" OK
else
  report 'Virtual environment' "$VENV_DIR is missing or incomplete" FAIL
fi

if command -v tmux >/dev/null 2>&1; then
  report 'tmux' "$(tmux -V 2>/dev/null || command -v tmux)" OK
else
  report 'tmux' 'not found (brew install tmux)' FAIL
fi

if command -v ngrok >/dev/null 2>&1; then
  report 'ngrok' "$(command -v ngrok)" OK
else
  report 'ngrok' 'not installed (optional; brew install ngrok)' INFO
fi

if [[ "$OS_NAME" == "Darwin" ]]; then
  if [[ -x /usr/bin/open ]]; then
    report 'macOS app launcher' '/usr/bin/open' OK
  else
    report 'macOS app launcher' '/usr/bin/open missing' FAIL
  fi
fi

if [[ -f "$ROOT_DIR/.env" || -f "$ROOT_DIR/.env.example" ]]; then
  report 'Configuration' 'configuration file present' OK
else
  report 'Configuration' '.env.example missing' FAIL
fi

if [[ -x "$VENV_DIR/bin/python" ]]; then
  if "$VENV_DIR/bin/python" -m py_compile "$ROOT_DIR/terminal_mcp.py" "$ROOT_DIR/chat_watchdog.py" "$ROOT_DIR/platform_support.py" >/dev/null 2>&1; then
    report 'Python compilation' 'core files compile' OK
  else
    report 'Python compilation' 'core files failed to compile' FAIL
  fi
fi

printf '\nSummary: %d checks OK, %d checks failed.\n' "$ok" "$fail"
if (( fail > 0 )); then
  exit 1
fi
