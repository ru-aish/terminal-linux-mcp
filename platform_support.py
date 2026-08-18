"""Small OS-level compatibility layer for Terminal MCP.

The application logic intentionally stays platform-neutral.  Linux keeps its
systemd-based workload scopes when available; macOS uses normal process groups
(created with ``start_new_session=True`` by the terminal runner) and native
application launching through ``open``.
"""
from __future__ import annotations

import os
import platform
import shlex
from pathlib import Path


SYSTEM = platform.system().lower()
IS_MACOS = SYSTEM == "darwin"
IS_LINUX = SYSTEM == "linux"


def default_shell() -> str:
    return os.environ.get("SHELL") or ("/bin/zsh" if IS_MACOS else "/bin/bash")


def app_state_root(app_name: str = "terminal-mcp") -> Path:
    """Return a native per-user state root, without forcing the caller to use it."""
    override = os.environ.get("MCP_STATE_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    if IS_MACOS:
        return (Path.home() / "Library" / "Application Support" / app_name).resolve()
    return (Path.home() / ".local" / "share" / app_name).resolve()


def app_log_root(app_name: str = "terminal-mcp") -> Path:
    override = os.environ.get("MCP_APP_LOG_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    if IS_MACOS:
        return (Path.home() / "Library" / "Logs" / app_name).resolve()
    return (Path.home() / ".local" / "state" / app_name / "logs").resolve()


def default_chat_watchdog_root() -> Path:
    override = os.environ.get("MCP_CHAT_WATCHDOG_DIR")
    if override:
        return Path(override).expanduser().resolve()
    if IS_MACOS:
        return (Path.home() / "Library" / "Application Support" / "terminal-mcp" / "chat-watchdog").resolve()
    return (Path.home() / ".GPT" / "chat-watchdog").resolve()


def default_chat_agent_db() -> Path:
    override = os.environ.get("MCP_CHAT_AGENT_DB")
    if override:
        return Path(override).expanduser().resolve()
    if IS_MACOS:
        return (Path.home() / "Library" / "Application Support" / "terminal-mcp" / "chat-agent-orchestrator.db").resolve()
    return (Path.home() / ".GPT" / "chat-agent-orchestrator.db").resolve()


def default_chat_gateway_db() -> Path:
    override = os.environ.get("MCP_CHAT_GATEWAY_DB")
    if override:
        return Path(override).expanduser().resolve()
    if IS_MACOS:
        return (Path.home() / "Library" / "Application Support" / "terminal-mcp" / "chat-agent-gateway.db").resolve()
    return (Path.home() / ".GPT" / "chat-agent-gateway.db").resolve()


def default_codex_launch_command() -> str:
    """Return an executable command string for the managed Codex desktop host."""
    override = os.environ.get("MCP_CHAT_WATCHDOG_APP_COMMAND")
    if override:
        return override.strip()
    if IS_MACOS:
        # ``--args`` is intentional: the watchdog may append --new-chat when
        # the application is already running but its primary renderer is not ready.
        return '/usr/bin/open -a "Codex" --args'
    return "/usr/bin/codex-desktop"


def prepare_desktop_command(command: str, *, reopen: bool) -> list[str]:
    """Tokenize a configured desktop command and append the supported reopen flag."""
    argv = shlex.split(command)
    if not argv:
        raise ValueError("desktop launch command is empty")
    if reopen and not any(
        argument in {"--new-chat", "--quick-chat", "--prompt-chat", "--hotkey-window"}
        for argument in argv
    ):
        argv.append("--new-chat")
    return argv


def desktop_launch_environment(base: dict[str, str] | None = None) -> dict[str, str]:
    """Return a GUI launch environment.

    macOS applications inherit the user's logged-in GUI environment via
    ``open``; Linux keeps the existing environment and X/Wayland discovery is
    performed by the watchdog itself where needed.
    """
    return dict(os.environ if base is None else base)
