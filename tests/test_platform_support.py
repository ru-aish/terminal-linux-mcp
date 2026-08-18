import os
from pathlib import Path

import platform_support


def test_default_shell_prefers_environment(monkeypatch):
    monkeypatch.setenv("SHELL", "/custom/shell")
    assert platform_support.default_shell() == "/custom/shell"

def test_default_shell_uses_zsh_on_macos_without_shell(monkeypatch):
    monkeypatch.delenv("SHELL", raising=False)
    monkeypatch.setattr(platform_support, "IS_MACOS", True)
    assert platform_support.default_shell() == "/bin/zsh"

def test_default_shell_uses_bash_on_linux_without_shell(monkeypatch):
    monkeypatch.delenv("SHELL", raising=False)
    monkeypatch.setattr(platform_support, "IS_MACOS", False)
    assert platform_support.default_shell() == "/bin/bash"

def test_macos_codex_launcher(monkeypatch):
    monkeypatch.setattr(platform_support, "IS_MACOS", True)
    monkeypatch.delenv("MCP_CHAT_WATCHDOG_APP_COMMAND", raising=False)
    command = platform_support.default_codex_launch_command()
    assert command == '/usr/bin/open -a "Codex" --args'
    assert platform_support.prepare_desktop_command(command, reopen=False) == ["/usr/bin/open", "-a", "Codex", "--args"]
    assert platform_support.prepare_desktop_command(command, reopen=True) == ["/usr/bin/open", "-a", "Codex", "--args", "--new-chat"]

def test_configured_codex_launcher_wins(monkeypatch):
    monkeypatch.setenv("MCP_CHAT_WATCHDOG_APP_COMMAND", "/custom/codex --flag")
    assert platform_support.default_codex_launch_command() == "/custom/codex --flag"

def test_macos_state_paths_are_native(monkeypatch, tmp_path):
    monkeypatch.setattr(platform_support, "IS_MACOS", True)
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    assert str(platform_support.app_state_root()).endswith("Library/Application Support/terminal-mcp")
    assert str(platform_support.app_log_root()).endswith("Library/Logs/terminal-mcp")
    assert str(platform_support.default_chat_watchdog_root()).endswith("Library/Application Support/terminal-mcp/chat-watchdog")
    assert str(platform_support.default_chat_agent_db()).endswith("Library/Application Support/terminal-mcp/chat-agent-orchestrator.db")

