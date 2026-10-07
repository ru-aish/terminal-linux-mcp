"""Keep imports, global coordinators, and subprocess defaults off live state."""
import os
from pathlib import Path
import tempfile

_runtime_directory = None
_original_environment = {}


def pytest_configure(config):
    global _runtime_directory
    _runtime_directory = tempfile.TemporaryDirectory(prefix="terminal-mcp-pytest-")
    root = Path(_runtime_directory.name)
    isolated = {
        "HOME": str(root / "home"),
        "MCP_GPT_HOME": None,
        "GPT_HOME": None,
        "MCP_CHAT_AGENT_DB": str(root / "agents.db"),
        "MCP_CHAT_GATEWAY_DB": str(root / "gateway.db"),
        "MCP_CHAT_WATCHDOG_DIR": str(root / "watchdog"),
        "MCP_CHAT_WATCHDOG_ENABLED": "0",
        "MCP_CHAT_AGENT_ENABLED": "0",
        "MCP_CHAT_IDENTITY_DISCOVERY_ENABLED": "0",
    }
    for key, value in isolated.items():
        _original_environment[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def pytest_unconfigure(config):
    for key, value in _original_environment.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    if _runtime_directory is not None:
        _runtime_directory.cleanup()
