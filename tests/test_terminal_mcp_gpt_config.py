import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import terminal_mcp


def test_gpt_mcp_config_overrides_inherited_browser_servers(tmp_path, monkeypatch):
    home = tmp_path / "home"
    codex = home / ".codex"
    gemini = home / ".gemini" / "config"
    gpt = home / ".GPT"
    project = tmp_path / "project"
    for path in (codex, gemini, gpt, project):
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(terminal_mcp, "_gpt_home", lambda: gpt)
    (codex / "config.toml").write_text(
        '[mcp_servers.node_repl]\ncommand = "node_repl"\n'
        '[mcp_servers."stealth-browser"]\ncommand = "node"\n'
        '[mcp_servers.openai-computer-use]\ncommand = "/tmp/unsafe-backend"\n',
        encoding="utf-8",
    )
    (gemini / "mcp_config.json").write_text(json.dumps({
        "mcpServers": {
            "camoufox": {"command": "node"},
            "puppeteer-real-browser": {"command": "npx"},
            "colab-mcp": {"command": "uvx"},
        }
    }), encoding="utf-8")
    (project / ".mcp.json").write_text(json.dumps({
        "mcpServers": {"openai-computer-use": {"command": "/tmp/project-backend"}}
    }), encoding="utf-8")
    (gpt / "mcp.json").write_text(json.dumps({
        "mcpServers": {
            "openai-computer-use": {
                "command": "/home/coder/.local/bin/codex-computer-use-linux",
                "args": ["mcp"],
            },
            "stealth-browser": {"disabled": True},
            "camoufox": {"disabled": True},
            "puppeteer-real-browser": {"disabled": True},
            "node_repl": {"disabled": True},
        }
    }), encoding="utf-8")

    servers = terminal_mcp._discover_mcp_servers(project)

    assert set(servers) == {"openai-computer-use", "colab-mcp"}
    assert servers["openai-computer-use"]["source"] == str(gpt / "mcp.json")
    assert servers["openai-computer-use"]["command"] == (
        "/home/coder/.local/bin/codex-computer-use-linux"
    )
    assert servers["colab-mcp"]["source"] == str(gemini / "mcp_config.json")


def test_gpt_config_masks_browser_from_every_project(tmp_path, monkeypatch):
    home = tmp_path / "home"
    gpt = home / ".GPT"
    codex = home / ".codex"
    project_a = tmp_path / "a"
    project_b = tmp_path / "b"
    for path in (gpt, codex, project_a, project_b):
        path.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setattr(terminal_mcp, "_gpt_home", lambda: gpt)
    (codex / "config.toml").write_text(
        '[mcp_servers."stealth-browser"]\ncommand = "node"\n',
        encoding="utf-8",
    )
    (project_b / "mcp.json").write_text(
        json.dumps({"mcpServers": {"stealth-browser": {"command": "node"}}}),
        encoding="utf-8",
    )
    (gpt / "mcp.json").write_text(
        json.dumps({"mcpServers": {"stealth-browser": {"disabled": True}}}),
        encoding="utf-8",
    )
    assert "stealth-browser" not in terminal_mcp._discover_mcp_servers(project_a)
    assert "stealth-browser" not in terminal_mcp._discover_mcp_servers(project_b)
