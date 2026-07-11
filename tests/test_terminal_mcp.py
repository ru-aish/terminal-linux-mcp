import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import terminal_mcp


EXPECTED_TOOLS = [
    "project_context", "local_skills", "local_mcp", "run_command", "start_process",
    "poll_process", "stop_process", "set_session_env", "read_file", "write_file",
    "replace_in_file", "apply_patch", "list_dir", "stat_path", "make_dir", "copy_path",
    "move_path", "run_codex_yolo", "start_codex_yolo", "run_agy_yolo", "start_agy_yolo",
]


def run(coro):
    return asyncio.run(coro)


def test_exact_public_tool_list():
    tools = run(terminal_mcp.mcp.list_tools())
    assert [tool.name for tool in tools] == EXPECTED_TOOLS


def test_project_context_load_reblock_and_truncation(tmp_path, monkeypatch):
    monkeypatch.setattr(terminal_mcp, "AGENT_CONTEXT_MAX_CHARS", 280)
    (tmp_path / "AGENTS.md").write_text("rule-one\n" + "a" * 500, encoding="utf-8")
    first = run(terminal_mcp.project_context("continue", project_root=str(tmp_path), session_id="context-test"))
    second = run(terminal_mcp.project_context("continue", project_root=str(tmp_path), session_id="context-test"))
    (tmp_path / "AGENTS.md").write_text("rule-two\n" + "b" * 500, encoding="utf-8")
    third = run(terminal_mcp.project_context("continue", project_root=str(tmp_path), session_id="context-test"))
    assert "context_gate: reblock" in first
    assert "context_gate: load" in second
    assert "context_gate: reblock" in third
    assert "[...context capsule truncated" in first
    assert "agents_fingerprint:" in third


def test_local_skills_list_read_and_search(tmp_path):
    skill = tmp_path / "skills" / "demo" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("# Demo\nUseful terminal workflow\n", encoding="utf-8")
    listing = json.loads(run(terminal_mcp.local_skills(project_root=str(tmp_path))))
    assert "skills/demo/SKILL.md" in listing["skills"]
    read = json.loads(run(terminal_mcp.local_skills("read", path="skills/demo/SKILL.md", project_root=str(tmp_path))))
    assert "Useful terminal workflow" in read["content"]
    search = json.loads(run(terminal_mcp.local_skills("search", query="terminal", project_root=str(tmp_path))))
    assert len(search["matches"]) == 1


def test_local_mcp_temporary_echo_and_redaction(tmp_path):
    echo = tmp_path / "echo_server.py"
    echo.write_text(
        "from mcp.server.fastmcp import FastMCP\n"
        "m = FastMCP('echo')\n"
        "@m.tool()\n"
        "def echo(value: str) -> str:\n"
        "    return 'echo:' + value\n"
        "m.run(transport='stdio')\n",
        encoding="utf-8",
    )
    config = tmp_path / "config.toml"
    config.write_text(
        "[mcp_servers.echo]\ncommand = " + json.dumps(sys.executable) + "\nargs = [" + json.dumps(str(echo)) + "]\n"
        "[mcp_servers.echo.env]\nTOKEN = 'must-not-leak'\n",
        encoding="utf-8",
    )
    listing = run(terminal_mcp.local_mcp(config_path=str(config)))
    assert "echo" in listing and "must-not-leak" not in listing
    tools = run(terminal_mcp.local_mcp("tools", server="echo", config_path=str(config)))
    assert '"echo"' in tools
    called = run(terminal_mcp.local_mcp("call", server="echo", tool_name="echo", arguments={"value": "ok"}, config_path=str(config)))
    assert "echo:ok" in called


def test_filesystem_and_process_lifecycle(tmp_path):
    async def lifecycle():
        session = "fs-test"
        await terminal_mcp.write_file("one.txt", "alpha", session_id=session, cwd=str(tmp_path))
        assert "Occurrences Replaced: 1" in await terminal_mcp.replace_in_file("one.txt", "alpha", "beta", session_id=session, cwd=str(tmp_path))
        assert "beta" in await terminal_mcp.read_file("one.txt", session_id=session, cwd=str(tmp_path))
        await terminal_mcp.make_dir("nested", session_id=session, cwd=str(tmp_path))
        await terminal_mcp.copy_path("one.txt", "nested/two.txt", session_id=session, cwd=str(tmp_path))
        assert "Moved:" in await terminal_mcp.move_path("nested/two.txt", "three.txt", session_id=session, cwd=str(tmp_path))
        started = await terminal_mcp.start_process("sleep 30", session_id=session, cwd=str(tmp_path))
        process_id = next(line.split(": ", 1)[1] for line in started.splitlines() if line.startswith("Process ID:"))
        assert "Status: running" in await terminal_mcp.poll_process(process_id)
        assert "exited" in await terminal_mcp.stop_process(process_id)
    run(lifecycle())
