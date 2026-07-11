import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import mcp
import terminal_mcp


EXPECTED_TOOLS = [
    "project_context", "local_skills", "local_mcp", "run_command", "start_process",
    "poll_process", "stop_process", "set_session_env", "read_file", "write_file",
    "replace_in_file", "apply_patch", "list_dir", "stat_path", "make_dir", "copy_path",
    "move_path", "run_codex_yolo", "start_codex_yolo", "run_agy_yolo", "start_agy_yolo",
]


def run(coro):
    return asyncio.run(coro)


def isolated_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    return home


def test_exact_public_tool_list():
    tools = run(terminal_mcp.mcp.list_tools())
    assert [tool.name for tool in tools] == EXPECTED_TOOLS


def test_bearer_auth_middleware():
    async def invoke(authorization=None):
        messages = []

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            messages.append(message)

        headers = []
        if authorization is not None:
            headers.append((b"authorization", authorization.encode("latin-1")))
        middleware = terminal_mcp.BearerAuthMiddleware(app, "correct-token")
        await middleware({"type": "http", "headers": headers}, receive, send)
        return messages

    assert run(invoke())[0]["status"] == 401
    assert run(invoke("Bearer wrong"))[0]["status"] == 401
    assert run(invoke("Bearer correct-token"))[0]["status"] == 204


def test_project_context_gate_and_truncation(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "project"
    project.mkdir()
    (project / ".git").mkdir()
    agents = project / "AGENTS.md"
    agents.write_text("rule-one\n" + "a" * 500, encoding="utf-8")

    truncated = run(terminal_mcp.project_context(session_id="context-test", cwd=str(project), max_chars=280))
    assert "Context truncated" in truncated
    assert "Context Gate: satisfied" not in truncated

    loaded = run(terminal_mcp.project_context(session_id="context-test", cwd=str(project), max_chars=2000))
    assert "rule-one" in loaded
    assert "Context Gate: satisfied" in loaded

    agents.write_text("rule-two", encoding="utf-8")
    blocked = run(terminal_mcp.write_file("blocked.txt", "nope", session_id="context-test", cwd=str(project)))
    assert "Project context has not been loaded" in blocked
    assert not (project / "blocked.txt").exists()


def test_local_skills_list_read_and_search(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    (tmp_path / ".git").mkdir()
    skill = tmp_path / "skills" / "demo" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("# Demo\nUseful terminal workflow\n", encoding="utf-8")

    listing = json.loads(run(terminal_mcp.local_skills(cwd=str(tmp_path))))
    assert any(row["name"] == "demo" for row in listing["skills"])

    read = json.loads(run(terminal_mcp.local_skills("read", name="demo", cwd=str(tmp_path))))
    assert "Useful terminal workflow" in read["content"]

    search = json.loads(run(terminal_mcp.local_skills("search", query="terminal", cwd=str(tmp_path))))
    assert any(row["name"] == "demo" for row in search["matches"])


def test_local_mcp_persists_reconnects_resets_and_locks_profiles(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    (tmp_path / ".git").mkdir()
    server_script = tmp_path / "stateful_server.py"
    server_script.write_text(
        "import os\n"
        "from pathlib import Path\n"
        "from mcp.server.fastmcp import FastMCP\n"
        "m = FastMCP('stateful')\n"
        "counter_value = 0\n"
        "@m.tool()\n"
        "def counter() -> str:\n"
        "    global counter_value\n"
        "    counter_value += 1\n"
        "    return f'{os.getpid()}:{counter_value}'\n"
        "@m.tool()\n"
        "def crash_once(marker: str) -> str:\n"
        "    path = Path(marker)\n"
        "    if not path.exists():\n"
        "        path.write_text('crashed', encoding='utf-8')\n"
        "        os._exit(17)\n"
        "    return 'recovered'\n"
        "m.run(transport='stdio')\n",
        encoding="utf-8",
    )
    profile = tmp_path / "browser-profile"
    config = tmp_path / ".mcp.json"
    config.write_text(
        json.dumps({
            "mcpServers": {
                "stateful": {
                    "command": sys.executable,
                    "args": [str(server_script)],
                    "env": {
                        "TOKEN": "must-not-leak",
                        "PYTHONPATH": str(Path(mcp.__file__).resolve().parents[1]),
                        "SAB_USER_DATA_DIR": str(profile),
                    },
                }
            }
        }),
        encoding="utf-8",
    )

    def text(result):
        return "\n".join(getattr(item, "text", "") for item in result.content)

    async def lifecycle():
        session = "mcp-test"
        await terminal_mcp.project_context(session_id=session, cwd=str(tmp_path))

        async def request(*args, **kwargs):
            # Each proxy call runs in a fresh task, matching separate HTTP tool
            # requests while the actor-owned downstream transport stays alive.
            return await asyncio.create_task(terminal_mcp.local_mcp(*args, **kwargs))

        listing = json.loads(await request(cwd=str(tmp_path), session_id=session))
        assert listing["servers"][0]["status"] == "configured"
        assert "must-not-leak" not in json.dumps(listing)

        tools = await request(
            "tools", server="stateful", session_id=session, cwd=str(tmp_path)
        )
        assert '"counter"' in tools

        first = text(await request(
            "call", server="stateful", tool="counter", session_id=session, cwd=str(tmp_path)
        ))
        second = text(await request(
            "call", server="stateful", tool="counter", session_id=session, cwd=str(tmp_path)
        ))
        first_pid, first_count = first.split(":")
        second_pid, second_count = second.split(":")
        assert first_pid == second_pid
        assert (first_count, second_count) == ("1", "2")

        connected = json.loads(await request(cwd=str(tmp_path), session_id=session))
        assert connected["servers"][0]["status"] == "connected"

        await terminal_mcp.project_context(session_id="other-owner", cwd=str(tmp_path))
        busy = await request(
            "call",
            server="stateful",
            tool="counter",
            session_id="other-owner",
            cwd=str(tmp_path),
        )
        assert "already owned by session" in busy

        marker = tmp_path / "crash.marker"
        recovered = text(await request(
            "call",
            server="stateful",
            tool="crash_once",
            arguments={"marker": str(marker)},
            session_id=session,
            cwd=str(tmp_path),
        ))
        assert recovered == "recovered"

        reset = await request(
            "reset", server="stateful", session_id=session, cwd=str(tmp_path)
        )
        assert "closed 1 connection" in reset

        after_reset = text(await request(
            "call", server="stateful", tool="counter", session_id=session, cwd=str(tmp_path)
        ))
        reset_pid, reset_count = after_reset.split(":")
        assert reset_pid != second_pid
        assert reset_count == "1"

        await request("reset-all", session_id=session, cwd=str(tmp_path))
        await request("reset-all", session_id="other-owner", cwd=str(tmp_path))

    run(lifecycle())


def test_filesystem_and_process_lifecycle(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)

    async def lifecycle():
        session = "fs-test"
        await terminal_mcp.project_context(session_id=session, cwd=str(tmp_path))
        assert "Bytes Written: 5" in await terminal_mcp.write_file(
            "one.txt", "alpha", session_id=session, cwd=str(tmp_path)
        )
        assert "Occurrences Replaced: 1" in await terminal_mcp.replace_in_file(
            "one.txt", "alpha", "beta", session_id=session, cwd=str(tmp_path)
        )
        assert "beta" in await terminal_mcp.read_file("one.txt", session_id=session, cwd=str(tmp_path))
        await terminal_mcp.make_dir("nested", session_id=session, cwd=str(tmp_path))
        await terminal_mcp.copy_path("one.txt", "nested/two.txt", session_id=session, cwd=str(tmp_path))
        assert "Moved:" in await terminal_mcp.move_path(
            "nested/two.txt", "three.txt", session_id=session, cwd=str(tmp_path)
        )
        started = await terminal_mcp.start_process("sleep 30", session_id=session, cwd=str(tmp_path))
        process_id = next(
            line.split(": ", 1)[1]
            for line in started.splitlines()
            if line.startswith("Process ID:")
        )
        assert "Status: running" in await terminal_mcp.poll_process(process_id)
        assert "exited" in await terminal_mcp.stop_process(process_id)

    run(lifecycle())
