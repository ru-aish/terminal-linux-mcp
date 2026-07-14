import asyncio
import base64
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

from mcp import ClientSession, types as mcp_types
from mcp.client.streamable_http import streamable_http_client


REPO_ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_port(port: int, process: subprocess.Popen[str], timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=2)
            raise AssertionError(f"server exited early\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise AssertionError(f"server did not listen on port {port}")


def test_downstream_mcp_persists_across_distinct_http_sessions(tmp_path):
    (tmp_path / ".git").mkdir()
    stateful = tmp_path / "stateful_server.py"
    stateful.write_text(
        "import os\n"
        "from mcp.server.fastmcp import FastMCP\n"
        "m = FastMCP('http-stateful')\n"
        "value = 0\n"
        "@m.tool()\n"
        "def counter() -> str:\n"
        "    global value\n"
        "    value += 1\n"
        "    return f'{os.getpid()}:{value}'\n"
        "m.run(transport='stdio')\n",
        encoding="utf-8",
    )
    (tmp_path / ".mcp.json").write_text(
        json.dumps({
            "mcpServers": {
                "stateful-http": {
                    "command": sys.executable,
                    "args": [str(stateful)],
                }
            }
        }),
        encoding="utf-8",
    )

    port = _free_port()
    env = dict(**__import__("os").environ)
    env["MCP_WORKSPACE"] = str(tmp_path)
    env["MCP_LOG_DIR"] = str(tmp_path / "logs")
    process = subprocess.Popen(
        [
            sys.executable,
            str(REPO_ROOT / "terminal_mcp.py"),
            "--transport",
            "streamable-http",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    async def call(tool: str, arguments: dict):
        url = f"http://127.0.0.1:{port}/mcp"
        async with streamable_http_client(url) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                await session.initialize()
                return await session.call_tool(tool, arguments)

    def text(result) -> str:
        return "\n".join(getattr(item, "text", "") for item in result.content)

    async def exercise() -> None:
        session_id = "http-persistence-test"
        await call(
            "project_context",
            {"session_id": session_id, "cwd": str(tmp_path), "max_chars": 50000},
        )
        first = text(await call(
            "local_mcp",
            {
                "action": "call",
                "server": "stateful-http",
                "tool": "counter",
                "arguments": {},
                "session_id": session_id,
                "cwd": str(tmp_path),
                "timeout": 30,
            },
        ))
        second = text(await call(
            "local_mcp",
            {
                "action": "call",
                "server": "stateful-http",
                "tool": "counter",
                "arguments": {},
                "session_id": session_id,
                "cwd": str(tmp_path),
                "timeout": 30,
            },
        ))
        listing = text(await call(
            "local_mcp",
            {
                "action": "list",
                "session_id": session_id,
                "cwd": str(tmp_path),
            },
        ))

        first_pid, first_count = first.split(":")
        second_pid, second_count = second.split(":")
        assert first_pid == second_pid
        assert (first_count, second_count) == ("1", "2")
        assert '"status": "connected"' in listing

        await call(
            "local_mcp",
            {
                "action": "reset-all",
                "session_id": session_id,
                "cwd": str(tmp_path),
            },
        )

    try:
        _wait_for_port(port, process)
        asyncio.run(exercise())
    finally:
        process.terminate()
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)


def test_http_initialize_bootstrap_and_usage_accounting(tmp_path):
    (tmp_path / ".git").mkdir()
    project_gpt = tmp_path / ".GPT"
    project_gpt.mkdir()
    (project_gpt / "AGENTS.md").write_text("http-project-rule", encoding="utf-8")

    gpt_home = tmp_path / "gpt-home"
    gpt_home.mkdir()
    (gpt_home / "AGENTS.md").write_text("http-global-rule", encoding="utf-8")
    skill = gpt_home / "skills" / "http-skill" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text(
        "---\nname: http-skill\ndescription: HTTP bootstrap skill\n---\n# Workflow\n",
        encoding="utf-8",
    )
    image_bytes = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl2nqkAAAAASUVORK5CYII="
    )
    image_path = tmp_path / "http-image.png"
    image_path.write_bytes(image_bytes)

    port = _free_port()
    env = dict(**__import__("os").environ)
    env["MCP_WORKSPACE"] = str(tmp_path)
    env["MCP_LOG_DIR"] = str(tmp_path / "logs")
    env["MCP_GPT_HOME"] = str(gpt_home)
    process = subprocess.Popen(
        [
            sys.executable,
            str(REPO_ROOT / "terminal_mcp.py"),
            "--transport",
            "streamable-http",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    async def exercise() -> None:
        url = f"http://127.0.0.1:{port}/mcp"
        async with streamable_http_client(url) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                initialized = await session.initialize()
                assert "http-global-rule" in (initialized.instructions or "")
                assert "http-project-rule" not in (initialized.instructions or "")
                assert "Project-specific .GPT instructions are loaded" in (initialized.instructions or "")
                assert "http-skill" in (initialized.instructions or "")
                assert "bootstrap_thread" in (initialized.instructions or "")

                tools = await session.list_tools()
                names = [tool.name for tool in tools.tools]
                assert names[0] == "bootstrap_thread"
                assert "record_token_usage" in names
                assert "watch_image" in names
                assert len(names) == 28

                blocked = await session.call_tool(
                    "run_command",
                    {"command": "printf blocked", "cwd": str(tmp_path)},
                )
                blocked_text = "\n".join(getattr(item, "text", "") for item in blocked.content)
                assert "session_id='default' is intentionally rejected" in blocked_text

                bootstrapped = await session.call_tool(
                    "bootstrap_thread",
                    {
                        "thread_id": "http-thread",
                        "cwd": str(tmp_path),
                        "max_chars": 100000,
                    },
                )
                bootstrap_text = "\n".join(
                    getattr(item, "text", "") for item in bootstrapped.content
                )
                assert "Context Gate: satisfied" in bootstrap_text
                assert "http-global-rule" in bootstrap_text
                assert "http-project-rule" in bootstrap_text

                command = await session.call_tool(
                    "run_command",
                    {
                        "command": "printf ready",
                        "session_id": "http-thread",
                        "cwd": str(tmp_path),
                    },
                )
                command_text = "\n".join(getattr(item, "text", "") for item in command.content)
                assert "ready" in command_text

                watched = await session.call_tool(
                    "watch_image",
                    {
                        "path": str(image_path),
                        "session_id": "http-thread",
                        "cwd": str(tmp_path),
                    },
                )
                assert watched.isError is False
                assert isinstance(watched.content[0], mcp_types.ImageContent)
                assert watched.content[0].mimeType == "image/png"
                assert base64.b64decode(watched.content[0].data) == image_bytes

                await session.call_tool(
                    "record_token_usage",
                    {
                        "thread_id": "http-thread",
                        "input_tokens": 200,
                        "output_tokens": 50,
                        "cached_input_tokens": 25,
                        "model": "integration-model",
                    },
                )
                usage = await session.call_tool(
                    "get_token_usage",
                    {"thread_id": "http-thread", "limit": 10},
                )
                usage_text = "\n".join(getattr(item, "text", "") for item in usage.content)
                payload = json.loads(usage_text)
                assert payload["totals"]["exact_input_tokens"] == 200
                assert payload["totals"]["exact_output_tokens"] == 50
                assert payload["totals"]["exact_cached_input_tokens"] == 25
                assert payload["totals"]["estimated_input_tokens"] > 0

        (gpt_home / "AGENTS.md").write_text(
            "http-global-rule-updated", encoding="utf-8"
        )
        async with streamable_http_client(url) as streams:
            async with ClientSession(streams[0], streams[1]) as session:
                refreshed = await session.initialize()
                assert "http-global-rule-updated" in (refreshed.instructions or "")
                assert "http-global-rule\n" not in (refreshed.instructions or "")

    try:
        _wait_for_port(port, process)
        asyncio.run(exercise())
    finally:
        process.terminate()
        try:
            process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
