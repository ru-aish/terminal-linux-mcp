import asyncio
import json
import socket
import subprocess
import sys
import time
from pathlib import Path

from mcp import ClientSession
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
        "import time\n"
        "from pathlib import Path\n"
        "from mcp.server.fastmcp import FastMCP\n"
        "m = FastMCP('http-stateful')\n"
        "value = 0\n"
        "@m.tool()\n"
        "def counter() -> str:\n"
        "    global value\n"
        "    value += 1\n"
        "    return f'{os.getpid()}:{value}'\n"
        "@m.tool()\n"
        "def crash_once(marker: str) -> str:\n"
        "    path = Path(marker)\n"
        "    if not path.exists():\n"
        "        path.write_text('crashed', encoding='utf-8')\n"
        "        os._exit(17)\n"
        "    return 'recovered'\n"
        "@m.tool()\n"
        "def slow(seconds: float) -> str:\n"
        "    time.sleep(seconds)\n"
        "    return str(os.getpid())\n"
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

        marker = tmp_path / "http-crash.marker"
        recovered = text(await call(
            "local_mcp",
            {
                "action": "call",
                "server": "stateful-http",
                "tool": "crash_once",
                "arguments": {"marker": str(marker)},
                "session_id": session_id,
                "cwd": str(tmp_path),
                "timeout": 30,
            },
        ))
        assert recovered == "recovered"

        before_timeout = text(await call(
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
        before_timeout_pid, before_timeout_count = before_timeout.split(":")
        assert before_timeout_count == "1"

        started = time.monotonic()
        timed_out = text(await call(
            "local_mcp",
            {
                "action": "call",
                "server": "stateful-http",
                "tool": "slow",
                "arguments": {"seconds": 10},
                "session_id": session_id,
                "cwd": str(tmp_path),
                "timeout": 1,
            },
        ))
        elapsed = time.monotonic() - started
        assert "timed out" in timed_out
        assert elapsed < 2.5, f"one-second HTTP timeout took {elapsed:.2f}s"

        after_timeout = text(await call(
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
        after_timeout_pid, after_timeout_count = after_timeout.split(":")
        assert after_timeout_pid != before_timeout_pid
        assert after_timeout_count == "1"

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
