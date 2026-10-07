import asyncio
import base64
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import mcp
import terminal_mcp


EXPECTED_TOOLS = [
    "bootstrap_thread", "get_thread_context", "context_manifest", "refresh_startup_context",
    "thread_goal", "record_token_usage", "get_token_usage", "project_context", "local_skills", "local_mcp",
    "node_repl_js", "node_repl_js_reset",
    "run_command", "start_process", "poll_process", "stop_process", "set_session_env",
    "read_file", "watch_image", "write_file", "replace_in_file", "apply_patch", "list_dir", "stat_path",
    "make_dir", "copy_path", "move_path", "run_codex_yolo", "start_codex_yolo",
    "run_agy_yolo", "start_agy_yolo",
    "chat_runtime_status", "chat_runtime_health", "chat_runtime_control",
    "chat_runtime_logs", "chat_runtime_circuit",
    "agent_projects_list", "agent_project_get", "agent_project_threads",
    "agent_register_parent", "agent_spawn", "agent_status", "agent_context",
    "agent_tail", "agent_send", "agent_schedule_wakeup", "agent_schedule_continue",
    "agent_continue_after_process", "agent_queue_after_completion", "agent_automation",
    "agent_wait", "agent_sync", "agent_children",
    "agent_subscribe", "agent_ack", "agent_cancel",
]

def run(coro):
    return asyncio.run(coro)


def isolated_home(tmp_path, monkeypatch):
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("MCP_GPT_HOME", raising=False)
    monkeypatch.delenv("GPT_HOME", raising=False)
    return home


def bound_thread(label: str) -> str:
    conversation_id = f"test://{terminal_mcp.GPT_STORE.home()}/{label}"
    return terminal_mcp.GPT_STORE.ensure_chat_binding(
        conversation_id, source="test"
    )["thread_id"]


def test_ensure_layout_migrates_only_legacy_thread_identity_rules(tmp_path, monkeypatch):
    home = isolated_home(tmp_path, monkeypatch)
    gpt_home = home / ".GPT"
    gpt_home.mkdir()
    agents = gpt_home / "AGENTS.md"
    agents.write_text(
        "# Custom\n\n"
        "2. Start each new model thread by calling `bootstrap_thread` with a unique, stable `thread_id`.\n"
        "3. Reuse that same value as `session_id` for all later terminal tools in the thread.\n"
        "99. preserve-this-custom-rule\n",
        encoding="utf-8",
    )

    terminal_mcp.GPT_STORE.ensure_layout()
    migrated = agents.read_text(encoding="utf-8")
    assert "Never invent or replace a Terminal MCP thread ID" in migrated
    assert "Once assigned, bootstrap is forbidden" in migrated
    assert "unique, stable `thread_id`" not in migrated
    assert "99. preserve-this-custom-rule" in migrated


def test_chat_binding_preserves_existing_bootstrapped_thread_id(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    legacy_thread_id = "agent_legacy_thread"
    terminal_mcp.GPT_STORE.upsert_thread(
        legacy_thread_id,
        tmp_path,
        "legacy-fingerprint",
        loaded=True,
        bootstrap=True,
    )

    binding = terminal_mcp.GPT_STORE.ensure_chat_binding(
        "conversation-legacy",
        source="test-migration",
        preferred_thread_id=legacy_thread_id,
    )
    assert binding["thread_id"] == legacy_thread_id
    assert terminal_mcp.GPT_STORE.ensure_chat_binding(
        "conversation-legacy", source="test-repeat"
    )["thread_id"] == legacy_thread_id

    fresh = terminal_mcp.GPT_STORE.ensure_chat_binding(
        "conversation-fresh",
        source="test-migration",
        preferred_thread_id="agent_never_bootstrapped",
    )
    assert fresh["thread_id"] == terminal_mcp.GPT_STORE.derive_thread_id(
        "conversation-fresh"
    )


def test_exact_public_tool_list():
    tools = run(terminal_mcp.mcp.list_tools())
    assert [tool.name for tool in tools] == EXPECTED_TOOLS


def test_initial_bootstrap_commit_is_atomic_across_store_instances(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    thread_id = bound_thread("concurrent-bootstrap")
    stores = [terminal_mcp.GPT_STORE.__class__(lambda: tmp_path) for _ in range(8)]
    with ThreadPoolExecutor(max_workers=len(stores)) as pool:
        results = list(pool.map(
            lambda store: store.complete_initial_bootstrap(thread_id, tmp_path, "fingerprint"),
            stores,
        ))
    assert results.count(True) == 1
    assert terminal_mcp.GPT_STORE.get_thread(thread_id)["bootstrap_count"] == 1


def test_chat_binding_creation_is_idempotent_under_concurrency(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    stores = [terminal_mcp.GPT_STORE.__class__(lambda: tmp_path) for _ in range(8)]
    with ThreadPoolExecutor(max_workers=len(stores)) as pool:
        bindings = list(pool.map(lambda store: store.ensure_chat_binding("same-chat"), stores))
    expected = terminal_mcp.GPT_STORE.derive_thread_id("same-chat")
    assert {binding["thread_id"] for binding in bindings} == {expected}
    reopened = terminal_mcp.GPT_STORE.__class__(lambda: tmp_path)
    assert reopened.ensure_chat_binding("same-chat")["thread_id"] == expected
    assert reopened.ensure_chat_binding("different-chat")["thread_id"] != expected
    with reopened._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM chat_thread_bindings").fetchone()[0] == 2


def test_host_metadata_cannot_claim_unowned_legacy_bootstrap(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    legacy = "bootstrapped-by-another-chat"
    terminal_mcp.GPT_STORE.upsert_thread(legacy, tmp_path, "old", loaded=True, bootstrap=True)
    monkeypatch.setattr(terminal_mcp, "_host_conversation_identity", lambda _server: "new-chat")
    resolved, error = terminal_mcp._resolve_request_thread_id(legacy)
    assert resolved == ""
    assert "not allowed" in error
    assert terminal_mcp.GPT_STORE.binding_for_conversation("new-chat")["thread_id"] != legacy
    assert terminal_mcp.GPT_STORE.binding_for_thread(legacy) is None


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
    home = isolated_home(tmp_path, monkeypatch)
    (home / ".codex").mkdir()
    (home / ".codex" / "AGENTS.md").write_text("codex-rule-must-not-load", encoding="utf-8")
    (home / ".GPT").mkdir()
    (home / ".GPT" / "AGENTS.md").write_text("global-gpt-rule", encoding="utf-8")

    project = tmp_path / "project"
    project.mkdir()
    (project / ".git").mkdir()
    instruction_dir = project / ".GPT"
    instruction_dir.mkdir()
    agents = instruction_dir / "AGENTS.md"
    agents.write_text("rule-one\n" + "a" * 500, encoding="utf-8")

    blocked_default = run(terminal_mcp.write_file(
        "default-blocked.txt", "nope", cwd=str(project)
    ))
    assert "session_id='default' is intentionally rejected" in blocked_default

    thread_id = bound_thread("context-test")
    other_thread_id = bound_thread("context-test-other")
    blocked_new_thread = run(terminal_mcp.write_file(
        "blocked-before-bootstrap.txt", "nope", session_id=thread_id, cwd=str(project)
    ))
    assert "GPT thread context has not been loaded" in blocked_new_thread

    truncated = run(terminal_mcp.get_thread_context(
        thread_id=thread_id, cwd=str(project), max_chars=280
    ))
    assert "Context truncated" in truncated
    assert "Context Gate: satisfied" not in truncated
    assert "Call get_thread_context again with a larger max_chars" in truncated

    loaded = run(terminal_mcp.get_thread_context(
        thread_id=thread_id, cwd=str(project), max_chars=100000
    ))
    assert "global-gpt-rule" in loaded
    assert "rule-one" in loaded
    assert "codex-rule-must-not-load" not in loaded
    assert "Context Gate: satisfied" in loaded
    assert "Public Terminal GPT tools" in loaded

    allowed = run(terminal_mcp.write_file(
        "allowed.txt", "yes", session_id=thread_id, cwd=str(project)
    ))
    assert "Bytes Written" in allowed

    # A service restart clears the in-memory session map. Durable bootstrap
    # state must restore the session without forcing a model thread that
    # already loaded the same instructions to bootstrap again.
    terminal_mcp.sessions.pop(thread_id, None)
    restored = run(terminal_mcp.write_file(
        "restored-after-restart.txt", "yes", session_id=thread_id, cwd=str(project)
    ))
    assert "Bytes Written" in restored

    other_thread = run(terminal_mcp.write_file(
        "other-thread.txt", "nope", session_id=other_thread_id, cwd=str(project)
    ))
    assert "GPT thread context has not been loaded" in other_thread

    agents.write_text("rule-two", encoding="utf-8")
    terminal_mcp.sessions.pop(thread_id, None)
    blocked = run(terminal_mcp.write_file(
        "blocked.txt", "nope", session_id=thread_id, cwd=str(project)
    ))
    assert "GPT thread context has not been loaded" in blocked
    assert not (project / "blocked.txt").exists()

    reloaded = run(terminal_mcp.get_thread_context(
        thread_id=thread_id, cwd=str(project), max_chars=100000
    ))
    assert "rule-two" in reloaded
    assert "Context Gate: satisfied" in reloaded


def test_thread_goal_lifecycle_and_bootstrap_restoration(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "goal-project"
    project.mkdir()
    (project / ".git").mkdir()
    thread_id = bound_thread("goal-lifecycle-thread")

    run(terminal_mcp.get_thread_context(thread_id, str(project), max_chars=100000))
    created = json.loads(run(terminal_mcp.thread_goal(
        action="set",
        session_id=thread_id,
        objective="Finish the goal implementation",
        finish_conditions=["Feature exists", "Tests pass"],
        cwd=str(project),
    )))
    assert created["goal"]["status"] == "active"
    assert created["goal"]["finish_conditions"] == ["Feature exists", "Tests pass"]
    fresh_store = terminal_mcp.GPTThreadStore(lambda: project)
    assert fresh_store.get_goal(thread_id)["objective"] == "Finish the goal implementation"

    incomplete = run(terminal_mcp.thread_goal(
        action="complete",
        session_id=thread_id,
        evidence=["Feature exists"],
        cwd=str(project),
    ))
    assert "exactly one evidence entry" in incomplete
    assert terminal_mcp.GPT_STORE.get_goal(thread_id)["status"] == "active"

    terminal_mcp.sessions.pop(thread_id, None)
    restored = run(terminal_mcp.get_thread_context(thread_id, str(project), max_chars=100000))
    assert "## Active thread goal" in restored
    assert "Finish the goal implementation" in restored
    assert "Tests pass" in restored

    completed = json.loads(run(terminal_mcp.thread_goal(
        action="complete",
        session_id=thread_id,
        evidence=["Implemented in terminal_mcp.py", "Targeted and full tests passed"],
        cwd=str(project),
    )))
    assert completed["goal"]["status"] == "completed"
    assert len(completed["goal"]["completion_evidence"]) == 2

    run(terminal_mcp.thread_goal(
        action="set",
        session_id=thread_id,
        objective="Recover from a genuine infrastructure failure",
        finish_conditions=["Infrastructure is available"],
        cwd=str(project),
    ))
    technical_error = json.loads(run(terminal_mcp.thread_goal(
        action="technical_error",
        session_id=thread_id,
        reason="Required remote endpoint is unavailable",
        cwd=str(project),
    )))
    assert technical_error["goal"]["status"] == "technical_error"
    resumed = json.loads(run(terminal_mcp.thread_goal(
        action="resume",
        session_id=thread_id,
        cwd=str(project),
    )))
    assert resumed["goal"]["status"] == "active"
    cleared = json.loads(run(terminal_mcp.thread_goal(
        action="clear",
        session_id=thread_id,
        cwd=str(project),
    )))
    assert cleared["cleared"] is True
    assert terminal_mcp.GPT_STORE.get_goal(thread_id) is None


def test_due_goal_reminder_appends_once_to_text_image_and_error(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "goal-reminder-project"
    project.mkdir()
    (project / ".git").mkdir()
    text_path = project / "note.txt"
    text_path.write_text("hello", encoding="utf-8")
    image_bytes = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl2nqkAAAAASUVORK5CYII="
    )
    image_path = project / "pixel.png"
    image_path.write_bytes(image_bytes)
    thread_id = bound_thread("goal-reminder-thread")

    run(terminal_mcp.get_thread_context(thread_id, str(project), max_chars=100000))
    run(terminal_mcp.thread_goal(
        action="set",
        session_id=thread_id,
        objective="Inspect all requested artifacts",
        finish_conditions=["Text inspected", "Image inspected"],
        cwd=str(project),
    ))

    def make_due() -> None:
        with terminal_mcp.GPT_STORE._connect() as connection:
            connection.execute(
                "UPDATE thread_goals SET last_seen_at = '2000-01-01T00:00:00Z' WHERE thread_id = ?",
                (thread_id,),
            )

    make_due()
    text_result = run(terminal_mcp.mcp.call_tool(
        "read_file",
        {"path": str(text_path), "session_id": thread_id, "cwd": str(project)},
    ))
    text_blocks = text_result[0]
    assert any("Periodic active-goal reminder" in block.text for block in text_blocks)

    second_result = run(terminal_mcp.mcp.call_tool(
        "read_file",
        {"path": str(text_path), "session_id": thread_id, "cwd": str(project)},
    ))
    assert not any("Periodic active-goal reminder" in block.text for block in second_result[0])

    make_due()
    image_result = run(terminal_mcp.mcp.call_tool(
        "watch_image",
        {"path": str(image_path), "session_id": thread_id, "cwd": str(project)},
    ))
    assert isinstance(image_result, mcp.types.CallToolResult)
    assert any(isinstance(block, mcp.types.ImageContent) for block in image_result.content)
    assert any(
        isinstance(block, mcp.types.TextContent)
        and "Periodic active-goal reminder" in block.text
        for block in image_result.content
    )

    make_due()
    error_result = run(terminal_mcp.mcp.call_tool(
        "watch_image",
        {"path": str(project / "missing.png"), "session_id": thread_id, "cwd": str(project)},
    ))
    assert error_result.isError is True
    assert any(
        isinstance(block, mcp.types.TextContent)
        and "Periodic active-goal reminder" in block.text
        for block in error_result.content
    )


def test_goal_reminder_claim_is_atomic(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    thread_id = "goal-atomic-thread"
    terminal_mcp.GPT_STORE.set_goal(thread_id, "Atomic reminder", ["Only one caller receives it"])
    with terminal_mcp.GPT_STORE._connect() as connection:
        connection.execute(
            "UPDATE thread_goals SET last_seen_at = '2000-01-01T00:00:00Z' WHERE thread_id = ?",
            (thread_id,),
        )

    def claim():
        return terminal_mcp.GPT_STORE.claim_goal_reminder(thread_id, interval_seconds=900)

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: claim(), range(4)))
    assert sum(result is not None for result in results) == 1


def test_bounded_command_capture_spills_large_output(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "project"
    project.mkdir()
    (project / ".git").mkdir()
    (project / ".GPT").mkdir()
    (project / ".GPT" / "AGENTS.md").write_text("rule", encoding="utf-8")
    monkeypatch.setattr(terminal_mcp, "CAPTURE_MEMORY_CHARS", 128)
    thread_id = bound_thread("bounded-output")
    run(terminal_mcp.get_thread_context(thread_id=thread_id, cwd=str(project), max_chars=100000))
    result = run(terminal_mcp.run_command(
        "python -c \"print('x' * 4096)\"",
        session_id=thread_id,
        cwd=str(project),
        max_output_chars=96,
    ))
    assert "Full Output Logs:" in result
    log = next((line for line in result.splitlines() if line.startswith(str(terminal_mcp.LOG_DIR))), None)
    assert log and Path(log).read_text(encoding="utf-8").count("x") == 4096


def test_run_command_preserves_original_exit_code(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "exit-code-project"
    project.mkdir()
    (project / ".git").mkdir()
    (project / ".GPT").mkdir()
    (project / ".GPT" / "AGENTS.md").write_text("rule", encoding="utf-8")
    thread_id = bound_thread("exit-code-thread")
    run(terminal_mcp.get_thread_context(thread_id=thread_id, cwd=str(project), max_chars=100000))

    result = run(terminal_mcp.run_command(
        "python -c 'raise SystemExit(7)'",
        session_id=thread_id,
        cwd=str(project),
    ))

    assert "Exit Code: 7" in result
    assert "Working Directory:" in result


def test_workload_scope_prefix_is_optional_and_sanitized(monkeypatch):
    monkeypatch.setattr(terminal_mcp, "WORKLOAD_ISOLATION", "auto")
    monkeypatch.setattr(terminal_mcp.shutil, "which", lambda name: "/usr/bin/systemd-run" if name == "systemd-run" else None)
    args, unit = terminal_mcp._systemd_workload_prefix("a / b", "request:1")
    assert unit == "mcp-workload-a-b-request-1.scope"
    assert args[:4] == ["systemd-run", "--user", "--scope", "--quiet"]


def test_local_skills_list_read_and_search(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    (tmp_path / ".git").mkdir()
    skill = tmp_path / "skills" / "demo" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text("# Demo\nUseful terminal workflow\n", encoding="utf-8")

    session = bound_thread("skills-test")
    run(terminal_mcp.get_thread_context(thread_id=session, cwd=str(tmp_path), max_chars=100000))

    listing = json.loads(run(terminal_mcp.local_skills(
        cwd=str(tmp_path), session_id=session
    )))
    assert any(row["name"] == "demo" for row in listing["skills"])

    read = json.loads(run(terminal_mcp.local_skills(
        "read", name="demo", cwd=str(tmp_path), session_id=session
    )))
    assert "Useful terminal workflow" in read["content"]

    search = json.loads(run(terminal_mcp.local_skills(
        "search", query="terminal", cwd=str(tmp_path), session_id=session
    )))
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
        session = bound_thread("mcp-test")
        await terminal_mcp.get_thread_context(thread_id=session, cwd=str(tmp_path), max_chars=100000)

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

        other_owner = bound_thread("other-owner")
        await terminal_mcp.get_thread_context(thread_id=other_owner, cwd=str(tmp_path), max_chars=100000)
        busy = await request(
            "call",
            server="stateful",
            tool="counter",
            session_id=other_owner,
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
        await request("reset-all", session_id=other_owner, cwd=str(tmp_path))

    run(lifecycle())


def test_filesystem_and_process_lifecycle(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)

    async def lifecycle():
        session = bound_thread("fs-test")
        await terminal_mcp.get_thread_context(thread_id=session, cwd=str(tmp_path), max_chars=100000)
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
        assert "Status: running" in await terminal_mcp.poll_process(
            process_id, session_id=session
        )

        other_session = bound_thread("fs-test-other")
        await terminal_mcp.get_thread_context(thread_id=other_session, cwd=str(tmp_path), max_chars=100000)
        denied = await terminal_mcp.poll_process(process_id, session_id=other_session)
        assert f"not owned by session {other_session}" in denied
        denied_stop = await terminal_mcp.stop_process(process_id, session_id=other_session)
        assert f"not owned by session {other_session}" in denied_stop

        assert "exited" in await terminal_mcp.stop_process(
            process_id, session_id=session
        )

    run(lifecycle())


def test_process_completion_bot_queues_exactly_one_continue(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "process-wake-project"
    project.mkdir()
    (project / ".git").mkdir()
    session_id = bound_thread("process-wake-thread")

    class FakeCoordinator:
        def __init__(self):
            self.wakeup_calls = []

        async def automations(self, **kwargs):
            return {"automations": [], "options": kwargs}

        async def schedule_wakeup(self, thread_id, wake_at, **kwargs):
            call = {"thread_id": thread_id, "wake_at": wake_at, "options": kwargs}
            self.wakeup_calls.append(call)
            return {
                "automation_id": "auto-process-finished",
                "target_agent_id": thread_id,
                "due_at": float(wake_at),
                "status": "scheduled",
                "options": kwargs,
            }

    class FakeService:
        def __init__(self):
            self.wakes = 0

        def wake(self):
            self.wakes += 1

    coordinator = FakeCoordinator()
    service = FakeService()
    monkeypatch.setattr(terminal_mcp, "get_chat_agent_coordinator", lambda: coordinator)
    monkeypatch.setattr(terminal_mcp, "get_chat_agent_service", lambda: service)

    async def lifecycle():
        await terminal_mcp.get_thread_context(
            session_id, str(project), max_chars=100000
        )
        started = await terminal_mcp.start_process(
            f"{sys.executable} -c 'import time; time.sleep(0.35)'",
            session_id=session_id,
            cwd=str(project),
        )
        process_id = next(
            line.split(": ", 1)[1]
            for line in started.splitlines()
            if line.startswith("Process ID:")
        )
        watched = json.loads(
            await terminal_mcp.agent_continue_after_process(
                process_id,
                "agent-child",
                check_seconds=0.05,
                idempotency_key="resume-after-process",
                session_id=session_id,
                cwd=str(project),
            )
        )
        assert watched["status"] == "watching"
        assert watched["continue_message"] == "continue"
        watcher_id = watched["watcher_process_id"]

        duplicate = json.loads(
            await terminal_mcp.agent_continue_after_process(
                process_id,
                "agent-child",
                check_seconds=0.05,
                idempotency_key="resume-after-process",
                session_id=session_id,
                cwd=str(project),
            )
        )
        assert duplicate["status"] == "already_watching"
        assert duplicate["watcher_process_id"] == watcher_id

        for _ in range(80):
            if coordinator.wakeup_calls:
                break
            await asyncio.sleep(0.05)
        assert len(coordinator.wakeup_calls) == 1
        call = coordinator.wakeup_calls[0]
        assert call["thread_id"] == "agent-child"
        assert call["options"] == {
            "prompt": "continue",
            "idempotency_key": "resume-after-process",
        }
        assert service.wakes == 1

        await asyncio.sleep(0.2)
        assert len(coordinator.wakeup_calls) == 1
        watcher_status = await terminal_mcp.poll_process(
            watcher_id,
            session_id=session_id,
        )
        assert "exited(0)" in watcher_status
        assert "queued one continue" in watcher_status

    run(lifecycle())


def test_watch_image_returns_native_image_content(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    (tmp_path / ".git").mkdir()
    png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Wl2nqkAAAAASUVORK5CYII="
    )
    image = tmp_path / "pixel.data"
    image.write_bytes(png)

    blocked = run(terminal_mcp.watch_image(
        str(image), session_id="default", cwd=str(tmp_path)
    ))
    assert blocked.isError is True
    assert "session_id='default' is intentionally rejected" in blocked.content[0].text

    session = bound_thread("watch-image-test")
    run(terminal_mcp.get_thread_context(session, str(tmp_path), max_chars=100000))
    result = run(terminal_mcp.watch_image(
        str(image), session_id=session, cwd=str(tmp_path)
    ))
    assert result.isError is False
    assert isinstance(result.content[0], mcp.types.ImageContent)
    assert result.content[0].mimeType == "image/png"
    assert base64.b64decode(result.content[0].data) == png
    assert isinstance(result.content[1], mcp.types.TextContent)
    assert "Payload: original file bytes" in result.content[1].text

    invalid = tmp_path / "not-an-image.png"
    invalid.write_text("not an image", encoding="utf-8")
    rejected = run(terminal_mcp.watch_image(
        str(invalid), session_id=session, cwd=str(tmp_path)
    ))
    assert rejected.isError is True
    assert "unsupported image format" in rejected.content[0].text

    one_frame_gif = base64.b64decode(
        "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
    )
    descriptor = one_frame_gif.index(b"\x2c")
    animated_gif = one_frame_gif[:-1] + one_frame_gif[descriptor:-1] + b"\x3b"
    gif_path = tmp_path / "animated.gif"
    gif_path.write_bytes(animated_gif)
    animated = run(terminal_mcp.watch_image(
        str(gif_path), session_id=session, cwd=str(tmp_path)
    ))
    assert animated.isError is True
    assert "animated GIFs are not supported" in animated.content[0].text

    monkeypatch.setattr(terminal_mcp, "WATCH_IMAGE_MAX_BYTES", len(png) - 1)
    oversized = run(terminal_mcp.watch_image(
        str(image), session_id=session, cwd=str(tmp_path)
    ))
    assert oversized.isError is True
    assert "MCP_WATCH_IMAGE_MAX_BYTES" in oversized.content[0].text

def test_startup_instructions_include_gpt_rules_skills_and_tools(tmp_path, monkeypatch):
    home = isolated_home(tmp_path, monkeypatch)
    gpt_home = home / ".GPT"
    gpt_home.mkdir()
    (gpt_home / "AGENTS.md").write_text("startup-gpt-rule", encoding="utf-8")
    skill = gpt_home / "skills" / "startup-demo" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text(
        "---\nname: startup-demo\ndescription: Startup demo workflow\n---\n# Workflow\n",
        encoding="utf-8",
    )
    project = tmp_path / "startup-project"
    project.mkdir()
    (project / ".git").mkdir()

    previous = terminal_mcp.mcp._mcp_server.instructions
    monkeypatch.setattr(terminal_mcp, "WORKSPACE_DIR", project)
    try:
        instructions = terminal_mcp._set_startup_instructions()
        assert "startup-gpt-rule" in instructions
        assert "startup-demo" in instructions
        assert "bootstrap_thread" in instructions
        assert "thread_goal" in instructions
        assert "/goal <objective>" in instructions
        assert "record_token_usage" in instructions
        assert "session_id='default' is rejected" in instructions
    finally:
        terminal_mcp.mcp._mcp_server.instructions = previous


def test_token_usage_database_separates_exact_and_estimated(tmp_path, monkeypatch):
    home = isolated_home(tmp_path, monkeypatch)
    gpt_home = home / ".GPT"
    gpt_home.mkdir()
    (gpt_home / "AGENTS.md").write_text("usage-rule", encoding="utf-8")
    project = tmp_path / "usage-project"
    project.mkdir()
    (project / ".git").mkdir()

    bootstrap = run(terminal_mcp.get_thread_context(
        thread_id=(thread_id := bound_thread("usage-thread")), cwd=str(project), max_chars=100000
    ))
    assert "Context Gate: satisfied" in bootstrap
    assert "Estimated Context Input Tokens" in bootstrap

    recorded = json.loads(run(terminal_mcp.record_token_usage(
        thread_id=thread_id,
        input_tokens=120,
        output_tokens=35,
        cached_input_tokens=20,
        model="test-model",
        request_id="response-1",
        metadata={"provider": "test"},
    )))
    totals = recorded["totals"]
    assert totals["exact_input_tokens"] == 120
    assert totals["exact_output_tokens"] == 35
    assert totals["exact_cached_input_tokens"] == 20
    assert totals["estimated_input_tokens"] > 0

    duplicate = json.loads(run(terminal_mcp.record_token_usage(
        thread_id=thread_id,
        input_tokens=120,
        output_tokens=35,
        cached_input_tokens=20,
        model="test-model",
        request_id="response-1",
    )))
    assert duplicate["event_id"] == recorded["event_id"]

    summary = json.loads(run(terminal_mcp.get_token_usage(thread_id)))
    assert summary["database"] == str(gpt_home / "thread_usage.db")
    assert summary["totals"]["events"] == 2
    assert summary["by_thread"][0]["thread_id"] == thread_id
    assert summary["recent_events"][0]["model"] == "test-model"
    assert summary["recent_events"][0]["is_exact"] is True

    invalid = run(terminal_mcp.record_token_usage(
        thread_id=thread_id, input_tokens=-1, output_tokens=0
    ))
    assert invalid.startswith("Error recording token usage")


def test_proxy_records_tool_call_output_and_tool_result_input_with_o200k(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "proxy-accounting"
    project.mkdir()
    (project / ".git").mkdir()

    async def exercise():
        thread_id = bound_thread("proxy-accounting-thread")
        await terminal_mcp.mcp.call_tool(
            "bootstrap_thread", {"thread_id": thread_id, "cwd": str(project), "max_chars": 100000}
        )
        await terminal_mcp.mcp.call_tool("context_manifest", {"thread_id": thread_id, "cwd": str(project)})
        return terminal_mcp._usage_summary(thread_id, limit=20)

    summary = run(exercise())
    events = [event for event in summary["recent_events"] if event["source"] == "proxy_estimate"]
    assert len(events) == 2
    assert all(event["output_tokens"] > 0 for event in events)
    assert all(event["input_tokens"] > 0 for event in events)
    assert all(event["metadata"]["input"]["tokenizer"] == "o200k_base" for event in events)



def test_chat_agent_tool_wrappers_use_injected_coordinator(tmp_path, monkeypatch):
    isolated_home(tmp_path, monkeypatch)
    project = tmp_path / "agent-tools-project"
    project.mkdir()
    (project / ".git").mkdir()
    session_id = bound_thread("agent-tools-thread")
    run(terminal_mcp.get_thread_context(session_id, str(project), max_chars=100000))

    class FakeCoordinator:
        def __init__(self):
            self.wakeup_calls = []

        async def list_projects(self, *, limit=20, cursor=None):
            return {"items": [{"id": "g-p-one", "name": "One"}], "cursor": None}

        async def register_parent(self, chat_id, **kwargs):
            return {"agent_id": "agent-root", "chat_id": chat_id, **kwargs}

        async def spawn(self, parent_agent_id, prompt, **kwargs):
            return {"agent": {"agent_id": "agent-child", "parent_agent_id": parent_agent_id}, "prompt": prompt, "options": kwargs}

        async def status(self, agent_id):
            return {"agent": {"agent_id": agent_id, "status": "running"}}

        async def schedule_wakeup(self, thread_id, wake_at, **kwargs):
            self.wakeup_calls.append(
                {"thread_id": thread_id, "wake_at": wake_at, "options": kwargs}
            )
            return {
                "automation_id": "auto-wakeup",
                "target_agent_id": thread_id,
                "due_at": float(wake_at),
                "status": "scheduled",
                "options": kwargs,
            }

        async def queue_after_completion(self, source_thread_id, target_thread_id, prompt, **kwargs):
            return {
                "automation_id": "auto-completion",
                "source_agent_id": source_thread_id,
                "target_agent_id": target_thread_id,
                "message": prompt,
                "status": "waiting",
                "options": kwargs,
            }

        async def automations(self, **kwargs):
            return {"automations": [{"automation_id": "auto-wakeup"}], "options": kwargs}

        async def cancel_automation(self, automation_id):
            return {"automation_id": automation_id, "status": "cancelled"}

    class FakeService:
        def __init__(self):
            self.wakes = 0

        def wake(self):
            self.wakes += 1

        async def sync_now(self):
            return {"status": "ok", "write_count": 0}

    coordinator = FakeCoordinator()
    service = FakeService()
    monkeypatch.setattr(terminal_mcp, "get_chat_agent_coordinator", lambda: coordinator)
    monkeypatch.setattr(terminal_mcp, "get_chat_agent_service", lambda: service)

    projects = json.loads(run(terminal_mcp.agent_projects_list(
        session_id=session_id, cwd=str(project)
    )))
    assert projects["items"][0]["id"] == "g-p-one"

    parent = json.loads(run(terminal_mcp.agent_register_parent(
        "https://chatgpt.com/c/11111111-1111-1111-1111-111111111111",
        session_id=session_id,
        cwd=str(project),
    )))
    assert parent["agent_id"] == "agent-root"

    child = json.loads(run(terminal_mcp.agent_spawn(
        "agent-root",
        "do work",
        project_policy="none",
        session_id=session_id,
        cwd=str(project),
    )))
    assert child["agent"]["agent_id"] == "agent-child"
    assert service.wakes == 1

    wake = json.loads(run(terminal_mcp.agent_schedule_wakeup(
        "agent-child",
        "12345",
        prompt="Review children",
        session_id=session_id,
        cwd=str(project),
    )))
    assert wake["automation_id"] == "auto-wakeup"

    monkeypatch.setattr(terminal_mcp.time, "time", lambda: 1_000.0)
    continuation = json.loads(run(terminal_mcp.agent_schedule_continue(
        "agent-child",
        after_seconds=15,
        idempotency_key="resume-once",
        session_id=session_id,
        cwd=str(project),
    )))
    assert continuation["automation_id"] == "auto-wakeup"
    assert continuation["due_at"] == 1_015.0
    assert coordinator.wakeup_calls[-1] == {
        "thread_id": "agent-child",
        "wake_at": 1_015.0,
        "options": {"prompt": "continue", "idempotency_key": "resume-once"},
    }

    completion = json.loads(run(terminal_mcp.agent_queue_after_completion(
        "agent-child",
        "agent-root",
        "Synthesize result",
        session_id=session_id,
        cwd=str(project),
    )))
    assert completion["status"] == "waiting"

    listed = json.loads(run(terminal_mcp.agent_automation(
        action="list",
        thread_id="agent-child",
        session_id=session_id,
        cwd=str(project),
    )))
    assert listed["automations"][0]["automation_id"] == "auto-wakeup"

    cancelled = json.loads(run(terminal_mcp.agent_automation(
        action="cancel",
        automation_id="auto-wakeup",
        session_id=session_id,
        cwd=str(project),
    )))
    assert cancelled["status"] == "cancelled"
    assert service.wakes == 5

    synced = json.loads(run(terminal_mcp.agent_sync(
        session_id=session_id, cwd=str(project)
    )))
    assert synced["write_count"] == 0


def test_agent_send_preserves_legacy_positional_parameter_order():
    import inspect
    import terminal_mcp

    parameters = list(inspect.signature(terminal_mcp.agent_send).parameters)
    assert parameters[:7] == [
        "from_agent_id",
        "to_agent_id",
        "message",
        "interrupt_policy",
        "idempotency_key",
        "session_id",
        "cwd",
    ]
    assert parameters[7] == "purpose"


def test_chat_runtime_control_pauses_recovers_and_resumes(tmp_path, monkeypatch):
    from chat_gateway import CircuitState

    project = tmp_path / "runtime-control-project"
    project.mkdir()
    (project / ".git").mkdir()
    isolated_home(tmp_path, monkeypatch)
    session_id = bound_thread("runtime-control-thread")
    run(terminal_mcp.get_thread_context(session_id, str(project), max_chars=100000))

    class CircuitRecord:
        scope = "runtime"
        opened_at = None
        retry_at = None
        probe_failures = 0
        half_open_successes = 0
        last_success_at = None
        updated_at = 1.0

        def __init__(self):
            self.state = CircuitState.CLOSED

    class Ledger:
        def __init__(self):
            self.record = CircuitRecord()

        def list_circuits(self):
            return [self.record]

    class Gateway:
        def __init__(self):
            self.ledger = Ledger()
            self.transitions = []

        def pause_runtime(self):
            self.transitions.append("pause")
            self.ledger.record.state = CircuitState.OPEN
            return self.ledger.record.state

        def resume_runtime(self):
            self.transitions.append("resume")
            self.ledger.record.state = CircuitState.CLOSED
            return self.ledger.record.state

    class Controller:
        def __init__(self):
            self.actions = []

        async def health(self):
            return {"ready": True, "webview": {"ready": True}}

        async def restart(self, *, wait=True):
            self.actions.append(("restart", wait))
            return {"action": "restart", "health": await self.health()}

    class Service:
        def __init__(self):
            self.wakes = 0

        def wake(self):
            self.wakes += 1

    gateway = Gateway()
    controller = Controller()
    service = Service()
    monkeypatch.setattr(terminal_mcp, "get_chat_gateway", lambda: gateway)
    monkeypatch.setattr(
        terminal_mcp, "get_chat_runtime_controller", lambda: controller
    )
    monkeypatch.setattr(terminal_mcp, "get_chat_agent_service", lambda: service)

    health = json.loads(
        run(
            terminal_mcp.chat_runtime_health(
                session_id=session_id, cwd=str(project)
            )
        )
    )
    assert health["ready"] is True

    result = json.loads(
        run(
            terminal_mcp.chat_runtime_control(
                "restart", wait=True, session_id=session_id, cwd=str(project)
            )
        )
    )
    assert controller.actions == [("restart", True)]
    assert gateway.transitions == ["pause", "resume"]
    assert result["circuits"]["runtime"]["state"] == "CLOSED"
    assert service.wakes == 1

    paused = json.loads(
        run(
            terminal_mcp.chat_runtime_circuit(
                "pause", session_id=session_id, cwd=str(project)
            )
        )
    )
    assert paused["runtime"]["state"] == "OPEN"
