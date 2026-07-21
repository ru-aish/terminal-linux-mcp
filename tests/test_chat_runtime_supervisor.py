from __future__ import annotations

import asyncio
import inspect
import json
import sys
from pathlib import Path

import pytest

import chat_runtime_supervisor
from chat_gateway.adapters import codex_renderer
from chat_internal_client import CHAT_RENDERER_BRIDGE_JS
from chat_runtime_controller import (
    ChatRuntimeControllerConfig,
)
from chat_runtime_supervisor import ChatRuntimeSupervisor, SupervisorConfig


def test_supervisor_uses_configured_health_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: list[ChatRuntimeControllerConfig] = []

    class FakeController:
        def __init__(self, config: ChatRuntimeControllerConfig) -> None:
            captured.append(config)

        async def health(self) -> dict[str, object]:
            return {"ready": True}

    monkeypatch.setattr(
        chat_runtime_supervisor, "ChatRuntimeController", FakeController
    )
    config = SupervisorConfig(
        command=(sys.executable, "-c", "pass"),
        app_id="codex-timeout-test",
        state_dir=tmp_path / "state",
        runtime_dir=tmp_path / "runtime",
        status_path=tmp_path / "status.json",
        cdp_endpoint="http://127.0.0.1:19924",
        webview_url="http://127.0.0.1:15177/index.html",
        health_timeout_seconds=75,
    )
    supervisor = ChatRuntimeSupervisor(config)

    assert asyncio.run(supervisor._default_health_probe()) == {"ready": True}
    assert len(captured) == 1
    assert captured[0].internal_timeout_seconds == 75


def test_gateway_adapter_reuses_deep_renderer_resolver() -> None:
    assert codex_renderer._BRIDGE.startswith(CHAT_RENDERER_BRIDGE_JS)
    assert "discoverGraph" in codex_renderer._BRIDGE
    assert "performance.getEntriesByType('resource')" in codex_renderer._BRIDGE
    assert "const resolved = await resolveClient();" in inspect.getsource(
        codex_renderer.CodexRendererBackend.create_thread
    )


def test_controller_reads_runtime_internal_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MCP_CHAT_RUNTIME_INTERNAL_TIMEOUT", "87")
    monkeypatch.setenv("MCP_CHAT_WATCHDOG_INTERNAL_TIMEOUT_SECONDS", "12")

    config = ChatRuntimeControllerConfig.from_env()

    assert config.internal_timeout_seconds == 87


def test_controller_rejects_non_loopback_endpoints() -> None:
    with pytest.raises(ValueError, match="loopback"):
        ChatRuntimeControllerConfig(
            cdp_endpoint="https://example.com",
            webview_url="http://127.0.0.1:5175/index.html",
        ).validate()


def test_supervisor_forces_single_ownership_environment(tmp_path: Path) -> None:
    async def scenario() -> None:
        environment_path = tmp_path / "environment.json"
        child = tmp_path / "child.py"
        child.write_text(
            "import json, os, signal, time\n"
            + "payload = {'KDE_APPLICATIONS_AS_SCOPE': os.getenv('KDE_APPLICATIONS_AS_SCOPE'), "
            + "'CODEX_LINUX_APP_ID': os.getenv('CODEX_LINUX_APP_ID')}\n"
            + f"open({str(environment_path)!r}, 'w').write(json.dumps(payload))\n"
            + "signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(0)))\n"
            + "while True: time.sleep(1)\n",
            encoding="utf-8",
        )
        config = SupervisorConfig(
            command=(sys.executable, str(child)),
            app_id="codex-isolated-test",
            state_dir=tmp_path / "state",
            runtime_dir=tmp_path / "runtime",
            status_path=tmp_path / "status.json",
            cdp_endpoint="http://127.0.0.1:19922",
            webview_url="http://127.0.0.1:15175/index.html",
            startup_grace_seconds=0.1,
            probe_interval_seconds=0.1,
            failure_threshold=10,
            restart_backoff_seconds=0.1,
            stop_timeout_seconds=2,
        )

        async def healthy() -> dict[str, object]:
            return {"ready": True}

        supervisor = ChatRuntimeSupervisor(config, health_probe=healthy)
        task = asyncio.create_task(supervisor.run())
        for _ in range(100):
            if environment_path.exists():
                break
            await asyncio.sleep(0.02)
        payload = json.loads(environment_path.read_text(encoding="utf-8"))
        assert payload == {
            "KDE_APPLICATIONS_AS_SCOPE": "0",
            "CODEX_LINUX_APP_ID": "codex-isolated-test",
        }
        supervisor.request_stop()
        assert await asyncio.wait_for(task, timeout=5) == 0

    asyncio.run(scenario())


def test_supervisor_cleanup_stops_only_matching_generated_scopes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    listings = iter(
        [
            "app-codex-owned-123.scope loaded active running owned\n"
            "app-other-456.scope loaded active running other\n",
            "",
            "",
        ]
    )

    class Completed:
        def __init__(self, stdout: str = "") -> None:
            self.stdout = stdout

    def fake_run(command, **kwargs):
        calls.append(list(command))
        if "list-units" in command:
            return Completed(next(listings))
        return Completed()

    monkeypatch.setattr(chat_runtime_supervisor.subprocess, "run", fake_run)
    config = SupervisorConfig(
        command=(sys.executable, "-c", "pass"),
        app_id="codex-owned",
        state_dir=tmp_path / "state",
        runtime_dir=tmp_path / "runtime",
        status_path=tmp_path / "status.json",
        cdp_endpoint="http://127.0.0.1:19925",
        webview_url="http://127.0.0.1:15178/index.html",
    )
    config.state_dir.mkdir()
    config.runtime_dir.mkdir()
    for path in (
        config.state_dir / "app.pid",
        config.state_dir / "webview.pid",
        config.runtime_dir / "launch-action.sock",
    ):
        path.write_text("stale", encoding="utf-8")

    ChatRuntimeSupervisor(config).cleanup()

    assert calls[0][:4] == ["systemctl", "--user", "list-units", "--type=scope"]
    assert ["systemctl", "--user", "stop", "app-codex-owned-123.scope"] in calls
    assert not any("app-other-456.scope" in command for command in calls[1:])
    assert not any(
        path.exists()
        for path in (
            config.state_dir / "app.pid",
            config.state_dir / "webview.pid",
            config.runtime_dir / "launch-action.sock",
        )
    )


def test_supervisor_restarts_whole_generation_after_health_threshold(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        pid_path = tmp_path / "pid.txt"
        child = tmp_path / "child.py"
        child.write_text(
            "import os, signal, time\n"
            f"open({str(pid_path)!r}, 'w').write(str(os.getpid()))\n"
            "signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(SystemExit(0)))\n"
            "while True: time.sleep(1)\n",
            encoding="utf-8",
        )
        calls = 0

        async def probe() -> dict[str, object]:
            nonlocal calls
            calls += 1
            return {"ready": calls not in {2, 3}}

        config = SupervisorConfig(
            command=(sys.executable, str(child)),
            app_id="codex-health-test",
            state_dir=tmp_path / "state",
            runtime_dir=tmp_path / "runtime",
            status_path=tmp_path / "status.json",
            cdp_endpoint="http://127.0.0.1:19923",
            webview_url="http://127.0.0.1:15176/index.html",
            startup_grace_seconds=0.05,
            probe_interval_seconds=0.05,
            failure_threshold=2,
            restart_backoff_seconds=0.05,
            stop_timeout_seconds=2,
        )
        supervisor = ChatRuntimeSupervisor(config, health_probe=probe)
        task = asyncio.create_task(supervisor.run())
        first_pid = 0
        second_pid = 0
        for _ in range(200):
            if pid_path.exists():
                value = int(pid_path.read_text())
                if not first_pid:
                    first_pid = value
                elif value != first_pid:
                    second_pid = value
                    break
            await asyncio.sleep(0.02)
        assert first_pid and second_pid and first_pid != second_pid
        assert not Path(f"/proc/{first_pid}").exists()
        supervisor.request_stop()
        assert await asyncio.wait_for(task, timeout=5) == 0

    asyncio.run(scenario())


def test_gateway_create_and_continue_send_configured_high_reasoning(
    monkeypatch,
) -> None:
    backend = codex_renderer.CodexRendererBackend(
        model_slug="gpt-5.6-terra",
        thinking_effort="extended",
        require_high_reasoning=True,
    )
    expressions: list[str] = []

    def evaluate(expression: str, *, timeout=None):
        expressions.append(expression)
        if "title_requested" in expression:
            return {
                "accepted": True,
                "conversation_id": "conversation-1",
                "message_id": "message-1",
                "running": True,
            }
        return {
            "accepted": True,
            "conversation_id": "conversation-1",
            "message_id": "message-2",
            "running": True,
        }

    monkeypatch.setattr(backend, "_evaluate", evaluate)
    asyncio.run(
        backend.create_thread(
            project_id="",
            prompt="Create with high reasoning.",
            title="Reasoning",
            idempotency_key="create-reasoning",
        )
    )
    asyncio.run(
        backend.continue_thread(
            conversation_id="conversation-1",
            message="Continue with high reasoning.",
            idempotency_key="continue-reasoning",
        )
    )
    assert len(expressions) == 2
    for expression in expressions:
        assert 'const thinkingEffort = "extended"' in expression
        assert "request.thinking_effort = thinkingEffort" in expression
        assert "client.models()" not in expression


def test_gateway_rejects_required_reasoning_without_an_effort() -> None:
    with pytest.raises(ValueError, match="thinking_effort is required"):
        codex_renderer.CodexRendererBackend(
            thinking_effort="", require_high_reasoning=True
        )


def test_gateway_rejects_non_high_effort_when_high_reasoning_is_required() -> None:
    with pytest.raises(ValueError, match="must be high or extended"):
        codex_renderer.CodexRendererBackend(
            thinking_effort="medium", require_high_reasoning=True
        )
