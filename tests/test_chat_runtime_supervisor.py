from __future__ import annotations

import asyncio
import inspect
import json
import sys
from pathlib import Path

import pytest

from chat_gateway.adapters import codex_renderer
from chat_internal_client import CHAT_RENDERER_BRIDGE_JS
from chat_runtime_controller import (
    ChatRuntimeControllerConfig,
)
from chat_runtime_supervisor import ChatRuntimeSupervisor, SupervisorConfig


def test_gateway_adapter_reuses_deep_renderer_resolver() -> None:
    assert codex_renderer._BRIDGE.startswith(CHAT_RENDERER_BRIDGE_JS)
    assert "discoverGraph" in codex_renderer._BRIDGE
    assert "performance.getEntriesByType('resource')" in codex_renderer._BRIDGE
    assert "const resolved = await resolveClient();" in inspect.getsource(
        codex_renderer.CodexRendererBackend.create_thread
    )


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
