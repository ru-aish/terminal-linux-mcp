from __future__ import annotations

import asyncio
import pytest

from chat_runtime_controller import ChatRuntimeController, ChatRuntimeControllerConfig


def test_official_chatgpt_embedded_renderer_is_valid_and_skips_http_probe():
    config = ChatRuntimeControllerConfig(webview_url="app://-/index.html")
    config.validate()
    controller = ChatRuntimeController(config)

    result = asyncio.run(controller._webview_health())

    assert result == {
        "ready": True,
        "status_code": None,
        "reason": "",
        "transport": "embedded",
    }


def test_user_stop_inhibits_automatic_runtime_recovery(tmp_path):
    inhibit_path = tmp_path / "user-stopped"
    inhibit_path.touch()
    controller = ChatRuntimeController(
        ChatRuntimeControllerConfig(inhibit_path=str(inhibit_path))
    )
    restart_called = False

    async def unhealthy():
        return {"ready": False}

    async def unexpected_restart(*, wait=True):
        nonlocal restart_called
        restart_called = True
        return {"health": {"ready": True}}

    controller.health = unhealthy
    controller.restart = unexpected_restart

    with pytest.raises(RuntimeError, match="stopped by the user"):
        asyncio.run(controller.ensure_ready())

    assert restart_called is False
