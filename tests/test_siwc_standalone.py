"""The standalone app never imports legacy ChatGPT Desktop workers."""
import asyncio
import importlib
import os
import sys
import subprocess
import httpx
import pytest
from siwc_gateway.standalone import create_app, serve


def test_daemon_requires_private_control_token(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_SIWC_HOME", str(tmp_path / "siwc"))
    monkeypatch.delenv("MCP_SIWC_CONTROL_TOKEN", raising=False)
    with pytest.raises(ValueError, match="random secret"):
        create_app()
    monkeypatch.setenv("MCP_SIWC_CONTROL_TOKEN", "a" * 24)
    app = create_app()
    assert "/v1/siwc/responses" in [r.path for r in app.routes]
    result = subprocess.run([sys.executable, "-c",
        "import sys; from siwc_gateway.standalone import create_app; create_app(); "
        "assert not any(n.startswith(('chat_direct', 'chat_internal', 'chat_watchdog')) for n in sys.modules)"],
        env=os.environ.copy(), capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr


def test_daemon_requires_loopback(monkeypatch):
    with pytest.raises(ValueError, match="loopback"):
        serve(host="0.0.0.0")
    monkeypatch.setenv("MCP_SIWC_CONTROL_TOKEN", "a" * 24)
    with pytest.raises(ValueError, match="Invalid server port"):
        serve(port=0)


def test_status_bearer_is_private(monkeypatch, tmp_path):
    monkeypatch.setenv("MCP_SIWC_HOME", str(tmp_path / "siwc"))
    app = create_app("a" * 24)
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as c:
            assert (await c.get("/v1/siwc/status")).status_code == 401
            r = await c.get("/v1/siwc/status", headers={"Authorization": "Bearer " + "a" * 24})
            assert r.json()["authenticated"] is False
    asyncio.run(run())
