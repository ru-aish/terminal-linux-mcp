from __future__ import annotations

import sys
from pathlib import Path

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, StreamingResponse
from starlette.routing import Route
from starlette.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import terminal_mcp


def test_privacy_proxy_only_handles_configured_path_and_streams_headers_body(monkeypatch):
    calls: list[dict[str, object]] = []

    async def upstream(request: Request):
        calls.append({
            "method": request.method,
            "path": request.url.path,
            "query": request.url.query,
            "authorization": request.headers.get("authorization"),
            "session": request.headers.get("mcp-session-id"),
            "body": await request.body(),
        })

        async def chunks():
            yield b"first-"
            yield b"second"

        return StreamingResponse(
            chunks(),
            status_code=207,
            media_type="text/event-stream",
            headers={"mcp-session-id": "upstream-session"},
        )

    upstream_app = Starlette(routes=[Route("/mcp", upstream, methods=["GET", "POST", "DELETE"])])
    real_async_client = httpx.AsyncClient

    def async_client_factory(*args, **kwargs):
        kwargs["transport"] = httpx.ASGITransport(app=upstream_app)
        return real_async_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", async_client_factory)

    async def fallback(scope, receive, send):
        response = PlainTextResponse("original-terminal", status_code=203)
        await response(scope, receive, send)

    app = terminal_mcp.PrivacyCloneProxyMiddleware(
        fallback,
        path="/privacy-mcp",
        upstream="http://privacy-clone.test/mcp",
    )

    client = TestClient(app)
    proxied = client.post(
        "/privacy-mcp/?source=test",
        headers={
            "Authorization": "Bearer clone-only-id",
            "Mcp-Session-Id": "client-session",
        },
        content=b"request-payload",
    )
    fallback_response = client.get("/mcp")

    assert proxied.status_code == 207
    assert proxied.content == b"first-second"
    assert proxied.headers["mcp-session-id"] == "upstream-session"
    assert calls == [{
        "method": "POST",
        "path": "/mcp",
        "query": "source=test",
        "authorization": "Bearer clone-only-id",
        "session": "client-session",
        "body": b"request-payload",
    }]
    assert fallback_response.status_code == 203
    assert fallback_response.text == "original-terminal"


def test_privacy_proxy_returns_502_without_leaking_exception_message(monkeypatch):
    class BrokenAsyncClient:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("sensitive upstream detail")

    monkeypatch.setattr(httpx, "AsyncClient", BrokenAsyncClient)

    async def fallback(scope, receive, send):
        response = PlainTextResponse("fallback")
        await response(scope, receive, send)

    app = terminal_mcp.PrivacyCloneProxyMiddleware(
        fallback,
        path="privacy-mcp",
        upstream="http://privacy-clone.test/mcp",
    )

    client = TestClient(app)
    response = client.post("/privacy-mcp", content=b"{}")

    assert response.status_code == 502
    assert response.json() == {
        "error": "privacy terminal unavailable",
        "type": "RuntimeError",
    }
    assert "sensitive upstream detail" not in response.text
