from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import secrets
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from starlette.requests import Request
from starlette.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from starlette.routing import Route

from gpt_thread_store import GPTThreadStore
from chat_watchdog import ChatWatchdog, QueueConflictError, QueueValidationError


ASSET_DIR = Path(__file__).resolve().parent / "dashboard"
DASHBOARD_COOKIE = "terminal_usage_dashboard"


def _dashboard_token() -> str:
    return os.environ.get("MCP_DASHBOARD_TOKEN", "").strip()


def _session_cookie_value(token: str) -> str:
    return hmac.new(
        token.encode("utf-8"),
        b"terminal-mcp-dashboard-session-v1",
        hashlib.sha256,
    ).hexdigest()


def _authorized(request: Request) -> bool:
    expected = _dashboard_token()
    if not expected:
        return True
    cookie = request.cookies.get(DASHBOARD_COOKIE, "")
    if cookie and secrets.compare_digest(cookie, _session_cookie_value(expected)):
        return True
    authorization = request.headers.get("authorization", "")
    supplied = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
    return bool(supplied) and secrets.compare_digest(supplied, expected)


def _secure_cookie(request: Request) -> bool:
    forwarded = request.headers.get("x-forwarded-proto", "")
    return request.url.scheme == "https" or forwarded.lower() == "https"


def _security_headers(response: Response, *, cache: str = "no-store") -> Response:
    response.headers["cache-control"] = cache
    response.headers["x-content-type-options"] = "nosniff"
    response.headers["referrer-policy"] = "no-referrer"
    response.headers["x-frame-options"] = "DENY"
    response.headers["content-security-policy"] = (
        "default-src 'self'; connect-src 'self'; img-src 'self' data:; "
        "style-src 'self'; script-src 'self'; font-src 'self'; "
        "base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
    )
    return response


def _unauthorized_page(message: str = "Enter the dashboard access token.") -> HTMLResponse:
    body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Usage ledger access</title>
  <link rel="stylesheet" href="/dashboard/assets/dashboard.css">
</head>
<body class="login-shell">
  <main class="login-panel">
    <div class="brand-mark" aria-hidden="true"><span></span><span></span><span></span></div>
    <p class="eyebrow">Terminal MCP · private telemetry</p>
    <h1>Open the usage ledger.</h1>
    <p class="login-copy">{message}</p>
    <form method="post" action="/dashboard/login" class="login-form">
      <label for="token">Access token</label>
      <input id="token" name="token" type="password" autocomplete="current-password" required autofocus>
      <button type="submit">Open dashboard</button>
    </form>
  </main>
</body>
</html>"""
    return _security_headers(HTMLResponse(body, status_code=401))


def _parse_positive_int(value: str | None, default: int, maximum: int) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        return default
    return max(1, min(parsed, maximum))


def install_usage_dashboard(app: Any, store: GPTThreadStore, watchdog: ChatWatchdog | None = None) -> None:
    """Mount the live usage dashboard into an existing Starlette/FastMCP app."""

    existing_paths = {getattr(route, "path", "") for route in app.routes}
    if "/dashboard" in existing_paths:
        return

    async def dashboard_page(request: Request) -> Response:
        if not _authorized(request):
            return _unauthorized_page()
        return _security_headers(FileResponse(ASSET_DIR / "index.html", media_type="text/html"))

    async def dashboard_login(request: Request) -> Response:
        expected = _dashboard_token()
        if not expected:
            return RedirectResponse("/dashboard", status_code=303)
        raw_body = (await request.body()).decode("utf-8", errors="replace")
        supplied = parse_qs(raw_body).get("token", [""])[0]
        if not secrets.compare_digest(supplied, expected):
            return _unauthorized_page("That token does not match. Try again.")
        response = RedirectResponse("/dashboard", status_code=303)
        response.set_cookie(
            DASHBOARD_COOKIE,
            _session_cookie_value(expected),
            max_age=60 * 60 * 24 * 30,
            httponly=True,
            secure=_secure_cookie(request),
            samesite="strict",
            path="/dashboard",
        )
        return _security_headers(response)

    async def dashboard_logout(request: Request) -> Response:
        response = RedirectResponse("/dashboard", status_code=303)
        response.delete_cookie(DASHBOARD_COOKIE, path="/dashboard")
        return _security_headers(response)

    async def dashboard_api(request: Request) -> Response:
        if not _authorized(request):
            return _security_headers(JSONResponse({"error": "unauthorized"}, status_code=401))
        hours = _parse_positive_int(request.query_params.get("hours"), 24, 24 * 7)
        limit = _parse_positive_int(request.query_params.get("limit"), 40, 100)
        snapshot = await asyncio.to_thread(store.dashboard_snapshot, hours, limit)
        return _security_headers(JSONResponse(snapshot))

    def watchdog_mutation_allowed(request: Request) -> bool:
        if not _authorized(request):
            return False
        origin = request.headers.get("origin", "")
        if origin and origin.rstrip("/") != str(request.base_url).rstrip("/"):
            return False
        referer = request.headers.get("referer", "")
        if referer and not referer.startswith(str(request.base_url)):
            return False
        return secrets.compare_digest(request.headers.get("x-mcp-dashboard-csrf", ""), "chat-watchdog")

    async def watchdog_api(request: Request) -> Response:
        if watchdog is None or not _authorized(request):
            return _security_headers(JSONResponse({"error": "not found"}, status_code=404 if watchdog is None else 401))
        return _security_headers(JSONResponse(await asyncio.to_thread(watchdog.snapshot)))

    async def watchdog_replace(request: Request) -> Response:
        if watchdog is None:
            return _security_headers(JSONResponse({"error": "not found"}, status_code=404))
        if not watchdog_mutation_allowed(request):
            return _security_headers(JSONResponse({"error": "unauthorized or csrf check failed"}, status_code=403))
        body = await request.body()
        if len(body) > 128 * 1024:
            return _security_headers(JSONResponse({"error": "queue body too large"}, status_code=413))
        expected_version = request.headers.get("x-watchdog-queue-version", "").strip() or None
        try:
            await asyncio.to_thread(
                watchdog.replace_queue,
                body.decode("utf-8"),
                expected_version,
            )
        except QueueConflictError as exc:
            current = await asyncio.to_thread(watchdog.snapshot)
            return _security_headers(
                JSONResponse({"error": str(exc), "watchdog": current}, status_code=409)
            )
        except (UnicodeDecodeError, ValueError, QueueValidationError) as exc:
            return _security_headers(JSONResponse({"error": str(exc)}, status_code=400))
        return _security_headers(JSONResponse(await asyncio.to_thread(watchdog.snapshot)))

    async def watchdog_add(request: Request) -> Response:
        if watchdog is None:
            return _security_headers(JSONResponse({"error": "not found"}, status_code=404))
        if not watchdog_mutation_allowed(request):
            return _security_headers(JSONResponse({"error": "unauthorized or csrf check failed"}, status_code=403))
        try:
            payload = await request.json()
            await asyncio.to_thread(watchdog.add_url, str(payload.get("url", "")))
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            return _security_headers(JSONResponse({"error": str(exc)}, status_code=400))
        return _security_headers(
            JSONResponse(await asyncio.to_thread(watchdog.snapshot), status_code=201)
        )

    async def watchdog_remove(request: Request) -> Response:
        if watchdog is None:
            return _security_headers(JSONResponse({"error": "not found"}, status_code=404))
        if not watchdog_mutation_allowed(request):
            return _security_headers(JSONResponse({"error": "unauthorized or csrf check failed"}, status_code=403))
        conversation_id = request.path_params["conversation_id"]
        removed = await asyncio.to_thread(watchdog.remove_url, conversation_id)
        snapshot = await asyncio.to_thread(watchdog.snapshot)
        return _security_headers(JSONResponse({"removed": removed, **snapshot}))

    async def watchdog_scan(request: Request) -> Response:
        if watchdog is None:
            return _security_headers(JSONResponse({"error": "not found"}, status_code=404))
        if not watchdog_mutation_allowed(request):
            return _security_headers(JSONResponse({"error": "unauthorized or csrf check failed"}, status_code=403))
        return _security_headers(JSONResponse(await watchdog.scan_once(trigger="dashboard")))

    async def dashboard_events(request: Request) -> Response:
        if not _authorized(request):
            return _security_headers(JSONResponse({"error": "unauthorized"}, status_code=401))
        hours = _parse_positive_int(request.query_params.get("hours"), 24, 24 * 7)
        limit = _parse_positive_int(request.query_params.get("limit"), 40, 100)

        async def stream():
            last_version = ""
            heartbeat_at = 0.0
            loop = asyncio.get_running_loop()
            while True:
                if await request.is_disconnected():
                    break
                version = await asyncio.to_thread(store.dashboard_version)
                now = loop.time()
                if version != last_version:
                    snapshot = await asyncio.to_thread(store.dashboard_snapshot, hours, limit)
                    payload = json.dumps(snapshot, separators=(",", ":"), ensure_ascii=False)
                    yield f"event: snapshot\ndata: {payload}\n\n"
                    last_version = version
                    heartbeat_at = now
                elif now - heartbeat_at >= 12:
                    yield ": heartbeat\n\n"
                    heartbeat_at = now
                await asyncio.sleep(1.5)

        response = StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={
                "cache-control": "no-cache, no-store",
                "connection": "keep-alive",
                "x-accel-buffering": "no",
            },
        )
        return _security_headers(response)

    async def dashboard_asset(request: Request) -> Response:
        name = request.path_params["name"]
        asset_map = {
            "dashboard.css": (ASSET_DIR / "dashboard.css", "text/css"),
            "dashboard.js": (ASSET_DIR / "dashboard.js", "text/javascript"),
        }
        selected = asset_map.get(name)
        if selected is None:
            return Response("Not found", status_code=404)
        path, media_type = selected
        return _security_headers(
            FileResponse(path, media_type=media_type),
            cache="public, max-age=300",
        )

    app.routes.extend(
        [
            Route("/dashboard", dashboard_page, methods=["GET"]),
            Route("/dashboard/", dashboard_page, methods=["GET"]),
            Route("/dashboard/login", dashboard_login, methods=["POST"]),
            Route("/dashboard/logout", dashboard_logout, methods=["POST"]),
            Route("/dashboard/api", dashboard_api, methods=["GET"]),
            Route("/dashboard/watchdog", watchdog_api, methods=["GET"]),
            Route("/dashboard/watchdog/queue", watchdog_replace, methods=["PUT", "POST"]),
            Route("/dashboard/watchdog/queue/add", watchdog_add, methods=["POST"]),
            Route("/dashboard/watchdog/queue/{conversation_id:str}", watchdog_remove, methods=["DELETE"]),
            Route("/dashboard/watchdog/scan", watchdog_scan, methods=["POST"]),
            Route("/dashboard/events", dashboard_events, methods=["GET"]),
            Route("/dashboard/assets/{name:str}", dashboard_asset, methods=["GET"]),
        ]
    )
