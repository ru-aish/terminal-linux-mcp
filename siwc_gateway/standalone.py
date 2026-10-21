"""Self-contained, loopback-only SIWC daemon; imports no ChatGPT Desktop code."""
from __future__ import annotations

import os
import fcntl
from pathlib import Path

import uvicorn
from starlette.applications import Starlette

from .gateway import SIWCGateway
from .session_routes import SessionRoutes
from .sessions import SessionStore


def create_app(control_token: str | None = None, *, gateway: SIWCGateway | None = None) -> Starlette:
    token = control_token or os.environ.get("MCP_SIWC_CONTROL_TOKEN", "")
    if not token or len(token) < 24:
        raise ValueError("Set MCP_SIWC_CONTROL_TOKEN to a random secret of at least 24 characters")
    service = gateway or SIWCGateway(token)
    data_root = os.environ.get("MCP_SIWC_SESSION_HOME")
    sessions = SessionStore(Path(data_root) if data_root else None)
    return Starlette(routes=service.routes() + SessionRoutes(service, sessions).routes())


def serve(port: int = 8768, host: str = "127.0.0.1") -> None:
    if host not in ("127.0.0.1", "::1"):
        raise ValueError("Standalone SIWC must bind loopback; use an authenticated tunnel for remote access")
    if not 1 <= port <= 65535:
        raise ValueError("Invalid server port")
    # Enforce a single local writer to make restart recovery unambiguous.
    data_root = os.environ.get("MCP_SIWC_SESSION_HOME")
    sessions = SessionStore(Path(data_root) if data_root else None)
    lock_path = sessions.root / "daemon.lock"
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another standalone SIWC daemon owns this session store") from exc
        sessions.recover_interrupted()
        app = create_app()
        uvicorn.run(app, host=host, port=port, workers=1, log_config=None, access_log=False)
    finally:
        os.close(fd)
