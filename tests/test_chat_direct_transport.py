from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest

from chat_direct_client import DirectChatTransport
from chat_internal_client import RuntimeUnavailableError


def _jwt(claims: dict) -> str:
    def part(value: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    return f"{part({'alg': 'none'})}.{part(claims)}.signature"


def test_fresh_stored_auth_works_without_invoking_refresh_helper(tmp_path, monkeypatch) -> None:
    calls = 0

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            nonlocal calls
            calls += 1
            if calls == 1:
                self.send_response(503)
                self.end_headers()
                return
            body = json.dumps({"models": [{"slug": "gpt-test"}]}).encode()
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    auth_path = tmp_path / "auth.json"
    auth_path.write_text(
        json.dumps(
            {
                "tokens": {
                    "access_token": _jwt(
                        {
                            "exp": int(time.time()) + 3600,
                            "https://api.openai.com/auth": {
                                "chatgpt_account_id": "account-test"
                            },
                        }
                    ),
                    "account_id": "account-test",
                }
            }
        )
    )
    os.chmod(auth_path, 0o600)
    monkeypatch.setenv("MCP_CHAT_DIRECT_AUTH_FILE", str(auth_path))
    monkeypatch.setenv(
        "MCP_CHAT_DIRECT_BASE_URL", f"http://127.0.0.1:{server.server_port}"
    )
    monkeypatch.setenv("MCP_CHAT_DIRECT_CODEX", str(tmp_path / "must-not-run"))

    async def scenario() -> None:
        transport = DirectChatTransport(timeout=5)
        try:
            assert transport._process is None
            health = await transport.request("health")
            assert health["ready"] is True
            assert health["model_count"] == 1
            assert calls == 2  # The transient 503 was retried inside the worker.
        finally:
            await transport.close()

    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
        server.server_close()


def test_read_recovers_after_owned_worker_dies(tmp_path) -> None:
    worker = tmp_path / "worker.mjs"
    worker.write_text(
        """
import {createInterface} from 'node:readline';
const lines=createInterface({input:process.stdin,crlfDelay:Infinity});
lines.on('line', line => {
  const value=JSON.parse(line);
  process.stdout.write(JSON.stringify({id:value.id,ok:true,result:{ready:true,pid:process.pid}})+'\\n');
});
lines.on('close',()=>process.exit(0));
"""
    )

    async def scenario() -> None:
        transport = DirectChatTransport(worker_path=worker, timeout=5)
        try:
            first = await transport.request("health")
            assert transport._process is not None
            transport._process.terminate()
            await transport._process.wait()
            second = await transport.request("health")
            assert second["pid"] != first["pid"]
        finally:
            await transport.close()

    asyncio.run(scenario())


def test_mutation_is_not_blindly_retried_after_worker_loss(tmp_path, monkeypatch) -> None:
    starts = tmp_path / "starts"
    worker = tmp_path / "worker.mjs"
    worker.write_text(
        f"""
import {{appendFileSync}} from 'node:fs';
import {{createInterface}} from 'node:readline';
appendFileSync({json.dumps(str(starts))}, 'start\\n');
const lines=createInterface({{input:process.stdin,crlfDelay:Infinity}});
lines.on('line', () => process.exit(23));
"""
    )

    async def scenario() -> None:
        transport = DirectChatTransport(worker_path=worker, timeout=2)
        with pytest.raises(RuntimeUnavailableError):
            await transport.request("create_thread", prompt="do not retry")
        assert starts.read_text().splitlines() == ["start"]

    asyncio.run(scenario())
