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

from chat_direct_client import (
    DirectBackendResponseError,
    DirectChatRuntimeController,
    DirectChatTransport,
    DirectSubmissionUncertainError,
)
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


def test_worker_response_can_exceed_asyncio_default_line_limit(tmp_path) -> None:
    worker = tmp_path / "worker.mjs"
    worker.write_text(
        """
import {createInterface} from 'node:readline';
const lines=createInterface({input:process.stdin,crlfDelay:Infinity});
lines.on('line', line => {
  const value=JSON.parse(line);
  process.stdout.write(JSON.stringify({id:value.id,ok:true,result:{payload:'x'.repeat(128*1024)}})+'\\n');
});
"""
    )

    async def scenario() -> None:
        transport = DirectChatTransport(worker_path=worker, timeout=5)
        try:
            result = await transport.request("get_thread", conversation_id="large-thread")
            assert len(result["payload"]) == 128 * 1024
        finally:
            await transport.close()

    asyncio.run(scenario())


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


def test_worker_loss_after_submission_phase_is_typed_uncertain(tmp_path) -> None:
    worker = tmp_path / "worker.mjs"
    worker.write_text(
        """
import {createInterface} from 'node:readline';
const lines=createInterface({input:process.stdin,crlfDelay:Infinity});
lines.on('line', line => {
  const value=JSON.parse(line);
  process.stdout.write(JSON.stringify({id:value.id,event:'submission_started'})+'\\n');
  setTimeout(()=>process.exit(24),10);
});
"""
    )

    async def scenario() -> None:
        transport = DirectChatTransport(worker_path=worker, timeout=2)
        with pytest.raises(DirectSubmissionUncertainError):
            await transport.request("create_thread", prompt="maybe accepted")

    asyncio.run(scenario())


def test_worker_error_preserves_http_status(tmp_path) -> None:
    worker = tmp_path / "worker.mjs"
    worker.write_text(
        """
import {createInterface} from 'node:readline';
const lines=createInterface({input:process.stdin,crlfDelay:Infinity});
lines.on('line', line => {
  const value=JSON.parse(line);
  process.stdout.write(JSON.stringify({id:value.id,ok:false,error:{message:'limited',status:429}})+'\\n');
});
"""
    )

    async def scenario() -> None:
        transport = DirectChatTransport(worker_path=worker, timeout=5)
        try:
            with pytest.raises(DirectBackendResponseError) as captured:
                await transport.request("health")
            assert captured.value.status_code == 429
        finally:
            await transport.close()

    asyncio.run(scenario())


def test_completion_http_429_is_preserved_by_real_worker(tmp_path, monkeypatch) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            if self.path.endswith("/sentinel/chat-requirements/prepare"):
                body = b"{}"
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(429)
            self.end_headers()

        def log_message(self, *_args) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
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

    async def scenario() -> None:
        transport = DirectChatTransport(timeout=5)
        try:
            with pytest.raises(DirectBackendResponseError) as captured:
                await transport.request(
                    "create_thread",
                    prompt="rate limit",
                    project_id="",
                    user_message_id="message-test",
                    _request_timeout=10,
                )
            assert captured.value.status_code == 429
        finally:
            await transport.close()

    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
        server.server_close()


def test_runtime_stop_remains_stopped_until_explicit_start() -> None:
    class Transport:
        paused = False
        requests = 0

        async def request(self, method: str, **_params):
            self.requests += 1
            return {"ready": True, "transport": "direct"}

        async def pause(self):
            self.paused = True

        def resume(self):
            self.paused = False

        async def close(self):
            return None

    async def scenario() -> None:
        transport = Transport()
        controller = DirectChatRuntimeController()
        controller.transport = transport
        await controller.stop()
        status = await controller.status()
        assert status["ready"] is False
        assert transport.requests == 0
        started = await controller.start()
        assert started["health"]["ready"] is True
        assert transport.requests == 1

    asyncio.run(scenario())


def test_pause_terminates_worker_without_waiting_for_request_lock(tmp_path) -> None:
    worker = tmp_path / "worker.mjs"
    worker.write_text(
        """
import {createInterface} from 'node:readline';
const lines=createInterface({input:process.stdin,crlfDelay:Infinity});
lines.on('line', () => {});
"""
    )

    async def scenario() -> None:
        transport = DirectChatTransport(worker_path=worker, timeout=30)
        request = asyncio.create_task(transport.request("health"))
        for _ in range(100):
            if transport._process is not None:
                break
            await asyncio.sleep(0.01)
        await asyncio.wait_for(transport.pause(), timeout=1)
        with pytest.raises(RuntimeUnavailableError):
            await request
        assert transport.paused is True

    asyncio.run(scenario())


def test_pause_wins_race_with_lazy_worker_start(tmp_path) -> None:
    worker = tmp_path / "worker.mjs"
    worker.write_text(
        """
import {createInterface} from 'node:readline';
const lines=createInterface({input:process.stdin,crlfDelay:Infinity});
lines.on('line', line => {
  const value=JSON.parse(line);
  process.stdout.write(JSON.stringify({id:value.id,ok:true,result:{ready:true}})+'\\n');
});
"""
    )

    async def scenario() -> None:
        transport = DirectChatTransport(worker_path=worker, timeout=5)
        original_start = transport._start
        entered = asyncio.Event()
        release = asyncio.Event()

        async def delayed_start():
            entered.set()
            await release.wait()
            return await original_start()

        transport._start = delayed_start  # type: ignore[method-assign]
        request = asyncio.create_task(transport.request("health"))
        await entered.wait()
        await transport.pause()
        release.set()
        with pytest.raises(RuntimeUnavailableError, match="stopped"):
            await request
        assert transport.paused is True
        assert transport._process is None

    asyncio.run(scenario())
