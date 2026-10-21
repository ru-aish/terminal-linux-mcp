"""No live credential or OpenAI network calls: deterministic headless tests."""
import asyncio
import json
import os
from pathlib import Path
import time
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from starlette.applications import Starlette

from siwc_gateway.auth import (
    AuthError, CredentialStore, begin_authorization, validate_callback,
)
from siwc_gateway.gateway import SIWCGateway


def record(store, *, expires_in=3600, saved_at=None):
    data = dict(client_id="oaiapp_test", subject="subject-test", email="test@example.test",
                access_token="mock-secret-access", refresh_token="mock-secret-refresh",
                scopes=["openid", "chatgpt.tokens.use.direct", "offline_access"],
                saved_at=time.time() if saved_at is None else saved_at, expires_in=expires_in)
    store.save(data)
    return data


def test_pkce_dynamic_registration_and_callback(tmp_path):
    store = CredentialStore(tmp_path / "siwc")
    host = store.host_id()
    assert host == store.host_id() and host.startswith("urn:uuid:")
    assert store.root.stat().st_mode & 0o077 == 0
    assert store.host_path.stat().st_mode & 0o077 == 0
    attempt = begin_authorization(host, "http://127.0.0.1:3333/auth/callback")
    params = parse_qs(urlparse(attempt.url).query)
    assert params["client_id"] == ["dynamic_agent_client"]
    assert params["code_challenge_method"] == ["S256"]
    assert params["ext_agent_host_id"] == [host]
    assert params["agent_name_hint"] == ["Terminal MCP"]
    assert "id_token_hint" not in params
    with pytest.raises(AuthError, match="state mismatch"):
        validate_callback(attempt, {"state": "wrong", "code": "anything", "client_id": "oaiapp_one"})
    with pytest.raises(AuthError, match="issued client ID"):
        validate_callback(attempt, {"state": attempt.state, "code": "anything"})
    assert validate_callback(attempt, {"state": attempt.state, "code": "code", "client_id": "oaiapp_one"}) == ("code", "oaiapp_one")
    returning = begin_authorization(host, "http://127.0.0.1:4444/auth/callback", {"client_id": "oaiapp_one", "id_token": "old-token"})
    assert "id_token_hint" not in parse_qs(urlparse(returning.url).query)
    with pytest.raises(AuthError, match="switched"):
        validate_callback(returning, {"state": returning.state, "code": "code", "client_id": "oaiapp_two"})
    assert validate_callback(returning, {"state": returning.state, "code": "code"}) == ("code", "oaiapp_one")


def test_credential_file_rejects_unsafe_permissions_and_symlinks(tmp_path):
    store = CredentialStore(tmp_path / "protected")
    record(store)
    assert store.credentials_path.stat().st_mode & 0o077 == 0
    store.credentials_path.chmod(0o644)
    with pytest.raises(AuthError, match="owner-only"):
        store.load()
    store.credentials_path.chmod(0o600)
    store.credentials_path.rename(store.root / "actual")
    store.credentials_path.symlink_to(store.root / "actual")
    with pytest.raises(AuthError, match="symbolic link"):
        store.load()


def test_refresh_rotates_and_verifies_granted_scope(tmp_path):
    store = CredentialStore(tmp_path / "siwc")
    record(store, saved_at=0)
    requests = []

    def transport(request):
        requests.append(request)
        assert request.url.path.endswith("/oauth/token")
        form = dict(pair.split("=", 1) for pair in request.content.decode().split("&"))
        assert form["grant_type"] == "refresh_token"
        assert form["refresh_token"] == "mock-secret-refresh"
        return httpx.Response(200, json={"access_token": "new-access", "refresh_token": "new-refresh",
                                         "scope": "openid offline_access chatgpt.tokens.use.direct", "expires_in": 3600})

    async def test():
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            assert await store.access_token(client) == "new-access"
            assert await store.access_token(client) == "new-access"
    asyncio.run(test())
    assert len(requests) == 1
    assert store.load()["refresh_token"] == "new-refresh"


def mock_service(tmp_path, events, *, status=200, model_catalog=None):
    store = CredentialStore(tmp_path / "siwc")
    record(store)
    seen = []

    def upstream(request):
        seen.append(request)
        assert request.headers.get("authorization") == "Bearer mock-secret-access"
        if request.method == "GET":
            return httpx.Response(200, json=model_catalog or {"models": [{"slug": "test-model", "display_name": "Test Model", "visibility": "list"}, {"slug": "hidden-model", "visibility": "hidden"}]})
        assert request.url.path == "/v1/responses"
        return httpx.Response(status, text="".join(f"event: {kind}\ndata: {json.dumps({'type': kind})}\n\n" for kind in events))

    gateway = SIWCGateway("local-control-token", store, httpx.MockTransport(upstream))
    return gateway, Starlette(routes=gateway.routes()), seen


def test_private_auth_and_catalog(tmp_path):
    gateway, app, seen = mock_service(tmp_path, [])
    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            assert (await client.get("/v1/siwc/models")).status_code == 401
            assert (await client.get("/v1/siwc/models", headers={"Authorization": "Bearer wrong"})).status_code == 401
            response = await client.get("/v1/siwc/models", headers={"Authorization": "Bearer local-control-token"})
            assert response.status_code == 200
            assert response.json() == {"models": [{"slug": "test-model", "display_name": "Test Model"}]}
    asyncio.run(test())
    assert len(seen) == 1


@pytest.mark.parametrize("events,terminal", [
    (["response.created", "response.output_text.delta", "response.completed"], "response.completed"),
    (["response.created", "response.failed"], "response.failed"),
    (["response.incomplete"], "response.incomplete"),
    (["response.created"], "gateway.failed"),
])
def test_inference_never_promotes_failure_to_completion(tmp_path, events, terminal):
    gateway, app, seen = mock_service(tmp_path, events)
    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            headers = {"Authorization": "Bearer local-control-token"}
            body = {"model": "test-model", "input": [{"role": "user", "content": "Hi"}], "instructions": "Be brief"}
            response = await client.post("/v1/siwc/responses", headers=headers, json=body)
            assert response.status_code == 200
            assert response.headers["x-inference-request-id"]
            assert "event: gateway.started" in response.text
            assert f"event: {terminal}" in response.text
            if terminal != "response.completed":
                assert "event: response.completed" not in response.text
            assert gateway.active == {}
    asyncio.run(test())
    sent = json.loads(seen[0].content)
    assert sent == {"model": "test-model", "input": [{"role": "user", "content": "Hi"}],
                    "instructions": "Be brief", "store": False, "stream": True}


def test_upstream_http_failure_and_invalid_fields(tmp_path):
    gateway, app, seen = mock_service(tmp_path, [], status=429)
    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            headers = {"Authorization": "Bearer local-control-token"}
            body = {"model": "test-model", "input": [{"role": "user", "content": "Hi"}]}
            invalids = [dict(body, store=True), dict(body, input=[{"role": "system", "content": "bad"}]), dict(body, stream=False)]
            for invalid in invalids:
                assert (await client.post("/v1/siwc/responses", headers=headers, json=invalid)).status_code == 400
            assert not seen
            response = await client.post("/v1/siwc/responses", headers=headers, json=body)
            assert "event: gateway.failed" in response.text
            assert '"upstream_status":429' in response.text
    asyncio.run(test())


def test_cancel_is_explicit_and_private(tmp_path):
    gateway, app, seen = mock_service(tmp_path, [])
    async def test():
        flag = asyncio.Event()
        sleeper = asyncio.create_task(asyncio.sleep(10))
        gateway.active["run-id"] = (flag, sleeper)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            assert (await client.post("/v1/siwc/cancel/run-id")).status_code == 401
            assert not flag.is_set()
            response = await client.post("/v1/siwc/cancel/run-id", headers={"Authorization": "Bearer local-control-token"})
            assert response.status_code == 200
            assert response.json()["cancellationRequested"] is True
            assert flag.is_set()
            await asyncio.sleep(0)
            assert sleeper.cancelled()
    asyncio.run(test())


def test_signed_oidc_grant_validates_identity_and_subject(tmp_path):
    import jwt
    from types import SimpleNamespace
    from cryptography.hazmat.primitives.asymmetric import rsa
    from siwc_gateway.auth import exchange_code, ISSUER

    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    store = CredentialStore(tmp_path / "siwc")
    attempt = begin_authorization(store.host_id(), "http://127.0.0.1:3333/auth/callback")
    callback = {"state": attempt.state, "client_id": "oaiapp_test", "code": "mock-code"}
    claims = dict(iss=ISSUER, aud="oaiapp_test", sub="correct-person", nonce=attempt.nonce,
                  exp=int(time.time()) + 3600, iat=int(time.time()))

    class JWKS:
        def get_signing_key_from_jwt(self, raw):
            return SimpleNamespace(key=private.public_key())

    def transport(request):
        if request.url.path.endswith("/oauth/token"):
            return httpx.Response(200, json={"access_token": "signed-access", "refresh_token": "signed-refresh",
                                             "id_token": jwt.encode(claims, private, algorithm="RS256"),
                                             "scope": "openid offline_access chatgpt.tokens.use.direct", "expires_in": 3600})
        return httpx.Response(200, json={"issuer": ISSUER, "jwks_uri": ISSUER + "/.well-known/jwks.json"})

    async def test():
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            identity = await exchange_code(client, attempt, callback, store, jwks_client=JWKS())
            assert identity["authenticated"] and identity["plan_usage"]
            assert store.load()["subject"] == "correct-person"
            wrong_account = dict(store.load(), subject="another-person")
            with pytest.raises(AuthError, match="account changed"):
                await exchange_code(client, attempt, callback, store, previous=wrong_account, jwks_client=JWKS())
            original = claims["nonce"]
            claims["nonce"] = "wrong-nonce"
            with pytest.raises(AuthError, match="nonce mismatch"):
                await exchange_code(client, attempt, callback, store, jwks_client=JWKS())
            claims["nonce"] = original
            claims["aud"] = "not-the-registered-client"
            with pytest.raises(AuthError, match="identity validation failed"):
                await exchange_code(client, attempt, callback, store, jwks_client=JWKS())
    asyncio.run(test())


def test_opt_in_route_only_on_private_t3_plane(tmp_path, monkeypatch):
    import terminal_mcp as core
    from t3_terminal_server import build
    from t3_thread_state import ThreadState

    monkeypatch.setenv("MCP_SIWC_ENABLED", "1")
    monkeypatch.setenv("MCP_GPT_HOME", str(tmp_path / "gpt"))
    for name in ("mcp", "_spawn_workload", "_set_startup_instructions", "_core_behavior_prompt"):
        monkeypatch.setattr(core, name, getattr(core, name))
    state = ThreadState(tmp_path / "t3.db")
    try:
        public, private = build(core, state, "test-control-token")
        paths = [route.path for route in private.routes]
        assert "/v1/siwc/status" in paths
        assert "/v1/siwc/capabilities" in paths
        assert "/v1/siwc/models" in paths
        assert "/v1/siwc/responses" in paths
        assert "/v1/siwc/cancel/{request_id}" in paths
        assert not any(route.path.startswith("/v1/siwc") for route in public._custom_starlette_routes)
    finally:
        state.close()


def test_invalid_credential_payload_fails_safely(tmp_path):
    store = CredentialStore(tmp_path / "protected")
    record(store)
    store.credentials_path.write_text("{not-json}")
    with pytest.raises(AuthError, match="not valid JSON"):
        store.load()


def test_capabilities_are_private_and_report_actual_limits(tmp_path):
    gateway, app, _ = mock_service(tmp_path, [])
    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            assert (await client.get("/v1/siwc/capabilities")).status_code == 401
            result = await client.get("/v1/siwc/capabilities", headers={"Authorization": "Bearer local-control-token"})
            assert result.status_code == 200
            assert "namespace_function" in result.json()["tools"]
            assert "chatgpt_conversation_history" in result.json()["not_available"]
    asyncio.run(test())


def test_function_call_roundtrip_preserves_client_owned_history(tmp_path):
    store = CredentialStore(tmp_path / "siwc")
    record(store)
    recorded = []
    function = {"type": "namespace", "name": "local", "description": "Local tools", "tools": [{
        "type": "function", "name": "echo", "description": "Echo text", "strict": True,
        "parameters": {"type": "object", "properties": {"message": {"type": "string"}},
                       "required": ["message"], "additionalProperties": False},
    }]}
    call = {"type": "function_call", "call_id": "call_example", "name": "echo", "arguments": '{"message":"hello"}'}
    def event(kind, extra=None):
        return f"event: {kind}\ndata: {json.dumps({'type': kind, **(extra or {})})}\n\n"
    def upstream(request):
        body = json.loads(request.content)
        recorded.append(body)
        if len(recorded) == 1:
            stream = event("response.output_item.done", {"item": call}) + event("response.completed")
        else:
            stream = event("response.output_text.delta", {"delta": "ECHO_OK"}) + event("response.completed")
        return httpx.Response(200, text=stream)

    gateway = SIWCGateway("local-control-token", store, httpx.MockTransport(upstream))
    app = Starlette(routes=gateway.routes())
    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            headers = {"Authorization": "Bearer local-control-token"}
            user = {"role": "user", "content": "Echo hello"}
            first = await client.post("/v1/siwc/responses", headers=headers,
                                      json={"model": "test-model", "input": [user], "tools": [function]})
            assert first.status_code == 200
            assert "response.output_item.done" in first.text
            assert '"call_id":"call_example"' in first.text
            history = [user, call, {"type": "function_call_output", "call_id": "call_example", "output": "hello"}]
            second = await client.post("/v1/siwc/responses", headers=headers,
                                       json={"model": "test-model", "input": history, "tools": [function]})
            assert second.status_code == 200
            assert "ECHO_OK" in second.text
            assert "event: response.completed" in second.text
            assert gateway.active == {}
    asyncio.run(test())
    assert len(recorded) == 2
    assert recorded[0]["tools"] == recorded[1]["tools"] == [function]
    assert recorded[1]["input"][-1]["output"] == "hello"
    assert all(x["store"] is False and x["stream"] is True for x in recorded)


def test_disallow_non_siwc_endpoints_and_unsupported_tools(tmp_path):
    gateway, app, upstream = mock_service(tmp_path, [])
    body = {"model": "test-model", "input": [{"role": "user", "content": "Hi"}]}
    invalid = [
        {**body, "tools": [{"type": "file_search"}]},
        {**body, "tools": [{"type": "function", "name": "echo"}]},
        {**body, "tools": [{"type": "namespace", "name": "x", "tools": [{"type": "tool_search"}]}]},
        {**body, "input": [{"role": "tool", "content": "Hello"}]},
        {**body, "input": [{"type": "function_call_output", "output": "missing call id"}]},
        {**body, "input": [{"role": "user", "content": [{"type": "input_audio", "audio": "invalid"}]}]},
        {**body, "previous_response_id": "not-supported"},
    ]
    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            for rejected in invalid:
                response = await client.post("/v1/siwc/responses", headers={"Authorization": "Bearer local-control-token"}, json=rejected)
                assert response.status_code == 400, response.text
    asyncio.run(test())
    assert not upstream


def test_status_is_private_and_never_returns_credentials(tmp_path):
    gateway, app, _ = mock_service(tmp_path, [])
    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as client:
            assert (await client.get("/v1/siwc/status")).status_code == 401
            result = await client.get("/v1/siwc/status", headers={"Authorization": "Bearer local-control-token"})
            assert result.status_code == 200
            assert result.json()["authenticated"] and result.json()["plan_usage_granted"]
            assert "mock-secret-access" not in result.text and "mock-secret-refresh" not in result.text
            gateway.store.credentials_path.unlink()
            missing = await client.get("/v1/siwc/status", headers={"Authorization": "Bearer local-control-token"})
            assert missing.status_code == 200
            assert missing.json()["authenticated"] is False
    asyncio.run(test())


def test_cancel_active_upstream_does_not_complete(tmp_path):
    store = CredentialStore(tmp_path / "siwc")
    record(store)
    started = asyncio.Event()
    stream_closed = asyncio.Event()

    class SuspendedStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            try:
                yield b'event: response.created\ndata: {"type":"response.created"}\n\n'
                started.set()
                await asyncio.Event().wait()
            finally:
                stream_closed.set()
        async def aclose(self):
            stream_closed.set()

    def upstream(request):
        return httpx.Response(200, stream=SuspendedStream())

    gateway = SIWCGateway("local-control-token", store, httpx.MockTransport(upstream))
    app = Starlette(routes=gateway.routes())

    async def test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local", timeout=5) as client:
            headers = {"Authorization": "Bearer local-control-token"}
            pending = asyncio.create_task(client.post('/v1/siwc/responses', headers=headers,
                json={"model": "test-model", "input": [{"role": "user", "content": "Hello"}]}))
            await asyncio.wait_for(started.wait(), 3)
            assert len(gateway.active) == 1
            rid = next(iter(gateway.active))
            cancelled = await client.post(f'/v1/siwc/cancel/{rid}', headers=headers)
            assert cancelled.status_code == 200
            assert cancelled.json()["cancellationRequested"] is True
            try:
                result = await asyncio.wait_for(pending, 3)
            except asyncio.CancelledError:
                pass  # A disconnected downstream may receive cancellation directly.
            else:
                assert 'response.completed' not in result.text
            await asyncio.wait_for(stream_closed.wait(), 3)
            assert gateway.active == {}
    asyncio.run(test())


def test_oauth_refresh_preserves_optional_fields_and_rejects_reduced_scope(tmp_path):
    store = CredentialStore(tmp_path / "siwc")
    record(store, saved_at=0)
    answer = {"access_token": "refreshed-access", "expires_in": 3600}
    def upstream(_request):
        return httpx.Response(200, json=answer)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
            assert await store.access_token(client) == "refreshed-access"
            saved = store.load()
            assert saved["refresh_token"] == "mock-secret-refresh"
            assert "chatgpt.tokens.use.direct" in saved["scopes"]
            saved["saved_at"] = 0
            store.save(saved)
            answer.update(access_token="reduced-access", scope="openid email")
            with pytest.raises(AuthError, match="does not permit"):
                await store.access_token(client)
            assert store.load()["access_token"] == "refreshed-access"
    asyncio.run(run())


def test_encrypted_reasoning_include_acceptance_and_strict_allowlist():
    from siwc_gateway.gateway import _validate_response_body
    body = {"model": "gpt-6-astra", "input": [{"role": "user", "content": "Hi"}],
            "include": ["reasoning.encrypted_content"]}
    prepared = _validate_response_body(body)
    assert prepared["include"] == ["reasoning.encrypted_content"]
    assert prepared["store"] is False
    for invalid in ["reasoning.encrypted_content", [], ["file_search_call.results"],
                    ["reasoning.encrypted_content", "file_search_call.results"]]:
        with pytest.raises(ValueError, match="encrypted reasoning"):
            _validate_response_body({**body, "include": invalid})


def test_multiline_sse_data_is_parsed_as_one_valid_event(tmp_path):
    store = CredentialStore(tmp_path / "siwc")
    record(store)
    # A standard SSE frame can contain several data fields; together they
    # represent one JSON document with a newline between the fragments.
    multiline = ('event: response.completed\n'
                 'data: {"type": "response.completed",\n'
                 'data: "response": {"output": []}}\n\n')
    gateway = SIWCGateway("local-control-token", store,
                          httpx.MockTransport(lambda _: httpx.Response(200, text=multiline)))
    app = Starlette(routes=gateway.routes())
    async def run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://local") as c:
            r = await c.post("/v1/siwc/responses", headers={"Authorization": "Bearer local-control-token"},
                             json={"model": "gpt-6-astra", "input": [{"role": "user", "content": "Hi"}]})
            assert "event: response.completed" in r.text
            assert "gateway.failed" not in r.text
        assert gateway.active == {}
    asyncio.run(run())


def test_unstarted_stream_never_registers_live_request(tmp_path):
    store = CredentialStore(tmp_path / "siwc")
    record(store)
    gateway = SIWCGateway("local-control-token", store, httpx.MockTransport(lambda _: httpx.Response(200)))
    class Request:
        headers = {"authorization": "Bearer local-control-token"}
        async def body(self):
            return b'{"model":"gpt-6-astra","input":[{"role":"user","content":"Hi"}]}'
    async def run():
        result = await gateway.responses(Request())
        assert isinstance(result, StreamingResponse)
        assert result.headers["X-Inference-Request-Id"]
        # A lost client before the ASGI server calls the iterator has no task
        # to cancel and must not leave an active record indefinitely.
        assert gateway.active == {}
        await result.body_iterator.aclose()
        assert gateway.active == {}
    from starlette.responses import StreamingResponse
    asyncio.run(run())
