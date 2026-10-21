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
