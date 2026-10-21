"""Public-client Sign in with ChatGPT (OAuth PKCE) credential management."""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import secrets
import stat
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import jwt

ISSUER = "https://auth.openai.com"
RESOURCE = "https://api.openai.com/v1"
AUTHORIZE_URL = f"{ISSUER}/api/accounts/authorize"
TOKEN_URL = f"{ISSUER}/api/accounts/oauth/token"
DISCOVERY_URL = f"{ISSUER}/.well-known/openid-configuration"
SCOPES = "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct"
REQUIRED_SCOPE = "chatgpt.tokens.use.direct"


class AuthError(Exception):
    """A sanitized, non-secret-bearing authentication error."""


def _private_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.parent.is_symlink() or (path.parent.stat().st_mode & 0o077):
        raise AuthError("Credential directory must be private (0700)")
    if path.exists():
        _private_read(path)  # Refuse symlinks or permissive existing files.
    temp = path.with_name(f".{path.name}.{secrets.token_hex(8)}.tmp")
    descriptor = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(value, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _private_read(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise AuthError("Credential file cannot be a symbolic link")
    metadata = path.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise AuthError("Credential file must be a regular file")
    if metadata.st_mode & 0o077 or (hasattr(os, "getuid") and metadata.st_uid != os.getuid()):
        raise AuthError("Credential file must be owner-only (0600)")
    try:
        with path.open(encoding="utf-8") as stream:
            result = json.load(stream)
    except (ValueError, UnicodeError) as exc:
        raise AuthError("Credential file is not valid JSON") from exc
    if not isinstance(result, dict):
        raise AuthError("Credential file has invalid structure")
    return result


class CredentialStore:
    def __init__(self, root: Path | None = None):
        self.root = (root or Path(os.environ.get("MCP_SIWC_HOME", "~/.config/terminal-mcp-t3/siwc"))).expanduser()
        self.lock = asyncio.Lock()

    @property
    def credentials_path(self) -> Path:
        return self.root / "credentials.json"

    @property
    def host_path(self) -> Path:
        return self.root / "host.json"

    def host_id(self) -> str:
        try:
            host = _private_read(self.host_path)
            value = host["ext_agent_host_id"]
            if not isinstance(value, str) or not value.startswith("urn:uuid:"):
                raise AuthError("Invalid saved host identifier")
            return value
        except FileNotFoundError:
            value = "urn:uuid:" + str(uuid.uuid4())
            _private_write(self.host_path, {"ext_agent_host_id": value})
            return value

    def load(self) -> dict[str, Any]:
        try:
            return _private_read(self.credentials_path)
        except FileNotFoundError as exc:
            raise AuthError("No ChatGPT grant; run python -m siwc_gateway login") from exc

    def save(self, record: dict[str, Any]) -> None:
        _private_write(self.credentials_path, record)

    async def access_token(self, client: httpx.AsyncClient) -> str:
        async with self.lock:
            record = self.load()
            if REQUIRED_SCOPE not in record.get("scopes", []):
                raise AuthError("ChatGPT plan usage was not authorized")
            if not record.get("access_token") or not record.get("client_id"):
                raise AuthError("Incomplete ChatGPT grant")
            # Avoid refreshing tokens merely because a second turn begins.
            if time.time() < float(record.get("saved_at", 0)) + int(record.get("expires_in", 0)) - 300:
                return record["access_token"]
            if not record.get("refresh_token"):
                raise AuthError("ChatGPT grant expired; sign in again")
            try:
                response = await client.post(TOKEN_URL, data={
                    "grant_type": "refresh_token",
                    "client_id": record["client_id"],
                    "refresh_token": record["refresh_token"],
                    "resource": RESOURCE,
                })
                response.raise_for_status()
                new = response.json()
            except (httpx.HTTPError, ValueError) as exc:
                raise AuthError("ChatGPT token renewal failed; sign in again if necessary") from exc
            if not isinstance(new, dict) or not isinstance(new.get("access_token"), str) or not new["access_token"]:
                raise AuthError("ChatGPT token renewal omitted an access token")
            # RFC 6749: a refresh response may omit scope and refresh_token.
            # An explicit reduced scope must still be rejected, never silently restored.
            refresh_token = new.get("refresh_token", record["refresh_token"])
            scopes = new.get("scope")
            scopes = str(scopes).split() if scopes is not None else record["scopes"]
            if not isinstance(refresh_token, str) or not refresh_token or REQUIRED_SCOPE not in scopes:
                raise AuthError("Renewed grant does not permit ChatGPT plan usage")
            record.update(access_token=new["access_token"], refresh_token=refresh_token,
                          scopes=scopes, expires_in=int(new.get("expires_in", 3600)),
                          saved_at=time.time())
            self.save(record)
            return record["access_token"]


@dataclass(frozen=True)
class OAuthAttempt:
    state: str
    nonce: str
    verifier: str
    client_id: str
    redirect_uri: str
    url: str


def begin_authorization(host_id: str, redirect_uri: str, previous: dict[str, Any] | None = None) -> OAuthAttempt:
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    client_id = previous["client_id"] if previous else "dynamic_agent_client"
    values = dict(client_id=client_id, ext_agent_host_id=host_id,
                  response_type="code", redirect_uri=redirect_uri, scope=SCOPES,
                  resource=RESOURCE, state=state, nonce=nonce,
                  code_challenge_method="S256", code_challenge=challenge)
    if not previous:
        values["agent_name_hint"] = "Terminal MCP"
    return OAuthAttempt(state, nonce, verifier, client_id, redirect_uri,
                        AUTHORIZE_URL + "?" + urlencode(values))


def validate_callback(attempt: OAuthAttempt, query: dict[str, str]) -> tuple[str, str]:
    if not secrets.compare_digest(query.get("state", ""), attempt.state):
        raise AuthError("OAuth callback state mismatch")
    if query.get("error"):
        raise AuthError("ChatGPT authorization declined or unsuccessful")
    code = query.get("code")
    if not code:
        raise AuthError("OAuth callback did not contain a code")
    issued_id = query.get("client_id", "")
    if attempt.client_id == "dynamic_agent_client":
        if not issued_id.startswith("oaiapp_"):
            raise AuthError("New registration did not return an issued client ID")
        return code, issued_id
    if issued_id and issued_id != attempt.client_id:
        raise AuthError("OAuth callback switched the registered client")
    return code, attempt.client_id


async def exchange_code(client: httpx.AsyncClient, attempt: OAuthAttempt, query: dict[str, str],
                        store: CredentialStore, previous: dict[str, Any] | None = None,
                        jwks_client: Any = None) -> dict[str, Any]:
    code, issued_id = validate_callback(attempt, query)
    try:
        response = await client.post(TOKEN_URL, data={
            "grant_type": "authorization_code", "client_id": issued_id,
            "code": code, "code_verifier": attempt.verifier,
            "redirect_uri": attempt.redirect_uri, "resource": RESOURCE,
        })
        response.raise_for_status()
        tokens = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise AuthError("ChatGPT authorization code exchange failed") from exc
    scopes = str(tokens.get("scope", "")).split()
    if REQUIRED_SCOPE not in scopes:
        raise AuthError("ChatGPT plan usage permission was not granted")
    if not tokens.get("access_token") or not tokens.get("refresh_token") or not tokens.get("id_token"):
        raise AuthError("ChatGPT grant is missing necessary tokens")
    # Resolve issuer-supplied JWKS location rather than accepting attacker-provided keys.
    try:
        discovery = await client.get(DISCOVERY_URL)
        discovery.raise_for_status()
        oidc = discovery.json()
        if oidc.get("issuer") != ISSUER or not str(oidc.get("jwks_uri", "")).startswith(ISSUER + "/"):
            raise AuthError("Unexpected OpenAI identity issuer/key endpoint")
        jwks_client = jwks_client or jwt.PyJWKClient(oidc["jwks_uri"])
        key = await asyncio.to_thread(jwks_client.get_signing_key_from_jwt, tokens["id_token"])
        identity = jwt.decode(tokens["id_token"], key.key, algorithms=["RS256", "ES256"],
                              audience=issued_id, issuer=ISSUER,
                              options={"require": ["sub", "exp", "iat"]}, leeway=5)
    except (httpx.HTTPError, jwt.PyJWTError, KeyError, ValueError) as exc:
        raise AuthError("ChatGPT identity validation failed") from exc
    if not secrets.compare_digest(str(identity.get("nonce", "")), attempt.nonce):
        raise AuthError("ChatGPT identity nonce mismatch")
    if not identity.get("sub") or (previous and identity["sub"] != previous["subject"]):
        raise AuthError("Authenticated ChatGPT account changed")
    record = {"issuer": ISSUER, "subject": identity["sub"], "email": identity.get("email"),
              "client_id": issued_id, "ext_agent_host_id": store.host_id(),
              "access_token": tokens["access_token"], "refresh_token": tokens["refresh_token"],
              "id_token": tokens["id_token"], "scopes": scopes,
              "expires_in": int(tokens.get("expires_in", 3600)), "saved_at": time.time()}
    store.save(record)
    return {"authenticated": True, "plan_usage": True, "account": identity.get("email") or identity["sub"]}
