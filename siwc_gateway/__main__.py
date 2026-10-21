"""One-time browser sign-in to authorize the headless Terminal MCP daemon."""
from __future__ import annotations

import argparse
import asyncio
from http.server import BaseHTTPRequestHandler, HTTPServer
import secrets
import sys
import time
from urllib.parse import parse_qs, urlparse
import webbrowser

import httpx

from .auth import AuthError, CredentialStore, begin_authorization, exchange_code, validate_callback


async def login(no_browser: bool = False) -> None:
    store = CredentialStore()
    try:
        previous = store.load()
    except AuthError:
        previous = None
    result: dict[str, str] = {}
    expected_state = ""

    class Callback(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Authorization code and state must never be written to logs.

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path != "/auth/callback" or self.headers.get("Host") != f"127.0.0.1:{server.server_port}":
                self.send_error(404)
                return
            query = parse_qs(parsed.query, keep_blank_values=True)
            if not secrets.compare_digest(query.get("state", [""])[0], expected_state):
                self.send_error(400, "Invalid sign-in state")
                return
            if any(len(values) != 1 for values in query.values()):
                self.send_error(400, "Invalid sign-in callback")
                return
            result.update({name: values[0] for name, values in query.items()})
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(b"Terminal MCP sign-in received. You can close this tab.")

    server = HTTPServer(("127.0.0.1", 0), Callback)
    server.timeout = 1
    try:
        redirect = f"http://127.0.0.1:{server.server_port}/auth/callback"
        attempt = begin_authorization(store.host_id(), redirect, previous)
        expected_state = attempt.state
        # No retained ID token in printed URLs, even on returning sign-in.
        print("Visit this URL in a browser on the SAME machine to authorize Terminal MCP:\n")
        print(attempt.url)
        if not no_browser:
            webbrowser.open(attempt.url)
        deadline = time.monotonic() + 300
        while not result and time.monotonic() < deadline:
            await asyncio.to_thread(server.handle_request)
        if not result:
            raise AuthError("ChatGPT browser authorization timed out")
        validate_callback(attempt, result)
        async with httpx.AsyncClient(timeout=30) as client:
            account = await exchange_code(client, attempt, result, store, previous)
        print("ChatGPT plan authorization saved for:", account["account"])
    finally:
        server.server_close()


def main():
    parser = argparse.ArgumentParser(prog="python -m siwc_gateway", description="Headless ChatGPT-plan login for Terminal MCP")
    parser.add_argument("action", choices=["login", "status"])
    parser.add_argument("--no-browser", action="store_true", help="Print authorization URL instead of opening a browser")
    args = parser.parse_args()
    try:
        if args.action == "login":
            asyncio.run(login(args.no_browser))
        else:
            record = CredentialStore().load()
            print("Registered ChatGPT account:", record.get("email") or "(hidden)")
            print("Plan usage granted:", "chatgpt.tokens.use.direct" in record.get("scopes", []))
    except AuthError as exc:
        print("Authorization error:", exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
