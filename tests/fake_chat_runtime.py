from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


class WebviewHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        body = b"<!doctype html><title>ChatGPT</title><div id='startup-loader'></div>"
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: Any) -> None:
        return


class CdpHandler(BaseHTTPRequestHandler):
    renderer_pid = 0
    webview_port = 0
    cdp_port = 0

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/json/list":
            self.send_error(404)
            return
        payload = json.dumps(
            [
                {
                    "id": f"fake-renderer-{self.renderer_pid}",
                    "type": "page",
                    "title": "ChatGPT test renderer",
                    "url": f"http://127.0.0.1:{self.webview_port}/",
                    "webSocketDebuggerUrl": (
                        f"ws://127.0.0.1:{self.cdp_port}/devtools/page/"
                        f"fake-renderer-{self.renderer_pid}"
                    ),
                }
            ]
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def serve_webview(port: int) -> int:
    ThreadingHTTPServer(("127.0.0.1", port), WebviewHandler).serve_forever()
    return 0


def serve_cdp(port: int, webview_port: int, renderer_pid: int) -> int:
    CdpHandler.renderer_pid = renderer_pid
    CdpHandler.webview_port = webview_port
    CdpHandler.cdp_port = port
    ThreadingHTTPServer(("127.0.0.1", port), CdpHandler).serve_forever()
    return 0


def idle() -> int:
    while True:
        time.sleep(60)


def launcher(state_path: Path, generation_path: Path, webview_port: int, cdp_port: int) -> int:
    generation_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        generation = int(generation_path.read_text().strip()) + 1
    except (FileNotFoundError, ValueError):
        generation = 1
    generation_path.write_text(f"{generation}\n", encoding="utf-8")

    children: list[subprocess.Popen[bytes]] = []
    stopping = False

    def stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True
        for child in children:
            if child.poll() is None:
                child.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    electron = subprocess.Popen([sys.executable, __file__, "idle"])
    renderer = subprocess.Popen([sys.executable, __file__, "idle"])
    webview = subprocess.Popen(
        [sys.executable, __file__, "webview", "--port", str(webview_port)]
    )
    cdp = subprocess.Popen(
        [
            sys.executable,
            __file__,
            "cdp",
            "--port",
            str(cdp_port),
            "--webview-port",
            str(webview_port),
            "--renderer-pid",
            str(renderer.pid),
        ]
    )
    children.extend((electron, renderer, webview, cdp))
    state_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = state_path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(
            {
                "generation": generation,
                "launcher_pid": os.getpid(),
                "electron_pid": electron.pid,
                "renderer_pid": renderer.pid,
                "webview_pid": webview.pid,
                "cdp_pid": cdp.pid,
                "webview_port": webview_port,
                "cdp_port": cdp_port,
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(state_path)

    while not stopping:
        for child in children:
            if child.poll() is not None:
                time.sleep(0.2)
        time.sleep(0.2)
    for child in children:
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
    return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    sub = result.add_subparsers(dest="mode", required=True)
    launch = sub.add_parser("launcher")
    launch.add_argument("--state", type=Path, required=True)
    launch.add_argument("--generation", type=Path, required=True)
    launch.add_argument("--webview-port", type=int, required=True)
    launch.add_argument("--cdp-port", type=int, required=True)
    webview = sub.add_parser("webview")
    webview.add_argument("--port", type=int, required=True)
    cdp = sub.add_parser("cdp")
    cdp.add_argument("--port", type=int, required=True)
    cdp.add_argument("--webview-port", type=int, required=True)
    cdp.add_argument("--renderer-pid", type=int, required=True)
    sub.add_parser("idle")
    return result


def main() -> int:
    args = parser().parse_args()
    if args.mode == "launcher":
        return launcher(args.state, args.generation, args.webview_port, args.cdp_port)
    if args.mode == "webview":
        return serve_webview(args.port)
    if args.mode == "cdp":
        return serve_cdp(args.port, args.webview_port, args.renderer_pid)
    return idle()


if __name__ == "__main__":
    raise SystemExit(main())
