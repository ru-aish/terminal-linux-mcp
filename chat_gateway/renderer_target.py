from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlsplit


class RendererTargetError(RuntimeError):
    """The CDP endpoint is alive but has no usable main ChatGPT renderer."""


@dataclass(frozen=True)
class RendererTarget:
    target_id: str
    url: str
    title: str
    websocket_url: str


def _is_auxiliary_target(url: str) -> bool:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    if "/avatar-overlay" in query.get("initialRoute", []):
        return True
    return "avatar-overlay-composition-surface" in parsed.path


def select_main_renderer_target(
    targets: list[dict[str, Any]], *, expected_webview_port: int = 5175
) -> RendererTarget:
    pages = [item for item in targets if item.get("type") == "page"]
    candidates: list[dict[str, Any]] = []
    for item in pages:
        url = str(item.get("url", ""))
        parsed = urlsplit(url)
        is_renderer = (
            parsed.scheme in {"http", "https"}
            and parsed.hostname in {"127.0.0.1", "localhost"}
            and parsed.port == expected_webview_port
        )
        if is_renderer and not _is_auxiliary_target(url):
            candidates.append(item)
    if not candidates:
        if pages:
            raise RendererTargetError(
                "Codex is running, but only auxiliary renderers are available; "
                "open the main app window"
            )
        raise RendererTargetError("Codex is running, but no renderer page is available")

    def rank(item: dict[str, Any]) -> tuple[int, int, str]:
        url = str(item.get("url", ""))
        query = parse_qs(urlsplit(url).query)
        return (
            1 if "mcpAppSandboxDevtools" in query else 0,
            1 if query.get("initialRoute") else 0,
            url,
        )

    selected = min(candidates, key=rank)
    websocket_url = str(selected.get("webSocketDebuggerUrl", ""))
    if not websocket_url:
        raise RendererTargetError("the main Codex renderer has no debugging websocket")
    return RendererTarget(
        target_id=str(selected.get("id", "")),
        url=str(selected.get("url", "")),
        title=str(selected.get("title", "")),
        websocket_url=websocket_url,
    )
