from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chat_internal_client import InternalChatClient


def _turn_summary(turn: dict[str, Any]) -> dict[str, Any]:
    text = str(turn.get("text") or "")
    return {
        "node_id": str(turn.get("node_id") or turn.get("key") or ""),
        "parent_id": str(turn.get("parent_id") or ""),
        "role": str(turn.get("role") or ""),
        "status": str(turn.get("status") or ""),
        "end_turn": turn.get("end_turn") if isinstance(turn.get("end_turn"), bool) else None,
        "model_slug": str(turn.get("model_slug") or ""),
        "text_length": len(text),
        "text_sha256": hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
        if text
        else "",
    }


async def run(endpoint: str, conversation_id: str | None) -> dict[str, Any]:
    async with InternalChatClient(endpoint, timeout=30) as client:
        health = await client.health()
        models = await client.models()
        result: dict[str, Any] = {
            "health": health,
            "models": models,
        }
        if conversation_id:
            thread = await client.get_thread(conversation_id)
            result["thread"] = {
                "found": bool(thread.get("found")),
                "reason": str(thread.get("reason") or ""),
                "conversation_id": str(thread.get("conversation_id") or conversation_id),
                "title": str(thread.get("title") or ""),
                "current_node": str(thread.get("current_node") or ""),
                "state_verified": bool(thread.get("state_verified")),
                "running": bool(thread.get("running")),
                "active_stream": bool(thread.get("active_stream")),
                "update_time": thread.get("update_time"),
                "turns": [
                    _turn_summary(turn)
                    for turn in thread.get("turns", [])
                    if isinstance(turn, dict)
                ],
            }
        return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe the app-owned normal ChatGPT client without UI interaction."
    )
    parser.add_argument("conversation_id", nargs="?")
    parser.add_argument("--endpoint", default="http://127.0.0.1:9222")
    args = parser.parse_args()
    print(
        json.dumps(
            asyncio.run(run(args.endpoint, args.conversation_id)),
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
