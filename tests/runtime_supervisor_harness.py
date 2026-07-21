from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from urllib.request import urlopen

from chat_runtime_supervisor import ChatRuntimeSupervisor, SupervisorConfig


async def main() -> int:
    config = SupervisorConfig.from_env()
    state_path = Path(os.environ["FAKE_RUNTIME_STATE"])

    async def health() -> dict[str, object]:
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
            for name in ("launcher_pid", "electron_pid", "renderer_pid", "webview_pid", "cdp_pid"):
                if not Path(f"/proc/{int(state[name])}").exists():
                    return {"ready": False, "reason": f"{name} is not alive", "state": state}
            with urlopen(config.webview_url, timeout=0.5) as response:
                body = response.read().decode("utf-8", errors="replace")
            if "startup-loader" not in body:
                return {"ready": False, "reason": "webview marker is absent", "state": state}
            with urlopen(f"{config.cdp_endpoint}/json/list", timeout=0.5) as response:
                targets = json.load(response)
            if not isinstance(targets, list) or not targets:
                return {"ready": False, "reason": "CDP target is absent", "state": state}
            return {"ready": True, "state": state}
        except Exception as error:
            return {"ready": False, "reason": f"{type(error).__name__}: {error}"}

    return await ChatRuntimeSupervisor(config, health_probe=health).run()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
