from __future__ import annotations

import argparse
import asyncio
import json
import time
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

from .cli import load_backend
from .clock import SystemClock
from .config import GatewayConfig
from .ledger import SQLiteLedger
from .scheduler import ChatGateway

PROMPTS = (
    "Read-only smoke task. Report RAM status briefly. Do not delegate or modify anything. End exactly with DONE_I_HAVE_COMPLETED_ALL_THE_STEPS.",
    "Read-only smoke task. Report system uptime briefly. Do not delegate or modify anything. End exactly with DONE_I_HAVE_COMPLETED_ALL_THE_STEPS.",
    "Read-only smoke task. Report root filesystem usage briefly. Do not delegate or modify anything. End exactly with DONE_I_HAVE_COMPLETED_ALL_THE_STEPS.",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded live gateway smoke test")
    parser.add_argument(
        "--adapter", required=True, help="module:factory backend adapter"
    )
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--journal", required=True)
    parser.add_argument("--config")
    parser.add_argument("--max-agents", type=int, default=3)
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--confirm-live", action="store_true")
    return parser


def _journal(path: Path, event: str, **details: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"time": time.time(), "event": event, **details}
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        handle.flush()


async def run_smoke(
    *,
    adapter_path: str,
    project_id: str,
    database: str,
    journal: str,
    config: GatewayConfig,
    maximum_agents: int,
    timeout: float,
    sleep_seconds: float,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> dict[str, Any]:
    if not 1 <= maximum_agents <= 3:
        raise ValueError("maximum_agents must be between 1 and 3")
    if timeout <= 0 or sleep_seconds < 0:
        raise ValueError("timeout must be positive and sleep cannot be negative")

    journal_path = Path(journal).expanduser().resolve()
    ledger = SQLiteLedger(database)
    gateway = ChatGateway(ledger, load_backend(adapter_path), config, SystemClock())
    started = time.monotonic()
    agent_ids: list[str] = []
    created_conversation_ids: set[str] = set()
    _journal(journal_path, "smoke_started", project_id=project_id)

    for index in range(maximum_agents):
        agent_id, operation_id = gateway.enqueue_create(
            project_id=project_id,
            prompt=PROMPTS[index],
            title=f"Gateway smoke {index + 1}",
            idempotency_key=f"live-smoke:{journal_path}:{index}",
        )
        agent_ids.append(agent_id)
        _journal(
            journal_path,
            "agent_enqueued",
            agent_id=agent_id,
            operation_id=operation_id,
        )

    timed_out = False
    try:
        while True:
            if time.monotonic() - started >= timeout:
                timed_out = True
                _journal(journal_path, "work_timeout")
                break
            result = await gateway.tick()
            _journal(journal_path, "tick", result=result.as_dict())
            for agent_id in agent_ids:
                agent = ledger.get_agent(agent_id)
                if agent and agent.conversation_id:
                    created_conversation_ids.add(agent.conversation_id)
            current_agents = [
                agent
                for agent in (ledger.get_agent(agent_id) for agent_id in agent_ids)
                if agent is not None
            ]
            if len(current_agents) != len(agent_ids):
                raise RuntimeError(
                    "an isolated smoke agent disappeared from the ledger"
                )
            if all(agent.state.terminal for agent in current_agents):
                break
            await sleep(sleep_seconds)
    finally:
        # Cleanup is driven only from the isolated ledger's own agent IDs. The
        # runner never lists or deletes unrelated project conversations.
        for agent_id in agent_ids:
            agent = ledger.get_agent(agent_id)
            if (
                agent is None
                or not agent.conversation_id
                or agent.deleted_at is not None
            ):
                continue
            if not agent.state.terminal:
                with suppress(ValueError):
                    gateway.enqueue_cancel(
                        agent_id=agent_id,
                        idempotency_key=f"live-smoke-cancel:{agent_id}",
                    )
            gateway.enqueue_delete(
                agent_id=agent_id,
                idempotency_key=f"live-smoke-delete:{agent_id}",
            )

        cleanup_started = time.monotonic()
        while time.monotonic() - cleanup_started < timeout:
            remaining_agents = [
                agent
                for agent in (ledger.get_agent(agent_id) for agent_id in agent_ids)
                if agent is not None
                and agent.conversation_id
                and agent.deleted_at is None
            ]
            if not remaining_agents:
                break
            result = await gateway.tick()
            _journal(journal_path, "cleanup_tick", result=result.as_dict())
            await sleep(sleep_seconds)

        remaining_agent_ids = [
            agent.id
            for agent in (ledger.get_agent(agent_id) for agent_id in agent_ids)
            if agent is not None and agent.conversation_id and agent.deleted_at is None
        ]
        _journal(
            journal_path,
            "smoke_finished",
            timed_out=timed_out,
            created_conversation_ids=sorted(created_conversation_ids),
            remaining_agent_ids=remaining_agent_ids,
        )

    return {
        "agent_ids": agent_ids,
        "created_conversation_ids": sorted(created_conversation_ids),
        "timed_out": timed_out,
        "remaining_agent_ids": remaining_agent_ids,
        "journal": str(journal_path),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.confirm_live:
        print("Refusing live execution without --confirm-live", flush=True)
        return 2
    config = GatewayConfig.from_toml(args.config) if args.config else GatewayConfig()
    config = replace(config, database_path=args.database)
    try:
        result = asyncio.run(
            run_smoke(
                adapter_path=args.adapter,
                project_id=args.project_id,
                database=args.database,
                journal=args.journal,
                config=config,
                maximum_agents=args.max_agents,
                timeout=args.timeout,
                sleep_seconds=args.sleep,
            )
        )
    except (ValueError, RuntimeError, TypeError) as error:
        print(json.dumps({"error": str(error), "type": type(error).__name__}))
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not result["remaining_agent_ids"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
