from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import time
from dataclasses import asdict
from typing import Any, Optional, Sequence, cast

from .backend import BackendAdapter
from .clock import SystemClock
from .config import GatewayConfig
from .ledger import SQLiteLedger
from .models import OperationState
from .scheduler import ChatGateway

_SENSITIVE_KEYS = {
    "prompt",
    "message",
    "password",
    "secret",
    "token",
    "authorization",
    "api_key",
}


class _UnavailableBackend:
    @staticmethod
    def _error(method: str) -> RuntimeError:
        return RuntimeError(
            f"backend method {method} is unavailable; pass --adapter module:factory"
        )

    async def create_thread(self, **_kwargs: Any) -> Any:
        raise self._error("create_thread")

    async def get_thread(self, **_kwargs: Any) -> Any:
        raise self._error("get_thread")

    async def continue_thread(self, **_kwargs: Any) -> Any:
        raise self._error("continue_thread")

    async def cancel_thread(self, **_kwargs: Any) -> Any:
        raise self._error("cancel_thread")

    async def delete_thread(self, **_kwargs: Any) -> Any:
        raise self._error("delete_thread")

    async def list_project_threads(self, **_kwargs: Any) -> Any:
        raise self._error("list_project_threads")


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if key.lower() in _SENSITIVE_KEYS:
                length = len(item) if isinstance(item, str) else 0
                result[key] = f"<redacted:{length}>"
            else:
                result[key] = redact(item)
        return result
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


def print_json(value: Any) -> None:
    print(json.dumps(redact(value), indent=2, sort_keys=True, default=str))


def load_backend(factory_path: str) -> BackendAdapter:
    if ":" not in factory_path:
        raise ValueError("adapter must use module:factory syntax")
    module_name, attribute_name = factory_path.split(":", 1)
    module = importlib.import_module(module_name)
    value = getattr(module, attribute_name)
    backend = value() if callable(value) else value
    required = (
        "create_thread",
        "get_thread",
        "continue_thread",
        "cancel_thread",
        "delete_thread",
        "list_project_threads",
    )
    missing = [name for name in required if not callable(getattr(backend, name, None))]
    if missing:
        raise TypeError(f"adapter is missing methods: {', '.join(missing)}")
    return cast(BackendAdapter, backend)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="chat-gateway",
        description="Durable single-request ChatGPT backend gateway",
    )
    parser.add_argument("--config", help="TOML configuration path")
    parser.add_argument("--database", help="override database path")
    parser.add_argument(
        "--adapter",
        help="backend factory in module:attribute form; required for tick/run-loop",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="initialize and verify the SQLite ledger")

    create = subparsers.add_parser("enqueue-create", help="queue a child creation")
    create.add_argument("project_id")
    create.add_argument("prompt")
    create.add_argument("--title", default="")
    create.add_argument("--completion-marker")
    create.add_argument("--idempotency-key")

    inspect = subparsers.add_parser("enqueue-inspect", help="queue one canonical read")
    inspect.add_argument("agent_id")
    inspect.add_argument("--completion-sensitive", action="store_true")

    continuation = subparsers.add_parser(
        "enqueue-continue", help="queue one continuation submission"
    )
    continuation.add_argument("agent_id")
    continuation.add_argument("message")
    continuation.add_argument("--idempotency-key")

    cancel = subparsers.add_parser("enqueue-cancel", help="queue cancellation")
    cancel.add_argument("agent_id")
    cancel.add_argument("--idempotency-key")

    delete = subparsers.add_parser("enqueue-delete", help="queue deletion")
    delete.add_argument("agent_id")
    delete.add_argument("--idempotency-key")

    project = subparsers.add_parser(
        "enqueue-project-list", help="queue project metadata refresh"
    )
    project.add_argument("project_id")
    project.add_argument("--idempotency-key")

    subparsers.add_parser("tick", help="execute zero or one physical backend request")

    loop = subparsers.add_parser("run-loop", help="run repeated scheduler ticks")
    loop.add_argument("--sleep", type=float, default=1.0)
    loop.add_argument("--max-ticks", type=int)

    status = subparsers.add_parser("status", help="show one durable agent")
    status.add_argument("agent_id")

    subparsers.add_parser("list-agents", help="list durable agents")
    operations = subparsers.add_parser(
        "list-operations", help="list durable operations"
    )
    operations.add_argument("--state", choices=[item.value for item in OperationState])

    statistics = subparsers.add_parser(
        "request-stats", help="summarize physical requests"
    )
    statistics.add_argument("--since-seconds", type=float)

    subparsers.add_parser("circuits", help="show durable circuit breakers")
    return parser


def _load_config(args: argparse.Namespace) -> GatewayConfig:
    config = GatewayConfig.from_toml(args.config) if args.config else GatewayConfig()
    if args.database:
        from dataclasses import replace

        config = replace(config, database_path=args.database)
    config.validate()
    return config


def _gateway(
    ledger: SQLiteLedger,
    config: GatewayConfig,
    adapter_path: Optional[str],
) -> ChatGateway:
    backend: BackendAdapter = (
        load_backend(adapter_path)
        if adapter_path
        else cast(BackendAdapter, _UnavailableBackend())
    )
    return ChatGateway(ledger, backend, config, SystemClock())


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = _load_config(args)
        ledger = SQLiteLedger(config.database_path)
        gateway = _gateway(ledger, config, args.adapter)

        if args.command == "init-db":
            integrity, foreign_key_violations = ledger.integrity_check()
            print_json(
                {
                    "database": ledger.path,
                    "integrity": integrity,
                    "foreign_key_violations": foreign_key_violations,
                }
            )
            return 0

        if args.command == "enqueue-create":
            agent_id, operation_id = gateway.enqueue_create(
                project_id=args.project_id,
                prompt=args.prompt,
                title=args.title,
                completion_marker=args.completion_marker,
                idempotency_key=args.idempotency_key,
            )
            print_json({"agent_id": agent_id, "operation_id": operation_id})
            return 0

        if args.command == "enqueue-inspect":
            operation_id = gateway.enqueue_inspect(
                agent_id=args.agent_id,
                completion_sensitive=args.completion_sensitive,
            )
            print_json({"operation_id": operation_id})
            return 0

        if args.command == "enqueue-continue":
            operation_id = gateway.enqueue_continue(
                agent_id=args.agent_id,
                message=args.message,
                idempotency_key=args.idempotency_key,
            )
            print_json({"operation_id": operation_id})
            return 0

        if args.command == "enqueue-cancel":
            operation_id = gateway.enqueue_cancel(
                agent_id=args.agent_id,
                idempotency_key=args.idempotency_key,
            )
            print_json({"operation_id": operation_id})
            return 0

        if args.command == "enqueue-delete":
            operation_id = gateway.enqueue_delete(
                agent_id=args.agent_id,
                idempotency_key=args.idempotency_key,
            )
            print_json({"operation_id": operation_id})
            return 0

        if args.command == "enqueue-project-list":
            operation_id = gateway.enqueue_project_list(
                project_id=args.project_id,
                idempotency_key=args.idempotency_key,
            )
            print_json({"operation_id": operation_id})
            return 0

        if args.command in {"tick", "run-loop"} and not args.adapter:
            parser.error("--adapter is required for tick and run-loop")

        if args.command == "tick":
            print_json(asyncio.run(gateway.tick()).as_dict())
            return 0

        if args.command == "run-loop":
            if args.sleep < 0:
                raise ValueError("--sleep cannot be negative")
            ticks = 0
            while args.max_ticks is None or ticks < args.max_ticks:
                result = asyncio.run(gateway.tick())
                print_json(result.as_dict())
                ticks += 1
                if args.max_ticks is None or ticks < args.max_ticks:
                    time.sleep(args.sleep)
            return 0

        if args.command == "status":
            agent = ledger.get_agent(args.agent_id)
            if agent is None:
                raise KeyError(f"unknown agent: {args.agent_id}")
            print_json(asdict(agent))
            return 0

        if args.command == "list-agents":
            print_json([asdict(agent) for agent in ledger.list_agents()])
            return 0

        if args.command == "list-operations":
            state = OperationState(args.state) if args.state else None
            operations = []
            for operation in ledger.list_operations(state=state):
                item = asdict(operation)
                item["payload"] = redact(dict(operation.payload))
                operations.append(item)
            print_json(operations)
            return 0

        if args.command == "request-stats":
            since = None
            if args.since_seconds is not None:
                if args.since_seconds < 0:
                    raise ValueError("--since-seconds cannot be negative")
                since = time.time() - args.since_seconds
            print_json(ledger.request_stats(since=since))
            return 0

        if args.command == "circuits":
            print_json([asdict(item) for item in ledger.list_circuits()])
            return 0

        parser.error(f"unhandled command: {args.command}")
    except (KeyError, ValueError, TypeError, RuntimeError) as error:
        print_json({"error": str(error), "type": type(error).__name__})
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
