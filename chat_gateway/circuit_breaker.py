from __future__ import annotations

import random
import sqlite3
from dataclasses import replace
from typing import Callable, Optional

from .config import CircuitConfig
from .ledger import SQLiteLedger
from .models import CircuitRecord, CircuitState, Lane, OperationType

JitterFunction = Callable[[float, str, int], float]


def default_jitter(delay: float, _scope: str, _failure_index: int) -> float:
    return delay * (1.0 + random.uniform(-0.10, 0.10))


def scope_for_lane(lane: Lane) -> str:
    if lane in {Lane.READ, Lane.HEAVY}:
        return "conversation"
    return str(lane.value)


class CircuitManager:
    def __init__(
        self,
        ledger: SQLiteLedger,
        config: CircuitConfig,
        *,
        jitter: Optional[JitterFunction] = None,
    ) -> None:
        self.ledger = ledger
        self.config = config
        self.jitter = jitter or default_jitter

    def admit(
        self,
        connection: sqlite3.Connection,
        *,
        lane: Lane,
        operation_type: OperationType,
        now: float,
    ) -> tuple[bool, Optional[float], CircuitRecord]:
        scope = scope_for_lane(lane)
        record = self.ledger.get_circuit(connection, scope=scope, now=now)

        if record.state is CircuitState.PACED:
            assert record.paced_started_at is not None
            if now >= record.paced_started_at + self.config.paced_duration:
                record = replace(
                    record,
                    state=CircuitState.CLOSED,
                    opened_at=None,
                    retry_at=None,
                    probe_failures=0,
                    half_open_successes=0,
                    last_success_at=now,
                    paced_started_at=None,
                    last_attempt_at=None,
                    updated_at=now,
                )
                self.ledger.save_circuit(connection, record)

        if record.state is CircuitState.CLOSED:
            return True, None, record

        if record.state is CircuitState.OPEN:
            if operation_type is not OperationType.RECOVERY_PROBE:
                return False, record.retry_at, record
            if record.retry_at is None or now < record.retry_at:
                return False, record.retry_at, record
            record = replace(
                record,
                state=CircuitState.HALF_OPEN,
                last_attempt_at=now,
                updated_at=now,
            )
            self.ledger.save_circuit(connection, record)
            return True, None, record

        if record.state is CircuitState.HALF_OPEN:
            if operation_type is not OperationType.RECOVERY_PROBE:
                next_at = (
                    None
                    if record.last_attempt_at is None
                    else record.last_attempt_at + self.config.half_open_spacing
                )
                return False, next_at, record
            if record.last_attempt_at is not None:
                next_at = record.last_attempt_at + self.config.half_open_spacing
                if now < next_at:
                    return False, next_at, record
            record = replace(record, last_attempt_at=now, updated_at=now)
            self.ledger.save_circuit(connection, record)
            return True, None, record

        if record.state is CircuitState.PACED:
            if operation_type is OperationType.RECOVERY_PROBE:
                return False, record.paced_started_at, record
            if record.last_attempt_at is not None:
                next_at = record.last_attempt_at + self.config.paced_interval
                if now < next_at:
                    return False, next_at, record
            record = replace(record, last_attempt_at=now, updated_at=now)
            self.ledger.save_circuit(connection, record)
            return True, None, record

        return False, record.retry_at, record

    def record_success(
        self,
        connection: sqlite3.Connection,
        *,
        scope: str,
        operation_type: OperationType,
        now: float,
    ) -> CircuitRecord:
        record = self.ledger.get_circuit(connection, scope=scope, now=now)
        if (
            record.state is CircuitState.HALF_OPEN
            and operation_type is OperationType.RECOVERY_PROBE
        ):
            successes = record.half_open_successes + 1
            if successes >= self.config.half_open_successes:
                record = replace(
                    record,
                    state=CircuitState.PACED,
                    retry_at=None,
                    half_open_successes=successes,
                    last_success_at=now,
                    paced_started_at=now,
                    last_attempt_at=now,
                    updated_at=now,
                )
            else:
                record = replace(
                    record,
                    half_open_successes=successes,
                    last_success_at=now,
                    last_attempt_at=now,
                    updated_at=now,
                )
            self.ledger.save_circuit(connection, record)
        elif record.state is CircuitState.PACED:
            record = replace(record, last_success_at=now, updated_at=now)
            self.ledger.save_circuit(connection, record)
        return record

    def record_rate_limit(
        self,
        connection: sqlite3.Connection,
        *,
        scope: str,
        now: float,
    ) -> CircuitRecord:
        record = self.ledger.get_circuit(connection, scope=scope, now=now)
        if record.state is CircuitState.CLOSED:
            failures = 0
            base_delay = self.config.initial_cooldown
        else:
            failures = record.probe_failures + 1
            index = min(failures - 1, len(self.config.failed_probe_delays) - 1)
            base_delay = self.config.failed_probe_delays[index]
        delay = max(0.0, self.jitter(base_delay, scope, failures))
        record = replace(
            record,
            state=CircuitState.OPEN,
            opened_at=now,
            retry_at=now + delay,
            probe_failures=failures,
            half_open_successes=0,
            last_success_at=None,
            paced_started_at=None,
            last_attempt_at=None,
            updated_at=now,
        )
        self.ledger.save_circuit(connection, record)
        return record

    def record_probe_error(
        self,
        connection: sqlite3.Connection,
        *,
        scope: str,
        now: float,
    ) -> CircuitRecord:
        # A transport failure while half-open is treated conservatively as a
        # failed probe, even when it is not an explicit 429.
        record = self.ledger.get_circuit(connection, scope=scope, now=now)
        failures = record.probe_failures + 1
        index = min(failures - 1, len(self.config.failed_probe_delays) - 1)
        base_delay = self.config.failed_probe_delays[index]
        delay = max(0.0, self.jitter(base_delay, scope, failures))
        record = replace(
            record,
            state=CircuitState.OPEN,
            opened_at=now,
            retry_at=now + delay,
            probe_failures=failures,
            half_open_successes=0,
            last_success_at=None,
            paced_started_at=None,
            last_attempt_at=None,
            updated_at=now,
        )
        self.ledger.save_circuit(connection, record)
        return record
