from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

from .config import GatewayConfig, LaneLimit
from .ledger import SQLiteLedger
from .models import Lane


@dataclass(frozen=True)
class Eligibility:
    allowed: bool
    next_at: Optional[float]
    reasons: tuple[str, ...] = ()


class DurableRateLimiter:
    """Evaluates durable global and per-lane request reservations."""

    def __init__(self, ledger: SQLiteLedger, config: GatewayConfig) -> None:
        self.ledger = ledger
        self.config = config

    def check(
        self,
        connection: sqlite3.Connection,
        *,
        lane: Lane,
        now: float,
    ) -> Eligibility:
        next_times: list[float] = []
        reasons: list[str] = []
        in_flight_until = self.ledger.active_request_lock_until(connection, now=now)
        if in_flight_until is not None:
            next_times.append(in_flight_until)
            reasons.append("global:in_flight_request")
        self._check_limit(
            connection,
            lane=None,
            limit=self.config.global_limit,
            now=now,
            label="global",
            next_times=next_times,
            reasons=reasons,
        )
        self._check_limit(
            connection,
            lane=lane,
            limit=self.config.lane_limit(lane),
            now=now,
            label=lane.value,
            next_times=next_times,
            reasons=reasons,
        )
        if not next_times:
            return Eligibility(True, None, ())
        return Eligibility(False, max(next_times), tuple(reasons))

    def _check_limit(
        self,
        connection: sqlite3.Connection,
        *,
        lane: Optional[Lane],
        limit: LaneLimit,
        now: float,
        label: str,
        next_times: list[float],
        reasons: list[str],
    ) -> None:
        last = self.ledger.last_request_time(connection, lane=lane)
        if last is not None:
            eligible_at = last + limit.minimum_interval
            if eligible_at > now:
                next_times.append(eligible_at)
                reasons.append(f"{label}:minimum_interval")

        for window in limit.windows:
            times = self.ledger.request_times_since(
                connection,
                since=now - window.seconds,
                lane=lane,
            )
            if len(times) < window.count:
                continue
            blocking_index = len(times) - window.count
            eligible_at = times[blocking_index] + window.seconds
            if eligible_at > now:
                next_times.append(eligible_at)
                reasons.append(f"{label}:{window.count}/{window.seconds:g}s")
