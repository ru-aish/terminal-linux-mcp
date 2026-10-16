from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.time()


@dataclass
class FakeClock:
    value: float = 0.0

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("cannot move FakeClock backwards")
        self.value += seconds
        return self.value

    def set(self, value: float) -> None:
        if value < self.value:
            raise ValueError("cannot move FakeClock backwards")
        self.value = value
