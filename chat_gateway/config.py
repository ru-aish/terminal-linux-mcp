from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from .errors import ConfigurationError
from .models import Lane


@dataclass(frozen=True)
class WindowLimit:
    count: int
    seconds: float

    def validate(self, name: str) -> None:
        if self.count <= 0:
            raise ConfigurationError(f"{name}.count must be positive")
        if self.seconds <= 0:
            raise ConfigurationError(f"{name}.seconds must be positive")


@dataclass(frozen=True)
class LaneLimit:
    minimum_interval: float
    windows: tuple[WindowLimit, ...]
    cache_ttl: float = 0.0

    def validate(self, name: str) -> None:
        if self.minimum_interval < 0:
            raise ConfigurationError(f"{name}.minimum_interval cannot be negative")
        if self.cache_ttl < 0:
            raise ConfigurationError(f"{name}.cache_ttl cannot be negative")
        for index, window in enumerate(self.windows):
            window.validate(f"{name}.windows[{index}]")


@dataclass(frozen=True)
class PollingConfig:
    first_creation_verification: float = 20.0
    second_creation_verification: float = 60.0
    continue_verification: float = 60.0
    active_interval: float = 100.0
    unchanged_once_interval: float = 120.0
    unchanged_interval: float = 300.0

    def validate(self) -> None:
        for name, value in self.__dict__.items():
            if value <= 0:
                raise ConfigurationError(f"polling.{name} must be positive")


@dataclass(frozen=True)
class CircuitConfig:
    initial_cooldown: float = 120.0
    failed_probe_delays: tuple[float, ...] = (360.0, 720.0, 1440.0, 2880.0, 3600.0)
    half_open_successes: int = 3
    half_open_spacing: float = 60.0
    paced_interval: float = 30.0
    paced_duration: float = 300.0
    jitter_fraction: float = 0.10

    def validate(self) -> None:
        if self.initial_cooldown <= 0:
            raise ConfigurationError("circuit.initial_cooldown must be positive")
        if not self.failed_probe_delays or any(
            value <= 0 for value in self.failed_probe_delays
        ):
            raise ConfigurationError("circuit.failed_probe_delays must be positive")
        if self.half_open_successes <= 0:
            raise ConfigurationError("circuit.half_open_successes must be positive")
        if (
            self.half_open_spacing <= 0
            or self.paced_interval <= 0
            or self.paced_duration <= 0
        ):
            raise ConfigurationError("circuit timing values must be positive")
        if not 0 <= self.jitter_fraction <= 1:
            raise ConfigurationError("circuit.jitter_fraction must be between 0 and 1")


@dataclass(frozen=True)
class RetryConfig:
    base_delay: float = 30.0
    maximum_delay: float = 900.0
    maximum_attempts: int = 5
    claim_ttl: float = 120.0

    def validate(self) -> None:
        if self.base_delay <= 0 or self.maximum_delay <= 0 or self.claim_ttl <= 0:
            raise ConfigurationError("retry timing values must be positive")
        if self.maximum_delay < self.base_delay:
            raise ConfigurationError("retry.maximum_delay cannot be below base_delay")
        if self.maximum_attempts <= 0:
            raise ConfigurationError("retry.maximum_attempts must be positive")


@dataclass(frozen=True)
class GatewayConfig:
    database_path: str = "chat_gateway.db"
    maximum_active_agents: int = 5
    completion_marker: str = "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS"
    global_limit: LaneLimit = field(
        default_factory=lambda: LaneLimit(
            20.0,
            (WindowLimit(15, 300.0), WindowLimit(120, 3600.0)),
        )
    )
    conversation_read: LaneLimit = field(
        default_factory=lambda: LaneLimit(
            20.0,
            (WindowLimit(15, 300.0), WindowLimit(120, 3600.0)),
        )
    )
    heavy: LaneLimit = field(
        default_factory=lambda: LaneLimit(
            60.0,
            (WindowLimit(5, 600.0), WindowLimit(20, 3600.0)),
        )
    )
    metadata: LaneLimit = field(
        default_factory=lambda: LaneLimit(5.0, (WindowLimit(10, 60.0),), cache_ttl=30.0)
    )
    cleanup: LaneLimit = field(
        default_factory=lambda: LaneLimit(
            5.0,
            (WindowLimit(10, 60.0), WindowLimit(60, 3600.0)),
        )
    )
    polling: PollingConfig = field(default_factory=PollingConfig)
    circuit: CircuitConfig = field(default_factory=CircuitConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)

    def validate(self) -> None:
        if self.maximum_active_agents <= 0:
            raise ConfigurationError("maximum_active_agents must be positive")
        if not self.completion_marker.strip():
            raise ConfigurationError("completion_marker cannot be empty")
        self.global_limit.validate("global")
        self.conversation_read.validate("conversation_read")
        self.heavy.validate("heavy")
        self.metadata.validate("metadata")
        self.cleanup.validate("cleanup")
        self.polling.validate()
        self.circuit.validate()
        self.retry.validate()

    def lane_limit(self, lane: Lane) -> LaneLimit:
        return {
            Lane.READ: self.conversation_read,
            Lane.HEAVY: self.heavy,
            Lane.METADATA: self.metadata,
            Lane.CLEANUP: self.cleanup,
        }[lane]

    @classmethod
    def from_toml(cls, path: str | Path) -> "GatewayConfig":
        with Path(path).open("rb") as handle:
            raw = tomllib.load(handle)
        config = cls.from_mapping(raw)
        config.validate()
        return config

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "GatewayConfig":
        gateway = _mapping(raw.get("gateway"))
        limits = _mapping(raw.get("limits"))
        polling_raw = _mapping(raw.get("polling"))
        circuit_raw = _mapping(raw.get("circuit"))
        retry_raw = _mapping(raw.get("retry"))

        config = cls(
            database_path=str(gateway.get("database_path", "chat_gateway.db")),
            maximum_active_agents=int(gateway.get("maximum_active_agents", 5)),
            completion_marker=str(
                gateway.get("completion_marker", "DONE_I_HAVE_COMPLETED_ALL_THE_STEPS")
            ),
            global_limit=_lane_from_mapping(limits.get("global"), cls().global_limit),
            conversation_read=_lane_from_mapping(
                limits.get("conversation_read"), cls().conversation_read
            ),
            heavy=_lane_from_mapping(limits.get("heavy"), cls().heavy),
            metadata=_lane_from_mapping(limits.get("metadata"), cls().metadata),
            cleanup=_lane_from_mapping(limits.get("cleanup"), cls().cleanup),
            polling=PollingConfig(
                first_creation_verification=float(
                    polling_raw.get("first_creation_verification", 20.0)
                ),
                second_creation_verification=float(
                    polling_raw.get("second_creation_verification", 60.0)
                ),
                continue_verification=float(
                    polling_raw.get("continue_verification", 60.0)
                ),
                active_interval=float(polling_raw.get("active_interval", 100.0)),
                unchanged_once_interval=float(
                    polling_raw.get("unchanged_once_interval", 120.0)
                ),
                unchanged_interval=float(polling_raw.get("unchanged_interval", 300.0)),
            ),
            circuit=CircuitConfig(
                initial_cooldown=float(circuit_raw.get("initial_cooldown", 120.0)),
                failed_probe_delays=tuple(
                    float(value)
                    for value in circuit_raw.get(
                        "failed_probe_delays", (360, 720, 1440, 2880, 3600)
                    )
                ),
                half_open_successes=int(circuit_raw.get("half_open_successes", 3)),
                half_open_spacing=float(circuit_raw.get("half_open_spacing", 60.0)),
                paced_interval=float(circuit_raw.get("paced_interval", 30.0)),
                paced_duration=float(circuit_raw.get("paced_duration", 300.0)),
                jitter_fraction=float(circuit_raw.get("jitter_fraction", 0.10)),
            ),
            retry=RetryConfig(
                base_delay=float(retry_raw.get("base_delay", 30.0)),
                maximum_delay=float(retry_raw.get("maximum_delay", 900.0)),
                maximum_attempts=int(retry_raw.get("maximum_attempts", 5)),
                claim_ttl=float(retry_raw.get("claim_ttl", 120.0)),
            ),
        )
        config.validate()
        return config


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _lane_from_mapping(value: Any, default: LaneLimit) -> LaneLimit:
    raw = _mapping(value)
    windows_raw = raw.get("windows")
    windows: Sequence[WindowLimit]
    if isinstance(windows_raw, list):
        windows = tuple(
            WindowLimit(int(item["count"]), float(item["seconds"]))
            for item in windows_raw
            if isinstance(item, Mapping)
        )
    else:
        windows = default.windows
    return LaneLimit(
        minimum_interval=float(raw.get("minimum_interval", default.minimum_interval)),
        windows=tuple(windows),
        cache_ttl=float(raw.get("cache_ttl", default.cache_ttl)),
    )
