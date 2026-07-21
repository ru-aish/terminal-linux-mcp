from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, IntEnum
from typing import Any, Mapping, Optional, Sequence


class AgentState(str, Enum):
    CREATING = "CREATING"
    RUNNING = "RUNNING"
    UNKNOWN = "UNKNOWN"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in {self.COMPLETED, self.FAILED, self.CANCELLED}


class OperationType(str, Enum):
    CREATE = "CREATE"
    VERIFY_CREATION = "VERIFY_CREATION"
    INSPECT = "INSPECT"
    CONTINUE = "CONTINUE"
    VERIFY_CONTINUE = "VERIFY_CONTINUE"
    CANCEL = "CANCEL"
    DELETE = "DELETE"
    PROJECT_LIST = "PROJECT_LIST"
    RECOVERY_PROBE = "RECOVERY_PROBE"


class OperationState(str, Enum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in {self.SUCCEEDED, self.FAILED, self.CANCELLED}


class Lane(str, Enum):
    READ = "conversation_read"
    HEAVY = "heavy"
    METADATA = "metadata"
    CLEANUP = "cleanup"


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"
    PACED = "PACED"


class Priority(IntEnum):
    RECOVERY_PROBE = 10
    RECONCILIATION = 20
    CANCEL = 30
    CONTINUE = 40
    CREATE = 50
    COMPLETION_INSPECTION = 60
    INSPECTION = 70
    METADATA = 80


@dataclass(frozen=True)
class TurnSnapshot:
    message_id: str
    role: str
    status: str
    text: str = ""
    end_turn: Optional[bool] = None
    created_at: Optional[float] = None


@dataclass(frozen=True)
class ThreadSnapshot:
    conversation_id: str
    found: bool
    running: bool
    turns: Sequence[TurnSnapshot]
    title: str = ""
    current_node: str = ""


@dataclass(frozen=True)
class MutationResult:
    accepted: bool
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None
    running: Optional[bool] = None
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class AgentRecord:
    id: str
    project_id: str
    conversation_id: Optional[str]
    title: str
    completion_marker: str
    state: AgentState
    snapshot_hash: Optional[str]
    unchanged_count: int
    last_inspected_at: Optional[float]
    next_inspection_at: Optional[float]
    created_at: float
    updated_at: float
    completed_at: Optional[float]
    deleted_at: Optional[float]
    last_error: Optional[str]


@dataclass(frozen=True)
class OperationRecord:
    id: str
    agent_id: Optional[str]
    type: OperationType
    lane: Lane
    priority: int
    state: OperationState
    idempotency_key: str
    coalesce_key: Optional[str]
    payload: Mapping[str, Any]
    due_at: float
    attempts: int
    max_attempts: int
    claim_token: Optional[str]
    claim_expires_at: Optional[float]
    created_at: float
    updated_at: float
    last_error: Optional[str]
    result: Mapping[str, Any] | None


@dataclass(frozen=True)
class CircuitRecord:
    scope: str
    state: CircuitState
    opened_at: Optional[float]
    retry_at: Optional[float]
    probe_failures: int
    half_open_successes: int
    last_success_at: Optional[float]
    paced_started_at: Optional[float]
    last_attempt_at: Optional[float]
    updated_at: float


@dataclass(frozen=True)
class Reservation:
    operation: OperationRecord
    request_event_id: int
    claim_token: str
    circuit_scope: str


@dataclass(frozen=True)
class TickResult:
    status: str
    selected_operation: Optional[str]
    operation_type: Optional[str]
    lane: Optional[str]
    physical_requests: int
    circuit_state: str
    next_eligible_at: Optional[float]
    agent_id: Optional[str] = None
    outcome: Optional[str] = None
    error: Optional[str] = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "selected_operation": self.selected_operation,
            "operation_type": self.operation_type,
            "lane": self.lane,
            "physical_requests": self.physical_requests,
            "circuit_state": self.circuit_state,
            "next_eligible_at": self.next_eligible_at,
            "agent_id": self.agent_id,
            "outcome": self.outcome,
            "error": self.error,
        }
