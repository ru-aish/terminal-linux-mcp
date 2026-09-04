from __future__ import annotations

from typing import Optional


class GatewayError(Exception):
    """Base exception for gateway failures."""


class ConfigurationError(GatewayError):
    pass


class CapacityError(GatewayError):
    pass


class BackendError(GatewayError):
    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        transient: bool = True,
        permanent: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.transient = transient
        self.permanent = permanent


class InfrastructureError(BackendError):
    """Shared local-runtime outage that pauses all backend operations."""

    def __init__(self, message: str) -> None:
        super().__init__(message, transient=True, permanent=False)


class RateLimitError(BackendError):
    def __init__(self, message: str = "too many requests") -> None:
        super().__init__(message, status_code=429, transient=True, permanent=False)


class MalformedBackendResponse(BackendError):
    def __init__(self, message: str) -> None:
        super().__init__(message, transient=False, permanent=True)


class SubmissionUncertainError(BackendError):
    """A write may have reached the provider and must not be replayed blindly."""

    def __init__(self, message: str) -> None:
        # Retrying the scheduler operation is safe because the direct backend
        # turns journaled creates into reconciliation-only reads.
        super().__init__(message, transient=True, permanent=False)
