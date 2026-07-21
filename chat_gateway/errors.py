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


class RateLimitError(BackendError):
    def __init__(self, message: str = "too many requests") -> None:
        super().__init__(message, status_code=429, transient=True, permanent=False)


class MalformedBackendResponse(BackendError):
    def __init__(self, message: str) -> None:
        super().__init__(message, transient=False, permanent=True)
