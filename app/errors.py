"""Unified gateway error taxonomy.

All failures that can escape the integration layer are normalized into
:class:`GatewayError` subclasses.  Callers never see raw httpx/asyncio
exceptions; each error carries a machine-readable ``kind`` and an HTTP
status code suitable for the outward-facing response.
"""

from __future__ import annotations

import enum


class ErrorKind(str, enum.Enum):
    """Machine-readable failure categories exposed to callers."""

    TIMEOUT = "upstream_timeout"
    CONNECTION = "upstream_connection_error"
    UPSTREAM_CLIENT_ERROR = "upstream_client_error"  # upstream 4xx
    UPSTREAM_SERVER_ERROR = "upstream_server_error"  # upstream 5xx
    UPSTREAM_BAD_RESPONSE = "upstream_bad_response"  # undecodable body
    CIRCUIT_OPEN = "circuit_open"
    RATE_LIMITED = "rate_limited"
    RETRIES_EXHAUSTED = "retries_exhausted"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    UNSUPPORTED_VERSION = "unsupported_version"
    MISSING_FIELD = "missing_field"


class GatewayError(Exception):
    """Base class for all normalized gateway errors."""

    kind: ErrorKind = ErrorKind.UPSTREAM_SERVER_ERROR
    http_status: int = 502

    def __init__(self, message: str = "", *, detail: dict | None = None) -> None:
        super().__init__(message or self.kind.value)
        self.message = message or self.kind.value
        self.detail = detail or {}

    def to_payload(self) -> dict:
        """Serialize to the outward-facing error response body."""
        return {
            "error": {
                "kind": self.kind.value,
                "message": self.message,
                "detail": self.detail,
            }
        }


class UpstreamTimeoutError(GatewayError):
    kind = ErrorKind.TIMEOUT
    http_status = 504


class UpstreamConnectionError(GatewayError):
    kind = ErrorKind.CONNECTION
    http_status = 502


class UpstreamClientError(GatewayError):
    """Upstream rejected the request with a 4xx."""

    kind = ErrorKind.UPSTREAM_CLIENT_ERROR
    http_status = 502

    def __init__(self, message: str = "", *, upstream_status: int = 0, detail: dict | None = None) -> None:
        super().__init__(message, detail=detail)
        self.upstream_status = upstream_status


class UpstreamServerError(GatewayError):
    """Upstream failed with a 5xx."""

    kind = ErrorKind.UPSTREAM_SERVER_ERROR
    http_status = 502

    def __init__(self, message: str = "", *, upstream_status: int = 0, detail: dict | None = None) -> None:
        super().__init__(message, detail=detail)
        self.upstream_status = upstream_status


class UpstreamBadResponseError(GatewayError):
    kind = ErrorKind.UPSTREAM_BAD_RESPONSE
    http_status = 502


class CircuitOpenError(GatewayError):
    kind = ErrorKind.CIRCUIT_OPEN
    http_status = 503


class RateLimitedError(GatewayError):
    kind = ErrorKind.RATE_LIMITED
    http_status = 429


class RetriesExhaustedError(GatewayError):
    """All retry attempts failed with transient errors."""

    kind = ErrorKind.RETRIES_EXHAUSTED
    http_status = 502

    def __init__(self, message: str = "", *, last_error: GatewayError | None = None, attempts: int = 0) -> None:
        super().__init__(message, detail={"attempts": attempts, "last_error": last_error.kind.value if last_error else None})
        self.last_error = last_error
        self.attempts = attempts


class IdempotencyConflictError(GatewayError):
    """Same idempotency key was reused with a different payload."""

    kind = ErrorKind.IDEMPOTENCY_CONFLICT
    http_status = 409


class UnsupportedVersionError(GatewayError):
    kind = ErrorKind.UNSUPPORTED_VERSION
    http_status = 400


class MissingFieldError(GatewayError):
    """A required field is absent from the upstream response."""

    kind = ErrorKind.MISSING_FIELD
    http_status = 502
