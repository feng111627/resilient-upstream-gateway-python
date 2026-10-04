"""弹性上游网关：多上游登记、错误归一化、重试、熔断、限流、并发隔离、幂等、版本兼容。"""

from __future__ import annotations

from .bulkhead import Bulkhead
from .circuit_breaker import CircuitBreaker, CircuitState
from .errors import (
    CapacityFullError,
    CircuitOpenError,
    ErrorKind,
    GatewayError,
    IdempotencyConflictError,
    InvalidJsonError,
    RateLimitedError,
    RequestInvalidError,
    SchemaMismatchError,
    UpstreamClientError,
    UpstreamConnectionError,
    UpstreamNotFoundError,
    UpstreamServerError,
    UpstreamTimeoutError,
    VersionUnsupportedError,
)
from .idempotency import IdempotencyStore
from .rate_limiter import TokenBucketRateLimiter
from .registry import UpstreamConfig, UpstreamRegistry
from .retry import RetryPolicy
from .versioning import SUPPORTED_VERSIONS, negotiate_version, prepare_request, validate_response

__all__ = [
    "Bulkhead",
    "CircuitBreaker",
    "CircuitState",
    "ErrorKind",
    "GatewayError",
    "CapacityFullError",
    "CircuitOpenError",
    "IdempotencyConflictError",
    "InvalidJsonError",
    "RateLimitedError",
    "RequestInvalidError",
    "SchemaMismatchError",
    "UpstreamClientError",
    "UpstreamConnectionError",
    "UpstreamNotFoundError",
    "UpstreamServerError",
    "UpstreamTimeoutError",
    "VersionUnsupportedError",
    "IdempotencyStore",
    "TokenBucketRateLimiter",
    "UpstreamConfig",
    "UpstreamRegistry",
    "RetryPolicy",
    "SUPPORTED_VERSIONS",
    "negotiate_version",
    "prepare_request",
    "validate_response",
]
