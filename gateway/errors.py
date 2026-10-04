"""统一的错误归一化层：把底层库异常映射为我们自己的错误类型。"""

from __future__ import annotations

import enum


class ErrorKind(enum.Enum):
    """调用方可区分的失败结论类别。"""

    TIMEOUT = "timeout"                    # 上游超时
    CONNECTION = "connection"              # 连接失败（DNS/拒绝/重置等）
    UPSTREAM_CLIENT_ERROR = "upstream_4xx" # 上游 4xx（请求方问题，一般不可重试）
    UPSTREAM_SERVER_ERROR = "upstream_5xx" # 上游 5xx（上游故障，可重试）
    CIRCUIT_OPEN = "circuit_open"          # 熔断打开，快速失败
    RATE_LIMITED = "rate_limited"          # 本地限流拒绝
    CAPACITY_FULL = "capacity_full"        # 该上游并发名额满/太忙（区别于超时、熔断）
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"  # 同幂等键换了请求内容
    VERSION_UNSUPPORTED = "version_unsupported"  # 不支持的接口版本
    REQUEST_INVALID = "request_invalid"    # 请求报文不符合该版本要求（缺字段/字段不支持）
    INVALID_JSON = "invalid_json"          # 上游返回的不是合法 JSON
    SCHEMA_MISMATCH = "schema_mismatch"    # 响应字段缺失/版本差异无法兼容
    UPSTREAM_NOT_FOUND = "upstream_not_found"  # 指定名字的上游未登记
    UNKNOWN = "unknown"                    # 兜底


class GatewayError(Exception):
    """所有对外暴露的网关错误基类。绝不把底层库异常原样透出。"""

    kind: ErrorKind = ErrorKind.UNKNOWN
    retryable: bool = False

    def __init__(self, message: str, *, detail: object = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail

    def to_dict(self) -> dict:
        return {
            "error": self.kind.value,
            "message": self.message,
            "retryable": self.retryable,
        }


class UpstreamTimeoutError(GatewayError):
    kind = ErrorKind.TIMEOUT
    retryable = True


class UpstreamConnectionError(GatewayError):
    kind = ErrorKind.CONNECTION
    retryable = True


class UpstreamClientError(GatewayError):
    """上游 4xx：请求本身有问题，重试无意义。"""

    kind = ErrorKind.UPSTREAM_CLIENT_ERROR
    retryable = False

    def __init__(self, message: str, *, status_code: int, detail: object = None) -> None:
        super().__init__(message, detail=detail)
        self.status_code = status_code


class UpstreamServerError(GatewayError):
    """上游 5xx：上游临时故障，可重试。"""

    kind = ErrorKind.UPSTREAM_SERVER_ERROR
    retryable = True

    def __init__(self, message: str, *, status_code: int, detail: object = None) -> None:
        super().__init__(message, detail=detail)
        self.status_code = status_code


class CircuitOpenError(GatewayError):
    kind = ErrorKind.CIRCUIT_OPEN
    retryable = True  # 语义上“稍后可能恢复”，但不应由调用方立即重试


class RateLimitedError(GatewayError):
    kind = ErrorKind.RATE_LIMITED
    retryable = True


class CapacityFullError(GatewayError):
    """该上游并发名额已满（隔离舱拒绝）：和超时、熔断是不同结论。"""

    kind = ErrorKind.CAPACITY_FULL
    retryable = True


class IdempotencyConflictError(GatewayError):
    """同一幂等键此前已用于不同请求内容，不能复用旧结论。"""

    kind = ErrorKind.IDEMPOTENCY_CONFLICT
    retryable = False


class VersionUnsupportedError(GatewayError):
    kind = ErrorKind.VERSION_UNSUPPORTED
    retryable = False


class RequestInvalidError(GatewayError):
    """请求报文不符合目标版本要求：缺必填字段，或带了该版本不支持的字段。"""

    kind = ErrorKind.REQUEST_INVALID
    retryable = False


class InvalidJsonError(GatewayError):
    """上游 HTTP 成功/错误响应体不是合法 JSON，区别于超时与 4xx/5xx 状态码。"""

    kind = ErrorKind.INVALID_JSON
    retryable = False


class SchemaMismatchError(GatewayError):
    kind = ErrorKind.SCHEMA_MISMATCH
    retryable = False


class UpstreamNotFoundError(GatewayError):
    """调用方指定了未登记的上游名字。"""

    kind = ErrorKind.UPSTREAM_NOT_FOUND
    retryable = False


def map_transport_error(exc: Exception) -> GatewayError:
    """把 httpx 等底层异常归一成 GatewayError，绝不原样透出底层异常。

    映射规则：
    - httpx.TimeoutException（含 ConnectTimeout/ReadTimeout 等）-> 超时
    - httpx.TransportError（连接拒绝、DNS、网络不可达等）-> 连接失败
    - 其他未知异常 -> UNKNOWN 兜底
    """
    import httpx

    if isinstance(exc, GatewayError):
        return exc
    if isinstance(exc, httpx.TimeoutException):
        return UpstreamTimeoutError("上游调用超时", detail=exc.__class__.__name__)
    if isinstance(exc, httpx.TransportError):
        return UpstreamConnectionError("上游连接失败", detail=exc.__class__.__name__)
    return GatewayError("上游调用出现未分类异常", detail=exc.__class__.__name__)
