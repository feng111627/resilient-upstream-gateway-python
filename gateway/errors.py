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
    VERSION_UNSUPPORTED = "version_unsupported"  # 不支持的接口版本
    SCHEMA_MISMATCH = "schema_mismatch"    # 响应字段缺失/版本差异无法兼容
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


class VersionUnsupportedError(GatewayError):
    kind = ErrorKind.VERSION_UNSUPPORTED
    retryable = False


class SchemaMismatchError(GatewayError):
    kind = ErrorKind.SCHEMA_MISMATCH
    retryable = False


def map_transport_error(exc: Exception) -> GatewayError:
    """把 httpx 等底层异常归一成 GatewayError。占位实现。"""
    raise NotImplementedError
