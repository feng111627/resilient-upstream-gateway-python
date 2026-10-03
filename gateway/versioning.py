"""版本协商与响应校验：版本差异/字段缺失给出明确结论，绝不默默取值。

规则：
- 支持 v1、v2 两个上游接口版本；不支持的版本直接报 VERSION_UNSUPPORTED。
- 每个版本声明自己的必填字段；响应缺字段报 SCHEMA_MISMATCH 并列出缺失字段。
- 归一化输出显式携带 version；v1 没有的字段（如 currency）不会出现在输出里，
  调用方不会被塞一个来路不明的默认值。
"""

from __future__ import annotations

from .errors import SchemaMismatchError, VersionUnsupportedError

SUPPORTED_VERSIONS: tuple[str, ...] = ("v1", "v2")
DEFAULT_VERSION = "v2"

# 各版本响应的必填字段
SCHEMAS: dict[str, frozenset[str]] = {
    "v1": frozenset({"id", "status", "amount"}),
    "v2": frozenset({"id", "status", "amount", "currency"}),
}

# 归一化输出中各版本允许携带的字段（必填 + 可选）
_OPTIONAL_FIELDS: dict[str, frozenset[str]] = {
    "v1": frozenset(),
    "v2": frozenset({"memo"}),
}


def negotiate_version(requested: str | None) -> str:
    """协商接口版本：未指定用默认版本；不支持的版本给出明确错误。"""
    if requested is None:
        return DEFAULT_VERSION
    if requested not in SUPPORTED_VERSIONS:
        raise VersionUnsupportedError(
            f"不支持的接口版本 {requested!r}，支持: {', '.join(SUPPORTED_VERSIONS)}"
        )
    return requested


def validate_response(version: str, payload: dict) -> dict:
    """校验并归一化上游响应。缺字段抛 SchemaMismatchError，不默默补默认值。"""
    if version not in SCHEMAS:
        raise VersionUnsupportedError(
            f"无法校验未知版本 {version!r} 的响应，支持: {', '.join(SUPPORTED_VERSIONS)}"
        )
    if not isinstance(payload, dict):
        raise SchemaMismatchError(f"上游响应不是 JSON 对象: {type(payload).__name__}")

    required = SCHEMAS[version]
    missing = sorted(f for f in required if f not in payload or payload[f] is None)
    if missing:
        raise SchemaMismatchError(
            f"上游 {version} 响应缺少必填字段: {', '.join(missing)}",
            detail={"version": version, "missing": missing},
        )

    allowed = required | _OPTIONAL_FIELDS[version]
    normalized = {k: payload[k] for k in sorted(allowed) if k in payload}
    normalized["version"] = version
    return normalized
