"""版本协商、请求报文适配与响应校验：版本差异/字段问题都给明确结论，绝不默默处理。

规则：
- 支持 v1、v2 两个上游接口版本；不支持的版本直接报 VERSION_UNSUPPORTED。
- 请求侧：每个版本声明自己接受的字段（必填/可选）。
  * 缺必填字段 -> REQUEST_INVALID 并列出缺失字段；
  * 带了该版本不支持的字段 -> REQUEST_INVALID 并列出不支持字段（不静默丢弃）；
  * 报文按各版本的线上格式（wire format）组装后才发给上游。
- 响应侧：每个版本声明必填字段；缺字段报 SCHEMA_MISMATCH 并列出缺失字段。
- 归一化输出显式携带 version；v1 没有的字段（如 currency）不会出现在输出里，
  调用方不会被塞一个来路不明的默认值。
"""

from __future__ import annotations

from typing import Any

from .errors import RequestInvalidError, SchemaMismatchError, VersionUnsupportedError

SUPPORTED_VERSIONS: tuple[str, ...] = ("v1", "v2")
DEFAULT_VERSION = "v2"

# ---- 请求侧：调用方语义字段 -> 各版本约束 ----

REQUEST_FIELDS: dict[str, dict[str, frozenset[str]]] = {
    # v1：旧版要求回调地址，没有币种概念
    "v1": {
        "required": frozenset({"id", "amount", "callback_url"}),
        "optional": frozenset(),
    },
    # v2：引入币种与备注，回调地址被废弃
    "v2": {
        "required": frozenset({"id", "amount", "currency"}),
        "optional": frozenset({"note"}),
    },
}

# ---- 响应侧：各版本必填字段与可选字段 ----

SCHEMAS: dict[str, frozenset[str]] = {
    "v1": frozenset({"id", "status", "amount"}),
    "v2": frozenset({"id", "status", "amount", "currency"}),
}

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


def prepare_request(version: str, payload: dict | None) -> dict:
    """校验调用方请求并按目标版本组装成发往上游的报文。

    缺字段/不支持字段都抛 RequestInvalidError；两个版本的报文外壳不同：
    v1 是扁平结构 {"order_id", "amount", "callback_url"}；
    v2 是 {"payment": {...}, "note"} 的嵌套结构。
    """
    fields = REQUEST_FIELDS.get(version)
    if fields is None:
        raise VersionUnsupportedError(
            f"无法组装未知版本 {version!r} 的请求，支持: {', '.join(SUPPORTED_VERSIONS)}"
        )

    data = payload or {}
    if not isinstance(data, dict):
        raise RequestInvalidError(
            f"请求体必须是 JSON 对象: {type(data).__name__}",
            detail={"version": version},
        )

    required, optional = fields["required"], fields["optional"]
    allowed = required | optional
    missing = sorted(f for f in required if f not in data or data[f] is None)
    unknown = sorted(f for f in data if f not in allowed)
    problems: list[str] = []
    if missing:
        problems.append(f"缺少必填字段: {', '.join(missing)}")
    if unknown:
        problems.append(f"该版本不支持的字段: {', '.join(unknown)}")
    if problems:
        raise RequestInvalidError(
            f"请求不符合 {version} 要求（{'；'.join(problems)}）",
            detail={"version": version, "missing": missing, "unsupported": unknown},
        )

    return _build_wire(version, data)


def _build_wire(version: str, data: dict) -> dict:
    if version == "v1":
        # 旧版字段名与扁平外壳
        return {
            "order_id": data["id"],
            "amount": data["amount"],
            "callback_url": data["callback_url"],
        }
    # v2：嵌套外壳，备注改名 memo
    wire: dict[str, Any] = {
        "payment": {
            "id": data["id"],
            "amount": data["amount"],
            "currency": data["currency"],
        }
    }
    if "note" in data:
        wire["note"] = data["note"]
    return wire


def validate_response(version: str, payload: object) -> dict:
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
