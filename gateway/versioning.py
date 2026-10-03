"""版本协商与响应校验：版本差异/字段缺失给出明确结论。"""

from __future__ import annotations

SUPPORTED_VERSIONS: tuple[str, ...] = ("v1", "v2")


def negotiate_version(requested: str | None) -> str:
    raise NotImplementedError


def validate_response(version: str, payload: dict) -> dict:
    raise NotImplementedError
