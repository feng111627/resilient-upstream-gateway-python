"""Upstream API version negotiation and response adaptation.

The upstream exists in two versions with different wire shapes:

* ``v1`` -- flat body: ``{"order_id", "amount"}`` ->
  ``{"status", "transaction_id"}``
* ``v2`` -- nested body: ``{"order": {"id", "amount"}, "currency"}`` ->
  ``{"result": {"status", "txId"}}``

Both are adapted to one canonical internal shape
``{"status", "transaction_id"}``.  Asking for an unknown version raises
:class:`~app.errors.UnsupportedVersionError`; a required field missing
from the upstream response raises :class:`~app.errors.MissingFieldError`
naming the field -- we never silently substitute a default.
"""

from __future__ import annotations

from typing import Any

from app.errors import MissingFieldError, UnsupportedVersionError

SUPPORTED_VERSIONS: tuple[str, ...] = ("v1", "v2")


def _require(body: dict[str, Any], path: str, version: str) -> Any:
    """Fetch a dotted path from a nested body or raise MissingFieldError."""
    current: Any = body
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise MissingFieldError(
                f"upstream {version} response is missing required field '{path}'",
                detail={"version": version, "field": path},
            )
        current = current[part]
    return current


class UpstreamAdapter:
    supported_versions: tuple[str, ...] = SUPPORTED_VERSIONS

    def check_version(self, version: str) -> None:
        if version not in self.supported_versions:
            raise UnsupportedVersionError(
                f"unsupported upstream version '{version}'",
                detail={"supported": list(self.supported_versions)},
            )

    def endpoint(self, version: str) -> str:
        self.check_version(version)
        return f"/{version}/orders"

    def build_request(self, version: str, order_id: str, amount: float) -> dict:
        self.check_version(version)
        if version == "v1":
            return {"order_id": order_id, "amount": amount}
        return {"order": {"id": order_id, "amount": amount}, "currency": "CNY"}

    def parse_response(self, version: str, body: dict[str, Any]) -> dict[str, Any]:
        self.check_version(version)
        if version == "v1":
            status = _require(body, "status", version)
            tx = _require(body, "transaction_id", version)
        else:
            status = _require(body, "result.status", version)
            tx = _require(body, "result.txId", version)
        return {"status": status, "transaction_id": tx}
