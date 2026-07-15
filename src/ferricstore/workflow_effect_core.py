from __future__ import annotations

from collections.abc import Callable
from typing import Any


def resolve_operation_digest(
    effect_type: str,
    effect_key: str,
    operation_digest: str | None,
    idempotency_key: str | None,
) -> str:
    if operation_digest is not None:
        return operation_digest
    if idempotency_key is not None:
        return idempotency_key
    return f"{effect_type}:{effect_key}"


def resolve_external_id(
    configured: str | Callable[[Any], str | None] | None,
    result: Any,
) -> str | None:
    if callable(configured):
        return configured(result)
    if isinstance(configured, str):
        return configured
    if isinstance(result, str):
        return result
    if isinstance(result, bytes):
        try:
            return result.decode()
        except UnicodeDecodeError:
            return None
    return None


__all__ = ["resolve_external_id", "resolve_operation_digest"]
