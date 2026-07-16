from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ferricstore.errors import EffectAlreadyReservedError
from ferricstore.types import EffectResult


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


def resolve_effect_replay(
    reservation: EffectResult,
    replay: Callable[[EffectResult], Any] | None,
) -> tuple[bool, Any]:
    """Resolve an existing durable reservation without calling the effect again."""
    if reservation.decision != "already_reserved":
        return False, None
    if replay is None:
        raise EffectAlreadyReservedError(reservation)
    return True, replay(reservation)


__all__ = ["resolve_effect_replay", "resolve_external_id", "resolve_operation_digest"]
