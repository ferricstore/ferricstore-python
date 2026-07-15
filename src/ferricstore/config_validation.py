from __future__ import annotations

import math
import threading
from collections.abc import Sequence
from numbers import Real


def validate_positive_int(value: object, *, name: str) -> int:
    """Return a positive integer without accepting bools or lossy coercions."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be a positive integer")
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def validate_nonnegative_int(value: object, *, name: str) -> int:
    """Return a non-negative integer without accepting bools or coercions."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be a non-negative integer")
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def validate_optional_positive_int(value: object | None, *, name: str) -> int | None:
    if value is None:
        return None
    return validate_positive_int(value, name=name)


def validate_optional_nonnegative_int(value: object | None, *, name: str) -> int | None:
    if value is None:
        return None
    return validate_nonnegative_int(value, name=name)


def validate_optional_flow_priority(value: object | None, *, name: str = "priority") -> int | None:
    """Validate the finite priority range supported by FerricStore's indexes."""
    if value is None:
        return None
    return validate_bounded_nonnegative_int(value, name=name, maximum=2)


def validate_optional_bool(value: object | None, *, name: str) -> bool | None:
    """Return an optional boolean without silently coercing truthy values."""
    if value is not None and not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def validate_bool(value: object, *, name: str) -> bool:
    """Return a boolean without accepting integers or truthy objects."""
    if not isinstance(value, bool):
        raise ValueError(f"{name} must be a boolean")
    return value


def validate_bounded_positive_int(
    value: object,
    *,
    name: str,
    maximum: int,
) -> int:
    """Return a positive integer that fits the caller's finite topology."""
    validated = validate_positive_int(value, name=name)
    if validated > maximum:
        raise ValueError(f"{name} cannot exceed {maximum}")
    return validated


def validate_bounded_nonnegative_int(
    value: object,
    *,
    name: str,
    maximum: int,
) -> int:
    """Return a non-negative integer no greater than a protocol limit."""
    validated = validate_nonnegative_int(value, name=name)
    if validated > maximum:
        raise ValueError(f"{name} cannot exceed {maximum}")
    return validated


def validate_string_sequence(
    value: object,
    *,
    name: str,
    allow_empty: bool = True,
) -> tuple[str, ...]:
    """Validate collection-valued string options without splitting scalar strings."""
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise ValueError(f"{name} must be a sequence of strings")
    items = tuple(value)
    if not allow_empty and not items:
        raise ValueError(f"{name} must be non-empty")
    if any(not isinstance(item, str) or not item for item in items):
        raise ValueError(f"{name} must contain only non-empty strings")
    return items


def validate_host(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("host must be a non-empty string")
    return value


def validate_port(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65_535:
        raise ValueError("port must be between 1 and 65535")
    return value


def validate_finite_nonnegative(value: object, *, name: str) -> float:
    """Return a finite non-negative float or reject unsafe timing input."""
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be non-negative and finite")
    try:
        normalized = float(value)
    except (OverflowError, TypeError, ValueError):
        raise ValueError(f"{name} must be non-negative and finite") from None
    if normalized < 0 or not math.isfinite(normalized):
        raise ValueError(f"{name} must be non-negative and finite")
    return normalized


def validate_thread_wait_seconds(value: object, *, name: str) -> float:
    """Return a duration that every supported threading wait can represent."""
    normalized = validate_finite_nonnegative(value, name=name)
    if normalized > threading.TIMEOUT_MAX:
        raise ValueError(f"{name} exceeds platform wait limit")
    return normalized


def validate_optional_thread_wait_seconds(
    value: object | None,
    *,
    name: str,
) -> float | None:
    if value is None:
        return None
    return validate_thread_wait_seconds(value, name=name)


def validate_thread_wait_milliseconds(value: object, *, name: str) -> float:
    normalized = validate_finite_nonnegative(value, name=name)
    validate_thread_wait_seconds(normalized / 1000.0, name=name)
    return normalized
