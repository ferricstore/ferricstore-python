from __future__ import annotations

from dataclasses import dataclass

from ferricstore.config_validation import validate_bounded_nonnegative_int

_MAX_RETRIES = 1_000
_MAX_DELAY_MS = 2_592_000_000
_BACKOFF_KINDS = frozenset({"none", "fixed", "linear", "exponential"})


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Validated client representation of the FerricFlow retry policy."""

    max_retries: int = 3
    backoff: str = "fixed"
    base_ms: int = 100
    max_ms: int = 1_000
    jitter_pct: int = 0
    exhausted_to: str = "failed"

    def __post_init__(self) -> None:
        validate_bounded_nonnegative_int(
            self.max_retries,
            name="max_retries",
            maximum=_MAX_RETRIES,
        )
        if self.backoff not in _BACKOFF_KINDS:
            raise ValueError("backoff must be one of: none, fixed, linear, exponential")
        validate_bounded_nonnegative_int(
            self.base_ms,
            name="base_ms",
            maximum=_MAX_DELAY_MS,
        )
        validate_bounded_nonnegative_int(
            self.max_ms,
            name="max_ms",
            maximum=_MAX_DELAY_MS,
        )
        validate_bounded_nonnegative_int(
            self.jitter_pct,
            name="jitter_pct",
            maximum=100,
        )
        if not isinstance(self.exhausted_to, str) or not self.exhausted_to:
            raise ValueError("exhausted_to must be a non-empty string")
        if self.exhausted_to == "running":
            raise ValueError("exhausted_to cannot be running")


__all__ = ["RetryPolicy"]
