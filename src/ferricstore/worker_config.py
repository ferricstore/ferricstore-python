from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields
from enum import Enum
from typing import Any

from ferricstore.config_validation import (
    validate_bool,
    validate_bounded_nonnegative_int,
    validate_bounded_positive_int,
    validate_finite_nonnegative,
    validate_optional_bool,
    validate_optional_flow_priority,
    validate_optional_nonnegative_int,
    validate_optional_positive_int,
    validate_positive_int,
    validate_string_sequence,
)

_SERVER_SHARD_LIMIT = 1024


class ExceptionPolicy(str, Enum):
    """Policy for unexpected Python handler exceptions."""

    RETRY = "retry"
    FAIL = "fail"
    RAISE = "raise"


_EXCEPTION_POLICY_VALUES = {policy.value for policy in ExceptionPolicy}


def normalize_exception_policy(
    value: ExceptionPolicy | str | None,
    *,
    argument: str = "exception_policy",
) -> str:
    if value is None:
        return ExceptionPolicy.RETRY.value
    if isinstance(value, ExceptionPolicy):
        return value.value
    if isinstance(value, str) and value in _EXCEPTION_POLICY_VALUES:
        return value
    raise ValueError(
        f"{argument} must be ExceptionPolicy.RETRY, ExceptionPolicy.FAIL, "
        "ExceptionPolicy.RAISE, or 'retry', 'fail', 'raise'"
    )


@dataclass(frozen=True, slots=True)
class ValueConfig:
    """Named-value hydration defaults for queue/workflow handlers.

    Keep ``local_cache`` disabled unless a handler reads the same named value
    repeatedly. Large values should stay explicit and capped with
    ``value_max_bytes``.
    """

    value_max_bytes: int | None = None
    local_cache: bool = False

    def __post_init__(self) -> None:
        validate_optional_nonnegative_int(
            self.value_max_bytes,
            name="value_max_bytes",
        )
        validate_bool(self.local_cache, name="local_cache")


_OPTIONAL_POSITIVE_FIELDS = (
    "workers",
    "concurrency",
    "command_connections",
    "claim_connections",
    "batch_size",
    "lease_ms",
    "claim_partition_batch_size",
    "claim_drain_batches",
)
_OPTIONAL_NONNEGATIVE_FIELDS = (
    "value_max_bytes",
    "block_ms",
    "claim_scan_block_ms",
    "claim_prefetch",
    "complete_async_depth",
    "apply_async_depth",
)
_OPTIONAL_BOOLEAN_FIELDS = (
    "reclaim_expired",
    "complete_independent",
    "protocol_wake_hints",
    "scan_before_blocking",
    "fuse_complete_claim",
    "producer_loop_thread",
)
_OPTIONAL_DURATION_FIELDS = (
    "idle_sleep_s",
    "max_idle_sleep_s",
    "empty_claim_cooldown_s",
    "partial_claim_cooldown_s",
)


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Validated runtime defaults for queue and workflow workers.

    The normal knobs are ``concurrency``, ``batch_size``, ``lease_ms``,
    retry/exception policy, and named-value hydration. ``command_connections``
    and ``claim_connections`` are advanced transport overrides.
    """

    workers: int | None = None
    concurrency: int | None = None
    command_connections: int | None = None
    claim_connections: int | None = None
    batch_size: int | None = 10
    lease_ms: int | None = None
    priority: int | None = None
    reclaim_expired: bool | None = None
    reclaim_ratio: int | None = None
    claim_values: Sequence[str] | None = None
    value_max_bytes: int | None = None
    block_ms: int | None = None
    claim_scan_block_ms: int | None = None
    idle_sleep_s: float | None = None
    max_idle_sleep_s: float | None = None
    exception_policy: ExceptionPolicy | str | None = None
    complete_independent: bool | None = None
    claim_partition_batch_size: int | None = 1
    claim_drain_batches: int | None = None
    claim_prefetch: int | None = None
    protocol_wake_hints: bool | None = None
    scan_before_blocking: bool | None = None
    complete_async_depth: int | None = None
    fuse_complete_claim: bool | None = None
    apply_async_depth: int | None = 0
    server_shards: int | None = None
    producer_loop_thread: bool | None = None
    empty_claim_cooldown_s: float | None = None
    partial_claim_cooldown_s: float | None = None

    def __post_init__(self) -> None:
        for name in _OPTIONAL_POSITIVE_FIELDS:
            validate_optional_positive_int(getattr(self, name), name=name)
        for name in _OPTIONAL_NONNEGATIVE_FIELDS:
            validate_optional_nonnegative_int(getattr(self, name), name=name)
        for name in _OPTIONAL_BOOLEAN_FIELDS:
            validate_optional_bool(getattr(self, name), name=name)
        for name in _OPTIONAL_DURATION_FIELDS:
            value = getattr(self, name)
            if value is not None:
                validate_finite_nonnegative(value, name=name)
        validate_optional_flow_priority(self.priority)
        if self.reclaim_ratio is not None:
            validate_bounded_nonnegative_int(
                self.reclaim_ratio,
                name="reclaim_ratio",
                maximum=100,
            )
        if self.claim_values is not None:
            values = validate_string_sequence(self.claim_values, name="claim_values")
            object.__setattr__(self, "claim_values", values)
        if self.exception_policy is not None:
            normalize_exception_policy(self.exception_policy)
        if self.server_shards is not None:
            validate_bounded_positive_int(
                self.server_shards,
                name="server_shards",
                maximum=_SERVER_SHARD_LIMIT,
            )

    def to_kwargs(self, allowed: set[str] | frozenset[str] | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        for name in _WORKER_CONFIG_FIELD_NAMES:
            if allowed is not None and name not in allowed:
                continue
            value = getattr(self, name)
            if value is not None:
                kwargs[name] = value
        return kwargs


_WORKER_CONFIG_FIELD_NAMES = tuple(field.name for field in fields(WorkerConfig))


def resolve_worker_connection_counts(
    *,
    workers: int | None = None,
    concurrency: int | None = None,
    command_connections: int | None = None,
    claim_connections: int | None = None,
    worker_config: WorkerConfig | None = None,
    default_workers: int = 1,
) -> tuple[int, int]:
    """Return bounded command/claim pool sizes for blocking worker clients."""

    if worker_config is not None:
        workers = workers if workers is not None else worker_config.workers
        concurrency = concurrency if concurrency is not None else worker_config.concurrency
        command_connections = (
            command_connections
            if command_connections is not None
            else worker_config.command_connections
        )
        claim_connections = (
            claim_connections if claim_connections is not None else worker_config.claim_connections
        )

    if workers is not None:
        worker_count = validate_positive_int(workers, name="workers")
    elif concurrency is not None:
        worker_count = validate_positive_int(concurrency, name="concurrency")
    else:
        worker_count = validate_positive_int(default_workers, name="default_workers")

    command_count = command_connections if command_connections is not None else max(2, worker_count)
    claim_count = claim_connections if claim_connections is not None else worker_count
    validated_command = validate_positive_int(
        command_count,
        name="command_connections",
    )
    validated_claim = validate_positive_int(claim_count, name="claim_connections")
    return validated_command, validated_claim


__all__ = [
    "ExceptionPolicy",
    "ValueConfig",
    "WorkerConfig",
    "normalize_exception_policy",
    "resolve_worker_connection_counts",
]
