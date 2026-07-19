from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ferricstore.config_validation import (
    validate_bool,
    validate_bounded_nonnegative_int,
    validate_finite_nonnegative,
    validate_nonnegative_int,
    validate_optional_bool,
    validate_optional_flow_priority,
    validate_optional_nonnegative_int,
    validate_optional_positive_int,
    validate_partition_key_sequence,
    validate_positive_int,
    validate_string_sequence,
)
from ferricstore.types import ExceptionPolicy, normalize_exception_policy
from ferricstore.worker_core import validate_worker_idle_timing


@dataclass(frozen=True, slots=True)
class QueueWorkerRuntimeConfig:
    """Validated configuration shared by sync and async queue workers."""

    states: list[str] | None
    partition_keys: list[str | bytes] | None
    concurrency: int
    batch_size: int
    workers: int
    claim_partition_batch_size: int | None
    block_ms: int | None
    idle_sleep_s: float
    max_idle_sleep_s: float
    on_error: str
    empty_claim_cooldown_s: float | None
    partial_claim_cooldown_s: float | None
    lease_ms: int
    priority: int | None
    reclaim_expired: bool | None
    reclaim_ratio: int | None
    claim_values: list[str] | None
    value_max_bytes: int | None
    complete_independent: bool
    protocol_wake_hints: bool
    scan_before_blocking: bool
    fuse_complete_claim: bool
    auto_partitions: bool
    close_client: bool | None

    @classmethod
    def build(
        cls,
        *,
        state: str | None,
        states: Sequence[str] | None,
        partition_key: str | bytes | None,
        partition_keys: Sequence[str | bytes] | None,
        concurrency: int,
        batch_size: int,
        workers: int,
        claim_partition_batch_size: int | None,
        block_ms: int | None,
        idle_sleep_s: float,
        max_idle_sleep_s: float | None,
        exception_policy: ExceptionPolicy | str | None,
        on_error: ExceptionPolicy | str | None,
        empty_claim_cooldown_s: float | None = None,
        partial_claim_cooldown_s: float | None = None,
        lease_ms: int = 30_000,
        priority: int | None = 0,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        complete_independent: bool = True,
        protocol_wake_hints: bool = False,
        scan_before_blocking: bool = False,
        fuse_complete_claim: bool = False,
        auto_partitions: bool = False,
        close_client: bool | None = None,
    ) -> QueueWorkerRuntimeConfig:
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")
        resolved_states = (
            list(validate_string_sequence(states, name="states", allow_empty=False))
            if states is not None
            else None
        )
        resolved_partition_keys = (
            list(validate_partition_key_sequence(partition_keys, allow_empty=False))
            if partition_keys is not None
            else None
        )
        concurrency = validate_positive_int(concurrency, name="concurrency")
        batch_size = validate_positive_int(batch_size, name="batch_size")
        workers = validate_positive_int(workers, name="workers")
        claim_partition_batch_size = validate_optional_positive_int(
            claim_partition_batch_size,
            name="claim_partition_batch_size",
        )
        if block_ms is not None:
            block_ms = validate_nonnegative_int(block_ms, name="block_ms")
        minimum_idle, maximum_idle = validate_worker_idle_timing(
            idle_sleep_s,
            max_idle_sleep_s,
        )
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_error = normalize_exception_policy(
            exception_policy if exception_policy is not None else on_error,
            argument="exception_policy" if exception_policy is not None else "on_error",
        )
        if empty_claim_cooldown_s is not None:
            empty_claim_cooldown_s = validate_finite_nonnegative(
                empty_claim_cooldown_s,
                name="empty_claim_cooldown_s",
            )
        if partial_claim_cooldown_s is not None:
            partial_claim_cooldown_s = validate_finite_nonnegative(
                partial_claim_cooldown_s,
                name="partial_claim_cooldown_s",
            )
        lease_ms = validate_positive_int(lease_ms, name="lease_ms")
        priority = validate_optional_flow_priority(priority)
        reclaim_expired = validate_optional_bool(reclaim_expired, name="reclaim_expired")
        if reclaim_ratio is not None:
            reclaim_ratio = validate_bounded_nonnegative_int(
                reclaim_ratio,
                name="reclaim_ratio",
                maximum=100,
            )
        resolved_claim_values = (
            list(validate_string_sequence(claim_values, name="claim_values"))
            if claim_values is not None
            else None
        )
        value_max_bytes = validate_optional_nonnegative_int(
            value_max_bytes,
            name="value_max_bytes",
        )
        complete_independent = validate_bool(
            complete_independent,
            name="complete_independent",
        )
        protocol_wake_hints = validate_bool(
            protocol_wake_hints,
            name="protocol_wake_hints",
        )
        scan_before_blocking = validate_bool(
            scan_before_blocking,
            name="scan_before_blocking",
        )
        fuse_complete_claim = validate_bool(
            fuse_complete_claim,
            name="fuse_complete_claim",
        )
        auto_partitions = validate_bool(auto_partitions, name="auto_partitions")
        close_client = validate_optional_bool(close_client, name="close_client")
        return cls(
            states=resolved_states,
            partition_keys=resolved_partition_keys,
            concurrency=concurrency,
            batch_size=batch_size,
            workers=workers,
            claim_partition_batch_size=claim_partition_batch_size,
            block_ms=block_ms,
            idle_sleep_s=minimum_idle,
            max_idle_sleep_s=maximum_idle,
            on_error=resolved_error,
            empty_claim_cooldown_s=empty_claim_cooldown_s,
            partial_claim_cooldown_s=partial_claim_cooldown_s,
            lease_ms=lease_ms,
            priority=priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            claim_values=resolved_claim_values,
            value_max_bytes=value_max_bytes,
            complete_independent=complete_independent,
            protocol_wake_hints=protocol_wake_hints,
            scan_before_blocking=scan_before_blocking,
            fuse_complete_claim=fuse_complete_claim,
            auto_partitions=auto_partitions,
            close_client=close_client,
        )
