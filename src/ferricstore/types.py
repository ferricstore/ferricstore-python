from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, fields
from enum import Enum
from typing import Any


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


def _get(mapping: dict[Any, Any], key: str, default: Any = None) -> Any:
    if key in mapping:
        return mapping[key]
    raw = key.encode()
    if raw in mapping:
        return mapping[raw]
    return default


def _str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def _bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return str(value).encode()


def _int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    return int(value)


def _str_key_map(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    return {_str(key): _normalize_ref_meta(item) for key, item in value.items()}


def _normalize_ref_meta(value: Any) -> Any:
    if isinstance(value, dict):
        return {_str(key): _normalize_ref_meta(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_ref_meta(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize_ref_meta(item) for item in value)
    if isinstance(value, bytes):
        return value.decode()
    return value


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_retries: int = 3
    backoff: str = "fixed"
    base_ms: int = 100
    max_ms: int = 1_000
    jitter_pct: int = 0
    exhausted_to: str = "failed"


@dataclass(frozen=True, slots=True)
class ValueConfig:
    value_max_bytes: int | None = None
    local_cache: bool = False


@dataclass(frozen=True, slots=True)
class WorkerConfig:
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
    scan_before_blocking: bool | None = None
    complete_async_depth: int | None = None
    apply_async_depth: int | None = 0
    server_shards: int | None = None
    producer_loop_thread: bool | None = None
    empty_claim_cooldown_s: float | None = None
    partial_claim_cooldown_s: float | None = None

    def to_kwargs(self, allowed: set[str] | frozenset[str] | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {}
        for field in fields(self):
            name = field.name
            if allowed is not None and name not in allowed:
                continue
            value = getattr(self, name)
            if value is not None:
                kwargs[name] = value
        return kwargs


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

    worker_count = workers if workers is not None else concurrency
    if worker_count is None:
        worker_count = default_workers
    if worker_count <= 0:
        raise ValueError("workers/concurrency must be positive for connection sizing")

    command_count = command_connections if command_connections is not None else max(2, worker_count)
    claim_count = claim_connections if claim_connections is not None else worker_count
    if command_count <= 0:
        raise ValueError("command_connections must be positive")
    if claim_count <= 0:
        raise ValueError("claim_connections must be positive")
    return command_count, claim_count


@dataclass(frozen=True, slots=True)
class ChildSpec:
    id: str
    type: str
    payload: Any = None
    partition_key: str | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class CreateItem:
    id: str
    payload: Any = None
    partition_key: str | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None


@dataclass(frozen=True, slots=True)
class ClaimedItem:
    id: str
    lease_token: bytes
    fencing_token: int
    partition_key: str | None = None
    type: str = ""
    state: str = "running"
    run_state: str | None = None
    payload: Any = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any] | list[Any] | tuple[Any, ...]) -> ClaimedItem:
        if isinstance(value, (list, tuple)):
            return cls(
                id=_str(value[0]),
                partition_key=_optional_str(value[1]),
                lease_token=_bytes(value[2]),
                fencing_token=_int(value[3]),
                run_state=_optional_str(value[4]) if len(value) > 4 else None,
            )

        return cls(
            id=_str(_get(value, "id")),
            lease_token=_bytes(_get(value, "lease_token")),
            fencing_token=_int(_get(value, "fencing_token")),
            partition_key=_optional_str(_get(value, "partition_key")),
            type=_str(_get(value, "type")),
            state=_optional_str(_get(value, "state")) or "running",
            run_state=_optional_str(_get(value, "run_state")),
            payload=_get(value, "payload"),
        )


@dataclass(frozen=True, slots=True)
class FencedItem:
    id: str
    fencing_token: int
    lease_token: bytes | None = None
    partition_key: str | None = None


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    status: str
    count: int
    remaining: int
    reset_ms: int

    @property
    def allowed(self) -> bool:
        return self.status == "allowed"

    @classmethod
    def from_resp(cls, value: list[Any] | tuple[Any, ...]) -> RateLimitResult:
        return cls(
            status=_str(value[0]),
            count=_int(value[1]),
            remaining=_int(value[2]),
            reset_ms=_int(value[3]),
        )


@dataclass(frozen=True, slots=True)
class KeyInfo:
    type: str
    value_size: int
    ttl_ms: int
    hot_cache_status: str
    last_write_shard: int
    raw: dict[str, Any]

    @classmethod
    def from_resp(cls, value: dict[Any, Any] | list[Any] | tuple[Any, ...]) -> KeyInfo:
        if isinstance(value, dict):
            raw = {_str(key): item for key, item in value.items()}
        else:
            raw = {}
            items = list(value)
            for idx in range(0, len(items) - 1, 2):
                raw[_str(items[idx])] = items[idx + 1]

        return cls(
            type=_str(raw.get("type")),
            value_size=_int(raw.get("value_size")),
            ttl_ms=_int(raw.get("ttl_ms")),
            hot_cache_status=_str(raw.get("hot_cache_status")),
            last_write_shard=_int(raw.get("last_write_shard")),
            raw=raw,
        )


@dataclass(frozen=True, slots=True)
class FetchOrComputeResult:
    status: str
    value: Any = None
    compute_token: bytes = b""

    @property
    def hit(self) -> bool:
        return self.status == "hit"

    @property
    def should_compute(self) -> bool:
        return self.status == "compute"


@dataclass(frozen=True, slots=True)
class FlowRecord:
    id: str
    type: str
    state: str
    partition_key: str
    run_state: str | None = None
    payload: Any = None
    lease_token: bytes = b""
    fencing_token: int = 0
    version: int = 0
    parent_flow_id: str | None = None
    root_flow_id: str | None = None
    correlation_id: str | None = None
    value_refs: dict[str, Any] | None = None
    values: dict[str, Any] | None = None
    value_sizes: dict[str, Any] | None = None
    value_omitted: dict[str, Any] | None = None
    value_missing: dict[str, Any] | None = None
    raw: dict[Any, Any] | None = None

    @classmethod
    def from_resp(
        cls,
        value: dict[Any, Any],
        payload: Any = None,
        values: dict[str, Any] | None = None,
    ) -> FlowRecord:
        return cls(
            id=_str(_get(value, "id")),
            type=_str(_get(value, "type")),
            state=_str(_get(value, "state")),
            partition_key=_str(_get(value, "partition_key")),
            run_state=_optional_str(_get(value, "run_state")),
            payload=payload,
            lease_token=_bytes(_get(value, "lease_token")),
            fencing_token=_int(_get(value, "fencing_token")),
            version=_int(_get(value, "version")),
            parent_flow_id=_optional_str(_get(value, "parent_flow_id")),
            root_flow_id=_optional_str(_get(value, "root_flow_id")),
            correlation_id=_optional_str(_get(value, "correlation_id")),
            value_refs=_str_key_map(_get(value, "value_refs")),
            values=values,
            value_sizes=_str_key_map(_get(value, "value_sizes")),
            value_omitted=_str_key_map(_get(value, "value_omitted")),
            value_missing=_str_key_map(_get(value, "value_missing")),
            raw=value,
        )


def _optional_str(value: Any) -> str | None:
    if value is None or value == b"" or value == "":
        return None
    return _str(value)
