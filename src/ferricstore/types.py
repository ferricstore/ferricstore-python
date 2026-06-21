from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
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


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
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
class BudgetPolicy:
    """Workflow-state budget reservation policy.

    ``scope`` may be a string or a callable receiving the workflow context.
    Automatic enforcement reserves ``amount`` before the handler runs, commits
    actual usage on success, and releases unused reservation on failure.
    """

    scope: str | Callable[[Any], str]
    amount: int
    limit: int | None = None
    window_ms: int | None = None
    usage_key: str = "amount"
    attribute_prefix: str = "governance_budget"


@dataclass(frozen=True, slots=True)
class BudgetResult:
    """Typed response for Flow governance budget commands.

    The object is intentionally mapping-compatible for existing code that used
    raw dict responses, for example ``result["reservation_id"]``.
    """

    scope: str = ""
    limit: int = 0
    window_ms: int = 0
    window_start_ms: int = 0
    used: int = 0
    remaining: int = 0
    over_budget: bool = False
    reservations_count: int = 0
    reservation_id: str | None = None
    reserved_amount: int | None = None
    actual_amount: int | None = None
    status: str | None = None
    usage: dict[str, Any] | None = None
    overage_amount: int = 0
    reserved_at_ms: int | None = None
    settled_at_ms: int | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> BudgetResult:
        raw = {_str(key): _normalize_ref_meta(item) for key, item in value.items()}
        usage = raw.get("usage")
        return cls(
            scope=_str(raw.get("scope")),
            limit=_int(raw.get("limit")),
            window_ms=_int(raw.get("window_ms")),
            window_start_ms=_int(raw.get("window_start_ms")),
            used=_int(raw.get("used")),
            remaining=_int(raw.get("remaining")),
            over_budget=bool(raw.get("over_budget")),
            reservations_count=_int(raw.get("reservations_count")),
            reservation_id=_optional_str(raw.get("reservation_id")),
            reserved_amount=_optional_int(raw.get("reserved_amount")),
            actual_amount=_optional_int(raw.get("actual_amount")),
            status=_optional_str(raw.get("status")),
            usage=usage if isinstance(usage, dict) else None,
            overage_amount=_int(raw.get("overage_amount")),
            reserved_at_ms=_optional_int(raw.get("reserved_at_ms")),
            settled_at_ms=_optional_int(raw.get("settled_at_ms")),
            raw=raw,
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "scope": self.scope,
            "limit": self.limit,
            "window_ms": self.window_ms,
            "window_start_ms": self.window_start_ms,
            "used": self.used,
            "remaining": self.remaining,
            "over_budget": self.over_budget,
            "reservations_count": self.reservations_count,
            "overage_amount": self.overage_amount,
        }
        optional = {
            "reservation_id": self.reservation_id,
            "reserved_amount": self.reserved_amount,
            "actual_amount": self.actual_amount,
            "status": self.status,
            "usage": self.usage,
            "reserved_at_ms": self.reserved_at_ms,
            "settled_at_ms": self.settled_at_ms,
        }
        data.update({key: value for key, value in optional.items() if value is not None})
        return data

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(self.to_dict().items())


class _MappingResult:
    raw: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw or {})

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(self.to_dict().items())


def _raw_map(value: dict[Any, Any]) -> dict[str, Any]:
    return {_str(key): _normalize_ref_meta(item) for key, item in value.items()}


@dataclass(frozen=True, slots=True)
class PubSubMessage:
    """Typed native Pub/Sub event delivered by ``PubSubSession``.

    ``message`` is decoded by the client codec by default. Use
    ``get_message(decode=False)`` when the application wants the raw bytes.
    """

    kind: str
    channel: str
    message: Any
    pattern: str | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_event(
        cls,
        value: Any,
        *,
        decode: Callable[[Any], Any] | None = None,
    ) -> PubSubMessage:
        if not isinstance(value, dict):
            return cls(kind="event", channel="", message=value, raw={"event": value})

        raw = _raw_map(value)
        message = _get(value, "message")
        if decode is not None and message is not None:
            message = decode(message)

        return cls(
            kind=_str(raw.get("kind"), "message"),
            channel=_str(raw.get("channel")),
            message=message,
            pattern=_optional_str(raw.get("pattern")),
            raw=raw,
        )

    def to_dict(self) -> dict[str, Any]:
        data = {
            "kind": self.kind,
            "channel": self.channel,
            "message": self.message,
        }
        if self.pattern is not None:
            data["pattern"] = self.pattern
        return data

    def __getitem__(self, key: str) -> Any:
        return self.to_dict()[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)

    def items(self) -> Iterator[tuple[str, Any]]:
        return iter(self.to_dict().items())


@dataclass(frozen=True, slots=True)
class EffectResult(_MappingResult):
    """Typed response for Flow effect governance commands."""

    id: str = ""
    flow_id: str = ""
    effect_key: str = ""
    effect_type: str = ""
    status: str = ""
    decision: str | None = None
    scope: str | None = None
    external_id: str | None = None
    error: str | None = None
    reason: str | None = None
    operation_digest: str | None = None
    idempotency_key: str | None = None
    reserved_at_ms: int | None = None
    confirmed_at_ms: int | None = None
    failed_at_ms: int | None = None
    compensated_at_ms: int | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> EffectResult:
        raw = _raw_map(value)
        return cls(
            id=_str(raw.get("id")),
            flow_id=_str(raw.get("flow_id")),
            effect_key=_str(raw.get("effect_key")),
            effect_type=_str(raw.get("effect_type")),
            status=_str(raw.get("status")),
            decision=_optional_str(raw.get("decision")),
            scope=_optional_str(raw.get("scope")),
            external_id=_optional_str(raw.get("external_id")),
            error=_optional_str(raw.get("error")),
            reason=_optional_str(raw.get("reason")),
            operation_digest=_optional_str(raw.get("operation_digest")),
            idempotency_key=_optional_str(raw.get("idempotency_key")),
            reserved_at_ms=_optional_int(raw.get("reserved_at_ms")),
            confirmed_at_ms=_optional_int(raw.get("confirmed_at_ms")),
            failed_at_ms=_optional_int(raw.get("failed_at_ms")),
            compensated_at_ms=_optional_int(raw.get("compensated_at_ms")),
            raw=raw,
        )


@dataclass(frozen=True, slots=True)
class ApprovalResult(_MappingResult):
    """Typed response for Flow approval commands."""

    id: str = ""
    flow_id: str = ""
    scope: str = ""
    status: str = ""
    reason: str | None = None
    requested_by: str | None = None
    approver: str | None = None
    assignees: list[str] | None = None
    policy_hash: str | None = None
    policy_version: str | None = None
    requested_at_ms: int | None = None
    decided_at_ms: int | None = None
    expires_at_ms: int | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> ApprovalResult:
        raw = _raw_map(value)
        assignees = raw.get("assignees")
        return cls(
            id=_str(raw.get("id")),
            flow_id=_str(raw.get("flow_id")),
            scope=_str(raw.get("scope")),
            status=_str(raw.get("status")),
            reason=_optional_str(raw.get("reason")),
            requested_by=_optional_str(raw.get("requested_by")),
            approver=_optional_str(raw.get("approver")),
            assignees=[_str(item) for item in assignees] if isinstance(assignees, list) else None,
            policy_hash=_optional_str(raw.get("policy_hash")),
            policy_version=_optional_str(raw.get("policy_version")),
            requested_at_ms=_optional_int(raw.get("requested_at_ms")),
            decided_at_ms=_optional_int(raw.get("decided_at_ms")),
            expires_at_ms=_optional_int(raw.get("expires_at_ms")),
            raw=raw,
        )


@dataclass(frozen=True, slots=True)
class CircuitBreakerStatus(_MappingResult):
    """Typed response for Flow circuit breaker governance commands."""

    scope: str = ""
    status: str = ""
    failure_threshold: int = 0
    open_ms: int = 0
    opened_at_ms: int | None = None
    failures: int = 0
    failure_count: int = 0
    window_ms: int | None = None
    min_calls: int | None = None
    failure_rate_pct: int | None = None
    latency_threshold_ms: int | None = None
    error_classes: list[str] | None = None
    half_open_max_probes: int = 1
    half_open_success_threshold: int = 1
    half_open_in_flight: int = 0
    half_open_successes: int = 0
    last_failure_ms: int | None = None
    last_success_ms: int | None = None
    updated_at_ms: int | None = None
    events: list[dict[str, Any]] | None = None
    retry_after_ms: int | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> CircuitBreakerStatus:
        raw = _raw_map(value)
        return cls(
            scope=_str(raw.get("scope")),
            status=_str(raw.get("status")),
            failure_threshold=_int(raw.get("failure_threshold")),
            open_ms=_int(raw.get("open_ms")),
            opened_at_ms=_optional_int(raw.get("opened_at_ms")),
            failures=_int(raw.get("failures")),
            failure_count=_int(raw.get("failure_count")),
            window_ms=_optional_int(raw.get("window_ms")),
            min_calls=_optional_int(raw.get("min_calls")),
            failure_rate_pct=_optional_int(raw.get("failure_rate_pct")),
            latency_threshold_ms=_optional_int(raw.get("latency_threshold_ms")),
            error_classes=[_str(item) for item in raw.get("error_classes", [])]
            if isinstance(raw.get("error_classes"), list)
            else None,
            half_open_max_probes=_int(raw.get("half_open_max_probes") or 1),
            half_open_success_threshold=_int(raw.get("half_open_success_threshold") or 1),
            half_open_in_flight=_int(raw.get("half_open_in_flight")),
            half_open_successes=_int(raw.get("half_open_successes")),
            last_failure_ms=_optional_int(raw.get("last_failure_ms")),
            last_success_ms=_optional_int(raw.get("last_success_ms")),
            updated_at_ms=_optional_int(raw.get("updated_at_ms")),
            events=raw.get("events") if isinstance(raw.get("events"), list) else None,
            retry_after_ms=_optional_int(raw.get("retry_after_ms")),
            raw=raw,
        )


@dataclass(frozen=True, slots=True)
class ScheduleResult(_MappingResult):
    """Typed response for Flow scheduler commands."""

    id: str = ""
    kind: str = ""
    status: str = ""
    target: dict[str, Any] | None = None
    timezone: str | None = None
    cron: str | None = None
    overlap_policy: str | None = None
    next_fire_at_ms: int | None = None
    last_fire_at_ms: int | None = None
    fires: int = 0
    max_fires: int | None = None
    end_at_ms: int | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> ScheduleResult:
        raw = _raw_map(value)
        target = raw.get("target")
        return cls(
            id=_str(raw.get("id")),
            kind=_str(raw.get("kind")),
            status=_str(raw.get("status")),
            target=target if isinstance(target, dict) else None,
            timezone=_optional_str(raw.get("timezone")),
            cron=_optional_str(raw.get("cron")),
            overlap_policy=_optional_str(raw.get("overlap_policy")),
            next_fire_at_ms=_optional_int(raw.get("next_fire_at_ms")),
            last_fire_at_ms=_optional_int(raw.get("last_fire_at_ms")),
            fires=_int(raw.get("fires")),
            max_fires=_optional_int(raw.get("max_fires")),
            end_at_ms=_optional_int(raw.get("end_at_ms")),
            raw=raw,
        )


@dataclass(frozen=True, slots=True)
class GovernanceOverview(_MappingResult):
    """Typed response for Flow governance overview commands."""

    counts: dict[str, Any] | None = None
    approvals: list[ApprovalResult] | None = None
    budgets: list[BudgetResult] | None = None
    limits: list[dict[str, Any]] | None = None
    effects: list[EffectResult] | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> GovernanceOverview:
        raw = _raw_map(value)
        approvals = raw.get("approvals")
        budgets = raw.get("budgets")
        effects = raw.get("effects")
        limits = raw.get("limits")
        counts = raw.get("counts")
        return cls(
            counts=counts if isinstance(counts, dict) else None,
            approvals=[
                ApprovalResult.from_resp(item) for item in approvals if isinstance(item, dict)
            ]
            if isinstance(approvals, list)
            else None,
            budgets=[BudgetResult.from_resp(item) for item in budgets if isinstance(item, dict)]
            if isinstance(budgets, list)
            else None,
            limits=[_raw_map(item) for item in limits if isinstance(item, dict)]
            if isinstance(limits, list)
            else None,
            effects=[EffectResult.from_resp(item) for item in effects if isinstance(item, dict)]
            if isinstance(effects, list)
            else None,
            raw=raw,
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


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    """Runtime defaults for high-level queue/workflow workers.

    The normal knobs are ``concurrency``, ``batch_size``, ``lease_ms``,
    retry/exception policy, and named-value hydration. ``command_connections``
    and ``claim_connections`` are advanced transport overrides: for
    ``ferric://`` high-level clients, the latency-first default is one
    multiplexed protocol connection unless the caller explicitly opts into more.
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
    attributes: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class CreateItem:
    id: str
    payload: Any = None
    partition_key: str | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    attributes: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ClaimedFlow:
    id: str
    lease_token: bytes
    fencing_token: int
    partition_key: str | None = None
    type: str = ""
    state: str = "running"
    run_state: str | None = None
    payload: Any = None
    attributes: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any] | list[Any] | tuple[Any, ...]) -> ClaimedFlow:
        if isinstance(value, (list, tuple)):
            raw_id = value[0]
            raw_partition = value[1]
            raw_lease = value[2]
            raw_fencing = value[3]
            if raw_id is None:
                id_value = ""
            elif isinstance(raw_id, bytes):
                id_value = raw_id.decode()
            else:
                id_value = str(raw_id)

            if raw_partition is None or raw_partition == b"" or raw_partition == "":
                partition_key = None
            elif isinstance(raw_partition, bytes):
                partition_key = raw_partition.decode()
            else:
                partition_key = str(raw_partition)

            if isinstance(raw_lease, bytes):
                lease_token = raw_lease
            elif raw_lease is None:
                lease_token = b""
            else:
                lease_token = str(raw_lease).encode()

            fencing_token = raw_fencing if isinstance(raw_fencing, int) else int(raw_fencing)

            run_state: str | None = None
            attributes: dict[str, Any] | None = None
            if len(value) > 4:
                if isinstance(value[4], dict):
                    attributes = _str_key_map(value[4])
                else:
                    run_state = _optional_str(value[4])
            if len(value) > 5 and isinstance(value[5], dict):
                attributes = _str_key_map(value[5])

            return cls(
                id=id_value,
                partition_key=partition_key,
                lease_token=lease_token,
                fencing_token=fencing_token,
                run_state=run_state,
                attributes=attributes,
            )

        raw_attributes = _get(value, "attributes")

        return cls(
            id=_str(_get(value, "id")),
            lease_token=_bytes(_get(value, "lease_token")),
            fencing_token=_int(_get(value, "fencing_token")),
            partition_key=_optional_str(_get(value, "partition_key")),
            type=_str(_get(value, "type")),
            state=_optional_str(_get(value, "state")) or "running",
            run_state=_optional_str(_get(value, "run_state")),
            payload=_get(value, "payload"),
            attributes=_str_key_map(raw_attributes) if isinstance(raw_attributes, dict) else None,
        )

    @classmethod
    def from_compact_rows(cls, values: list[Any]) -> list[ClaimedFlow]:
        items: list[ClaimedFlow] = []
        append = items.append

        for value in values:
            if not isinstance(value, (list, tuple)):
                append(cls.from_resp(value))
                continue

            raw_id = value[0]
            raw_partition = value[1]
            raw_lease = value[2]
            raw_fencing = value[3]

            if raw_id is None:
                id_value = ""
            elif isinstance(raw_id, bytes):
                id_value = raw_id.decode()
            else:
                id_value = str(raw_id)

            if raw_partition is None or raw_partition == b"" or raw_partition == "":
                partition_key = None
            elif isinstance(raw_partition, bytes):
                partition_key = raw_partition.decode()
            else:
                partition_key = str(raw_partition)

            if isinstance(raw_lease, bytes):
                lease_token = raw_lease
            elif raw_lease is None:
                lease_token = b""
            else:
                lease_token = str(raw_lease).encode()

            run_state: str | None = None
            attributes: dict[str, Any] | None = None
            if len(value) > 4:
                if isinstance(value[4], dict):
                    attributes = _str_key_map(value[4])
                else:
                    run_state = _optional_str(value[4])
            if len(value) > 5 and isinstance(value[5], dict):
                attributes = _str_key_map(value[5])

            append(
                cls(
                    id=id_value,
                    partition_key=partition_key,
                    lease_token=lease_token,
                    fencing_token=raw_fencing if isinstance(raw_fencing, int) else int(raw_fencing),
                    run_state=run_state,
                    attributes=attributes,
                )
            )

        return items


ClaimedItem = ClaimedFlow


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
    attributes: dict[str, Any] | None = None
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
            attributes=_str_key_map(_get(value, "attributes")),
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
