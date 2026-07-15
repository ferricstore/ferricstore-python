from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from ferricstore.config_validation import (
    validate_optional_positive_int,
    validate_positive_int,
)
from ferricstore.model_core import (
    _bytes,
    _get,
    _int,
    _MappingResult,
    _normalize_ref_meta,
    _optional_int,
    _optional_str,
    _optional_str_or_int,
    _raw_map,
    _str,
    _str_key_map,
)
from ferricstore.schedule_types import ScheduleResult as ScheduleResult

if TYPE_CHECKING:
    from ferricstore.retry_policy import RetryPolicy as RetryPolicy
    from ferricstore.worker_config import ExceptionPolicy as ExceptionPolicy
    from ferricstore.worker_config import ValueConfig as ValueConfig
    from ferricstore.worker_config import WorkerConfig as WorkerConfig
    from ferricstore.worker_config import (
        normalize_exception_policy as normalize_exception_policy,
    )
    from ferricstore.worker_config import (
        resolve_worker_connection_counts as resolve_worker_connection_counts,
    )


class FlowStateMode(str, Enum):
    """Server scheduling mode for a Flow state."""

    PARALLEL = "parallel"
    FIFO = "fifo"


_FLOW_STATE_MODE_VALUES = {mode.value for mode in FlowStateMode}


def normalize_flow_state_mode(
    value: FlowStateMode | str | None,
    *,
    argument: str = "mode",
) -> str | None:
    if value is None:
        return None
    if isinstance(value, FlowStateMode):
        return value.value
    if isinstance(value, str):
        normalized = value.lower()
        if normalized in _FLOW_STATE_MODE_VALUES:
            return normalized
    raise ValueError(
        f"{argument} must be FlowStateMode.PARALLEL, FlowStateMode.FIFO, 'parallel', or 'fifo'"
    )


@dataclass(frozen=True, slots=True)
class FlowStatePolicy:
    """Per-state Flow policy.

    ``mode`` controls state scheduling. ``FIFO`` is opt-in per state; omitting
    mode leaves the server default of ``PARALLEL``.
    """

    mode: FlowStateMode | str | None = None
    retry: RetryPolicy | None = None

    def __post_init__(self) -> None:
        normalize_flow_state_mode(self.mode)

    @classmethod
    def fifo(cls, *, retry: RetryPolicy | None = None) -> FlowStatePolicy:
        return cls(mode=FlowStateMode.FIFO, retry=retry)

    @classmethod
    def parallel(cls, *, retry: RetryPolicy | None = None) -> FlowStatePolicy:
        return cls(mode=FlowStateMode.PARALLEL, retry=retry)


if TYPE_CHECKING:
    FlowStatePolicyLike = RetryPolicy | FlowStatePolicy
else:
    FlowStatePolicyLike = Any


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

    def __post_init__(self) -> None:
        if not callable(self.scope) and not (isinstance(self.scope, str) and self.scope != ""):
            raise ValueError("scope must be a non-empty string or callable")
        validate_positive_int(self.amount, name="amount")
        validate_optional_positive_int(self.limit, name="limit")
        validate_optional_positive_int(self.window_ms, name="window_ms")
        if not isinstance(self.usage_key, str) or not self.usage_key:
            raise ValueError("usage_key must be a non-empty string")
        if not isinstance(self.attribute_prefix, str) or not self.attribute_prefix:
            raise ValueError("attribute_prefix must be a non-empty string")


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
        if isinstance(value, (list, tuple)) and value:
            kind = _str(value[0], "event")
            if kind == "message" and len(value) >= 3:
                message = value[2]
                if decode is not None:
                    message = decode(message)
                return cls(kind=kind, channel=_str(value[1]), message=message, raw={"event": value})
            if kind == "pmessage" and len(value) >= 4:
                message = value[3]
                if decode is not None:
                    message = decode(message)
                return cls(
                    kind=kind,
                    pattern=_optional_str(value[1]),
                    channel=_str(value[2]),
                    message=message,
                    raw={"event": value},
                )

        if not isinstance(value, dict):
            return cls(kind="event", channel="", message=value, raw={"event": value})

        raw = _raw_map(value)
        payload = _get(value, "payload")
        message_source = value
        if _str(raw.get("event")) == "PUBSUB_MESSAGE" and isinstance(payload, dict):
            raw = _raw_map(payload)
            message_source = payload

        message = _get(message_source, "message")
        if message is None:
            message = raw.get("message")
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
    partition_key: str | None = None
    flow_type: str | None = None
    state: str | None = None
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
    policy_hash: str | None = None
    policy_version: str | int | None = None
    latency_ms: int | None = None
    created_at_ms: int | None = None
    updated_at_ms: int | None = None
    reserved_at_ms: int | None = None
    confirmed_at_ms: int | None = None
    failed_at_ms: int | None = None
    compensated_at_ms: int | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> EffectResult:
        raw = _raw_map(value)
        status = _str(raw.get("status"))
        created_at_ms = _optional_int(raw.get("created_at_ms"))
        updated_at_ms = _optional_int(raw.get("updated_at_ms"))
        reserved_at_ms = _optional_int(raw.get("reserved_at_ms"))
        confirmed_at_ms = _optional_int(raw.get("confirmed_at_ms"))
        failed_at_ms = _optional_int(raw.get("failed_at_ms"))
        compensated_at_ms = _optional_int(raw.get("compensated_at_ms"))
        return cls(
            id=_str(raw.get("id")),
            flow_id=_str(raw.get("flow_id")),
            partition_key=_optional_str(raw.get("partition_key")),
            flow_type=_optional_str(raw.get("type")),
            state=_optional_str(raw.get("state")),
            effect_key=_str(raw.get("effect_key")),
            effect_type=_str(raw.get("effect_type")),
            status=status,
            decision=_optional_str(raw.get("decision")),
            scope=_optional_str(raw.get("scope")),
            external_id=_optional_str(raw.get("external_id")),
            error=_optional_str(raw.get("error")),
            reason=_optional_str(raw.get("reason")),
            operation_digest=_optional_str(raw.get("operation_digest")),
            idempotency_key=_optional_str(raw.get("idempotency_key")),
            policy_hash=_optional_str(raw.get("policy_hash")),
            policy_version=_optional_str_or_int(raw.get("policy_version")),
            latency_ms=_optional_int(raw.get("latency_ms")),
            created_at_ms=created_at_ms,
            updated_at_ms=updated_at_ms,
            reserved_at_ms=created_at_ms if reserved_at_ms is None else reserved_at_ms,
            confirmed_at_ms=(updated_at_ms if status == "confirmed" else None)
            if confirmed_at_ms is None
            else confirmed_at_ms,
            failed_at_ms=(updated_at_ms if status == "failed" else None)
            if failed_at_ms is None
            else failed_at_ms,
            compensated_at_ms=(updated_at_ms if status == "compensated" else None)
            if compensated_at_ms is None
            else compensated_at_ms,
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
    decision_reason: str | None = None
    assignees: list[str] | None = None
    policy_hash: str | None = None
    policy_version: str | int | None = None
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
            approver=_optional_str(raw.get("approver", raw.get("decided_by"))),
            decision_reason=_optional_str(raw.get("decision_reason")),
            assignees=[_str(item) for item in assignees] if isinstance(assignees, list) else None,
            policy_hash=_optional_str(raw.get("policy_hash")),
            policy_version=_optional_str_or_int(raw.get("policy_version")),
            requested_at_ms=_optional_int(raw.get("requested_at_ms")),
            decided_at_ms=_optional_int(raw.get("decided_at_ms")),
            expires_at_ms=_optional_int(raw.get("expires_at_ms")),
            raw=raw,
        )

    @property
    def decided_by(self) -> str | None:
        """KV-native alias for ``approver``."""

        return self.approver


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
    half_open_started_at_ms: int | None = None
    last_failure_ms: int | None = None
    last_success_ms: int | None = None
    updated_at_ms: int | None = None
    events: list[dict[str, Any]] | None = None
    event_count: int = 0
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
            half_open_started_at_ms=_optional_int(raw.get("half_open_started_at_ms")),
            last_failure_ms=_optional_int(raw.get("last_failure_ms")),
            last_success_ms=_optional_int(raw.get("last_success_ms")),
            updated_at_ms=_optional_int(raw.get("updated_at_ms")),
            events=raw.get("events") if isinstance(raw.get("events"), list) else None,
            event_count=_int(raw.get("event_count")),
            retry_after_ms=_optional_int(raw.get("retry_after_ms")),
            raw=raw,
        )


@dataclass(frozen=True, slots=True)
class GovernanceOverview(_MappingResult):
    """Typed response for Flow governance overview commands."""

    counts: dict[str, Any] | None = None
    approvals: list[ApprovalResult] | None = None
    budgets: list[BudgetResult] | None = None
    limits: list[dict[str, Any]] | None = None
    circuits: list[CircuitBreakerStatus] | None = None
    effects: list[EffectResult] | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: dict[Any, Any]) -> GovernanceOverview:
        raw = _raw_map(value)
        approvals = raw.get("approvals")
        budgets = raw.get("budgets")
        circuits = raw.get("circuits")
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
            circuits=[
                CircuitBreakerStatus.from_resp(item) for item in circuits if isinstance(item, dict)
            ]
            if isinstance(circuits, list)
            else None,
            effects=[EffectResult.from_resp(item) for item in effects if isinstance(item, dict)]
            if isinstance(effects, list)
            else None,
            raw=raw,
        )


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
    state_meta: dict[str, Any] | None = None


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
    state_meta: dict[str, Any] | None = None
    indexed_state_meta: str | None = None
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
            state_meta=_str_key_map(_get(value, "state_meta")),
            indexed_state_meta=_optional_str(_get(value, "indexed_state_meta")),
            value_refs=_str_key_map(_get(value, "value_refs")),
            values=values,
            value_sizes=_str_key_map(_get(value, "value_sizes")),
            value_omitted=_str_key_map(_get(value, "value_omitted")),
            value_missing=_str_key_map(_get(value, "value_missing")),
            raw=value,
        )


def __getattr__(name: str) -> Any:
    if name == "RetryPolicy":
        from ferricstore.retry_policy import RetryPolicy

        globals()[name] = RetryPolicy
        return RetryPolicy
    worker_config_exports = {
        "ExceptionPolicy",
        "ValueConfig",
        "WorkerConfig",
        "normalize_exception_policy",
        "resolve_worker_connection_counts",
    }
    if name in worker_config_exports:
        from ferricstore import worker_config

        value = getattr(worker_config, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
