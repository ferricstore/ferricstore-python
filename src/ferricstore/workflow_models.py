from __future__ import annotations

import builtins
import contextlib
import time
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from ferricstore.lifecycle_core import (
    raise_primary_with_cleanup,
)
from ferricstore.types import (
    BudgetPolicy,
    BudgetResult,
    ChildSpec,
    ClaimedFlow,
    CreateItem,
    EffectResult,
    FlowRecord,
)
from ferricstore.workflow_budget import WorkflowBudget as WorkflowBudget
from ferricstore.workflow_effect_core import (
    resolve_effect_replay,
    resolve_external_id,
    resolve_operation_digest,
)

if TYPE_CHECKING:
    from ferricstore.client_core import FlowClient
    from ferricstore.workflow_runtime import Workflow

_PROTOCOL_URL_SCHEMES = {"ferric", "ferrics"}


def _close_resource_safely(resource: Any) -> None:
    with contextlib.suppress(BaseException):
        resource.close()


def _is_protocol_url(value: str) -> bool:
    return urlparse(value).scheme.lower() in _PROTOCOL_URL_SCHEMES


FLOW_MANY_BATCH_LIMIT = 1000

_CURRENT_PARTITION = object()

WORKFLOW_WORKER_CONFIG_KEYS = frozenset(
    {
        "batch_size",
        "priority",
        "reclaim_expired",
        "reclaim_ratio",
        "claim_partition_batch_size",
        "block_ms",
        "idle_sleep_s",
        "max_idle_sleep_s",
        "apply_async_depth",
    }
)


class WorkflowFlowCommands:
    """Flow command helpers bound to the currently handled workflow job."""

    __slots__ = ("_ctx",)

    def __init__(self, ctx: WorkflowContext) -> None:
        self._ctx = ctx

    @property
    def client(self) -> FlowClient:
        return self._ctx.workflow.client

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def _partition(self, partition_key: Any) -> str | None:
        if partition_key is _CURRENT_PARTITION:
            return self._ctx.partition_key
        return cast(str | None, partition_key)

    def _type(self, type: str | None) -> str:
        return self._ctx.workflow.type if type is None else type

    def _state(self, type: str | None, state: str | None) -> str:
        if state is not None:
            return state
        if type is None or type == self._ctx.workflow.type:
            return self._ctx.workflow.initial_state
        return "queued"

    def get(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
    ) -> FlowRecord | None:
        return self.client.get(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
        )

    def history(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return self.client.history(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    def create(
        self,
        id: str,
        *,
        type: str | None = None,
        state: str | None = None,
        payload: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.create(
            id,
            type=self._type(type),
            state=self._state(type, state),
            payload=payload,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def enqueue(
        self,
        id: str,
        *,
        type: str | None = None,
        state: str | None = None,
        payload: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.enqueue(
            id,
            type=self._type(type),
            state=self._state(type, state),
            payload=payload,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def start_and_claim(
        self,
        id: str,
        *,
        type: str | None = None,
        initial_state: str | None = None,
        worker: str,
        payload: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> FlowRecord:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.start_and_claim(
            id,
            type=self._type(type),
            initial_state=self._state(type, None) if initial_state is None else initial_state,
            worker=worker,
            payload=payload,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    def create_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str | None = None,
        state: str | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> builtins.list[FlowRecord] | Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.create_many(
            self._partition(partition_key),
            items,
            type=self._type(type),
            state=self._state(type, state),
            **kwargs,
        )

    def enqueue_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str | None = None,
        state: str | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any] | Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.enqueue_many(
            items,
            type=self._type(type),
            state=self._state(type, state),
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    def run_steps_many(
        self,
        items: builtins.list[str | dict[str, Any] | CreateItem],
        *,
        type: str | None = None,
        states: Sequence[str] | None = None,
        steps: int | None = None,
        worker: str,
        payload: Any = None,
        result: Any = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> bytes:
        return self.client.run_steps_many(
            items,
            type=self._type(type),
            states=states,
            steps=steps,
            worker=worker,
            payload=payload,
            result=result,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    def claim_due(
        self, type: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        return self.client.claim_due(self._type(type), **kwargs)

    def claim_flows(self, type: str | None = None, **kwargs: Any) -> builtins.list[ClaimedFlow]:
        return self.client.claim_flows(self._type(type), **kwargs)

    def claim_jobs(self, type: str | None = None, **kwargs: Any) -> builtins.list[ClaimedFlow]:
        """Compatibility alias for claim_flows()."""
        return self.claim_flows(type, **kwargs)

    def signal(self, id: str | None = None, **kwargs: Any) -> Any:
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        return self.client.signal(self._ctx.id if id is None else id, **kwargs)

    def flow_signal(self, id: str | None = None, **kwargs: Any) -> Any:
        return self.signal(id, **kwargs)

    def reclaim(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return cast(builtins.list[FlowRecord], self.client.reclaim(self._type(type), **kwargs))

    def extend_lease(
        self,
        id: str | None = None,
        lease_token: bytes | None = None,
        *,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> FlowRecord:
        return self.client.extend_lease(
            self._ctx.id if id is None else id,
            self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    def transition(
        self,
        to_state: str,
        *,
        id: str | None = None,
        from_state: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        return self.client.transition(
            self._ctx.id if id is None else id,
            from_state=self._ctx.state if from_state is None else from_state,
            to_state=to_state,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def step_continue(
        self,
        to_state: str,
        *,
        id: str | None = None,
        from_state: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> FlowRecord | ClaimedFlow:
        return self.client.step_continue(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            from_state=self._ctx.state if from_state is None else from_state,
            to_state=to_state,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    def complete(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        return self.client.complete(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def retry(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        return self.client.retry(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def fail(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        return self.client.fail(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def cancel(
        self,
        id: str | None = None,
        *,
        fencing_token: int | None = None,
        lease_token: bytes | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        return self.client.cancel(
            self._ctx.id if id is None else id,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def rewind(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> FlowRecord | bytes:
        return self.client.rewind(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    def list(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.list(self._type(type), **kwargs)

    def terminals(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.terminals(self._type(type), **kwargs)

    def failures(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.failures(self._type(type), **kwargs)

    def by_parent(
        self, parent_flow_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        target = self._ctx.id if parent_flow_id is None else parent_flow_id
        return self.client.by_parent(target, **kwargs)

    def by_root(self, root_flow_id: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        root = root_flow_id
        if root is None:
            root = self._ctx.root_flow_id or self._ctx.id
        return self.client.by_root(root, **kwargs)

    def by_correlation(
        self, correlation_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        correlation = correlation_id
        if correlation is None:
            correlation = self._ctx.correlation_id
        if correlation is None:
            raise ValueError("correlation_id is required when current flow has no correlation_id")
        return self.client.by_correlation(correlation, **kwargs)

    def info(self, type: str | None = None, **kwargs: Any) -> dict[Any, Any]:
        return self.client.info(self._type(type), **kwargs)

    def stuck(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.stuck(self._type(type), **kwargs)

    def value_put(self, value: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        kwargs.setdefault("owner_flow_id", self._ctx.id)
        return self.client.value_put(value, **kwargs)

    def put_value(self, name: str, value: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("name", name)
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        kwargs.setdefault("owner_flow_id", self._ctx.id)
        return self.client.value_put(value, **kwargs)

    def value_mget(
        self, refs: builtins.list[str], *, max_bytes: int | None = None
    ) -> builtins.list[Any]:
        return self.client.value_mget(refs, max_bytes=max_bytes)

    def value(self, name: str, default: Any = None, *, local_cache: bool = False) -> Any:
        return self._ctx.value(name, default, local_cache=local_cache)

    def values(self, names: builtins.list[str], *, local_cache: bool = False) -> dict[str, Any]:
        return self._ctx.value_many(names, local_cache=local_cache)

    def spawn_children(
        self,
        children: builtins.list[ChildSpec],
        *,
        parent_flow_id: str | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.spawn_children(
            self._ctx.id if parent_flow_id is None else parent_flow_id,
            children,
            partition_key=self._partition(partition_key),
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            **kwargs,
        )

    def install_policy(
        self,
        type: str | None = None,
        *,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return self.client.install_policy(self._type(type), **kwargs)

    def policy_get(self, type: str | None = None, **kwargs: Any) -> dict[Any, Any]:
        return self.client.policy_get(self._type(type), **kwargs)


class WorkflowEffect:
    """Synchronous external-effect helper bound to a workflow job.

    Use this for side effects such as payment calls, email sends, or model
    invocations where FerricStore should reserve/confirm/fail an effect record
    and apply circuit-breaker policy around the call.
    """

    __slots__ = (
        "_closed",
        "_result",
        "_started_at",
        "ctx",
        "effect_key",
        "effect_type",
        "external_id",
        "governance_scope",
        "idempotency_key",
        "operation_digest",
        "replay",
        "reservation",
    )

    def __init__(
        self,
        ctx: WorkflowContext,
        effect_key: str,
        effect_type: str,
        *,
        operation_digest: str | None = None,
        idempotency_key: str | None = None,
        governance_scope: str | None = None,
        external_id: str | Callable[[Any], str | None] | None = None,
        replay: Callable[[EffectResult], Any] | None = None,
    ) -> None:
        self.ctx = ctx
        self.effect_key = effect_key
        self.effect_type = effect_type
        self.operation_digest = resolve_operation_digest(
            effect_type,
            effect_key,
            operation_digest,
            idempotency_key,
        )
        self.idempotency_key = idempotency_key
        self.governance_scope = governance_scope
        self.external_id = external_id
        self.replay = replay
        self.reservation: EffectResult | None = None
        self._result: EffectResult | None = None
        self._started_at: float | None = None
        self._closed = False

    def reserve(self) -> EffectResult:
        if self.reservation is not None:
            return self.reservation
        self.reservation = self.ctx.client.effect_reserve(
            self.ctx.id,
            self.effect_key,
            self.effect_type,
            partition_key=self.ctx.partition_key,
            lease_token=self.ctx.lease_token,
            fencing_token=self.ctx.fencing_token,
            operation_digest=self.operation_digest,
            idempotency_key=self.idempotency_key,
            governance_scope=self.governance_scope,
        )
        self._started_at = time.perf_counter()
        return self.reservation

    def confirm(
        self,
        *,
        external_id: str | None = None,
        latency_ms: int | None = None,
    ) -> EffectResult:
        if self._closed and self._result is not None:
            return self._result
        self.reserve()
        self._result = self.ctx.client.effect_confirm(
            self.ctx.id,
            self.effect_key,
            partition_key=self.ctx.partition_key,
            lease_token=self.ctx.lease_token,
            fencing_token=self.ctx.fencing_token,
            external_id=external_id,
            latency_ms=self._latency_ms(latency_ms),
        )
        self._closed = True
        return self._result

    def fail(
        self,
        *,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
    ) -> EffectResult:
        if self._closed and self._result is not None:
            return self._result
        self.reserve()
        self._result = self.ctx.client.effect_fail(
            self.ctx.id,
            self.effect_key,
            partition_key=self.ctx.partition_key,
            lease_token=self.ctx.lease_token,
            fencing_token=self.ctx.fencing_token,
            error=error,
            reason=reason,
            latency_ms=self._latency_ms(latency_ms),
        )
        self._closed = True
        return self._result

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        reservation = self.reserve()
        replayed, replay_value = resolve_effect_replay(reservation, self.replay)
        if replayed:
            return replay_value
        try:
            result = func(*args, **kwargs)
        except BaseException as exc:
            try:
                self.fail(error=str(exc), reason=exc.__class__.__name__)
            except BaseException as cleanup:
                raise_primary_with_cleanup(exc, exc.__traceback__, cleanup)
            raise
        self.confirm(external_id=self._resolve_external_id(result))
        return result

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return self.call(func, *args, **kwargs)

        return wrapper

    def __enter__(self) -> WorkflowEffect:
        self.reserve()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            self.confirm()
        else:
            try:
                self.fail(
                    error=str(exc) if exc is not None else None,
                    reason=exc_type.__name__ if exc_type is not None else None,
                )
            except BaseException as cleanup:
                if exc is not None:
                    raise_primary_with_cleanup(exc, tb, cleanup)
                raise

    def _resolve_external_id(self, result: Any) -> str | None:
        return resolve_external_id(self.external_id, result)

    def _latency_ms(self, explicit: int | None = None) -> int | None:
        if explicit is not None:
            return explicit
        if self._started_at is None:
            return None
        return max(int((time.perf_counter() - self._started_at) * 1000), 0)


class WorkflowContext:
    """Handler context with current job metadata and Flow command helpers."""

    __slots__ = ("_governance_attributes", "_value_cache", "flow", "job", "state_name", "workflow")

    def __init__(
        self,
        workflow: Workflow,
        job: FlowRecord | ClaimedFlow,
        state_name: str,
    ) -> None:
        self.workflow = workflow
        self.job = job
        self.state_name = state_name
        self.flow = WorkflowFlowCommands(self)
        self._governance_attributes: dict[str, Any] = {}
        self._value_cache: dict[str, Any] = {}

    @property
    def client(self) -> FlowClient:
        return self.workflow.client

    @property
    def id(self) -> str:
        return self.job.id

    @property
    def type(self) -> str:
        return self.job.type or self.workflow.type

    @property
    def state(self) -> str:
        return self.job.state

    @property
    def run_state(self) -> str | None:
        return self.job.run_state

    @property
    def logical_state(self) -> str:
        return self.state_name

    @property
    def partition_key(self) -> str | None:
        return self.job.partition_key

    @property
    def payload(self) -> Any:
        return self.job.payload

    @property
    def values(self) -> dict[str, Any]:
        return getattr(self.job, "values", None) or {}

    @property
    def value_refs(self) -> dict[str, Any]:
        return getattr(self.job, "value_refs", None) or {}

    def value(self, name: str, default: Any = None, *, local_cache: bool | None = None) -> Any:
        use_local_cache = (
            self.workflow.value_config.local_cache if local_cache is None else local_cache
        )
        if use_local_cache and name in self._value_cache:
            return self._value_cache[name]

        if name in self.values:
            value = self.values[name]
            if use_local_cache:
                self._value_cache[name] = value
            return value

        meta = self.value_refs.get(name)
        ref = None
        if isinstance(meta, dict):
            ref = meta.get("ref") or meta.get(b"ref")
        elif isinstance(meta, str):
            ref = meta
        elif isinstance(meta, bytes):
            ref = meta.decode()

        if not ref:
            return default

        values = self.client.value_mget([ref], max_bytes=self._value_max_bytes())
        value = values[0] if values else default
        if use_local_cache and values:
            self._value_cache[name] = value
        return value

    def value_many(
        self, names: builtins.list[str], *, local_cache: bool | None = None
    ) -> dict[str, Any]:
        use_local_cache = (
            self.workflow.value_config.local_cache if local_cache is None else local_cache
        )
        values: dict[str, Any] = {}
        pending_names: builtins.list[str] = []
        pending_refs: builtins.list[str] = []

        for name in names:
            if use_local_cache and name in self._value_cache:
                values[name] = self._value_cache[name]
                continue

            if name in self.values:
                value = self.values[name]
                values[name] = value
                if use_local_cache:
                    self._value_cache[name] = value
                continue

            meta = self.value_refs.get(name)
            ref = None
            if isinstance(meta, dict):
                ref = meta.get("ref") or meta.get(b"ref")
            elif isinstance(meta, str):
                ref = meta
            elif isinstance(meta, bytes):
                ref = meta.decode()

            if ref:
                pending_names.append(name)
                pending_refs.append(ref)

        if pending_refs:
            fetched = self.client.value_mget(pending_refs, max_bytes=self._value_max_bytes())
            for name, value in zip(pending_names, fetched, strict=True):
                values[name] = value
                if use_local_cache:
                    self._value_cache[name] = value

        return values

    def _value_max_bytes(self) -> int | None:
        config = self.workflow._states.get(self.state_name)
        return None if config is None else config.value_max_bytes

    def budget(
        self,
        scope: str,
        amount: int,
        *,
        limit: int | None = None,
        window_ms: int | None = None,
        usage_key: str = "amount",
        attribute_prefix: str = "governance_budget",
    ) -> WorkflowBudget:
        return WorkflowBudget(
            self,
            scope=scope,
            amount=amount,
            limit=limit,
            window_ms=window_ms,
            usage_key=usage_key,
            attribute_prefix=attribute_prefix,
        )

    def effect(
        self,
        effect_key: str,
        effect_type: str,
        *,
        operation_digest: str | None = None,
        idempotency_key: str | None = None,
        governance_scope: str | None = None,
        external_id: str | Callable[[Any], str | None] | None = None,
        replay: Callable[[EffectResult], Any] | None = None,
    ) -> WorkflowEffect:
        return WorkflowEffect(
            self,
            effect_key,
            effect_type,
            operation_digest=operation_digest,
            idempotency_key=idempotency_key,
            governance_scope=governance_scope,
            external_id=external_id,
            replay=replay,
        )

    def _state_budget(self, policy: BudgetPolicy | None) -> WorkflowBudget | None:
        if policy is None:
            return None
        scope = policy.scope(self) if callable(policy.scope) else policy.scope
        return self.budget(
            scope,
            policy.amount,
            limit=policy.limit,
            window_ms=policy.window_ms,
            usage_key=policy.usage_key,
            attribute_prefix=policy.attribute_prefix,
        )

    def _record_budget_result(self, prefix: str, result: BudgetResult) -> None:
        attrs = {
            f"{prefix}_scope": result.scope,
            f"{prefix}_status": result.status,
            f"{prefix}_reservation_id": result.reservation_id,
            f"{prefix}_reserved_amount": result.reserved_amount,
            f"{prefix}_actual_amount": result.actual_amount,
            f"{prefix}_overage_amount": result.overage_amount,
            f"{prefix}_remaining": result.remaining,
            f"{prefix}_over_budget": result.over_budget,
        }
        self._governance_attributes.update(
            {key: value for key, value in attrs.items() if value is not None}
        )

    @property
    def lease_token(self) -> bytes:
        return self.job.lease_token

    @property
    def fencing_token(self) -> int:
        return self.job.fencing_token

    @property
    def version(self) -> int:
        return getattr(self.job, "version", 0)

    @property
    def parent_flow_id(self) -> str | None:
        return getattr(self.job, "parent_flow_id", None)

    @property
    def root_flow_id(self) -> str | None:
        return getattr(self.job, "root_flow_id", None)

    @property
    def correlation_id(self) -> str | None:
        return getattr(self.job, "correlation_id", None)

    @property
    def now_ms(self) -> int:
        return int(time.time() * 1000)
