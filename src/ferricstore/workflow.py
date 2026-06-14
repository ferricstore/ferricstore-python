from __future__ import annotations

import builtins
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from typing import Any, cast

from ferricstore.client import FlowClient
from ferricstore.errors import FerricStoreError
from ferricstore.types import (
    ChildSpec,
    ClaimedItem,
    CreateItem,
    ExceptionPolicy,
    FencedItem,
    FlowRecord,
    RetryPolicy,
    ValueConfig,
    WorkerConfig,
    normalize_exception_policy,
    resolve_worker_connection_counts,
)


@dataclass(frozen=True, slots=True)
class StateConfig:
    name: str
    lease_ms: int = 30_000
    claim_payload: bool = True
    claim_record: bool = True
    claim_values: builtins.list[str] | None = None
    value_max_bytes: int | None = None
    on_error: str = ExceptionPolicy.RETRY.value
    retry: RetryPolicy | None = None
    return_record: bool = False


@dataclass(frozen=True, slots=True)
class WorkflowWorkerResult:
    claimed: int = 0
    applied: int = 0
    claim_calls: int = 0
    empty_claims: int = 0


@dataclass(frozen=True, slots=True)
class Transition:
    to_state: str
    payload: Any = None
    run_at_ms: int | None = None
    priority: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None


@dataclass(frozen=True, slots=True)
class Complete:
    result: Any = None
    payload: Any = None
    ttl_ms: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None


@dataclass(frozen=True, slots=True)
class Retry:
    error: Any = None
    payload: Any = None
    run_at_ms: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None


@dataclass(frozen=True, slots=True)
class Fail:
    error: Any = None
    payload: Any = None
    ttl_ms: int | None = None
    values: dict[str, Any] | None = None
    value_refs: dict[str, str] | None = None
    drop_values: builtins.list[str] | None = None
    override_values: builtins.list[str] | None = None


Outcome = Transition | Complete | Retry | Fail
Handler = Callable[[Any], Outcome]
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
        return type or self._ctx.workflow.type

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
        return self.client.get(id or self._ctx.id, partition_key=self._partition(partition_key))

    def history(
        self,
        id: str | None = None,
        *,
        partition_key: str | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return self.client.history(
            id or self._ctx.id, partition_key=self._partition(partition_key), **kwargs
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
        **kwargs: Any,
    ) -> FlowRecord | bytes:
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
        **kwargs: Any,
    ) -> FlowRecord | bytes:
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
        **kwargs: Any,
    ) -> FlowRecord:
        return self.client.start_and_claim(
            id,
            type=self._type(type),
            initial_state=initial_state or self._state(type, None),
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
        **kwargs: Any,
    ) -> builtins.list[FlowRecord] | Any:
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
        **kwargs: Any,
    ) -> builtins.list[Any] | Any:
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
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedItem]:
        return self.client.claim_due(self._type(type), **kwargs)

    def claim_jobs(self, type: str | None = None, **kwargs: Any) -> builtins.list[ClaimedItem]:
        return self.client.claim_jobs(self._type(type), **kwargs)

    def signal(self, id: str | None = None, **kwargs: Any) -> Any:
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        return self.client.signal(id or self._ctx.id, **kwargs)

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
            id or self._ctx.id,
            lease_token or self._ctx.lease_token,
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
            id or self._ctx.id,
            from_state=from_state or self._ctx.state,
            to_state=to_state,
            lease_token=lease_token or self._ctx.lease_token,
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
    ) -> FlowRecord:
        return self.client.step_continue(
            id or self._ctx.id,
            lease_token=lease_token or self._ctx.lease_token,
            from_state=from_state or self._ctx.state,
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
            id or self._ctx.id,
            lease_token=lease_token or self._ctx.lease_token,
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
            id or self._ctx.id,
            lease_token=lease_token or self._ctx.lease_token,
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
            id or self._ctx.id,
            lease_token=lease_token or self._ctx.lease_token,
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
            id or self._ctx.id,
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
            id or self._ctx.id,
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
        return self.client.by_parent(parent_flow_id or self._ctx.id, **kwargs)

    def by_root(self, root_flow_id: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        root = root_flow_id or self._ctx.root_flow_id or self._ctx.id
        return self.client.by_root(root, **kwargs)

    def by_correlation(
        self, correlation_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        correlation = correlation_id or self._ctx.correlation_id
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
        parent_id: str | None = None,
        partition_key: str | None | object = _CURRENT_PARTITION,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        **kwargs: Any,
    ) -> Any:
        return self.client.spawn_children(
            parent_id or self._ctx.id,
            children,
            partition_key=self._partition(partition_key),
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            **kwargs,
        )

    def install_policy(self, type: str | None = None, **kwargs: Any) -> Any:
        return self.client.install_policy(self._type(type), **kwargs)

    def policy_get(self, type: str | None = None, **kwargs: Any) -> dict[Any, Any]:
        return self.client.policy_get(self._type(type), **kwargs)


class WorkflowContext:
    """Handler context with current job metadata and Flow command helpers."""

    __slots__ = ("_value_cache", "flow", "job", "state_name", "workflow")

    def __init__(
        self,
        workflow: Workflow,
        job: FlowRecord | ClaimedItem,
        state_name: str,
    ) -> None:
        self.workflow = workflow
        self.job = job
        self.state_name = state_name
        self.flow = WorkflowFlowCommands(self)
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
            for name, value in zip(pending_names, fetched, strict=False):
                values[name] = value
                if use_local_cache:
                    self._value_cache[name] = value

        return values

    def _value_max_bytes(self) -> int | None:
        config = self.workflow._states.get(self.state_name)
        return None if config is None else config.value_max_bytes

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


def transition(
    to_state: str,
    *,
    payload: Any = None,
    run_at_ms: int | None = None,
    priority: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
) -> Transition:
    return Transition(
        to_state=to_state,
        payload=payload,
        run_at_ms=run_at_ms,
        priority=priority,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )


def complete(
    *,
    result: Any = None,
    payload: Any = None,
    ttl_ms: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
) -> Complete:
    return Complete(
        result=result,
        payload=payload,
        ttl_ms=ttl_ms,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )


def retry(
    *,
    error: Any = None,
    payload: Any = None,
    run_at_ms: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
) -> Retry:
    return Retry(
        error=error,
        payload=payload,
        run_at_ms=run_at_ms,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )


def fail(
    *,
    error: Any = None,
    payload: Any = None,
    ttl_ms: int | None = None,
    values: dict[str, Any] | None = None,
    value_refs: dict[str, str] | None = None,
    drop_values: builtins.list[str] | None = None,
    override_values: builtins.list[str] | None = None,
) -> Fail:
    return Fail(
        error=error,
        payload=payload,
        ttl_ms=ttl_ms,
        values=values,
        value_refs=value_refs,
        drop_values=drop_values,
        override_values=override_values,
    )


def state(
    name: str,
    *,
    lease_ms: int = 30_000,
    claim_payload: bool = True,
    claim_record: bool = True,
    claim_values: builtins.list[str] | None = None,
    value_max_bytes: int | None = None,
    exception_policy: ExceptionPolicy | str | None = None,
    on_error: ExceptionPolicy | str | None = None,
    retry_policy: RetryPolicy | None = None,
    retry: RetryPolicy | None = None,
    return_record: bool = False,
) -> Callable[[Handler], Handler]:
    if exception_policy is not None and on_error is not None:
        raise ValueError("exception_policy and on_error are mutually exclusive")
    if retry_policy is not None and retry is not None:
        raise ValueError("retry_policy and retry are mutually exclusive")
    resolved_on_error = normalize_exception_policy(
        exception_policy if exception_policy is not None else on_error,
        argument="exception_policy" if exception_policy is not None else "on_error",
    )
    resolved_retry_policy = retry_policy if retry_policy is not None else retry

    def decorate(fn: Handler) -> Handler:
        cast(Any, fn).__ferric_state__ = StateConfig(
            name=name,
            lease_ms=lease_ms,
            claim_payload=claim_payload,
            claim_record=claim_record,
            claim_values=claim_values,
            value_max_bytes=value_max_bytes,
            on_error=resolved_on_error,
            retry=resolved_retry_policy,
            return_record=return_record,
        )
        return fn

    return decorate


class Workflow:
    """Base class for explicit FerricFlow state workflows."""

    type: str
    initial_state = "queued"
    partition_by: tuple[str, ...] = ()
    retry_policy: RetryPolicy | None = None
    worker_config: WorkerConfig | None = None
    value_config: ValueConfig = ValueConfig()

    def __init__(
        self,
        client: FlowClient,
        *,
        claim_client: FlowClient | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> None:
        self.client = client
        self.claim_client = claim_client if claim_client is not None else client
        self.retry_policy = retry_policy if retry_policy is not None else self.retry_policy
        self.worker_config = worker_config if worker_config is not None else self.worker_config
        self.value_config = value_config if value_config is not None else self.value_config
        self._states, self._handlers = self._discover_state_handlers()
        default_exception_policy = (
            self.worker_config.exception_policy if self.worker_config is not None else None
        )
        if self.value_config.value_max_bytes is not None or default_exception_policy is not None:
            self._states = {
                name: replace(
                    config,
                    value_max_bytes=(
                        config.value_max_bytes
                        if config.value_max_bytes is not None
                        else self.value_config.value_max_bytes
                    ),
                    on_error=(
                        normalize_exception_policy(default_exception_policy)
                        if default_exception_policy is not None
                        and config.on_error == ExceptionPolicy.RETRY.value
                        else config.on_error
                    ),
                )
                for name, config in self._states.items()
            }

    def create(self, id: str, *, payload: Any = None, **attrs: Any) -> FlowRecord | bytes:
        partition_key = attrs.pop("partition_key", None) or self.partition_key(attrs)
        for name in self.partition_by:
            attrs.pop(name, None)
        return self.client.create(
            id,
            type=self.type,
            state=attrs.pop("state", self.initial_state),
            payload=payload,
            partition_key=partition_key,
            **attrs,
        )

    def enqueue(self, id: str, *, payload: Any = None, **attrs: Any) -> FlowRecord | bytes:
        partition_key = attrs.pop("partition_key", None) or self.partition_key(attrs)
        for name in self.partition_by:
            attrs.pop(name, None)
        return self.client.enqueue(
            id,
            type=self.type,
            state=attrs.pop("state", self.initial_state),
            payload=payload,
            partition_key=partition_key,
            **attrs,
        )

    def start_and_claim(
        self,
        id: str,
        *,
        worker: str,
        payload: Any = None,
        initial_state: str | None = None,
        **attrs: Any,
    ) -> FlowRecord:
        partition_key = attrs.pop("partition_key", None) or self.partition_key(attrs)
        for name in self.partition_by:
            attrs.pop(name, None)
        return self.client.start_and_claim(
            id,
            type=self.type,
            initial_state=initial_state or self.initial_state,
            worker=worker,
            payload=payload,
            partition_key=partition_key,
            **attrs,
        )

    def create_many(
        self,
        partition_key: str | None,
        items: builtins.list[CreateItem],
        **attrs: Any,
    ) -> builtins.list[FlowRecord]:
        return self.client.create_many(
            partition_key,
            items,
            type=self.type,
            state=attrs.pop("state", self.initial_state),
            **attrs,
        )

    def run_steps_many(
        self,
        items: builtins.list[str | dict[str, Any] | CreateItem],
        *,
        states: Sequence[str] | None = None,
        steps: int | None = None,
        worker: str,
        payload: Any = None,
        result: Any = None,
        **attrs: Any,
    ) -> bytes:
        partition_key = attrs.pop("partition_key", None) or self.partition_key(attrs)
        for name in self.partition_by:
            attrs.pop(name, None)
        return self.client.run_steps_many(
            items,
            type=self.type,
            states=states,
            steps=steps,
            worker=worker,
            payload=payload,
            result=result,
            partition_key=partition_key,
            **attrs,
        )

    def partition_key(self, attrs: dict[str, Any]) -> str | None:
        if not self.partition_by:
            return None
        return ":".join(str(attrs[name]) for name in self.partition_by)

    def install_policy(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
    ) -> Any:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        state_policies = {
            config.name: config.retry
            for config in self._states.values()
            if config.retry is not None
        }
        return self.client.install_policy(
            self.type, retry=resolved_retry_policy, states=state_policies
        )

    def policy_get(self, *, state: str | None = None) -> dict[Any, Any]:
        return self.client.policy_get(self.type, state=state)

    def signal(self, id: str, **kwargs: Any) -> Any:
        return self.client.signal(id, **kwargs)

    def flow_signal(self, id: str, **kwargs: Any) -> Any:
        return self.signal(id, **kwargs)

    def worker(self, **kwargs: Any) -> WorkflowWorker:
        worker_kwargs = (
            self.worker_config.to_kwargs(WORKFLOW_WORKER_CONFIG_KEYS)
            if self.worker_config is not None
            else {}
        )
        worker_kwargs.update(kwargs)
        return WorkflowWorker(self, **worker_kwargs)

    def claim_due(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        limit: int = 1,
        priority: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        block_ms: int | None = None,
    ) -> builtins.list[FlowRecord | ClaimedItem]:
        config = self._states[state_name]
        claim_client = getattr(self, "claim_client", self.client)
        if not config.claim_record and not config.claim_payload and not config.claim_values:
            jobs = claim_client.claim_jobs(
                self.type,
                state=state_name,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=config.lease_ms,
                limit=limit,
                priority=priority,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                block_ms=block_ms,
            )
            return cast(
                builtins.list[FlowRecord | ClaimedItem],
                self._stamp_compact_jobs(jobs, state_name),
            )

        return cast(
            builtins.list[FlowRecord | ClaimedItem],
            claim_client.claim_due(
                self.type,
                state=state_name,
                worker=worker,
                partition_key=partition_key,
                partition_keys=partition_keys,
                lease_ms=config.lease_ms,
                limit=limit,
                priority=priority,
                reclaim_expired=reclaim_expired,
                reclaim_ratio=reclaim_ratio,
                block_ms=block_ms,
                payload=config.claim_payload,
                values=config.claim_values,
                value_max_bytes=config.value_max_bytes,
            ),
        )

    def reclaim(
        self,
        *,
        worker: str,
        partition_key: str | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
    ) -> builtins.list[FlowRecord]:
        return cast(
            builtins.list[FlowRecord],
            self.client.reclaim(
                self.type,
                worker=worker,
                partition_key=partition_key,
                lease_ms=lease_ms,
                limit=limit,
            ),
        )

    def run_once(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        limit: int = 1,
        priority: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
    ) -> builtins.list[FlowRecord | bytes]:
        jobs = self.claim_due(
            state_name,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=limit,
            priority=priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
        )
        return [self.handle(job) for job in jobs]

    def run_batch_once(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        limit: int = 1,
        priority: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
    ) -> builtins.list[FlowRecord | bytes]:
        jobs = self.claim_due(
            state_name,
            worker=worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=limit,
            priority=priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
        )
        return self.handle_claimed_batch(state_name, jobs)

    def handle_claimed_batch(
        self,
        state_name: str,
        jobs: Sequence[FlowRecord | ClaimedItem],
    ) -> builtins.list[FlowRecord | bytes]:
        return cast(
            builtins.list[FlowRecord | bytes],
            self._handle_known_state_batch(state_name, jobs),
        )

    def handle_claimed_batch_count(
        self,
        state_name: str,
        jobs: Sequence[FlowRecord | ClaimedItem],
    ) -> int:
        return cast(int, self._handle_known_state_batch(state_name, jobs, materialize=False))

    def get(self, id: str, *, partition_key: str | None = None) -> FlowRecord | None:
        return self.client.get(id, partition_key=partition_key)

    def history(self, id: str, **kwargs: Any) -> builtins.list[Any]:
        return self.client.history(id, **kwargs)

    def list(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.list(self.type, **kwargs)

    def terminals(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.terminals(self.type, **kwargs)

    def failures(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.failures(self.type, **kwargs)

    def by_parent(self, parent_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.by_parent(parent_flow_id, **kwargs)

    def by_root(self, root_flow_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.by_root(root_flow_id, **kwargs)

    def by_correlation(self, correlation_id: str, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.by_correlation(correlation_id, **kwargs)

    def info(self, **kwargs: Any) -> dict[Any, Any]:
        return self.client.info(self.type, **kwargs)

    def stuck(self, **kwargs: Any) -> builtins.list[FlowRecord]:
        return self.client.stuck(self.type, **kwargs)

    def cancel(self, id: str, **kwargs: Any) -> FlowRecord:
        return cast(FlowRecord, self.client.cancel(id, **kwargs))

    def rewind(self, id: str, **kwargs: Any) -> FlowRecord:
        return cast(FlowRecord, self.client.rewind(id, **kwargs))

    def spawn_children(
        self,
        parent: FlowRecord,
        children: builtins.list[ChildSpec],
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("partition_key", parent.partition_key)
        kwargs.setdefault("lease_token", parent.lease_token)
        kwargs.setdefault("fencing_token", parent.fencing_token)
        return self.client.spawn_children(parent.id, children, **kwargs)

    def child(
        self,
        id: str,
        *,
        payload: Any = None,
        partition_key: str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
    ) -> ChildSpec:
        return ChildSpec(
            id=id,
            type=self.type,
            payload=payload,
            partition_key=partition_key,
            values=values,
            value_refs=value_refs,
        )

    def context(
        self, job: FlowRecord | ClaimedItem, state_name: str | None = None
    ) -> WorkflowContext:
        return WorkflowContext(self, job, state_name or self._logical_state(job))

    def handle(self, job: FlowRecord | ClaimedItem) -> FlowRecord | bytes:
        state_name = self._logical_state(job)
        handler = self._handler_for(state_name)
        try:
            outcome = handler(self.context(job, state_name))
        except Exception as exc:
            return self._handle_exception(job, exc, state_name=state_name)
        return self.apply(job, outcome, state_name=state_name)

    def handle_batch(
        self, jobs: Sequence[FlowRecord | ClaimedItem]
    ) -> builtins.list[FlowRecord | bytes]:
        if not jobs:
            return []

        planned: builtins.list[tuple[FlowRecord | ClaimedItem, str, Outcome]] = []
        for job in jobs:
            state_name = self._logical_state(job)
            handler = self._handler_for(state_name)
            try:
                outcome = handler(self.context(job, state_name))
            except Exception as exc:
                outcome = self._exception_outcome(job, exc, state_name=state_name)
            planned.append((job, state_name, outcome))

        _first_job, first_state, first_outcome = planned[0]
        if all(
            state_name == first_state and outcome == first_outcome
            for _job, state_name, outcome in planned
        ):
            return cast(
                builtins.list[FlowRecord | bytes],
                self._apply_uniform_batch(
                    [job for job, _state_name, _outcome in planned],
                    first_state,
                    first_outcome,
                ),
            )

        return [
            self.apply(job, outcome, state_name=state_name) for job, state_name, outcome in planned
        ]

    def _handle_known_state_batch(
        self,
        state_name: str,
        jobs: Sequence[FlowRecord | ClaimedItem],
        *,
        materialize: bool = True,
    ) -> builtins.list[FlowRecord | bytes] | int:
        if not jobs:
            return [] if materialize else 0

        handler = self._handler_for(state_name)
        mixed_outcomes: builtins.list[Outcome] | None = None

        for idx, job in enumerate(jobs):
            try:
                outcome = handler(self.context(job, state_name))
            except Exception as exc:
                outcome = self._exception_outcome(job, exc, state_name=state_name)

            if idx == 0:
                first_outcome = outcome
                continue

            if mixed_outcomes is None:
                if outcome == first_outcome:
                    continue
                mixed_outcomes = [first_outcome for _ in range(idx)]

            mixed_outcomes.append(outcome)

        if mixed_outcomes is None:
            return self._apply_uniform_batch(
                jobs,
                state_name,
                first_outcome,
                materialize=materialize,
            )

        if materialize:
            return [
                self.apply(job, outcome, state_name=state_name)
                for job, outcome in zip(jobs, mixed_outcomes, strict=False)
            ]

        for job, outcome in zip(jobs, mixed_outcomes, strict=False):
            self.apply(job, outcome, state_name=state_name)
        return len(jobs)

    def apply(
        self,
        job: FlowRecord | ClaimedItem,
        outcome: Outcome,
        *,
        state_name: str | None = None,
    ) -> FlowRecord | bytes:
        logical_state = state_name or self._logical_state(job)
        common: dict[str, Any] = {
            "lease_token": job.lease_token,
            "fencing_token": job.fencing_token,
            "partition_key": job.partition_key,
            "return_record": self._states[logical_state].return_record,
        }
        if not isinstance(outcome, (Transition, Complete, Retry, Fail)):
            outcome = complete(result=outcome)

        if isinstance(outcome, Transition):
            return self.client.transition(
                job.id,
                from_state=job.state,
                to_state=outcome.to_state,
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                priority=outcome.priority,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                **common,
            )
        if isinstance(outcome, Complete):
            return self.client.complete(
                job.id,
                result=outcome.result,
                payload=outcome.payload,
                ttl_ms=outcome.ttl_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                **common,
            )
        if isinstance(outcome, Retry):
            return self.client.retry(
                job.id,
                error=outcome.error,
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                **common,
            )
        if isinstance(outcome, Fail):
            return self.client.fail(
                job.id,
                error=outcome.error,
                payload=outcome.payload,
                ttl_ms=outcome.ttl_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                **common,
            )
        raise FerricStoreError(f"unknown workflow outcome: {outcome!r}")

    def _apply_uniform_batch(
        self,
        jobs: Sequence[FlowRecord | ClaimedItem],
        state_name: str,
        outcome: Outcome,
        *,
        materialize: bool = True,
    ) -> builtins.list[FlowRecord | bytes] | int:
        if len(jobs) > FLOW_MANY_BATCH_LIMIT:
            if not materialize:
                total = 0
                for offset in range(0, len(jobs), FLOW_MANY_BATCH_LIMIT):
                    total += cast(
                        int,
                        self._apply_uniform_batch(
                            jobs[offset : offset + FLOW_MANY_BATCH_LIMIT],
                            state_name,
                            outcome,
                            materialize=False,
                        ),
                    )
                return total

            results: builtins.list[FlowRecord | bytes] = []
            for offset in range(0, len(jobs), FLOW_MANY_BATCH_LIMIT):
                chunk_response = cast(
                    builtins.list[FlowRecord | bytes],
                    self._apply_uniform_batch(
                        jobs[offset : offset + FLOW_MANY_BATCH_LIMIT],
                        state_name,
                        outcome,
                        materialize=True,
                    ),
                )
                results.extend(chunk_response)
            return results

        partition_key = self._uniform_partition_key(jobs)

        if not isinstance(outcome, (Transition, Complete, Retry, Fail)):
            outcome = complete(result=outcome)

        if isinstance(outcome, Transition):
            from_state = self._uniform_current_state(jobs)
            if from_state is None:
                if materialize:
                    return [self.apply(job, outcome, state_name=state_name) for job in jobs]
                for job in jobs:
                    self.apply(job, outcome, state_name=state_name)
                return len(jobs)
            response = self.client.transition_many(
                partition_key,
                from_state=from_state,
                to_state=outcome.to_state,
                items=cast(builtins.list[FencedItem], jobs),
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                priority=outcome.priority,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                independent=True,
            )
            if not materialize:
                return len(jobs)
            return self._batch_response_list(response, len(jobs))

        if isinstance(outcome, Complete):
            response = self.client.complete_many(
                partition_key,
                cast(builtins.list[ClaimedItem], jobs),
                result=outcome.result,
                payload=outcome.payload,
                ttl_ms=outcome.ttl_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                independent=True,
            )
            if not materialize:
                return len(jobs)
            return self._batch_response_list(response, len(jobs))

        if isinstance(outcome, Retry):
            response = self.client.retry_many(
                partition_key,
                cast(builtins.list[ClaimedItem], jobs),
                error=outcome.error,
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                independent=True,
            )
            if not materialize:
                return len(jobs)
            return self._batch_response_list(response, len(jobs))

        if isinstance(outcome, Fail):
            response = self.client.fail_many(
                partition_key,
                cast(builtins.list[ClaimedItem], jobs),
                error=outcome.error,
                payload=outcome.payload,
                ttl_ms=outcome.ttl_ms,
                values=outcome.values,
                value_refs=outcome.value_refs,
                drop_values=outcome.drop_values,
                override_values=outcome.override_values,
                independent=True,
            )
            if not materialize:
                return len(jobs)
            return self._batch_response_list(response, len(jobs))

        raise FerricStoreError(f"unknown workflow outcome: {outcome!r}")

    def _exception_outcome(
        self,
        job: FlowRecord | ClaimedItem,
        exc: Exception,
        *,
        state_name: str,
    ) -> Outcome:
        config = self._states[state_name]
        if config.on_error == "raise":
            raise exc
        if config.on_error == "fail":
            return Fail(error=str(exc))
        return Retry(error=str(exc))

    def _handle_exception(
        self,
        job: FlowRecord | ClaimedItem,
        exc: Exception,
        *,
        state_name: str | None = None,
    ) -> FlowRecord | bytes:
        logical_state = state_name or self._logical_state(job)
        return self.apply(
            job,
            self._exception_outcome(job, exc, state_name=logical_state),
            state_name=logical_state,
        )

    @staticmethod
    def _logical_state(job: FlowRecord | ClaimedItem) -> str:
        return job.run_state or job.state

    @staticmethod
    def _uniform_partition_key(jobs: Sequence[FlowRecord | ClaimedItem]) -> str | None:
        first = jobs[0].partition_key
        if first is not None and all(job.partition_key == first for job in jobs):
            return first
        return None

    @staticmethod
    def _uniform_current_state(jobs: Sequence[FlowRecord | ClaimedItem]) -> str | None:
        first = jobs[0].state
        if all(job.state == first for job in jobs):
            return first
        return None

    @staticmethod
    def _batch_response_list(response: Any, count: int) -> builtins.list[FlowRecord | bytes]:
        if isinstance(response, list):
            return response
        return [response] * count

    def _stamp_compact_jobs(
        self,
        jobs: builtins.list[ClaimedItem],
        state_name: str,
    ) -> builtins.list[ClaimedItem]:
        for job in jobs:
            object.__setattr__(job, "type", self.type)
            object.__setattr__(job, "state", "running")
            object.__setattr__(job, "run_state", state_name)
        return jobs

    def _discover_state_handlers(
        self,
    ) -> tuple[dict[str, StateConfig], dict[str, Handler]]:
        states: dict[str, StateConfig] = {}
        handlers: dict[str, Handler] = {}
        for name in dir(self):
            member = getattr(self, name)
            target = getattr(member, "__func__", member)
            config = getattr(target, "__ferric_state__", None)
            if isinstance(config, StateConfig):
                states[config.name] = config
                handlers[config.name] = member
        return states, handlers

    def _handler_for(self, state_name: str) -> Handler:
        handler = self._handlers.get(state_name)
        if handler is not None:
            return handler
        raise FerricStoreError(f"no handler for state {state_name!r}")


class FlowWorkflow(Workflow):
    """Constructor/decorator workflow API.

    This is the primary sync workflow API. It uses the same runtime as
    class-based ``Workflow`` subclasses, but keeps configuration in the
    constructor and registers handlers with ``@workflow.state(...)``.
    """

    def __init__(
        self,
        client: FlowClient | str,
        *,
        claim_client: FlowClient | str | None = None,
        type: str,
        initial_state: str = "queued",
        partition_by: Sequence[str] = (),
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> None:
        self.client = FlowClient.from_url(client) if isinstance(client, str) else client
        if claim_client is None:
            self.claim_client = self.client
        else:
            self.claim_client = (
                FlowClient.from_url(claim_client) if isinstance(claim_client, str) else claim_client
            )
        self.type = type
        self.initial_state = initial_state
        self.partition_by = tuple(partition_by)
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._states: dict[str, StateConfig] = {}
        self._handlers: dict[str, Handler] = {}

    def state(
        self,
        name: str,
        *,
        lease_ms: int = 30_000,
        claim_payload: bool = True,
        claim_record: bool = True,
        claim_values: builtins.list[str] | None = None,
        value_max_bytes: int | None = None,
        exception_policy: ExceptionPolicy | str | None = None,
        on_error: ExceptionPolicy | str | None = None,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        return_record: bool = False,
    ) -> Callable[[Handler], Handler]:
        def decorate(fn: Handler) -> Handler:
            if name in self._states:
                raise ValueError(f"duplicate workflow state: {name!r}")
            handler = state(
                name,
                lease_ms=lease_ms,
                claim_payload=claim_payload,
                claim_record=claim_record,
                claim_values=claim_values,
                value_max_bytes=(
                    value_max_bytes
                    if value_max_bytes is not None
                    else self.value_config.value_max_bytes
                ),
                exception_policy=(
                    exception_policy
                    if exception_policy is not None
                    or on_error is not None
                    or self.worker_config is None
                    else self.worker_config.exception_policy
                ),
                on_error=on_error,
                retry_policy=retry_policy,
                retry=retry,
                return_record=return_record,
            )(fn)
            config = cast(Any, handler).__ferric_state__
            self._states[config.name] = config
            self._handlers[config.name] = handler
            return handler

        return decorate

    def on(
        self,
        name: str,
        **kwargs: Any,
    ) -> Callable[[Handler], Handler]:
        return self.state(name, **kwargs)

    def start(self, id: str, *, payload: Any = None, **attrs: Any) -> FlowRecord | bytes:
        return self.enqueue(id, payload=payload, **attrs)


class WorkflowClient:
    """High-level client for durable state-machine workflows."""

    def __init__(
        self,
        client: FlowClient | str,
        *,
        claim_client: FlowClient | str | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _owns_clients: bool = False,
    ) -> None:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        self._url = client if isinstance(client, str) else None
        self._base_url_kwargs: dict[str, Any] = {}
        self._claim_client_explicit = claim_client is not None
        self._owned_extra_claim_flows: builtins.list[FlowClient] = []
        self._claim_pool_size = claim_pool_size
        self.flow = (
            FlowClient.from_url(client, max_connections=command_pool_size)
            if isinstance(client, str)
            else client
        )
        if claim_client is None:
            self.claim_flow = (
                FlowClient.from_url(client, max_connections=claim_pool_size)
                if isinstance(client, str)
                else self.flow
            )
        else:
            self.claim_flow = (
                FlowClient.from_url(claim_client, max_connections=claim_pool_size)
                if isinstance(claim_client, str)
                else claim_client
            )
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._owns_flow = _owns_clients or isinstance(client, str)
        self._owns_claim_flow = (
            _owns_clients
            or isinstance(claim_client, str)
            or (claim_client is None and isinstance(client, str))
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> WorkflowClient:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_kwargs = dict(kwargs)
        command_kwargs.setdefault("max_connections", command_pool_size)
        claim_kwargs = dict(kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        instance = cls(
            FlowClient.from_url(url, **command_kwargs),
            claim_client=FlowClient.from_url(url, **claim_kwargs),
            retry_policy=retry_policy,
            worker_config=worker_config,
            value_config=value_config,
            _owns_clients=True,
        )
        instance._url = url
        instance._base_url_kwargs = dict(kwargs)
        instance._claim_client_explicit = False
        instance._claim_pool_size = claim_pool_size
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> FlowClient:
        if self._claim_client_explicit or self._url is None or worker_config is None:
            return self.claim_flow
        _, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        if claim_pool_size == self._claim_pool_size:
            return self.claim_flow
        claim_kwargs = dict(self._base_url_kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        claim_flow = FlowClient.from_url(self._url, **claim_kwargs)
        self._owned_extra_claim_flows.append(claim_flow)
        return claim_flow

    def workflow(
        self,
        *,
        type: str,
        initial_state: str = "queued",
        partition_by: Sequence[str] = (),
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> FlowWorkflow:
        resolved_worker_config = worker_config if worker_config is not None else self.worker_config
        return FlowWorkflow(
            self.flow,
            claim_client=self._claim_flow_for_worker_config(resolved_worker_config),
            type=type,
            initial_state=initial_state,
            partition_by=partition_by,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            worker_config=resolved_worker_config,
            value_config=value_config if value_config is not None else self.value_config,
        )

    def install_policy(
        self,
        type: str,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        states: dict[str, RetryPolicy] | None = None,
    ) -> Any:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        return self.flow.install_policy(type, retry=resolved_retry_policy, states=states)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)

    def close(self) -> None:
        for claim_flow in self._owned_extra_claim_flows:
            claim_flow.close()
        self._owned_extra_claim_flows.clear()
        if self._owns_claim_flow and self.claim_flow is not self.flow:
            self.claim_flow.close()
        if self._owns_flow:
            self.flow.close()


class WorkflowWorker:
    """High-level state-machine worker for Workflow subclasses."""

    def __init__(
        self,
        workflow: Workflow,
        *,
        worker: str | None = None,
        state: str | None = None,
        states: Sequence[str] | None = None,
        batch_size: int = 10,
        priority: int | None = 0,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        partition_key: str | None = None,
        partition_keys: Sequence[str] | None = None,
        claim_partition_batch_size: int | None = None,
        block_ms: int | None = None,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        apply_async_depth: int = 0,
    ) -> None:
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")
        if states is not None and not states:
            raise ValueError("states must be non-empty")
        if partition_keys is not None and not partition_keys:
            raise ValueError("partition_keys must be non-empty")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if claim_partition_batch_size is not None and claim_partition_batch_size <= 0:
            raise ValueError("claim_partition_batch_size must be positive")
        if block_ms is not None and block_ms < 0:
            raise ValueError("block_ms must be non-negative")
        if apply_async_depth < 0:
            raise ValueError("apply_async_depth must be non-negative")
        if idle_sleep_s < 0:
            raise ValueError("idle_sleep_s must be non-negative")
        if max_idle_sleep_s is not None and max_idle_sleep_s < 0:
            raise ValueError("max_idle_sleep_s must be non-negative")

        self.workflow = workflow
        self.worker = worker or f"{workflow.type}:workflow-worker:{uuid.uuid4().hex}"
        if state is not None:
            self.states = [state]
        elif states is not None:
            self.states = list(states)
        else:
            self.states = list(workflow._states)
        if not self.states:
            raise ValueError("workflow has no states")
        unknown_states = [name for name in self.states if name not in workflow._states]
        if unknown_states:
            raise ValueError(f"unknown workflow states: {unknown_states!r}")

        self.batch_size = batch_size
        self.priority = priority
        self.reclaim_expired = reclaim_expired
        self.reclaim_ratio = reclaim_ratio
        self.partition_key = partition_key
        self.partition_keys = list(partition_keys) if partition_keys is not None else None
        self.claim_partition_batch_size = (
            claim_partition_batch_size
            if claim_partition_batch_size is not None
            else len(self.partition_keys or []) or 1
        )
        self.block_ms = block_ms
        self.idle_sleep_s = idle_sleep_s
        self.max_idle_sleep_s = max(max_idle_sleep_s or idle_sleep_s, idle_sleep_s)
        self.apply_async_depth = apply_async_depth
        self._state_cursor = 0
        self._partition_cursor = 0
        self._running = False
        self._thread: threading.Thread | None = None
        self._totals = WorkflowWorkerResult()
        self._apply_executor = (
            ThreadPoolExecutor(max_workers=apply_async_depth) if apply_async_depth > 0 else None
        )
        self._pending_applies: builtins.list[Future[int]] = []

    def run(self) -> None:
        self.run_forever()

    def run_forever(self) -> None:
        self._run_loop()

    def start(self, *, daemon: bool = True) -> WorkflowWorker:
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("workflow worker already started")

        self._thread = threading.Thread(target=self._run_loop, daemon=daemon)
        self._thread.start()
        return self

    def join(self, timeout: float | None = None) -> WorkflowWorkerResult:
        if self._thread is not None:
            self._thread.join(timeout)
        return self.stats

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> WorkflowWorkerResult:
        return self._totals

    def _run_loop(self) -> None:
        self._running = True
        idle_sleep_s = self.idle_sleep_s
        try:
            while self._running:
                result = self.run_once()
                self._totals = self._merge_results(self._totals, result)
                if result.claimed == 0:
                    time.sleep(idle_sleep_s)
                    idle_sleep_s = min(
                        self.max_idle_sleep_s,
                        max(idle_sleep_s * 2, self.idle_sleep_s),
                    )
                else:
                    idle_sleep_s = self.idle_sleep_s
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    def close(self) -> WorkflowWorkerResult:
        self.stop()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join()
        result = self.flush()
        self._totals = self._merge_results(self._totals, result)
        if self._apply_executor is not None:
            self._apply_executor.shutdown(wait=True)
            self._apply_executor = None
        return result

    def flush(self) -> WorkflowWorkerResult:
        return self._drain_pending_applies(block=True)

    def run_once(self) -> WorkflowWorkerResult:
        result = self._drain_pending_applies(block=False)
        if self._should_claim_any_state():
            return self._merge_results(result, self._run_once_any_state())

        state_name, partition_key, partition_keys = self._next_claim_target()
        jobs = self.workflow.claim_due(
            state_name,
            worker=self.worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=self.batch_size,
            priority=self.priority,
            reclaim_expired=self.reclaim_expired,
            reclaim_ratio=self.reclaim_ratio,
            block_ms=self.block_ms,
        )
        result = self._merge_results(result, WorkflowWorkerResult(claim_calls=1))
        if not jobs:
            return self._merge_results(result, WorkflowWorkerResult(empty_claims=1))

        claimed = len(jobs)
        result = self._merge_results(result, WorkflowWorkerResult(claimed=claimed))
        if self._apply_executor is not None and not self.workflow._states[state_name].return_record:
            while len(self._pending_applies) >= self.apply_async_depth:
                result = self._merge_results(
                    result, self._drain_pending_applies(block=True, limit=1)
                )
            self._pending_applies.append(
                self._apply_executor.submit(
                    self.workflow.handle_claimed_batch_count,
                    state_name,
                    jobs,
                )
            )
            return result

        if self.workflow._states[state_name].return_record:
            applied = len(self.workflow.handle_claimed_batch(state_name, jobs))
        else:
            applied = self.workflow.handle_claimed_batch_count(state_name, jobs)
        return self._merge_results(result, WorkflowWorkerResult(applied=applied))

    def _run_once_any_state(self) -> WorkflowWorkerResult:
        partition_key, partition_keys = self._next_claim_partition()
        jobs = self.workflow.claim_client.claim_jobs(
            self.workflow.type,
            worker=self.worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=self.batch_size,
            priority=self.priority,
            reclaim_expired=self.reclaim_expired,
            reclaim_ratio=self.reclaim_ratio,
            block_ms=self.block_ms,
            include_state=True,
        )
        result = WorkflowWorkerResult(claim_calls=1)
        if not jobs:
            return self._merge_results(result, WorkflowWorkerResult(empty_claims=1))

        for job in jobs:
            object.__setattr__(job, "type", self.workflow.type)
            object.__setattr__(job, "state", "running")

        result = self._merge_results(result, WorkflowWorkerResult(claimed=len(jobs)))

        for state_name, state_jobs in self._group_jobs_by_run_state(jobs).items():
            if (
                self._apply_executor is not None
                and not self.workflow._states[state_name].return_record
            ):
                while len(self._pending_applies) >= self.apply_async_depth:
                    result = self._merge_results(
                        result, self._drain_pending_applies(block=True, limit=1)
                    )
                self._pending_applies.append(
                    self._apply_executor.submit(
                        self.workflow.handle_claimed_batch_count,
                        state_name,
                        state_jobs,
                    )
                )
            elif self.workflow._states[state_name].return_record:
                applied = len(self.workflow.handle_claimed_batch(state_name, state_jobs))
                result = self._merge_results(result, WorkflowWorkerResult(applied=applied))
            else:
                applied = self.workflow.handle_claimed_batch_count(state_name, state_jobs)
                result = self._merge_results(result, WorkflowWorkerResult(applied=applied))

        return result

    def _should_claim_any_state(self) -> bool:
        if not self.block_ms or len(self.states) <= 1:
            return False
        if set(self.states) != set(self.workflow._states):
            return False
        return all(
            not config.claim_record and not config.claim_payload and not config.claim_values
            for name, config in self.workflow._states.items()
            if name in self.states
        )

    def _group_jobs_by_run_state(
        self, jobs: builtins.list[ClaimedItem]
    ) -> dict[str, builtins.list[ClaimedItem]]:
        grouped: dict[str, builtins.list[ClaimedItem]] = {}
        for job in jobs:
            state_name = job.run_state
            if state_name not in self.workflow._states:
                raise ValueError(f"no handler for workflow state: {state_name!r}")
            grouped.setdefault(state_name, []).append(job)
        return grouped

    def _next_state(self) -> str:
        state_name = self.states[self._state_cursor]
        self._state_cursor = (self._state_cursor + 1) % len(self.states)
        return state_name

    def _next_claim_target(self) -> tuple[str, str | None, builtins.list[str] | None]:
        if self.partition_key is not None:
            return self._next_state(), self.partition_key, None
        if not self.partition_keys:
            return self._next_state(), None, None

        state_name = self.states[self._state_cursor]
        count = min(
            self.claim_partition_batch_size,
            len(self.partition_keys) - self._partition_cursor,
        )
        keys = self.partition_keys[self._partition_cursor : self._partition_cursor + count]
        self._partition_cursor += count
        if self._partition_cursor >= len(self.partition_keys):
            self._partition_cursor = 0
            self._state_cursor = (self._state_cursor + 1) % len(self.states)
        if len(keys) == 1:
            return state_name, keys[0], None
        return state_name, None, keys

    def _next_claim_partition(self) -> tuple[str | None, builtins.list[str] | None]:
        if self.partition_key is not None:
            return self.partition_key, None
        if not self.partition_keys:
            return None, None

        count = min(self.claim_partition_batch_size, len(self.partition_keys))
        keys = [
            self.partition_keys[(self._partition_cursor + offset) % len(self.partition_keys)]
            for offset in range(count)
        ]
        self._partition_cursor = (self._partition_cursor + count) % len(self.partition_keys)
        if len(keys) == 1:
            return keys[0], None
        return None, keys

    def _drain_pending_applies(
        self,
        *,
        block: bool,
        limit: int | None = None,
    ) -> WorkflowWorkerResult:
        if not self._pending_applies:
            return WorkflowWorkerResult()

        drained = WorkflowWorkerResult()
        count = 0
        while self._pending_applies and (limit is None or count < limit):
            ready_index = None
            for idx, future in enumerate(self._pending_applies):
                if block or future.done():
                    ready_index = idx
                    break
            if ready_index is None:
                break
            future = self._pending_applies.pop(ready_index)
            drained = self._merge_results(drained, WorkflowWorkerResult(applied=future.result()))
            count += 1
        return drained

    @staticmethod
    def _merge_results(
        left: WorkflowWorkerResult, right: WorkflowWorkerResult
    ) -> WorkflowWorkerResult:
        return WorkflowWorkerResult(
            claimed=left.claimed + right.claimed,
            applied=left.applied + right.applied,
            claim_calls=left.claim_calls + right.claim_calls,
            empty_claims=left.empty_claims + right.empty_claims,
        )
