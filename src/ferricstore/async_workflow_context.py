from __future__ import annotations

import asyncio
import builtins
import inspect
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, cast

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_queue_runtime import (
    _CURRENT_PARTITION,
    AsyncFlowJob,
)
from ferricstore.async_workflow_budget import AsyncWorkflowBudget as AsyncWorkflowBudget
from ferricstore.lifecycle_core import (
    await_cancellation_safe,
    raise_primary_with_cleanup,
)
from ferricstore.policy_types import PolicySnapshot
from ferricstore.types import (
    BudgetPolicy,
    BudgetResult,
    ChildSpec,
    ClaimedFlow,
    CreateItem,
    EffectResult,
    FlowRecord,
)
from ferricstore.workflow_effect_core import (
    resolve_effect_replay,
    resolve_external_id,
    resolve_operation_digest,
)

if TYPE_CHECKING:
    from ferricstore.async_workflow_runtime import AsyncWorkflow


class AsyncWorkflowFlowCommands:
    """Async Flow command helper bound to the current workflow job."""

    __slots__ = ("_ctx",)

    def __init__(self, ctx: AsyncWorkflowContext) -> None:
        self._ctx = ctx

    @property
    def client(self) -> AsyncFlowClient:
        return self._ctx.client

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)

    def _partition(self, partition_key: str | bytes | None | object) -> str | bytes | None:
        if partition_key is _CURRENT_PARTITION:
            return self._ctx.partition_key
        return cast(str | bytes | None, partition_key)

    def _type(self, type: str | None) -> str:
        return self._ctx.workflow.type if type is None else type

    def _state(self, type: str | None, state: str | None) -> str:
        if state is not None:
            return state
        if type is None or type == self._ctx.workflow.type:
            return self._ctx.workflow.initial_state
        return "queued"

    async def get(
        self,
        id: str | None = None,
        *,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> Any:
        return await self.client.get(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def history(
        self,
        id: str | None = None,
        *,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> Any:
        return await self.client.history(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def create(
        self,
        id: str,
        *,
        type: str | None = None,
        state: str | None = None,
        payload: Any = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.create(
            id,
            type=self._type(type),
            state=self._state(type, state),
            payload=payload,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def enqueue(
        self,
        id: str,
        *,
        type: str | None = None,
        state: str | None = None,
        payload: Any = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.enqueue(
            id,
            type=self._type(type),
            state=self._state(type, state),
            payload=payload,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def start_and_claim(
        self,
        id: str,
        *,
        type: str | None = None,
        initial_state: str | None = None,
        worker: str,
        payload: Any = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> FlowRecord:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.start_and_claim(
            id,
            type=self._type(type),
            initial_state=self._state(type, None) if initial_state is None else initial_state,
            worker=worker,
            payload=payload,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def create_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str | None = None,
        state: str | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> builtins.list[FlowRecord] | Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.create_many(
            self._partition(partition_key),
            items,
            type=self._type(type),
            state=self._state(type, state),
            **kwargs,
        )

    async def enqueue_many(
        self,
        items: builtins.list[CreateItem],
        *,
        type: str | None = None,
        state: str | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any] | Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.enqueue_many(
            items,
            type=self._type(type),
            state=self._state(type, state),
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def run_steps_many(
        self,
        items: builtins.list[str | dict[str, Any] | CreateItem],
        *,
        type: str | None = None,
        states: Sequence[str] | None = None,
        steps: int | None = None,
        worker: str,
        payload: Any = None,
        result: Any = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> bytes:
        return await self.client.run_steps_many(
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

    async def claim_due(
        self, type: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord] | builtins.list[ClaimedFlow]:
        return await self.client.claim_due(self._type(type), **kwargs)

    async def claim_flows(
        self, type: str | None = None, **kwargs: Any
    ) -> builtins.list[ClaimedFlow]:
        return await self.client.claim_flows(self._type(type), **kwargs)

    async def claim_jobs(
        self, type: str | None = None, **kwargs: Any
    ) -> builtins.list[ClaimedFlow]:
        """Compatibility alias for claim_flows()."""
        return await self.claim_flows(type, **kwargs)

    async def signal(
        self,
        id: str | None = None,
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        return await self.client.signal(self._ctx.id if id is None else id, **kwargs)

    async def flow_signal(self, id: str | None = None, **kwargs: Any) -> Any:
        return await self.signal(id, **kwargs)

    async def reclaim(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return cast(
            builtins.list[FlowRecord],
            await self.client.reclaim(self._type(type), **kwargs),
        )

    async def extend_lease(
        self,
        id: str | None = None,
        lease_token: bytes | None = None,
        *,
        fencing_token: int | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> FlowRecord:
        return await self.client.extend_lease(
            self._ctx.id if id is None else id,
            self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def transition(
        self,
        to_state: str,
        *,
        id: str | None = None,
        from_state: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.transition(
            self._ctx.id if id is None else id,
            from_state=self._ctx.state if from_state is None else from_state,
            to_state=to_state,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def step_continue(
        self,
        to_state: str,
        *,
        id: str | None = None,
        from_state: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        **kwargs: Any,
    ) -> FlowRecord | ClaimedFlow:
        return await self.client.step_continue(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            from_state=self._ctx.state if from_state is None else from_state,
            to_state=to_state,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            **kwargs,
        )

    async def complete(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.complete(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def retry(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.retry(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def fail(
        self,
        id: str | None = None,
        *,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.fail(
            self._ctx.id if id is None else id,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def cancel(
        self,
        id: str | None = None,
        *,
        fencing_token: int | None = None,
        lease_token: bytes | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.cancel(
            self._ctx.id if id is None else id,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def rewind(
        self,
        id: str | None = None,
        *,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        return_record: bool = False,
        **kwargs: Any,
    ) -> Any:
        return await self.client.rewind(
            self._ctx.id if id is None else id,
            partition_key=self._partition(partition_key),
            return_record=return_record,
            **kwargs,
        )

    async def list(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.list(self._type(type), **kwargs)

    async def terminals(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.terminals(self._type(type), **kwargs)

    async def failures(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.failures(self._type(type), **kwargs)

    async def by_parent(
        self, parent_flow_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        target = self._ctx.id if parent_flow_id is None else parent_flow_id
        return await self.client.by_parent(target, **kwargs)

    async def by_root(
        self, root_flow_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        root = root_flow_id
        if root is None:
            root = getattr(self._ctx, "root_flow_id", None) or self._ctx.id
        return await self.client.by_root(root, **kwargs)

    async def by_correlation(
        self, correlation_id: str | None = None, **kwargs: Any
    ) -> builtins.list[FlowRecord]:
        correlation = correlation_id
        if correlation is None:
            correlation = getattr(self._ctx, "correlation_id", None)
        if correlation is None:
            raise ValueError("correlation_id is required when current flow has no correlation_id")
        return await self.client.by_correlation(correlation, **kwargs)

    async def info(self, type: str | None = None, **kwargs: Any) -> dict[Any, Any]:
        return await self.client.info(self._type(type), **kwargs)

    async def stuck(self, type: str | None = None, **kwargs: Any) -> builtins.list[FlowRecord]:
        return await self.client.stuck(self._type(type), **kwargs)

    async def value_put(self, value: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        kwargs.setdefault("owner_flow_id", self._ctx.id)
        return await self.client.value_put(value, **kwargs)

    async def put_value(self, name: str, value: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("name", name)
        kwargs.setdefault("partition_key", self._ctx.partition_key)
        kwargs.setdefault("owner_flow_id", self._ctx.id)
        return await self.client.value_put(value, **kwargs)

    async def value_mget(
        self, refs: builtins.list[str], *, max_bytes: int | None = None
    ) -> builtins.list[Any]:
        return await self.client.value_mget(refs, max_bytes=max_bytes)

    async def value(self, name: str, default: Any = None, *, local_cache: bool = False) -> Any:
        return await self._ctx.value(name, default, local_cache=local_cache)

    async def values(
        self, names: builtins.list[str], *, local_cache: bool = False
    ) -> dict[str, Any]:
        return await self._ctx.value_many(names, local_cache=local_cache)

    async def spawn_children(
        self,
        children: builtins.list[ChildSpec],
        *,
        parent_flow_id: str | None = None,
        partition_key: str | bytes | None | object = _CURRENT_PARTITION,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        max_active_ms: int | float | str | None = None,
        **kwargs: Any,
    ) -> Any:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.spawn_children(
            self._ctx.id if parent_flow_id is None else parent_flow_id,
            children,
            partition_key=self._partition(partition_key),
            lease_token=self._ctx.lease_token if lease_token is None else lease_token,
            fencing_token=self._ctx.fencing_token if fencing_token is None else fencing_token,
            **kwargs,
        )

    async def install_policy(
        self,
        type: str | None = None,
        *,
        max_active_ms: int | float | str | None = None,
        replace: bool = True,
        expected_generation: int | None = None,
        **kwargs: Any,
    ) -> PolicySnapshot:
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.install_policy(
            self._type(type),
            replace=replace,
            expected_generation=expected_generation,
            **kwargs,
        )

    async def policy_get(self, type: str | None = None, **kwargs: Any) -> PolicySnapshot:
        return await self.client.policy_get(self._type(type), **kwargs)


class AsyncWorkflowEffect:
    """Async external-effect helper bound to an async workflow job."""

    def __init__(
        self,
        ctx: AsyncWorkflowContext,
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
        self._reservation_task: asyncio.Task[EffectResult] | None = None
        self._settlement_task: asyncio.Task[EffectResult] | None = None
        self._settlement_kind: str | None = None

    async def reserve(self) -> EffectResult:
        if self.reservation is not None:
            return self.reservation

        async def reserve_and_record() -> EffectResult:
            reservation = await self.ctx.client.effect_reserve(
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
            self.reservation = reservation
            self._started_at = asyncio.get_running_loop().time()
            return reservation

        task = self._reservation_task
        if task is None:
            task = asyncio.create_task(reserve_and_record())
            self._reservation_task = task
        try:
            return cast(EffectResult, await await_cancellation_safe(task))
        except asyncio.CancelledError:
            if task.cancelled() and self._reservation_task is task:
                self._reservation_task = None
            raise
        except BaseException:
            if self._reservation_task is task:
                self._reservation_task = None
            raise

    async def confirm(
        self,
        *,
        external_id: str | None = None,
        latency_ms: int | None = None,
    ) -> EffectResult:
        if self._closed and self._result is not None:
            return self._result
        await self.reserve()

        async def confirm_and_record() -> EffectResult:
            self._result = await self.ctx.client.effect_confirm(
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

        task = self._settlement_task
        if task is None:
            task = asyncio.create_task(confirm_and_record())
            self._settlement_task = task
            self._settlement_kind = "confirm"
        try:
            return cast(EffectResult, await await_cancellation_safe(task))
        except asyncio.CancelledError:
            if task.cancelled() and self._settlement_task is task:
                self._settlement_task = None
                self._settlement_kind = None
            raise
        except BaseException:
            if self._settlement_task is task:
                self._settlement_task = None
                self._settlement_kind = None
            raise

    async def fail(
        self,
        *,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
    ) -> EffectResult:
        if self._closed and self._result is not None:
            return self._result
        await self.reserve()

        async def fail_and_record() -> EffectResult:
            self._result = await self.ctx.client.effect_fail(
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

        while True:
            task = self._settlement_task
            kind = self._settlement_kind
            if task is None:
                task = asyncio.create_task(fail_and_record())
                self._settlement_task = task
                self._settlement_kind = "fail"
                kind = "fail"
            try:
                return cast(EffectResult, await await_cancellation_safe(task))
            except asyncio.CancelledError:
                if not task.cancelled():
                    raise
                if self._settlement_task is task:
                    self._settlement_task = None
                    self._settlement_kind = None
                if kind == "confirm":
                    continue
                raise
            except BaseException:
                if self._settlement_task is task:
                    self._settlement_task = None
                    self._settlement_kind = None
                if kind == "confirm":
                    continue
                raise

    async def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        reservation = await self.reserve()
        replayed, replay_value = resolve_effect_replay(reservation, self.replay)
        if replayed:
            if inspect.isawaitable(replay_value):
                replay_value = await replay_value
            return replay_value
        try:
            result = func(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
        except BaseException as exc:
            try:
                await self.fail(error=str(exc), reason=exc.__class__.__name__)
            except BaseException as cleanup:
                raise_primary_with_cleanup(exc, exc.__traceback__, cleanup)
            raise
        await self.confirm(external_id=self._resolve_external_id(result))
        return result

    def __call__(self, func: Callable[..., Any]) -> Callable[..., Any]:
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.call(func, *args, **kwargs)

        return wrapper

    async def __aenter__(self) -> AsyncWorkflowEffect:
        await self.reserve()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            await self.confirm()
        else:
            try:
                await self.fail(
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
        return max(int((asyncio.get_running_loop().time() - self._started_at) * 1000), 0)


class AsyncWorkflowContext:
    """Async workflow handler context with value-ref helpers."""

    def __init__(self, workflow: AsyncWorkflow, job: AsyncFlowJob, state_name: str) -> None:
        self.workflow = workflow
        self.client = workflow.client
        self.job = job
        self.state_name = state_name
        self.flow = AsyncWorkflowFlowCommands(self)
        self._value_cache: dict[str, Any] = {}
        self._governance_attributes: dict[str, Any] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self.job, name)

    @property
    def id(self) -> str:
        return self.job.id

    @property
    def type(self) -> str:
        return cast(str, getattr(self.job, "type", self.workflow.type))

    @property
    def state(self) -> str:
        return getattr(self.job, "state", "running")

    @property
    def run_state(self) -> str | None:
        return getattr(self.job, "run_state", self.state_name)

    @property
    def partition_key(self) -> str | bytes | None:
        return self.job.partition_key

    @property
    def lease_token(self) -> bytes:
        return self.job.lease_token

    @property
    def fencing_token(self) -> int:
        return self.job.fencing_token

    @property
    def values(self) -> dict[str, Any]:
        return getattr(self.job, "values", None) or {}

    @property
    def value_refs(self) -> dict[str, Any]:
        return getattr(self.job, "value_refs", None) or {}

    async def value(
        self, name: str, default: Any = None, *, local_cache: bool | None = None
    ) -> Any:
        values = await self.value_many([name], local_cache=local_cache)
        return values.get(name, default)

    async def value_many(
        self, names: list[str], *, local_cache: bool | None = None
    ) -> dict[str, Any]:
        use_local_cache = (
            self.workflow.value_config.local_cache if local_cache is None else local_cache
        )
        values: dict[str, Any] = {}
        pending_names: list[str] = []
        pending_refs: list[str] = []

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
            fetched = await self.client.value_mget(
                pending_refs, max_bytes=self.workflow.value_max_bytes
            )
            for name, value in zip(pending_names, fetched, strict=True):
                values[name] = value
                if use_local_cache:
                    self._value_cache[name] = value

        return values

    def budget(
        self,
        scope: str,
        amount: int,
        *,
        limit: int | None = None,
        window_ms: int | None = None,
        usage_key: str = "amount",
        attribute_prefix: str = "governance_budget",
    ) -> AsyncWorkflowBudget:
        return AsyncWorkflowBudget(
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
    ) -> AsyncWorkflowEffect:
        return AsyncWorkflowEffect(
            self,
            effect_key,
            effect_type,
            operation_digest=operation_digest,
            idempotency_key=idempotency_key,
            governance_scope=governance_scope,
            external_id=external_id,
            replay=replay,
        )

    def _state_budget(self, policy: BudgetPolicy | None) -> AsyncWorkflowBudget | None:
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
