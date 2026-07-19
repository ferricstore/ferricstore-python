from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from types import TracebackType
from typing import Any

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_ownership import (
    AsyncOwnedClose,
    close_owned_resources_async,
    resolve_async_client_pair,
    rollback_async_resources,
)
from ferricstore.async_partitioning import (
    _validate_auto_partition_workers,
    _validate_server_shards,
)
from ferricstore.async_producer import AsyncProducerLoop
from ferricstore.async_queue_producer import _AsyncQueueProducerMixin
from ferricstore.async_queue_runtime import (
    ASYNC_QUEUE_WORKER_CONFIG_KEYS,
    FLOW_MANY_BATCH_LIMIT,
    AsyncErrorMode,
    AsyncFlowHandler,
    AsyncQueueFlowWorker,
    _client_from,
    _close_async_resource,
)
from ferricstore.async_wake import AsyncFlowWakeCoordinator
from ferricstore.client_core import FlowClient
from ferricstore.config_validation import (
    validate_bool,
    validate_nonnegative_int,
    validate_optional_nonnegative_int,
    validate_positive_int,
    validate_string_sequence,
)
from ferricstore.lifecycle_core import (
    AsyncCloseCoordinator,
    AsyncCloseTaskRegistry,
    await_cancellation_safe,
    chain_cleanup_errors,
    close_resources_async,
    raise_primary_with_cleanup,
)
from ferricstore.policy_types import PolicySnapshot
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    CreateItem,
    FlowStatePolicyLike,
    ValueConfig,
    WorkerConfig,
    normalize_exception_policy,
    resolve_worker_connection_counts,
)
from ferricstore.worker_core import (
    AsyncWorkerInvocationTracker,
    CloseDeadline,
    validate_worker_idle_timing,
)
from ferricstore.worker_models import QueueFlowWorkerResult


class AsyncQueueFlow(_AsyncQueueProducerMixin):
    """Simple async queue facade that hides FerricFlow hot-path tuning."""

    def __init__(
        self,
        client: AsyncFlowClient | FlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | FlowClient | str | Any | None = None,
        type: str,
        state: str = "queued",
        workers: int = 1,
        concurrency: int = 1,
        command_connections: int | None = None,
        claim_connections: int | None = None,
        batch_size: int = 10,
        claim_partition_batch_size: int = 1,
        complete_independent: bool = True,
        server_shards: int = 16,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        protocol_wake_hints: bool = False,
        fuse_complete_claim: bool = False,
        block_ms: int | None = None,
        producer_loop_thread: bool = False,
        exception_policy: AsyncErrorMode | None = None,
        on_error: AsyncErrorMode | None = None,
        _producer_url: str | None = None,
        _producer_url_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        workers = validate_positive_int(workers, name="workers")
        _validate_auto_partition_workers(workers)
        server_shards = _validate_server_shards(server_shards)
        concurrency = validate_positive_int(concurrency, name="concurrency")
        batch_size = validate_positive_int(batch_size, name="batch_size")
        claim_partition_batch_size = validate_positive_int(
            claim_partition_batch_size,
            name="claim_partition_batch_size",
        )
        if block_ms is not None:
            block_ms = validate_nonnegative_int(block_ms, name="block_ms")
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
        fuse_complete_claim = validate_bool(
            fuse_complete_claim,
            name="fuse_complete_claim",
        )
        producer_loop_thread = validate_bool(
            producer_loop_thread,
            name="producer_loop_thread",
        )
        idle_sleep_s, max_idle_sleep_s = validate_worker_idle_timing(
            idle_sleep_s,
            max_idle_sleep_s,
        )
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_on_error = normalize_exception_policy(
            exception_policy if exception_policy is not None else on_error,
            argument="exception_policy" if exception_policy is not None else "on_error",
        )
        resolved_claim_values = (
            list(validate_string_sequence(claim_values, name="claim_values"))
            if claim_values is not None
            else None
        )
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=workers,
            concurrency=concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
        self._url = _producer_url
        if self._url is None and isinstance(client, str):
            self._url = client
        self._producer_url_kwargs = dict(_producer_url_kwargs or {})
        command_max_connections = (
            1 if isinstance(client, str) and command_connections is None else command_pool_size
        )
        clients = resolve_async_client_pair(
            client,
            claim_client,
            from_url=AsyncFlowClient.from_url,
            normalize=_client_from,
            command_kwargs={"max_connections": command_max_connections},
            claim_kwargs={"max_connections": claim_pool_size},
        )
        self.client = clients.command
        self.claim_client = clients.claim
        self._owns_client = clients.owns_command
        self._owns_claim_client = clients.owns_claim
        self.type = type
        self.state = state
        self.workers = workers
        self.concurrency = concurrency
        self.batch_size = min(batch_size, FLOW_MANY_BATCH_LIMIT)
        self.claim_partition_batch_size = claim_partition_batch_size
        self.complete_independent = complete_independent
        self.server_shards = server_shards
        self.claim_values = resolved_claim_values
        self.value_max_bytes = value_max_bytes
        self.idle_sleep_s = idle_sleep_s
        self.max_idle_sleep_s = max_idle_sleep_s
        self.protocol_wake_hints = protocol_wake_hints
        self.fuse_complete_claim = fuse_complete_claim
        self.block_ms = block_ms
        self.producer_loop_thread = producer_loop_thread
        self._producer_loop = (
            AsyncProducerLoop(
                self._url,
                client_kwargs=self._producer_url_kwargs,
                client_factory=AsyncFlowClient.from_url,
            )
            if producer_loop_thread and self._url is not None
            else None
        )
        self.on_error = resolved_on_error
        try:
            self._wake_coordinator = (
                AsyncFlowWakeCoordinator(
                    self.claim_client,
                    type=self.type,
                    state=self.state,
                    states=None,
                    partition_key=None,
                    partition_keys=None,
                    priority=0,
                    limit=self.batch_size,
                    enabled=True,
                )
                if self.protocol_wake_hints
                else None
            )
        except BaseException:
            rollback_async_resources(clients.owned_resources())
            raise
        self._workers: list[AsyncQueueFlowWorker] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._close_started = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._invocations = AsyncWorkerInvocationTracker()
        self._close_operations = AsyncCloseTaskRegistry()

    async def run_once(
        self, handler: AsyncFlowHandler, *, worker_index: int = 0
    ) -> QueueFlowWorkerResult:
        async def run_worker_once() -> QueueFlowWorkerResult:
            worker = self._build_worker(worker_index)
            return await worker.run_once(handler)

        return await self._invocations.run_while_open(
            run_worker_once,
            closed_message="queue flow is closed",
        )

    async def run(self, handler: AsyncFlowHandler) -> None:
        self.start(handler)
        await self.join()

    def start(self, handler: AsyncFlowHandler) -> list[asyncio.Task[None]]:
        if self._close_started:
            raise RuntimeError("queue flow is closed")
        if self._tasks:
            raise RuntimeError("queue flow already started")
        self._workers = [self._build_worker(index) for index in range(self.workers)]
        self._tasks = [worker.start(handler) for worker in self._workers]
        return self._tasks

    async def join(self) -> list[QueueFlowWorkerResult]:
        tasks = [asyncio.create_task(worker.join()) for worker in self._workers]
        if not tasks:
            return []
        joined = asyncio.gather(*tasks)
        try:
            return await await_cancellation_safe(joined)
        except asyncio.CancelledError:
            self.stop()
            raise
        except BaseException:
            self.stop()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    def stop(self) -> None:
        for worker in self._workers:
            worker.stop()

    async def close(self, timeout: float | None = 5.0) -> None:
        deadline = CloseDeadline.start(timeout)
        if self._closed:
            return
        self._close_started = True
        self._invocations.begin_close()
        self.stop()
        close_task = self._close_task
        if close_task is None:
            close_task = asyncio.create_task(self._close_in_phases(CloseDeadline.start(None)))
            self._close_task = close_task
            close_task.add_done_callback(self._close_task_finished)
        await deadline.wait_task(close_task, "async queue flow close timed out")

    async def _close_in_phases(self, deadline: CloseDeadline) -> None:
        timeout_message = "async queue flow close timed out"
        await self._invocations.wait_for_idle(deadline, timeout_message)
        worker_error, worker_traceback = await self._close_workers(deadline, timeout_message)
        wake_error, wake_traceback = await self._close_wake(deadline, timeout_message)
        client_error = await self._close_owned_resources(deadline, timeout_message)

        if self._producer_loop is None and not self._owns_client and not self._owns_claim_client:
            self._closed = True

        if worker_error is not None:
            raise_primary_with_cleanup(
                worker_error,
                worker_traceback,
                chain_cleanup_errors((wake_error, client_error)),
            )
        if wake_error is not None:
            raise_primary_with_cleanup(wake_error, wake_traceback, client_error)
        if client_error is not None:
            raise client_error
        deadline.check(timeout_message)

    async def _close_workers(
        self,
        deadline: CloseDeadline,
        timeout_message: str,
    ) -> tuple[BaseException | None, TracebackType | None]:
        worker_resources: list[Callable[[], Awaitable[None]]] = []
        for worker in tuple(self._workers):

            async def close_worker(
                resource: AsyncQueueFlowWorker = worker,
            ) -> None:
                await _close_async_resource(
                    resource,
                    deadline,
                    timeout_message,
                    self._close_operations,
                )

            worker_resources.append(close_worker)
        worker_error: BaseException | None = None
        worker_traceback = None
        try:
            await close_resources_async(worker_resources)
        except BaseException as exc:
            if self._contains_timeout(exc):
                raise
            worker_error = exc
            worker_traceback = exc.__traceback__

        # A non-timeout worker failure is terminal: every worker close was
        # attempted, so transports can now be retired without racing work.
        self._workers.clear()
        self._tasks.clear()
        return worker_error, worker_traceback

    async def _close_wake(
        self,
        deadline: CloseDeadline,
        timeout_message: str,
    ) -> tuple[BaseException | None, TracebackType | None]:
        coordinator = self._wake_coordinator
        if coordinator is None:
            return None, None
        try:
            await _close_async_resource(
                coordinator,
                deadline,
                timeout_message,
                self._close_operations,
            )
        except BaseException as exc:
            if self._contains_timeout(exc):
                raise
            return exc, exc.__traceback__
        self._wake_coordinator = None
        return None, None

    async def _close_owned_resources(
        self,
        deadline: CloseDeadline,
        timeout_message: str,
    ) -> BaseException | None:
        resources: list[AsyncOwnedClose] = []
        if self._producer_loop is not None:
            resources.append(
                AsyncOwnedClose(
                    self._producer_loop,
                    lambda: setattr(self, "_producer_loop", None),
                )
            )
        if self.client is self.claim_client:
            if self._owns_client or self._owns_claim_client:
                resources.append(AsyncOwnedClose(self.client, self._release_shared_client))
        else:
            if self._owns_client:
                resources.append(
                    AsyncOwnedClose(
                        self.client,
                        lambda: setattr(self, "_owns_client", False),
                    )
                )
            if self._owns_claim_client:
                resources.append(
                    AsyncOwnedClose(
                        self.claim_client,
                        lambda: setattr(self, "_owns_claim_client", False),
                    )
                )

        async def close_resource(resource: Any) -> None:
            await _close_async_resource(
                resource,
                deadline,
                timeout_message,
                self._close_operations,
            )

        try:
            await close_owned_resources_async(resources, close_resource)
        except BaseException as exc:
            return exc
        return None

    def _release_shared_client(self) -> None:
        self._owns_client = False
        self._owns_claim_client = False

    def _close_task_finished(self, task: asyncio.Task[None]) -> None:
        if self._close_task is task and not self._closed:
            self._close_task = None

    @staticmethod
    def _contains_timeout(exc: BaseException) -> bool:
        pending = [exc]
        seen: set[int] = set()
        while pending:
            current = pending.pop()
            if id(current) in seen:
                continue
            seen.add(id(current))
            if isinstance(current, TimeoutError):
                return True
            if current.__cause__ is not None:
                pending.append(current.__cause__)
            if current.__context__ is not None:
                pending.append(current.__context__)
        return False

    def _build_worker(self, worker_index: int) -> AsyncQueueFlowWorker:
        return AsyncQueueFlowWorker(
            self.client,
            claim_client=self.claim_client,
            type=self.type,
            state=self.state,
            concurrency=self.concurrency,
            batch_size=self.batch_size,
            complete_independent=self.complete_independent,
            exception_policy=self.on_error,
            claim_values=self.claim_values,
            value_max_bytes=self.value_max_bytes,
            auto_partitions=True,
            worker_index=worker_index,
            workers=self.workers,
            server_shards=self.server_shards,
            claim_partition_batch_size=self.claim_partition_batch_size,
            idle_sleep_s=self.idle_sleep_s,
            max_idle_sleep_s=self.max_idle_sleep_s,
            protocol_wake_hints=self.protocol_wake_hints,
            fuse_complete_claim=self.fuse_complete_claim,
            block_ms=self.block_ms,
            close_client=False,
            wake_coordinator=self._wake_coordinator,
        )


class AsyncQueue:
    """High-level async queue handle bound to one Flow type/state."""

    def __init__(
        self,
        client: AsyncFlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | str | Any | None = None,
        type: str,
        state: str = "queued",
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _producer_url: str | None = None,
        _producer_url_kwargs: Mapping[str, Any] | None = None,
    ) -> None:
        self.client = _client_from(client)
        self.claim_client = self.client if claim_client is None else _client_from(claim_client)
        self.type = type
        self.state = state
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._producer_url = _producer_url
        self._producer_url_kwargs = dict(_producer_url_kwargs or {})

    async def enqueue(
        self,
        id: str,
        *,
        payload: Any = None,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> Any:
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        return await self.client.enqueue(
            id,
            type=self.type,
            state=attrs.pop("state", self.state),
            payload=payload,
            **attrs,
        )

    async def enqueue_many(
        self,
        items: Sequence[CreateItem | tuple[str, Any] | str],
        *,
        max_active_ms: int | float | str | None = None,
        **attrs: Any,
    ) -> Any:
        if max_active_ms is not None:
            attrs["max_active_ms"] = max_active_ms
        create_items = [
            item
            if isinstance(item, CreateItem)
            else CreateItem(item[0], item[1])
            if isinstance(item, tuple)
            else CreateItem(item)
            for item in items
        ]
        return await self.client.enqueue_many(
            create_items,
            type=self.type,
            state=attrs.pop("state", self.state),
            **attrs,
        )

    def worker(self, **kwargs: Any) -> AsyncQueueFlow:
        worker_kwargs = (
            self.worker_config.to_kwargs(ASYNC_QUEUE_WORKER_CONFIG_KEYS)
            if self.worker_config is not None
            else {}
        )
        if self.value_config.value_max_bytes is not None and "value_max_bytes" not in worker_kwargs:
            worker_kwargs["value_max_bytes"] = self.value_config.value_max_bytes
        worker_kwargs.update(kwargs)
        worker_kwargs.setdefault("state", self.state)
        return AsyncQueueFlow(
            self.client,
            claim_client=self.claim_client,
            type=self.type,
            _producer_url=self._producer_url,
            _producer_url_kwargs=self._producer_url_kwargs,
            **worker_kwargs,
        )

    async def install_policy(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        indexed_state_meta: str | None = None,
        max_active_ms: int | float | str | None = None,
        replace: bool = False,
        expected_generation: int | None = None,
    ) -> PolicySnapshot:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        kwargs: dict[str, Any] = {
            "retry": resolved_retry_policy,
            "replace": replace,
            "expected_generation": expected_generation,
        }
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.client.install_policy(self.type, **kwargs)


class AsyncQueueClient:
    """High-level async durable queue client."""

    def __init__(
        self,
        client: AsyncFlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | str | Any | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _owns_clients: bool = False,
    ) -> None:
        resolved_value_config = value_config if value_config is not None else ValueConfig()
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        self._url = client if isinstance(client, str) else None
        self._base_url_kwargs: dict[str, Any] = {}
        self._claim_client_explicit = claim_client is not None
        self._owned_extra_claim_flows: list[AsyncFlowClient] = []
        self._claim_flows_by_size: dict[int, AsyncFlowClient] = {}
        self._claim_pool_size = claim_pool_size
        command_max_connections = (
            1
            if isinstance(client, str)
            and (worker_config is None or worker_config.command_connections is None)
            else command_pool_size
        )
        clients = resolve_async_client_pair(
            client,
            claim_client,
            from_url=AsyncFlowClient.from_url,
            normalize=_client_from,
            command_kwargs={"max_connections": command_max_connections},
            claim_kwargs={"max_connections": claim_pool_size},
        )
        self.flow = clients.command
        self.claim_flow = clients.claim
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = resolved_value_config
        self._owns_flow = _owns_clients or clients.owns_command
        self._owns_claim_flow = self.claim_flow is not self.flow and (
            _owns_clients or clients.owns_claim
        )
        self._close_coordinator = AsyncCloseCoordinator()
        if self.claim_flow is not self.flow:
            self._claim_flows_by_size[claim_pool_size] = self.claim_flow

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> AsyncQueueClient:
        resolved_value_config = value_config if value_config is not None else ValueConfig()
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_kwargs = dict(kwargs)
        if worker_config is None or worker_config.command_connections is None:
            command_kwargs.setdefault("max_connections", 1)
        else:
            command_kwargs.setdefault("max_connections", command_pool_size)
        claim_kwargs = dict(kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        clients = resolve_async_client_pair(
            url,
            None,
            from_url=AsyncFlowClient.from_url,
            normalize=_client_from,
            command_kwargs=command_kwargs,
            claim_kwargs=claim_kwargs,
        )
        try:
            instance = cls(
                clients.command,
                claim_client=clients.claim,
                retry_policy=retry_policy,
                worker_config=worker_config,
                value_config=resolved_value_config,
                _owns_clients=True,
            )
        except BaseException:
            rollback_async_resources(clients.owned_resources())
            raise
        instance._url = url
        instance._base_url_kwargs = dict(kwargs)
        instance._claim_client_explicit = False
        instance._claim_pool_size = claim_pool_size
        instance._claim_flows_by_size = {claim_pool_size: instance.claim_flow}
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> AsyncFlowClient:
        if self._close_coordinator.started:
            raise RuntimeError("queue client is closed")
        if self._claim_client_explicit or self._url is None:
            return self.claim_flow
        _, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        existing = self._claim_flows_by_size.get(claim_pool_size)
        if existing is not None:
            return existing
        claim_kwargs = dict(self._base_url_kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        claim_flow = AsyncFlowClient.from_url(self._url, **claim_kwargs)
        self._claim_flows_by_size[claim_pool_size] = claim_flow
        self._owned_extra_claim_flows.append(claim_flow)
        return claim_flow

    def queue(
        self,
        *,
        type: str,
        state: str = "queued",
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> AsyncQueue:
        resolved_worker_config = worker_config if worker_config is not None else self.worker_config
        return AsyncQueue(
            self.flow,
            claim_client=self._claim_flow_for_worker_config(resolved_worker_config),
            type=type,
            state=state,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            worker_config=resolved_worker_config,
            value_config=value_config if value_config is not None else self.value_config,
            _producer_url=self._url,
            _producer_url_kwargs=self._base_url_kwargs,
        )

    async def install_policy(
        self,
        type: str,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        states: dict[str, FlowStatePolicyLike] | None = None,
        indexed_state_meta: str | None = None,
        max_active_ms: int | float | str | None = None,
        replace: bool = False,
        expected_generation: int | None = None,
    ) -> PolicySnapshot:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        kwargs: dict[str, Any] = {
            "retry": resolved_retry_policy,
            "states": states,
            "replace": replace,
            "expected_generation": expected_generation,
        }
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.flow.install_policy(type, **kwargs)

    async def close(self) -> None:
        await self._close_coordinator.run(self._close_owned_clients)

    async def _close_owned_clients(self) -> None:
        extra_claim_flows = tuple(self._owned_extra_claim_flows)
        self._claim_flows_by_size.clear()
        resources: list[Callable[[], Awaitable[None]]] = []
        for extra_claim_flow in extra_claim_flows:

            async def close_extra_claim_flow(
                flow: AsyncFlowClient = extra_claim_flow,
            ) -> None:
                await flow.close()
                self._owned_extra_claim_flows[:] = [
                    candidate
                    for candidate in self._owned_extra_claim_flows
                    if candidate is not flow
                ]

            resources.append(close_extra_claim_flow)
        if self._owns_claim_flow and self.claim_flow is not self.flow:

            async def close_claim_flow() -> None:
                await self.claim_flow.close()
                self._owns_claim_flow = False

            resources.append(close_claim_flow)
        if self._owns_flow:

            async def close_flow() -> None:
                await self.flow.close()
                self._owns_flow = False

            resources.append(close_flow)
        await close_resources_async(resources)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)


__all__ = ["AsyncQueue", "AsyncQueueClient", "AsyncQueueFlow"]
