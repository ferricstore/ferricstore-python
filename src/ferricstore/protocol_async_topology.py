from __future__ import annotations

import asyncio
import contextlib
import inspect
import time
from collections.abc import Callable, Mapping, Sequence
from functools import partial
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.batch_core import (
    run_async_fanout,
)
from ferricstore.config_validation import (
    validate_optional_positive_int,
    validate_optional_thread_wait_seconds,
)
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    AsyncCloseCoordinator,
    RetryableResourceSet,
    close_resources_async,
)
from ferricstore.protocol_async import (
    AsyncProtocolPipeline,
)
from ferricstore.protocol_async_endpoints import AsyncTopologyEndpointMixin
from ferricstore.protocol_commands import (
    build_protocol_command,
)
from ferricstore.protocol_common import (
    RoutingTopology,
    _async_adapter_outer_fanout_limit,
    _close_adapter_async,
    _command_name,
    _connection_endpoint_key,
    _endpoint_adapter_is_idle,
    _is_retryable_route_error,
    _is_safe_control_retry,
    _protocol_connection_count,
)
from ferricstore.protocol_constants import (
    _ASYNC_ADAPTER_FANOUT_LIMIT,
    _CONTROL_OPCODES,
    _FLAG_TRACE,
    _TLS_SCHEMES,
)
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_BATCH_ITEMS,
    PendingRequestCapacityError,
    check_batch_item_limit,
)
from ferricstore.protocol_planning import PreparedCommand, prepare_protocol_command
from ferricstore.topology_core import (
    ControlEndpointSelector,
    FlowWakeSubscriptionRegistry,
    RouteBatchPlan,
    RoutedBatchTarget,
    route_for_command,
    route_for_keys,
    validate_single_slot,
)
from ferricstore.topology_lifecycle import (
    AsyncSingleFlight,
    EndpointAdapterLease,
    EndpointAdapterLifecycle,
)
from ferricstore.topology_security import (
    TopologyEndpointTrust,
    TopologyRuntimeConfig,
    seed_urls_by_endpoint,
    validate_seed_credential_consistency,
    validate_topology_configuration,
)


class AsyncTopologyProtocolAdapterPool(AsyncTopologyEndpointMixin):
    """Async topology-aware native pool backed by the server SHARDS slot table."""

    client: AsyncTopologyProtocolAdapterPool
    requires_explicit_session = True
    supports_concurrent_fanout = True

    def __init__(
        self,
        urls: Sequence[str],
        *,
        endpoint_policy: str | tuple[str, Sequence[str]] = "seed_hosts",
        trusted_hosts: Sequence[str] | None = None,
        endpoint_validator: Callable[[Mapping[str, Any]], bool | None] | None = None,
        warm_connections: bool = False,
        **kwargs: Any,
    ) -> None:
        if not urls:
            raise ValueError("AsyncTopologyProtocolAdapterPool requires at least one seed URL")
        validate_topology_configuration(urls, endpoint_policy)
        runtime_config = TopologyRuntimeConfig.build(
            warm_connections=warm_connections,
            trusted_hosts=trusted_hosts,
            endpoint_validator=endpoint_validator,
            tls=kwargs.get("tls", False),
        )
        self.seed_urls = list(urls)
        self.client = self
        self.endpoint_policy = endpoint_policy
        self.endpoint_validator = runtime_config.endpoint_validator
        self.warm_connections = runtime_config.warm_connections
        self._max_connections = _protocol_connection_count(kwargs.pop("max_connections", 1))
        self._tls = runtime_config.tls or any(
            urlparse(url).scheme.lower() in _TLS_SCHEMES for url in self.seed_urls
        )
        self.max_batch_items = validate_optional_positive_int(
            kwargs.get("max_batch_items", DEFAULT_MAX_BATCH_ITEMS),
            name="max_batch_items",
        )
        self._adapter_kwargs = dict(kwargs)
        validate_seed_credential_consistency(self.seed_urls, self._adapter_kwargs)
        self._seed_urls_by_endpoint = seed_urls_by_endpoint(self.seed_urls)
        self._seed_endpoint_keys = set(self._seed_urls_by_endpoint)
        self._endpoint_trust = TopologyEndpointTrust(
            policy=endpoint_policy,
            seed_endpoint_keys=self._seed_endpoint_keys,
            trusted_hosts=runtime_config.trusted_hosts,
            tls=self._tls,
            validator=runtime_config.endpoint_validator,
        )
        self._trusted_hosts = set(self._endpoint_trust.trusted_hosts)
        self._endpoint_lifecycle = EndpointAdapterLifecycle[tuple[str, int]](
            is_idle=_endpoint_adapter_is_idle
        )
        self._adapters = self._endpoint_lifecycle.active
        self._retired_adapters = self._endpoint_lifecycle.retired
        self._topology_generation = 0
        self._lock = asyncio.Lock()
        self._closed = False
        self._close_coordinator = AsyncCloseCoordinator()
        self._refresh_singleflight = AsyncSingleFlight[RoutingTopology]()
        self._close_adapters_snapshot: list[Any] | None = None
        self._cleanup_adapters = RetryableResourceSet(())
        self._event_ready = asyncio.Event()
        self._event_listener = self._event_ready.set
        self._event_poll_fallback = False
        self._subscription_registry = FlowWakeSubscriptionRegistry()
        self._subscription_update_lock = asyncio.Lock()
        self._subscription_generation = 0
        self._control_endpoints = ControlEndpointSelector(self.seed_urls)
        self._cleanup_tasks: set[asyncio.Task[None]] = set()
        self._cleanup_tasks_by_adapter: dict[int, asyncio.Task[None]] = {}
        self._cleanup_retry_requested: set[int] = set()
        self._warm_semaphore = asyncio.Semaphore(_ASYNC_ADAPTER_FANOUT_LIMIT)
        self.topology = RoutingTopology.empty()

    @property
    def events(self) -> list[Any]:
        events: list[Any] = []
        for adapter in list(self._adapters.values()):
            events.extend(getattr(adapter, "events", []))
        return events

    @property
    def backpressure_scope(self) -> Any:
        return ("async-topology-protocol", tuple(sorted(self.seed_urls)))

    async def wait_event(self, timeout: float | None = None) -> Any | None:
        timeout = validate_optional_thread_wait_seconds(
            timeout,
            name="wait_event timeout",
        )
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            event = await self._take_event()
            if event is not None:
                return event
            if timeout == 0.0:
                return None
            self._event_ready.clear()
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            event = await self._take_event()
            if event is not None:
                return event
            wait_for: float | None = None
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                wait_for = remaining
            if self._event_poll_fallback:
                wait_for = 0.05 if wait_for is None else min(wait_for, 0.05)
            try:
                if wait_for is None:
                    await self._event_ready.wait()
                else:
                    await asyncio.wait_for(self._event_ready.wait(), wait_for)
            except asyncio.TimeoutError:
                if deadline is not None and time.monotonic() >= deadline:
                    return None

    async def _take_event(self) -> Any | None:
        lifecycle = getattr(self, "_endpoint_lifecycle", None)
        leases: list[EndpointAdapterLease[tuple[str, int]]] = []
        if lifecycle is None:
            adapters = list(self._adapters.values())
        else:
            for key, adapter in self._adapters.items():
                lease = lifecycle.acquire(key, adapter)
                if lease is not None:
                    leases.append(lease)
            adapters = [lease.adapter for lease in leases]
        try:
            for adapter in adapters:
                wait_event = getattr(adapter, "wait_event", None)
                if not callable(wait_event):
                    continue
                event = wait_event(timeout=0.0)
                if inspect.isawaitable(event):
                    event = await event
                if event is not None:
                    return event
            return None
        finally:
            for lease in reversed(leases):
                self._release_adapter_lease(lease)

    async def close(self) -> None:
        coordinator = getattr(self, "_close_coordinator", None)
        if coordinator is None:
            coordinator = AsyncCloseCoordinator()
            self._close_coordinator = coordinator
        await coordinator.run(self._close_once)

    async def _close_once(self) -> None:
        async with self._lock:
            adapters = getattr(self, "_close_adapters_snapshot", None)
            if adapters is None:
                self._closed = True
                lifecycle = getattr(self, "_endpoint_lifecycle", None)
                if lifecycle is not None:
                    owned = lifecycle.drain()
                else:
                    retired = getattr(self, "_retired_adapters", {})
                    owned = [
                        *self._adapters.values(),
                        *(adapter for adapter, _generation in retired.values()),
                    ]
                    self._adapters.clear()
                    retired.clear()
                cleanup_adapters = getattr(self, "_cleanup_adapters", None)
                if cleanup_adapters is None:
                    cleanup_adapters = RetryableResourceSet(owned)
                    self._cleanup_adapters = cleanup_adapters
                else:
                    for adapter in owned:
                        cleanup_adapters.add(adapter)
            cleanup_tasks = list(self._cleanup_tasks)
        self._event_ready.set()

        if cleanup_tasks:
            await asyncio.gather(*cleanup_tasks, return_exceptions=True)

        async with self._lock:
            adapters = getattr(self, "_close_adapters_snapshot", None)
            if adapters is None:
                adapters = list(self._cleanup_adapters.snapshot())
                self._close_adapters_snapshot = adapters
            adapters = list(adapters)

        async def close_adapter(adapter: Any) -> None:
            await _close_adapter_async(adapter, self._event_listener)
            self._cleanup_adapters.complete(adapter)
            async with self._lock:
                snapshot = self._close_adapters_snapshot
                if snapshot is not None:
                    snapshot[:] = [candidate for candidate in snapshot if candidate is not adapter]

        error: BaseException | None = None
        try:
            await close_resources_async(
                [partial(close_adapter, adapter) for adapter in adapters],
                max_concurrency=_async_adapter_outer_fanout_limit(adapters),
            )
        except BaseException as exc:
            error = exc
        if error is not None:
            raise error

    async def acquire_session(self) -> Any:
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        adapter = self._control_adapter()
        acquire_session = getattr(adapter, "acquire_session", None)
        if not callable(acquire_session):
            raise FerricStoreError("protocol adapter does not support affine sessions")
        session = acquire_session()
        if inspect.isawaitable(session):
            session = await session
        return session

    async def acquire_session_for_key(self, key: str | bytes) -> Any:
        return await self.acquire_session_for_keys((key,))

    async def acquire_session_for_keys(self, keys: Sequence[str | bytes]) -> Any:
        key = validate_single_slot(keys, slot_for_key=RoutingTopology.slot_for_key)
        if key is None:
            return await self.acquire_session()
        route = await self.route(key)
        lease: EndpointAdapterLease[tuple[str, int]] | None = None
        try:
            lease = self._leased_adapter_for_endpoint(route["endpoint"])
            adapter = lease.adapter
            acquire_session = getattr(adapter, "acquire_session_on_lane", None)
            if callable(acquire_session):
                session = acquire_session(int(route["lane_id"]))
                if inspect.isawaitable(session):
                    session = await session
                return session
            acquire_session = getattr(adapter, "acquire_session", None)
            if not callable(acquire_session):
                raise FerricStoreError("protocol adapter does not support affine sessions")
            session = acquire_session()
            if inspect.isawaitable(session):
                session = await session
            return session
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    await self.refresh_topology()
            raise
        finally:
            if lease is not None:
                self._release_adapter_lease(lease)

    def pipeline(self, transaction: bool = False) -> AsyncProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return AsyncProtocolPipeline(self)

    async def refresh_topology(self) -> RoutingTopology:
        singleflight = getattr(self, "_refresh_singleflight", None)
        if singleflight is None:
            singleflight = AsyncSingleFlight[RoutingTopology]()
            self._refresh_singleflight = singleflight
        return await singleflight.run(self._refresh_topology_once)

    async def _refresh_topology_once(self) -> RoutingTopology:
        last_error: BaseException | None = None
        async with self._lock:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            candidate_urls = self._refresh_candidate_urls()
        for url in candidate_urls:
            try:
                adapter = self._adapter_for_url(url)
                topology = RoutingTopology.build(await adapter.execute_command("SHARDS"))
                async with self._lock:
                    if self._closed:
                        raise FerricStoreError("protocol topology pool is closed")
                    self._install_topology(topology)
                    self._control_endpoints.mark_success(url)
                self._schedule_retained_adapter_cleanup()
                if self.warm_connections:
                    await run_async_fanout(
                        tuple(topology.endpoints.values()),
                        self._safe_warm_endpoint,
                        concurrent=True,
                    )
                return topology
            except Exception as exc:
                last_error = exc
                continue
        raise FerricStoreError("no FerricStore topology endpoint reachable", raw=last_error)

    async def route(self, key: str | bytes) -> dict[str, Any]:
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        if not self.topology.endpoints:
            await self.refresh_topology()
        route = self.topology.route_key(key)
        self._validate_endpoint(route["endpoint"])
        return route

    async def execute_command(self, *args: Any) -> Any:
        route_data = await self._route_data(args)
        if route_data is None:
            return await self._execute_control_method("execute_command", args)

        prepared, route = route_data
        lease: EndpointAdapterLease[tuple[str, int]] | None = None
        try:
            lease = self._leased_adapter_for_endpoint(route["endpoint"])
            adapter = lease.adapter
            return await self._execute_protocol_command(adapter, prepared, route["lane_id"])
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    await self.refresh_topology()
            raise
        finally:
            if lease is not None:
                self._release_adapter_lease(lease)

    async def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        route_data = await self._route_data(args)
        if route_data is None:
            return cast(
                dict[str, Any],
                await self._execute_control_method("execute_command_with_trace", args),
            )

        prepared, route = route_data
        command = prepared.command
        lease: EndpointAdapterLease[tuple[str, int]] | None = None
        try:
            lease = self._leased_adapter_for_endpoint(route["endpoint"])
            adapter = lease.adapter
            execute_prepared = getattr(
                adapter,
                "execute_prepared_command_with_trace_on_lane",
                None,
            )
            if callable(execute_prepared):
                result = execute_prepared(prepared, int(route["lane_id"]))
                return cast(
                    dict[str, Any],
                    await result if inspect.isawaitable(result) else result,
                )
            execute_on_lane = getattr(adapter, "execute_command_with_trace_on_lane", None)
            if callable(execute_on_lane):
                result = execute_on_lane(args, int(route["lane_id"]))
                return cast(
                    dict[str, Any],
                    await result if inspect.isawaitable(result) else result,
                )
            if not hasattr(adapter, "_request"):
                execute_with_trace = getattr(adapter, "execute_command_with_trace", None)
                if callable(execute_with_trace):
                    return cast(dict[str, Any], await execute_with_trace(*args))
                return {"value": await adapter.execute_command(*args), "trace": {}}
            await self._ensure_legacy_adapter_connected(adapter)
            request_on_lane = getattr(adapter, "_request_on_lane", adapter._request)
            response = await request_on_lane(
                command.opcode,
                int(route["lane_id"]),
                command.payload,
                command.flags | _FLAG_TRACE,
            )
            return {"value": adapter._response_value(response), "trace": response.trace or {}}
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    await self.refresh_topology()
            raise
        finally:
            if lease is not None:
                self._release_adapter_lease(lease)

    async def _execute_control_method(self, method: str, args: tuple[Any, ...]) -> Any:
        adapter = self._control_adapter()
        try:
            result = getattr(adapter, method)(*args)
            return await result if inspect.isawaitable(result) else result
        except Exception as exc:
            if not _is_retryable_route_error(exc):
                raise
            refreshed = False
            with contextlib.suppress(Exception):
                await self.refresh_topology()
                refreshed = True
            if refreshed and _is_safe_control_retry(args):
                result = getattr(self._control_adapter(), method)(*args)
                return await result if inspect.isawaitable(result) else result
            raise

    async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return await self._execute_batch(commands, ordered=False)

    async def execute_batch_ordered(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return await self._execute_batch(commands, ordered=True)

    async def _execute_batch(
        self,
        commands: list[tuple[Any, ...]],
        *,
        ordered: bool,
    ) -> list[Any]:
        if not commands:
            return []

        async def execute_group(
            target: RoutedBatchTarget[Any],
            group_commands: list[PreparedCommand],
        ) -> list[Any]:
            execute_prepared = (
                getattr(target.adapter, "execute_prepared_batch_on_lane", None)
                if target.lane_id is not None
                else None
            )
            if callable(execute_prepared):
                values = execute_prepared(
                    group_commands,
                    target.lane_id,
                    ordered=ordered,
                )
                if inspect.isawaitable(values):
                    values = await values
                return list(values)
            raw_commands = [prepared.args for prepared in group_commands]
            execute_batch = None
            if target.lane_id is not None:
                method_name = (
                    "execute_batch_ordered_on_lane" if ordered else "execute_batch_on_lane"
                )
                execute_batch = getattr(target.adapter, method_name, None)
            if callable(execute_batch):
                values = execute_batch(raw_commands, target.lane_id)
                if inspect.isawaitable(values):
                    values = await values
                return list(values)
            method_name = "execute_batch_ordered" if ordered else "execute_batch"
            execute_batch = getattr(target.adapter, method_name, None)
            if callable(execute_batch):
                values = execute_batch(raw_commands)
                if inspect.isawaitable(values):
                    values = await values
                return list(values)
            return [await target.adapter.execute_command(*args) for args in raw_commands]

        async def execute_planned_group(group: Any) -> list[Any]:
            return await execute_group(group.target, group.commands)

        leases: list[EndpointAdapterLease[tuple[str, int]]] = []
        try:
            routed_commands, leases = await self._prepare_routed_batch(commands)
            plan = RouteBatchPlan.build(
                routed_commands,
                group_key=lambda target: (id(target.adapter), target.lane_id),
            )
            group_values = await run_async_fanout(
                plan.groups,
                execute_planned_group,
                concurrent=True,
            )
            return plan.merge(group_values)
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    await self.refresh_topology()
            raise
        finally:
            for lease in reversed(leases):
                self._release_adapter_lease(lease)

    async def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> Any:
        update_lock = getattr(self, "_subscription_update_lock", None)
        if update_lock is None:
            update_lock = asyncio.Lock()
            self._subscription_update_lock = update_lock
        async with update_lock:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            self._subscription_registry.remember(args, kwargs)
            self._subscription_generation = int(getattr(self, "_subscription_generation", 0)) + 1
            adapters = list(self._adapters.values()) or [self._control_adapter()]
            adapters = [
                adapter
                for adapter in adapters
                if callable(getattr(adapter, "subscribe_flow_wake", None))
            ]

            async def subscribe(adapter: Any) -> Any:
                subscribe = getattr(adapter, "subscribe_flow_wake", None)
                if not callable(subscribe):
                    return None
                result = subscribe(*args, **kwargs)
                return await result if inspect.isawaitable(result) else result

            replies = await run_async_fanout(
                adapters,
                subscribe,
                concurrent=True,
                max_concurrency=_async_adapter_outer_fanout_limit(adapters),
            )
            return replies[0] if len(replies) == 1 else replies

    async def _execute_protocol_command(
        self,
        adapter: Any,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> Any:
        execute_prepared = getattr(adapter, "execute_prepared_command_on_lane", None)
        if callable(execute_prepared):
            result = execute_prepared(prepared, int(lane_id))
            return await result if inspect.isawaitable(result) else result
        args = prepared.args
        command = prepared.command
        execute_on_lane = getattr(adapter, "execute_command_on_lane", None)
        if callable(execute_on_lane):
            result = execute_on_lane(args, int(lane_id))
            return await result if inspect.isawaitable(result) else result
        if not hasattr(adapter, "_request") or not hasattr(adapter, "_response_value"):
            return await adapter.execute_command(*args)
        await self._ensure_legacy_adapter_connected(adapter)
        request_on_lane = getattr(adapter, "_request_on_lane", adapter._request)
        response = await request_on_lane(
            command.opcode,
            int(lane_id),
            command.payload,
            command.flags,
        )
        return adapter._response_value(response)

    @staticmethod
    async def _ensure_legacy_adapter_connected(adapter: Any) -> None:
        if getattr(adapter, "_request_ensures_connection", False):
            return
        ensure_connected = getattr(adapter, "_ensure_connected", None)
        if not callable(ensure_connected):
            return
        result = ensure_connected()
        if inspect.isawaitable(result):
            await result

    async def _route_data(
        self, args: tuple[Any, ...]
    ) -> tuple[PreparedCommand, dict[str, Any]] | None:
        if not args:
            return None
        try:
            prepared = prepare_protocol_command(args, builder=build_protocol_command)
        except Exception:
            return None
        route = await self._route_prepared(prepared)
        return None if route is None else (prepared, route)

    async def _route_prepared(self, prepared: PreparedCommand) -> dict[str, Any] | None:
        command = prepared.command
        decision = route_for_command(
            prepared.args,
            opcode=command.opcode,
            payload=command.payload,
            control_opcodes=_CONTROL_OPCODES,
            command_name=_command_name,
            slot_for_key=RoutingTopology.slot_for_key,
        )
        key = decision.require_routable_key()
        if key is None:
            return None
        route = dict(await self.route(key))
        route["_sdk_generation"] = self._topology_generation
        return route

    async def _prepare_routed_batch(
        self,
        commands: Sequence[tuple[Any, ...]],
    ) -> tuple[
        list[tuple[RoutedBatchTarget[Any], PreparedCommand]],
        list[EndpointAdapterLease[tuple[str, int]]],
    ]:
        try:
            check_batch_item_limit(len(commands), self.max_batch_items)
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc
        prepared_commands = [
            prepare_protocol_command(args, builder=build_protocol_command) for args in commands
        ]
        while True:
            generation = self._topology_generation
            routed_commands: list[tuple[RoutedBatchTarget[Any], PreparedCommand]] = []
            leases: list[EndpointAdapterLease[tuple[str, int]]] = []
            leases_by_endpoint: dict[tuple[str, int], EndpointAdapterLease[tuple[str, int]]] = {}
            try:
                for prepared in prepared_commands:
                    route = await self._route_prepared(prepared)
                    if route is None:
                        target = RoutedBatchTarget(self._control_adapter(), None)
                    elif int(route["_sdk_generation"]) != generation:
                        break
                    else:
                        key = _connection_endpoint_key(route["endpoint"], tls=self._tls)
                        lease = leases_by_endpoint.get(key)
                        if lease is None:
                            lease = self._leased_adapter_for_endpoint(route["endpoint"])
                            leases_by_endpoint[key] = lease
                            leases.append(lease)
                        target = RoutedBatchTarget(lease.adapter, int(route["lane_id"]))
                    routed_commands.append((target, prepared))
                if (
                    len(routed_commands) == len(prepared_commands)
                    and generation == self._topology_generation
                ):
                    return routed_commands, leases
            except BaseException:
                for lease in reversed(leases):
                    self._release_adapter_lease(lease)
                raise
            for lease in reversed(leases):
                self._release_adapter_lease(lease)

    def _single_shard_key(self, keys: Sequence[Any]) -> str | bytes | None:
        decision = route_for_keys(keys, slot_for_key=RoutingTopology.slot_for_key)
        return decision.require_routable_key()

    async def _adapter_for_command(self, args: tuple[Any, ...]) -> Any:
        route_data = await self._route_data(args)
        if route_data is None:
            return self._control_adapter()
        return self._adapter_for_endpoint(route_data[1]["endpoint"])

    async def _batch_target_for_command(
        self,
        args: tuple[Any, ...],
    ) -> RoutedBatchTarget[Any]:
        route_data = await self._route_data(args)
        if route_data is None:
            return RoutedBatchTarget(self._control_adapter(), None)
        route = route_data[1]
        return RoutedBatchTarget(
            self._adapter_for_endpoint(route["endpoint"]),
            int(route["lane_id"]),
        )


__all__ = [
    "AsyncTopologyProtocolAdapterPool",
]
