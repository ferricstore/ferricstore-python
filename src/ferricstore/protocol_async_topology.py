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
from ferricstore.protocol_async_pool import (
    AsyncProtocolAdapterPool,
)
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
    _endpoint_from_url,
    _is_retryable_route_error,
    _is_safe_control_retry,
    _map_get,
    _normalized_host_set,
    _protocol_connection_count,
    _set_seed_auth_defaults,
    _text_or_none,
    _url_from_endpoint,
)
from ferricstore.protocol_constants import (
    _ASYNC_ADAPTER_FANOUT_LIMIT,
    _CONTROL_OPCODES,
    _FLAG_TRACE,
    _TLS_SCHEMES,
    ProtocolCommand,
)
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
    EndpointAdapterLifecycle,
)


class AsyncTopologyProtocolAdapterPool:
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
        self.seed_urls = list(urls)
        self.client = self
        self.endpoint_policy = endpoint_policy
        self.endpoint_validator = endpoint_validator
        self.warm_connections = warm_connections
        self._max_connections = _protocol_connection_count(kwargs.pop("max_connections", 1))
        self._tls = bool(kwargs.get("tls")) or any(
            urlparse(url).scheme.lower() in _TLS_SCHEMES for url in self.seed_urls
        )
        self._adapter_kwargs = dict(kwargs)
        _set_seed_auth_defaults(self.seed_urls, self._adapter_kwargs)
        self._seed_endpoint_keys = {
            _connection_endpoint_key(
                _endpoint_from_url(url),
                tls=urlparse(url).scheme.lower() in _TLS_SCHEMES,
            )
            for url in self.seed_urls
        }
        self._trusted_hosts = _normalized_host_set(list(trusted_hosts or []))
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
        for adapter in list(self._adapters.values()):
            wait_event = getattr(adapter, "wait_event", None)
            if not callable(wait_event):
                continue
            event = wait_event(timeout=0.0)
            if inspect.isawaitable(event):
                event = await event
            if event is not None:
                return event
        return None

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
        try:
            adapter = self._adapter_for_endpoint(route["endpoint"])
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

        command, route = route_data
        try:
            adapter = self._adapter_for_endpoint(route["endpoint"])
            return await self._execute_protocol_command(adapter, command, route["lane_id"], args)
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    await self.refresh_topology()
            raise

    async def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        route_data = await self._route_data(args)
        if route_data is None:
            return cast(
                dict[str, Any],
                await self._execute_control_method("execute_command_with_trace", args),
            )

        command, route = route_data
        try:
            adapter = self._adapter_for_endpoint(route["endpoint"])
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
            group_commands: list[tuple[Any, ...]],
        ) -> list[Any]:
            execute_batch = None
            if target.lane_id is not None:
                method_name = (
                    "execute_batch_ordered_on_lane" if ordered else "execute_batch_on_lane"
                )
                execute_batch = getattr(target.adapter, method_name, None)
            if callable(execute_batch):
                values = execute_batch(group_commands, target.lane_id)
                if inspect.isawaitable(values):
                    values = await values
                return list(values)
            method_name = "execute_batch_ordered" if ordered else "execute_batch"
            execute_batch = getattr(target.adapter, method_name, None)
            if callable(execute_batch):
                values = execute_batch(group_commands)
                if inspect.isawaitable(values):
                    values = await values
                return list(values)
            return [await target.adapter.execute_command(*args) for args in group_commands]

        async def execute_planned_group(group: Any) -> list[Any]:
            return await execute_group(group.target, group.commands)

        try:
            routed_commands = [
                (await self._batch_target_for_command(args), args) for args in commands
            ]
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
        self, adapter: Any, command: ProtocolCommand, lane_id: int, args: tuple[Any, ...]
    ) -> Any:
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
    ) -> tuple[ProtocolCommand, dict[str, Any]] | None:
        if not args:
            return None
        try:
            command = build_protocol_command(*args)
        except Exception:
            return None
        decision = route_for_command(
            args,
            opcode=command.opcode,
            payload=command.payload,
            control_opcodes=_CONTROL_OPCODES,
            command_name=_command_name,
            slot_for_key=RoutingTopology.slot_for_key,
        )
        key = decision.require_routable_key()
        if key is None:
            return None
        return command, await self.route(key)

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

    def _control_adapter(self) -> Any:
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        for url in self._refresh_candidate_urls():
            with contextlib.suppress(Exception):
                return self._adapter_for_url(url)
        return self._adapter_for_url(self.seed_urls[0])

    def _adapter_for_url(self, url: str) -> Any:
        endpoint = _endpoint_from_url(url)
        key = _connection_endpoint_key(
            endpoint,
            tls=urlparse(url).scheme.lower() in _TLS_SCHEMES,
        )
        if key not in self._seed_endpoint_keys:
            self._validate_endpoint(endpoint)
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        adapter = self._endpoint_lifecycle.get(key)
        if adapter is None:
            adapter = self._new_endpoint_adapter(url)
            try:
                self._register_adapter(adapter)
            except BaseException:
                self._schedule_adapter_cleanup(adapter)
                raise
            self._endpoint_lifecycle.put(key, adapter)
        return adapter

    def _adapter_for_endpoint(self, endpoint: Mapping[str, Any]) -> Any:
        self._validate_endpoint(endpoint)
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        key = _connection_endpoint_key(endpoint, tls=self._tls)
        adapter = self._endpoint_lifecycle.get(key)
        if adapter is None:
            adapter = self._new_endpoint_adapter(_url_from_endpoint(endpoint, tls=self._tls))
            try:
                self._register_adapter(adapter)
            except BaseException:
                self._schedule_adapter_cleanup(adapter)
                raise
            self._endpoint_lifecycle.put(key, adapter)
        return adapter

    def _install_topology(self, topology: RoutingTopology) -> list[Any]:
        live_keys = set(self._seed_endpoint_keys)
        live_keys.update(
            _connection_endpoint_key(endpoint, tls=self._tls)
            for endpoint in topology.endpoints.values()
        )
        ready = self._endpoint_lifecycle.install(
            live_keys,
            self._retired_adapter_became_idle,
        )
        self._topology_generation = self._endpoint_lifecycle.generation
        self.topology = topology
        for adapter in ready:
            self._cleanup_adapters.add(adapter)
        return ready

    def _retired_adapter_became_idle(
        self,
        key: tuple[str, int],
        adapter: Any,
    ) -> None:
        if self._closed:
            return
        ready = self._endpoint_lifecycle.claim_idle(key, adapter)
        if ready is None:
            return
        self._cleanup_adapters.add(ready)
        self._schedule_retained_adapter_cleanup()

    def _new_endpoint_adapter(self, url: str) -> Any:
        return AsyncProtocolAdapterPool.from_url(
            url,
            max_connections=self._max_connections,
            **self._adapter_kwargs,
        )

    def _register_adapter(self, adapter: Any) -> None:
        add_listener = getattr(adapter, "add_event_listener", None)
        if callable(add_listener):
            add_listener(self._event_listener)
        else:
            self._event_poll_fallback = True
        self._subscription_registry.register_for_reconnect(adapter)

    def _schedule_adapter_cleanup(self, adapter: Any) -> None:
        self._cleanup_adapters.add(adapter)
        identity = id(adapter)
        retry_requested = getattr(self, "_cleanup_retry_requested", None)
        if retry_requested is None:
            retry_requested = set()
            self._cleanup_retry_requested = retry_requested
        if self._closed:
            return
        existing = self._cleanup_tasks_by_adapter.get(identity)
        if existing is not None and not existing.done():
            retry_requested.add(identity)
            return
        if existing is not None:
            self._cleanup_tasks.discard(existing)
            self._cleanup_tasks_by_adapter.pop(identity, None)

        async def cleanup() -> None:
            try:
                await _close_adapter_async(adapter, self._event_listener)
            except BaseException:
                return
            self._cleanup_adapters.complete(adapter)

        task = asyncio.create_task(cleanup())
        self._cleanup_tasks.add(task)
        self._cleanup_tasks_by_adapter[identity] = task

        def finished(completed: asyncio.Task[None]) -> None:
            self._cleanup_tasks.discard(completed)
            if self._cleanup_tasks_by_adapter.get(identity) is completed:
                self._cleanup_tasks_by_adapter.pop(identity, None)
            should_retry = (
                identity in retry_requested
                and not self._closed
                and self._cleanup_adapters.contains(adapter)
            )
            retry_requested.discard(identity)
            if should_retry:
                self._schedule_adapter_cleanup(adapter)

        task.add_done_callback(finished)

    def _schedule_retained_adapter_cleanup(self) -> None:
        for adapter in self._cleanup_adapters.snapshot():
            self._schedule_adapter_cleanup(adapter)

    async def _safe_warm_endpoint(self, endpoint: Mapping[str, Any]) -> None:
        adapter = self._adapter_for_endpoint(endpoint)
        connections = getattr(adapter, "adapters", None)
        if isinstance(connections, Sequence) and connections:
            await run_async_fanout(
                tuple(connections),
                self._safe_warm_connection,
                concurrent=True,
            )
            return
        await self._safe_warm_connection(adapter)

    async def _safe_warm_connection(self, adapter: Any) -> None:
        ensure_connected = getattr(adapter, "_ensure_connected", None)
        if callable(ensure_connected):
            try:
                async with self._warm_semaphore:
                    result = ensure_connected()
                    if inspect.isawaitable(result):
                        await result
            except Exception:
                return

    def _validate_endpoint(self, endpoint: Mapping[str, Any]) -> None:
        host = _text_or_none(_map_get(endpoint, "host", "native_host"))
        if host is None:
            raise FerricStoreError("invalid learned endpoint", raw=endpoint)
        allowed = False
        policy = self.endpoint_policy
        if policy in {"any", "none"}:
            allowed = True
        elif policy == "seed_hosts":
            allowed = (
                _connection_endpoint_key(endpoint, tls=self._tls) in self._seed_endpoint_keys
                or host.lower() in self._trusted_hosts
            )
        elif isinstance(policy, tuple) and len(policy) == 2 and policy[0] == "allow_hosts":
            allowed = host.lower() in _normalized_host_set(policy[1])
        else:
            raise FerricStoreError(f"invalid endpoint_policy {policy!r}")
        if not allowed:
            raise FerricStoreError("unsafe learned endpoint", raw=endpoint)
        if self.endpoint_validator is not None and not self.endpoint_validator(endpoint):
            raise FerricStoreError("unsafe learned endpoint", raw=endpoint)

    def _refresh_candidate_urls(self) -> list[str]:
        discovered_urls = [
            _url_from_endpoint(endpoint, tls=self._tls)
            for endpoint in self.topology.endpoints.values()
        ]
        live_urls = set(self.seed_urls)
        live_urls.update(discovered_urls)
        return [
            url for url in self._control_endpoints.candidates(discovered_urls) if url in live_urls
        ]


__all__ = [
    "AsyncTopologyProtocolAdapterPool",
]
