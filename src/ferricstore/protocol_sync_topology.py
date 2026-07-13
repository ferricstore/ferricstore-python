from __future__ import annotations

import contextlib
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from functools import partial
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.batch_core import (
    SyncFanoutExecutor,
)
from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    RetryableResourceSet,
    SyncCloseCoordinator,
    close_resources_sync,
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.protocol_commands import (
    build_protocol_command,
)
from ferricstore.protocol_common import (
    RoutingTopology,
    _close_adapter_sync,
    _close_adapters_sync,
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
    _set_wire_future_sources,
    _text_or_none,
    _unique_adapters,
    _url_from_endpoint,
)
from ferricstore.protocol_constants import (
    _CONTROL_OPCODES,
    _FLAG_TRACE,
    _TLS_SCHEMES,
    ProtocolCommand,
)
from ferricstore.protocol_sync import (
    ProtocolPipeline,
)
from ferricstore.protocol_sync_pool import (
    ProtocolAdapterPool,
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
    EndpointAdapterLifecycle,
    SyncSingleFlight,
)


class TopologyProtocolAdapterPool:
    """Topology-aware native pool backed by the server SHARDS slot table."""

    client: TopologyProtocolAdapterPool
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
            raise ValueError("TopologyProtocolAdapterPool requires at least one seed URL")
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
        self._lock = threading.RLock()
        self._adapter_creation_cv = threading.Condition(self._lock)
        self._adapter_creations: dict[tuple[str, int], Future[Any]] = {}
        self._closed = False
        self._close_coordinator = SyncCloseCoordinator()
        self._refresh_singleflight = SyncSingleFlight[RoutingTopology]()
        self._close_adapters_snapshot: list[Any] | None = None
        self._cleanup_adapters = RetryableResourceSet(())
        self._adapter_cleanup_lock = threading.Lock()
        self._batch_fanout = SyncFanoutExecutor(thread_name_prefix="ferricstore-topology-fanout")
        self._event_ready = threading.Event()
        self._event_listener = self._event_ready.set
        self._event_poll_fallback = False
        self._subscription_registry = FlowWakeSubscriptionRegistry()
        self._subscription_update_lock = threading.Lock()
        self._subscription_generation = 0
        self._control_endpoints = ControlEndpointSelector(self.seed_urls)
        self.topology = RoutingTopology.empty()
        try:
            self.refresh_topology()
        except BaseException:
            with contextlib.suppress(BaseException):
                self.close()
            raise

    @property
    def events(self) -> list[Any]:
        with self._lock:
            adapters = list(self._adapters.values())
        events: list[Any] = []
        for adapter in adapters:
            adapter_events = getattr(adapter, "events", [])
            events.extend(adapter_events)
        return events

    @property
    def backpressure_scope(self) -> Any:
        return ("topology-protocol", tuple(sorted(self.seed_urls)))

    def wait_event(self, timeout: float | None = None) -> Any | None:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            event = self._take_event()
            if event is not None:
                return event
            if timeout == 0.0:
                return None
            self._event_ready.clear()
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            event = self._take_event()
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
            if not self._event_ready.wait(wait_for) and deadline is not None:
                return None

    def _take_event(self) -> Any | None:
        with self._lock:
            adapters = list(self._adapters.values())
        for adapter in adapters:
            wait_event = getattr(adapter, "wait_event", None)
            if not callable(wait_event):
                continue
            event = wait_event(timeout=0.0)
            if event is not None:
                return event
        return None

    def close(self) -> None:
        coordinator = getattr(self, "_close_coordinator", None)
        if coordinator is None:
            coordinator = SyncCloseCoordinator()
            self._close_coordinator = coordinator
        coordinator.run(self._close_once)

    def _close_once(self) -> None:
        with self._lock:
            adapters = getattr(self, "_close_adapters_snapshot", None)
            if adapters is None:
                self._closed = True
                adapter_creations = getattr(self, "_adapter_creations", None)
                creation_cv = getattr(self, "_adapter_creation_cv", None)
                if adapter_creations is not None and creation_cv is not None:
                    while adapter_creations:
                        creation_cv.wait()
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
                    cleanup_adapters = RetryableResourceSet(())
                    self._cleanup_adapters = cleanup_adapters
                for adapter in owned:
                    cleanup_adapters.add(adapter)
                owned.extend(cleanup_adapters.snapshot())
                adapters = _unique_adapters(owned)
                self._close_adapters_snapshot = adapters
            adapters = list(adapters)
        self._event_ready.set()

        def close_adapter(adapter: Any) -> None:
            cleanup_adapters = getattr(self, "_cleanup_adapters", None)
            cleanup_lock = getattr(self, "_adapter_cleanup_lock", None)
            if cleanup_lock is None:
                cleanup_lock = threading.Lock()
                self._adapter_cleanup_lock = cleanup_lock
            with cleanup_lock:
                if cleanup_adapters is None or not cleanup_adapters.contains(adapter):
                    pass
                else:
                    _close_adapter_sync(adapter, self._event_listener)
                    cleanup_adapters.complete(adapter)
            with self._lock:
                snapshot = self._close_adapters_snapshot
                if snapshot is not None:
                    snapshot[:] = [candidate for candidate in snapshot if candidate is not adapter]

        resources: list[Callable[[], Any]] = []
        batch_fanout = getattr(self, "_batch_fanout", None)
        if batch_fanout is not None:
            resources.append(batch_fanout.close)
        resources.extend(partial(close_adapter, adapter) for adapter in adapters)
        close_resources_sync(resources)

    def pipeline(self, transaction: bool = False) -> ProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return ProtocolPipeline(self)

    def acquire_session(self) -> Any:
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        adapter = self._control_adapter()
        acquire_session = getattr(adapter, "acquire_session", None)
        if not callable(acquire_session):
            raise FerricStoreError("protocol adapter does not support affine sessions")
        return acquire_session()

    def acquire_session_for_key(self, key: str | bytes) -> Any:
        return self.acquire_session_for_keys((key,))

    def acquire_session_for_keys(self, keys: Sequence[str | bytes]) -> Any:
        key = validate_single_slot(keys, slot_for_key=RoutingTopology.slot_for_key)
        if key is None:
            return self.acquire_session()
        route = self.route(key)
        try:
            adapter = self._adapter_for_endpoint(route["endpoint"])
            acquire_session = getattr(adapter, "acquire_session_on_lane", None)
            if callable(acquire_session):
                return acquire_session(int(route["lane_id"]))
            acquire_session = getattr(adapter, "acquire_session", None)
            if not callable(acquire_session):
                raise FerricStoreError("protocol adapter does not support affine sessions")
            return acquire_session()
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    self.refresh_topology()
            raise

    def refresh_topology(self) -> RoutingTopology:
        singleflight = getattr(self, "_refresh_singleflight", None)
        if singleflight is None:
            singleflight = SyncSingleFlight[RoutingTopology]()
            self._refresh_singleflight = singleflight
        return singleflight.run(self._refresh_topology_once)

    def _refresh_topology_once(self) -> RoutingTopology:
        last_error: BaseException | None = None
        with self._lock:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            candidate_urls = self._refresh_candidate_urls()
        for url in candidate_urls:
            try:
                adapter = self._adapter_for_url(url)
                topology = RoutingTopology.build(adapter.execute_command("SHARDS"))
                with self._lock:
                    if self._closed:
                        raise FerricStoreError("protocol topology pool is closed")
                    ready = self._install_topology(topology)
                    self._control_endpoints.mark_success(url)
                self._cleanup_retired_adapters(ready)
                if self.warm_connections:
                    for endpoint in topology.endpoints.values():
                        with contextlib.suppress(Exception):
                            self._adapter_for_endpoint(endpoint)
                return topology
            except Exception as exc:
                last_error = exc
                continue
        raise FerricStoreError("no FerricStore topology endpoint reachable", raw=last_error)

    def route(self, key: str | bytes) -> dict[str, Any]:
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        route = self.topology.route_key(key)
        self._validate_endpoint(route["endpoint"])
        return route

    def execute_command(self, *args: Any) -> Any:
        route_data = self._route_data(args)
        if route_data is None:
            return self._execute_control_method("execute_command", args)

        command, route = route_data
        try:
            adapter = self._adapter_for_endpoint(route["endpoint"])
            return self._execute_protocol_command(adapter, command, route["lane_id"], args)
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    self.refresh_topology()
            raise

    def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        route_data = self._route_data(args)
        if route_data is None:
            return cast(
                dict[str, Any],
                self._execute_control_method("execute_command_with_trace", args),
            )

        command, route = route_data
        try:
            adapter = self._adapter_for_endpoint(route["endpoint"])
            execute_on_lane = getattr(adapter, "execute_command_with_trace_on_lane", None)
            if callable(execute_on_lane):
                return cast(
                    dict[str, Any],
                    execute_on_lane(args, int(route["lane_id"])),
                )
            if not hasattr(adapter, "_request"):
                execute_with_trace = getattr(adapter, "execute_command_with_trace", None)
                if callable(execute_with_trace):
                    return cast(dict[str, Any], execute_with_trace(*args))
                return {"value": adapter.execute_command(*args), "trace": {}}
            request_on_lane = getattr(adapter, "_request_on_lane", adapter._request)
            response = request_on_lane(
                command.opcode,
                int(route["lane_id"]),
                command.payload,
                command.flags | _FLAG_TRACE,
            )
            return {"value": adapter._response_value(response), "trace": response.trace or {}}
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    self.refresh_topology()
            raise

    def _execute_control_method(self, method: str, args: tuple[Any, ...]) -> Any:
        adapter = self._control_adapter()
        try:
            return getattr(adapter, method)(*args)
        except Exception as exc:
            if not _is_retryable_route_error(exc):
                raise
            refreshed = False
            with contextlib.suppress(Exception):
                self.refresh_topology()
                refreshed = True
            if refreshed and _is_safe_control_retry(args):
                return getattr(self._control_adapter(), method)(*args)
            raise

    def submit_command(self, *args: Any) -> Future[Any]:
        route_data = self._route_data(args)
        if route_data is None:
            return cast(Future[Any], self._control_adapter().submit_command(*args))

        command, route = route_data
        try:
            adapter = self._adapter_for_endpoint(route["endpoint"])
            submit_on_lane = getattr(adapter, "submit_command_on_lane", None)
            if callable(submit_on_lane):
                return cast(Future[Any], submit_on_lane(args, int(route["lane_id"])))
            if hasattr(adapter, "_submit_request") and hasattr(adapter, "_value_future"):
                submit_on_lane = getattr(
                    adapter,
                    "_submit_request_on_lane",
                    adapter._submit_request,
                )
                _request_id, response_future = submit_on_lane(
                    command.opcode,
                    int(route["lane_id"]),
                    command.payload,
                    command.flags,
                )
                return cast(Future[Any], adapter._value_future(response_future))
            return cast(Future[Any], adapter.submit_command(*args))
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    self.refresh_topology()
            raise

    def submit_commands(self, commands: list[tuple[Any, ...]]) -> list[Future[Any]]:
        if not commands:
            return []
        try:
            plan = RouteBatchPlan.build(
                ((self._batch_target_for_command(args), args) for args in commands),
                group_key=lambda target: (id(target.adapter), target.lane_id),
            )
            group_futures: list[list[Future[Any]]] = []
            for group in plan.groups:
                target = group.target
                submit_commands = (
                    getattr(target.adapter, "submit_commands_on_lane", None)
                    if target.lane_id is not None
                    else None
                )
                if callable(submit_commands):
                    futures = list(submit_commands(group.commands, target.lane_id))
                    group_futures.append(futures)
                    continue
                submit_commands = getattr(target.adapter, "submit_commands", None)
                if callable(submit_commands):
                    futures = list(submit_commands(group.commands))
                else:
                    futures = [target.adapter.submit_command(*args) for args in group.commands]
                group_futures.append(futures)
            return cast(list[Future[Any]], plan.merge(group_futures))
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    self.refresh_topology()
            raise

    def submit_batch(self, commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
        future: Future[list[Any]] = Future()
        if not commands:
            future.set_result([])
            return future
        try:
            plan = RouteBatchPlan.build(
                ((self._batch_target_for_command(args), args) for args in commands),
                group_key=lambda target: (id(target.adapter), target.lane_id),
            )
            group_futures: list[Future[list[Any]]] = []
            for group in plan.groups:
                target = group.target
                submit_batch = (
                    getattr(target.adapter, "submit_batch_on_lane", None)
                    if target.lane_id is not None
                    else None
                )
                if callable(submit_batch):
                    group_future = submit_batch(group.commands, target.lane_id)
                else:
                    group_future = target.adapter.submit_batch(group.commands)
                group_futures.append(cast(Future[list[Any]], group_future))
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    self.refresh_topology()
            raise
        _set_wire_future_sources(
            future,
            [
                source
                for group_future in group_futures
                for source in getattr(group_future, "_ferricstore_sources", (group_future,))
            ],
        )
        lock = threading.Lock()
        group_values: list[list[Any] | None] = [None] * len(group_futures)
        remaining = len(group_futures)

        def complete_group(index: int, source: Future[list[Any]]) -> None:
            nonlocal remaining
            try:
                values = list(source.result())
            except Exception as exc:
                with lock:
                    try_set_future_exception(future, exc)
                return
            with lock:
                if future.done():
                    return
                group_values[index] = values
                remaining -= 1
                if remaining == 0:
                    try:
                        merged = plan.merge(cast(list[list[Any]], group_values))
                    except Exception as exc:
                        try_set_future_exception(future, exc)
                    else:
                        try_set_future_result(future, merged)

        for index, group_future in enumerate(group_futures):
            group_future.add_done_callback(partial(complete_group, index))
        return future

    def submit_mget(self, keys: Sequence[Any]) -> Future[Any]:
        target = self._batch_target_for_keys(keys)
        submit_on_lane = getattr(target.adapter, "submit_command_on_lane", None)
        if target.lane_id is not None and callable(submit_on_lane):
            return cast(
                Future[Any],
                submit_on_lane(("MGET", *keys), target.lane_id),
            )
        return cast(Future[Any], target.adapter.submit_mget(keys))

    def submit_mset_same_value(self, keys: Sequence[Any], value: Any) -> Future[Any]:
        target = self._batch_target_for_keys(keys)
        submit_on_lane = getattr(target.adapter, "submit_command_on_lane", None)
        if target.lane_id is not None and callable(submit_on_lane):
            args: list[Any] = ["MSET"]
            for key in keys:
                args.extend([key, value])
            return cast(Future[Any], submit_on_lane(tuple(args), target.lane_id))
        return cast(Future[Any], target.adapter.submit_mset_same_value(keys, value))

    def submit_mset_payload(self, payload: bytes) -> Future[Any]:
        target = self._opaque_payload_target()
        submit_on_lane = getattr(target.adapter, "submit_mset_payload_on_lane", None)
        if callable(submit_on_lane):
            return cast(Future[Any], submit_on_lane(payload, target.lane_id))
        return cast(Future[Any], target.adapter.submit_mset_payload(payload))

    def submit_pipeline_payload(self, payload: bytes, count: int) -> Future[list[Any]]:
        target = self._opaque_payload_target()
        submit_on_lane = getattr(target.adapter, "submit_pipeline_payload_on_lane", None)
        if callable(submit_on_lane):
            return cast(Future[list[Any]], submit_on_lane(payload, count, target.lane_id))
        return cast(Future[list[Any]], target.adapter.submit_pipeline_payload(payload, count))

    def submit_flow_many_payload(
        self, command: str, payload: bytes, count: int
    ) -> Future[list[Any]]:
        target = self._opaque_payload_target()
        submit_on_lane = getattr(target.adapter, "submit_flow_many_payload_on_lane", None)
        if callable(submit_on_lane):
            return cast(
                Future[list[Any]],
                submit_on_lane(command, payload, count, target.lane_id),
            )
        return cast(
            Future[list[Any]],
            target.adapter.submit_flow_many_payload(command, payload, count),
        )

    def submit_flow_value_mget_payload(self, payload: bytes) -> Future[Any]:
        target = self._opaque_payload_target()
        submit_on_lane = getattr(
            target.adapter,
            "submit_flow_value_mget_payload_on_lane",
            None,
        )
        if callable(submit_on_lane):
            return cast(Future[Any], submit_on_lane(payload, target.lane_id))
        return cast(Future[Any], target.adapter.submit_flow_value_mget_payload(payload))

    def subscribe_flow_wake(self, *args: Any, **kwargs: Any) -> Any:
        update_lock = getattr(self, "_subscription_update_lock", None)
        if update_lock is None:
            update_lock = threading.Lock()
            self._subscription_update_lock = update_lock
        with update_lock:
            with self._lock:
                if self._closed:
                    raise FerricStoreError("protocol topology pool is closed")
                self._subscription_registry.remember(args, kwargs)
                self._subscription_generation += 1
                adapters = list(self._adapters.values())
            replies = []
            for adapter in adapters or [self._control_adapter()]:
                subscribe = getattr(adapter, "subscribe_flow_wake", None)
                if callable(subscribe):
                    replies.append(subscribe(*args, **kwargs))
            return replies[0] if len(replies) == 1 else replies

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        if not commands:
            return []

        def execute_group(group: Any) -> list[Any]:
            target = group.target
            execute_batch = (
                getattr(target.adapter, "execute_batch_on_lane", None)
                if target.lane_id is not None
                else None
            )
            if callable(execute_batch):
                return list(execute_batch(group.commands, target.lane_id))
            execute_batch = getattr(target.adapter, "execute_batch", None)
            return (
                list(execute_batch(group.commands))
                if callable(execute_batch)
                else [target.adapter.execute_command(*args) for args in group.commands]
            )

        try:
            plan = RouteBatchPlan.build(
                ((self._batch_target_for_command(args), args) for args in commands),
                group_key=lambda target: (id(target.adapter), target.lane_id),
            )
            group_values = self._batch_fanout.run(
                plan.groups,
                execute_group,
                concurrent=True,
            )
            return plan.merge(group_values)
        except Exception as exc:
            if _is_retryable_route_error(exc):
                with contextlib.suppress(Exception):
                    self.refresh_topology()
            raise

    def _execute_protocol_command(
        self, adapter: Any, command: ProtocolCommand, lane_id: int, args: tuple[Any, ...]
    ) -> Any:
        execute_on_lane = getattr(adapter, "execute_command_on_lane", None)
        if callable(execute_on_lane):
            return execute_on_lane(args, int(lane_id))
        if not hasattr(adapter, "_request") or not hasattr(adapter, "_response_value"):
            return adapter.execute_command(*args)
        request_on_lane = getattr(adapter, "_request_on_lane", adapter._request)
        response = request_on_lane(
            command.opcode,
            int(lane_id),
            command.payload,
            command.flags,
        )
        return adapter._response_value(response)

    def _route_data(self, args: tuple[Any, ...]) -> tuple[ProtocolCommand, dict[str, Any]] | None:
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
        return command, self.route(key)

    def _single_shard_key(self, keys: Sequence[Any]) -> str | bytes | None:
        decision = route_for_keys(keys, slot_for_key=RoutingTopology.slot_for_key)
        return decision.require_routable_key()

    def _adapter_for_command(self, args: tuple[Any, ...]) -> Any:
        route_data = self._route_data(args)
        if route_data is None:
            return self._control_adapter()
        return self._adapter_for_endpoint(route_data[1]["endpoint"])

    def _batch_target_for_command(self, args: tuple[Any, ...]) -> RoutedBatchTarget[Any]:
        route_data = self._route_data(args)
        if route_data is None:
            return RoutedBatchTarget(self._control_adapter(), None)
        route = route_data[1]
        return RoutedBatchTarget(
            self._adapter_for_endpoint(route["endpoint"]),
            int(route["lane_id"]),
        )

    def _batch_target_for_keys(self, keys: Sequence[Any]) -> RoutedBatchTarget[Any]:
        key = self._single_shard_key(keys)
        if key is None:
            return RoutedBatchTarget(self._control_adapter(), None)
        route = self.route(key)
        return RoutedBatchTarget(
            self._adapter_for_endpoint(route["endpoint"]),
            int(route["lane_id"]),
        )

    def _adapter_for_keys(self, keys: Sequence[Any]) -> Any | None:
        key = self._single_shard_key(keys)
        if key is None:
            return None
        route = self.route(key)
        return self._adapter_for_endpoint(route["endpoint"])

    def _control_adapter(self) -> Any:
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        for url in self._refresh_candidate_urls():
            with contextlib.suppress(Exception):
                return self._adapter_for_url(url)
        return self._adapter_for_url(self.seed_urls[0])

    def _opaque_payload_target(self) -> RoutedBatchTarget[Any]:
        """Route opaque payloads only when topology has one exact destination."""
        with self._lock:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            destinations = self.topology.route_destinations
        if len(destinations) != 1:
            raise InvalidCommandError(
                "opaque payload submission requires exactly one topology route; "
                "submit decoded commands when routing across multiple leaders"
            )
        route = destinations[0]
        return RoutedBatchTarget(
            self._adapter_for_endpoint(route["endpoint"]),
            int(route["lane_id"]),
        )

    def _adapter_for_url(self, url: str) -> Any:
        endpoint = _endpoint_from_url(url)
        key = _connection_endpoint_key(
            endpoint,
            tls=urlparse(url).scheme.lower() in _TLS_SCHEMES,
        )
        if key not in self._seed_endpoint_keys:
            self._validate_endpoint(endpoint)
        return self._adapter_for_key(key, lambda: self._new_endpoint_adapter(url))

    def _adapter_for_endpoint(self, endpoint: Mapping[str, Any]) -> Any:
        self._validate_endpoint(endpoint)
        key = _connection_endpoint_key(endpoint, tls=self._tls)
        return self._adapter_for_key(
            key,
            lambda: self._new_endpoint_adapter(_url_from_endpoint(endpoint, tls=self._tls)),
        )

    def _adapter_for_key(
        self,
        key: tuple[str, int],
        create: Callable[[], Any],
    ) -> Any:
        with self._lock:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            adapter = self._endpoint_lifecycle.get(key)
            if adapter is not None:
                return adapter
            creation = self._adapter_creations.get(key)
            if creation is None:
                creation = Future()
                self._adapter_creations[key] = creation
                creator = True
            else:
                creator = False
        if not creator:
            return creation.result()

        candidate: Any | None = None
        try:
            candidate = create()
            self._register_adapter_events(candidate)
            while True:
                with self._lock:
                    if self._closed:
                        raise FerricStoreError("protocol topology pool is closed")
                    subscription_generation = self._subscription_generation
                self._subscription_registry.activate_sync(candidate)
                with self._lock:
                    if self._closed:
                        raise FerricStoreError("protocol topology pool is closed")
                    if subscription_generation != self._subscription_generation:
                        continue
                    self._endpoint_lifecycle.put(key, candidate)
                    creation.set_result(candidate)
                    self._adapter_creations.pop(key, None)
                    self._adapter_creation_cv.notify_all()
                    self._event_ready.set()
                    return candidate
        except BaseException as error:
            if candidate is not None:
                self._cleanup_adapters.add(candidate)
                self._cleanup_retired_adapters([candidate])
            with self._lock:
                creation.set_exception(error)
                self._adapter_creations.pop(key, None)
                self._adapter_creation_cv.notify_all()
            raise

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
        cleanup_adapters = self._cleanup_adapters
        for adapter in ready:
            cleanup_adapters.add(adapter)
        return ready

    def _retired_adapter_became_idle(
        self,
        key: tuple[str, int],
        adapter: Any,
    ) -> None:
        with self._lock:
            if self._closed:
                return
            ready = self._endpoint_lifecycle.claim_idle(key, adapter)
            if ready is None:
                return
            self._cleanup_adapters.add(ready)
        self._cleanup_retired_adapters([ready])

    def _cleanup_retired_adapters(self, adapters: Sequence[Any]) -> None:
        cleanup_lock = self._adapter_cleanup_lock
        retained = self._cleanup_adapters.snapshot()
        for adapter in _unique_adapters([*adapters, *retained]):
            with cleanup_lock:
                if not self._cleanup_adapters.contains(adapter):
                    continue
                try:
                    _close_adapters_sync([adapter], self._event_listener)
                except BaseException:
                    continue
                self._cleanup_adapters.complete(adapter)
            with self._lock:
                snapshot = self._close_adapters_snapshot
                if snapshot is not None:
                    snapshot[:] = [candidate for candidate in snapshot if candidate is not adapter]

    def _new_endpoint_adapter(self, url: str) -> Any:
        return ProtocolAdapterPool.from_url(
            url,
            max_connections=self._max_connections,
            **self._adapter_kwargs,
        )

    def _register_adapter_events(self, adapter: Any) -> None:
        add_listener = getattr(adapter, "add_event_listener", None)
        if callable(add_listener):
            add_listener(self._event_listener)
        else:
            self._event_poll_fallback = True

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
    "RoutingTopology",
    "TopologyProtocolAdapterPool",
]
