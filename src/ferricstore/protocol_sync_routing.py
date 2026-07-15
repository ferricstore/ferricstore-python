from __future__ import annotations

import contextlib
import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import Future
from typing import TYPE_CHECKING, Any

from ferricstore.errors import FerricStoreError, InvalidCommandError
from ferricstore.protocol_common import RoutingTopology, _command_name, _connection_endpoint_key
from ferricstore.protocol_constants import _CONTROL_OPCODES
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_BATCH_ITEMS,
    PendingRequestCapacityError,
    check_batch_item_limit,
)
from ferricstore.protocol_planning import PreparedCommand
from ferricstore.topology_core import (
    ControlEndpointSelector,
    RoutedBatchTarget,
    route_for_command,
    route_for_keys,
)
from ferricstore.topology_lifecycle import EndpointAdapterLease


class _TopologyGenerationChanged(Exception):
    pass


class SyncTopologyRoutingMixin:
    """Route sync commands against one immutable topology generation."""

    if TYPE_CHECKING:
        seed_urls: list[str]
        topology: RoutingTopology
        _closed: bool
        _control_endpoints: ControlEndpointSelector
        _adapter_creation_cv: threading.Condition
        _adapter_creations: dict[tuple[str, int], Future[Any]]
        _cleanup_adapters: Any
        _endpoint_lifecycle: Any
        _event_ready: threading.Event
        _lock: threading.RLock
        _subscription_generation: int
        _subscription_registry: Any
        _tls: bool
        _topology_generation: int
        max_batch_items: int | None

        def route(self, key: str | bytes) -> dict[str, Any]: ...

        def _prepare_routed_command(self, args: tuple[Any, ...]) -> PreparedCommand: ...

        def _adapter_for_url(self, url: str) -> Any: ...

        def _adapter_for_endpoint(self, endpoint: Mapping[str, Any]) -> Any: ...

        def _leased_adapter_for_endpoint(
            self,
            endpoint: Mapping[str, Any],
            *,
            generation: int,
        ) -> EndpointAdapterLease[tuple[str, int]]: ...

        def _release_adapter_lease(
            self,
            lease: EndpointAdapterLease[tuple[str, int]],
        ) -> None: ...

        def _refresh_candidate_urls(self) -> list[str]: ...

        def _register_adapter_events(self, adapter: Any) -> None: ...

        def _cleanup_retired_adapters(self, adapters: Sequence[Any]) -> None: ...

    def _route_data(
        self,
        args: tuple[Any, ...],
    ) -> tuple[PreparedCommand, dict[str, Any]] | None:
        if not args:
            return None
        try:
            prepared = self._prepare_routed_command(args)
        except Exception:
            return None
        route = self._route_prepared(prepared)
        return None if route is None else (prepared, route)

    def _route_prepared(self, prepared: PreparedCommand) -> dict[str, Any] | None:
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
        with self._lock:
            route = dict(self.route(key))
            route["_sdk_generation"] = self._topology_generation
        return route

    def _prepare_routed_batch(
        self,
        commands: Sequence[tuple[Any, ...]],
    ) -> tuple[
        list[tuple[RoutedBatchTarget[Any], PreparedCommand]],
        list[EndpointAdapterLease[tuple[str, int]]],
    ]:
        try:
            check_batch_item_limit(
                len(commands),
                getattr(self, "max_batch_items", DEFAULT_MAX_BATCH_ITEMS),
            )
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc
        prepared_commands = [self._prepare_routed_command(args) for args in commands]
        while True:
            with self._lock:
                generation = self._topology_generation
            routed_commands: list[tuple[RoutedBatchTarget[Any], PreparedCommand]] = []
            leases: list[EndpointAdapterLease[tuple[str, int]]] = []
            leases_by_endpoint: dict[tuple[str, int], EndpointAdapterLease[tuple[str, int]]] = {}
            try:
                for prepared in prepared_commands:
                    route = self._route_prepared(prepared)
                    if route is None:
                        target = RoutedBatchTarget(self._control_adapter(), None)
                    elif int(route["_sdk_generation"]) != generation:
                        raise _TopologyGenerationChanged
                    else:
                        key = _connection_endpoint_key(route["endpoint"], tls=self._tls)
                        lease = leases_by_endpoint.get(key)
                        if lease is None:
                            lease = self._leased_adapter_for_endpoint(
                                route["endpoint"],
                                generation=generation,
                            )
                            leases_by_endpoint[key] = lease
                            leases.append(lease)
                        target = RoutedBatchTarget(lease.adapter, int(route["lane_id"]))
                    routed_commands.append((target, prepared))
                with self._lock:
                    if generation != self._topology_generation:
                        raise _TopologyGenerationChanged
            except _TopologyGenerationChanged:
                for lease in reversed(leases):
                    self._release_adapter_lease(lease)
                continue
            except BaseException:
                for lease in reversed(leases):
                    self._release_adapter_lease(lease)
                raise
            return routed_commands, leases

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

    def _leased_batch_target_for_keys(
        self,
        keys: Sequence[Any],
    ) -> tuple[
        RoutedBatchTarget[Any],
        EndpointAdapterLease[tuple[str, int]] | None,
    ]:
        key = self._single_shard_key(keys)
        if key is None:
            return RoutedBatchTarget(self._control_adapter(), None), None
        while True:
            with self._lock:
                route = dict(self.route(key))
                generation = self._topology_generation
            try:
                lease = self._leased_adapter_for_endpoint(
                    route["endpoint"],
                    generation=generation,
                )
            except _TopologyGenerationChanged:
                continue
            return RoutedBatchTarget(lease.adapter, int(route["lane_id"])), lease

    def _adapter_for_keys(self, keys: Sequence[Any]) -> Any | None:
        key = self._single_shard_key(keys)
        if key is None:
            return None
        route = self.route(key)
        return self._adapter_for_endpoint(route["endpoint"])

    def _adapter_for_key(
        self,
        key: tuple[str, int],
        create: Callable[[], Any],
        *,
        acquire: bool = False,
        expected_generation: int | None = None,
    ) -> Any:
        with self._lock:
            if self._closed:
                raise FerricStoreError("protocol topology pool is closed")
            if expected_generation is not None and expected_generation != self._topology_generation:
                raise _TopologyGenerationChanged
            adapter = self._endpoint_lifecycle.get(key)
            if adapter is not None:
                if not acquire:
                    return adapter
                lease = self._endpoint_lifecycle.acquire(key, adapter)
                if lease is None:
                    raise FerricStoreError("protocol endpoint adapter is unavailable")
                return lease
            creation = self._adapter_creations.get(key)
            if creation is None:
                creation = Future()
                self._adapter_creations[key] = creation
                creator = True
            else:
                creator = False
            if acquire:
                self._endpoint_lifecycle.reserve(key)
        if not creator:
            try:
                adapter = creation.result()
            except BaseException:
                if acquire:
                    with self._lock:
                        ready = self._endpoint_lifecycle.cancel_reservation(key)
                    if ready is not None:
                        self._cleanup_retired_adapters([ready])
                raise
            if not acquire:
                return adapter
            with self._lock:
                generation_changed = (
                    expected_generation is not None
                    and expected_generation != self._topology_generation
                )
                if generation_changed:
                    ready = self._endpoint_lifecycle.cancel_reservation(key)
                    lease = None
                else:
                    ready = None
                    lease = self._endpoint_lifecycle.acquire_reserved(key, adapter)
            if ready is not None:
                self._cleanup_retired_adapters([ready])
            if generation_changed:
                raise _TopologyGenerationChanged
            if lease is None:
                raise FerricStoreError("protocol endpoint adapter is unavailable")
            return lease

        candidate: Any | None = None
        try:
            candidate = create()
            self._register_adapter_events(candidate)
            while True:
                with self._lock:
                    if self._closed:
                        raise FerricStoreError("protocol topology pool is closed")
                    if (
                        expected_generation is not None
                        and expected_generation != self._topology_generation
                    ):
                        raise _TopologyGenerationChanged
                    subscription_generation = self._subscription_generation
                self._subscription_registry.activate_sync(candidate)
                with self._lock:
                    if self._closed:
                        raise FerricStoreError("protocol topology pool is closed")
                    if (
                        expected_generation is not None
                        and expected_generation != self._topology_generation
                    ):
                        raise _TopologyGenerationChanged
                    if subscription_generation != self._subscription_generation:
                        continue
                    self._endpoint_lifecycle.put(key, candidate)
                    lease = (
                        self._endpoint_lifecycle.acquire_reserved(key, candidate)
                        if acquire
                        else None
                    )
                    creation.set_result(candidate)
                    self._adapter_creations.pop(key, None)
                    self._adapter_creation_cv.notify_all()
                    self._event_ready.set()
                    if acquire:
                        if lease is None:
                            raise FerricStoreError("protocol endpoint adapter is unavailable")
                        return lease
                    return candidate
        except BaseException as error:
            if candidate is not None:
                self._cleanup_adapters.add(candidate)
                self._cleanup_retired_adapters([candidate])
            ready = None
            with self._lock:
                if acquire:
                    ready = self._endpoint_lifecycle.cancel_reservation(key)
                    if ready is not None:
                        self._cleanup_adapters.add(ready)
                creation.set_exception(error)
                self._adapter_creations.pop(key, None)
                self._adapter_creation_cv.notify_all()
            if ready is not None:
                self._cleanup_retired_adapters([ready])
            raise

    def _control_adapter(self) -> Any:
        if self._closed:
            raise FerricStoreError("protocol topology pool is closed")
        preferred_url = self._control_endpoints.preferred_url
        if preferred_url is not None:
            with contextlib.suppress(Exception):
                return self._adapter_for_url(preferred_url)
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

    def _leased_opaque_payload_target(
        self,
    ) -> tuple[RoutedBatchTarget[Any], EndpointAdapterLease[tuple[str, int]]]:
        while True:
            with self._lock:
                if self._closed:
                    raise FerricStoreError("protocol topology pool is closed")
                destinations = self.topology.route_destinations
                generation = self._topology_generation
            if len(destinations) != 1:
                raise InvalidCommandError(
                    "opaque payload submission requires exactly one topology route; "
                    "submit decoded commands when routing across multiple leaders"
                )
            route = destinations[0]
            try:
                lease = self._leased_adapter_for_endpoint(
                    route["endpoint"],
                    generation=generation,
                )
            except _TopologyGenerationChanged:
                continue
            return RoutedBatchTarget(lease.adapter, int(route["lane_id"])), lease


__all__ = ["SyncTopologyRoutingMixin", "_TopologyGenerationChanged"]
