from __future__ import annotations

import contextlib
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from ferricstore.flow_query_retry import flow_query_retry_is_managed
from ferricstore.protocol_common import (
    RoutingTopology,
    _is_retryable_route_error,
    _is_safe_control_retry,
    _is_topology_refresh_error,
    _server_allows_retry,
    _server_retry_delay_seconds,
)
from ferricstore.protocol_planning import PreparedCommand
from ferricstore.protocol_sync_routing import _TopologyGenerationChanged
from ferricstore.topology_lifecycle import EndpointAdapterLease


class SyncTopologyRetryMixin:
    """Safe one-shot command replay for a sync topology host."""

    if TYPE_CHECKING:

        def _control_adapter(self) -> Any: ...

        def _execute_protocol_command(
            self,
            adapter: Any,
            prepared: PreparedCommand,
            lane_id: int,
        ) -> Any: ...

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

        def _route_data(
            self,
            args: tuple[Any, ...],
        ) -> tuple[PreparedCommand, dict[str, Any]] | None: ...

        def refresh_topology(self) -> RoutingTopology: ...

    def execute_command(self, *args: Any) -> Any:
        route_data = self._route_data(args)
        if route_data is None:
            return self._execute_control_method("execute_command", args)

        for attempt in range(2):
            lease: EndpointAdapterLease[tuple[str, int]] | None = None
            try:
                while True:
                    prepared, route = route_data
                    try:
                        lease = self._leased_adapter_for_endpoint(
                            route["endpoint"],
                            generation=int(route["_sdk_generation"]),
                        )
                    except _TopologyGenerationChanged:
                        route_data = self._route_data(args)
                        if route_data is None:
                            return self._execute_control_method("execute_command", args)
                        continue
                    break
                adapter = lease.adapter
                return self._execute_protocol_command(adapter, prepared, route["lane_id"])
            except Exception as exc:
                if attempt != 0 or not _is_retryable_route_error(exc):
                    raise
                refresh_required = _is_topology_refresh_error(exc)
                refreshed = not refresh_required
                if refresh_required:
                    with contextlib.suppress(Exception):
                        self.refresh_topology()
                        refreshed = True
                if flow_query_retry_is_managed():
                    raise
                if not (refreshed and _is_safe_control_retry(args) and _server_allows_retry(exc)):
                    raise
                delay = _server_retry_delay_seconds(exc)
                if delay:
                    time.sleep(delay)
                route_data = self._route_data(args)
                if route_data is None:
                    return self._control_adapter().execute_command(*args)
            finally:
                if lease is not None:
                    self._release_adapter_lease(lease)

        raise AssertionError("unreachable")

    def _execute_control_method(self, method: str, args: tuple[Any, ...]) -> Any:
        adapter = self._control_adapter()
        try:
            return getattr(adapter, method)(*args)
        except Exception as exc:
            if not _is_retryable_route_error(exc):
                raise
            refresh_required = _is_topology_refresh_error(exc)
            refreshed = not refresh_required
            if refresh_required:
                with contextlib.suppress(Exception):
                    self.refresh_topology()
                    refreshed = True
            if flow_query_retry_is_managed():
                raise
            if refreshed and _is_safe_control_retry(args) and _server_allows_retry(exc):
                delay = _server_retry_delay_seconds(exc)
                if delay:
                    time.sleep(delay)
                return getattr(self._control_adapter(), method)(*args)
            raise


__all__ = ["SyncTopologyRetryMixin"]
