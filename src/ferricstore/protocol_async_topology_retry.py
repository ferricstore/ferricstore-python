from __future__ import annotations

import asyncio
import contextlib
import inspect
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
from ferricstore.topology_lifecycle import EndpointAdapterLease


class AsyncTopologyRetryMixin:
    """Safe one-shot command replay for an async topology host."""

    if TYPE_CHECKING:

        def _control_adapter(self) -> Any: ...

        async def _execute_protocol_command(
            self,
            adapter: Any,
            prepared: PreparedCommand,
            lane_id: int,
        ) -> Any: ...

        def _leased_adapter_for_endpoint(
            self,
            endpoint: Mapping[str, Any],
        ) -> EndpointAdapterLease[tuple[str, int]]: ...

        def _release_adapter_lease(
            self,
            lease: EndpointAdapterLease[tuple[str, int]],
        ) -> None: ...

        async def _route_data(
            self,
            args: tuple[Any, ...],
        ) -> tuple[PreparedCommand, dict[str, Any]] | None: ...

        async def refresh_topology(self) -> RoutingTopology: ...

    async def execute_command(self, *args: Any) -> Any:
        route_data = await self._route_data(args)
        if route_data is None:
            return await self._execute_control_method("execute_command", args)

        for attempt in range(2):
            prepared, route = route_data
            lease: EndpointAdapterLease[tuple[str, int]] | None = None
            try:
                lease = self._leased_adapter_for_endpoint(route["endpoint"])
                adapter = lease.adapter
                return await self._execute_protocol_command(
                    adapter,
                    prepared,
                    route["lane_id"],
                )
            except Exception as exc:
                if attempt != 0 or not _is_retryable_route_error(exc):
                    raise
                refresh_required = _is_topology_refresh_error(exc)
                refreshed = not refresh_required
                if refresh_required:
                    with contextlib.suppress(Exception):
                        await self.refresh_topology()
                        refreshed = True
                if flow_query_retry_is_managed():
                    raise
                if not (refreshed and _is_safe_control_retry(args) and _server_allows_retry(exc)):
                    raise
                delay = _server_retry_delay_seconds(exc)
                if delay:
                    await asyncio.sleep(delay)
                route_data = await self._route_data(args)
                if route_data is None:
                    result = self._control_adapter().execute_command(*args)
                    return await result if inspect.isawaitable(result) else result
            finally:
                if lease is not None:
                    self._release_adapter_lease(lease)

        raise AssertionError("unreachable")

    async def _execute_control_method(self, method: str, args: tuple[Any, ...]) -> Any:
        adapter = self._control_adapter()
        try:
            result = getattr(adapter, method)(*args)
            return await result if inspect.isawaitable(result) else result
        except Exception as exc:
            if not _is_retryable_route_error(exc):
                raise
            refresh_required = _is_topology_refresh_error(exc)
            refreshed = not refresh_required
            if refresh_required:
                with contextlib.suppress(Exception):
                    await self.refresh_topology()
                    refreshed = True
            if flow_query_retry_is_managed():
                raise
            if refreshed and _is_safe_control_retry(args) and _server_allows_retry(exc):
                delay = _server_retry_delay_seconds(exc)
                if delay:
                    await asyncio.sleep(delay)
                result = getattr(self._control_adapter(), method)(*args)
                return await result if inspect.isawaitable(result) else result
            raise


__all__ = ["AsyncTopologyRetryMixin"]
