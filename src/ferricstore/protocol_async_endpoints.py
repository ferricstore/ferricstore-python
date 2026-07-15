from __future__ import annotations

import asyncio
import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from ferricstore.batch_core import run_async_fanout
from ferricstore.errors import FerricStoreError
from ferricstore.protocol_async_pool import AsyncProtocolAdapterPool
from ferricstore.protocol_common import (
    RoutingTopology,
    _close_adapter_async,
    _connection_endpoint_key,
    _endpoint_from_url,
    _topology_update_required,
    _url_from_endpoint,
)
from ferricstore.protocol_constants import _TLS_SCHEMES
from ferricstore.topology_lifecycle import EndpointAdapterLease, EndpointAdapterLifecycle
from ferricstore.topology_security import TopologyEndpointTrust


class AsyncTopologyEndpointMixin:
    """Own endpoint creation, retirement, cleanup, warming, and trust checks."""

    if TYPE_CHECKING:
        _adapter_kwargs: dict[str, Any]
        _cleanup_adapters: Any
        _cleanup_retry_requested: set[int]
        _cleanup_tasks: set[asyncio.Task[None]]
        _cleanup_tasks_by_adapter: dict[int, asyncio.Task[None]]
        _closed: bool
        _control_endpoints: Any
        _endpoint_lifecycle: EndpointAdapterLifecycle[tuple[str, int]]
        _endpoint_trust: TopologyEndpointTrust
        _event_listener: Callable[[], None]
        _event_poll_fallback: bool
        _max_connections: int
        _seed_endpoint_keys: set[tuple[str, int]]
        _seed_urls_by_endpoint: dict[tuple[str, int], str]
        _subscription_registry: Any
        _tls: bool
        _topology_generation: int
        _trusted_hosts: set[str]
        _warm_semaphore: asyncio.Semaphore
        endpoint_policy: str | tuple[str, Sequence[str]]
        endpoint_validator: Callable[[Mapping[str, Any]], bool | None] | None
        seed_urls: list[str]
        topology: RoutingTopology

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
            seed_url = getattr(self, "_seed_urls_by_endpoint", {}).get(key)
            adapter = self._new_endpoint_adapter(
                seed_url or _url_from_endpoint(endpoint, tls=self._tls)
            )
            try:
                self._register_adapter(adapter)
            except BaseException:
                self._schedule_adapter_cleanup(adapter)
                raise
            self._endpoint_lifecycle.put(key, adapter)
        return adapter

    def _leased_adapter_for_endpoint(
        self,
        endpoint: Mapping[str, Any],
    ) -> EndpointAdapterLease[tuple[str, int]]:
        adapter = self._adapter_for_endpoint(endpoint)
        key = _connection_endpoint_key(endpoint, tls=self._tls)
        lease = self._endpoint_lifecycle.acquire(key, adapter)
        if lease is None:
            raise FerricStoreError("protocol endpoint adapter is unavailable")
        return lease

    def _release_adapter_lease(
        self,
        lease: EndpointAdapterLease[tuple[str, int]],
    ) -> None:
        ready = self._endpoint_lifecycle.release(lease)
        if ready is None:
            return
        self._cleanup_adapters.add(ready)
        self._schedule_retained_adapter_cleanup()

    def _install_topology(self, topology: RoutingTopology) -> list[Any]:
        if not _topology_update_required(self.topology, topology):
            return []
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
        trust = getattr(self, "_endpoint_trust", None)
        if trust is None:
            trust = TopologyEndpointTrust(
                policy=self.endpoint_policy,
                seed_endpoint_keys=self._seed_endpoint_keys,
                trusted_hosts=list(self._trusted_hosts),
                tls=self._tls,
                validator=self.endpoint_validator,
            )
            self._endpoint_trust = trust
        trust.validate(endpoint)

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


__all__ = ["AsyncTopologyEndpointMixin"]
