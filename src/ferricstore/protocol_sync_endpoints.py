from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from ferricstore.protocol_common import (
    RoutingTopology,
    _close_adapters_sync,
    _connection_endpoint_key,
    _endpoint_from_url,
    _topology_update_required,
    _unique_adapters,
    _url_from_endpoint,
)
from ferricstore.protocol_constants import _TLS_SCHEMES
from ferricstore.protocol_sync_pool import ProtocolAdapterPool
from ferricstore.topology_lifecycle import EndpointAdapterLease, EndpointAdapterLifecycle
from ferricstore.topology_security import TopologyEndpointTrust


class SyncTopologyEndpointMixin:
    """Own sync endpoint creation, retirement, cleanup, and trust checks."""

    if TYPE_CHECKING:
        _adapter_cleanup_lock: threading.Lock
        _adapter_kwargs: dict[str, Any]
        _cleanup_adapters: Any
        _close_adapters_snapshot: list[Any] | None
        _closed: bool
        _control_endpoints: Any
        _endpoint_lifecycle: EndpointAdapterLifecycle[tuple[str, int]]
        _endpoint_trust: TopologyEndpointTrust
        _event_listener: Callable[[], None]
        _event_poll_fallback: bool
        _lock: threading.RLock
        _max_connections: int
        _seed_endpoint_keys: set[tuple[str, int]]
        _seed_urls_by_endpoint: dict[tuple[str, int], str]
        _tls: bool
        _topology_generation: int
        _trusted_hosts: set[str]
        endpoint_policy: str | tuple[str, Sequence[str]]
        endpoint_validator: Callable[[Mapping[str, Any]], bool | None] | None
        seed_urls: list[str]
        topology: RoutingTopology

        def _adapter_for_key(
            self,
            key: tuple[str, int],
            create: Callable[[], Any],
            *,
            acquire: bool = False,
            expected_generation: int | None = None,
        ) -> Any: ...

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
        seed_url = getattr(self, "_seed_urls_by_endpoint", {}).get(key)
        return self._adapter_for_key(
            key,
            lambda: self._new_endpoint_adapter(
                seed_url or _url_from_endpoint(endpoint, tls=self._tls)
            ),
        )

    def _leased_adapter_for_endpoint(
        self,
        endpoint: Mapping[str, Any],
        *,
        generation: int,
    ) -> EndpointAdapterLease[tuple[str, int]]:
        self._validate_endpoint(endpoint)
        key = _connection_endpoint_key(endpoint, tls=self._tls)
        seed_url = getattr(self, "_seed_urls_by_endpoint", {}).get(key)
        lease = self._adapter_for_key(
            key,
            lambda: self._new_endpoint_adapter(
                seed_url or _url_from_endpoint(endpoint, tls=self._tls)
            ),
            acquire=True,
            expected_generation=generation,
        )
        return cast(EndpointAdapterLease[tuple[str, int]], lease)

    def _release_adapter_lease(
        self,
        lease: EndpointAdapterLease[tuple[str, int]],
    ) -> None:
        with self._lock:
            ready = self._endpoint_lifecycle.release(lease)
            if ready is not None:
                self._cleanup_adapters.add(ready)
        if ready is not None:
            self._cleanup_retired_adapters([ready])

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
        with self._lock:
            if self._closed:
                return
            ready = self._endpoint_lifecycle.claim_idle(key, adapter)
            if ready is None:
                return
            self._cleanup_adapters.add(ready)
        self._cleanup_retired_adapters([ready])

    def _cleanup_retired_adapters(self, adapters: Sequence[Any]) -> None:
        retained = self._cleanup_adapters.snapshot()
        for adapter in _unique_adapters([*adapters, *retained]):
            with self._adapter_cleanup_lock:
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


__all__ = ["SyncTopologyEndpointMixin"]
