from __future__ import annotations

import socket
import ssl
import threading
from concurrent.futures import Future
from typing import Any, Protocol, cast

from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import try_set_future_exception
from ferricstore.protocol_common import (
    _encode_request_body,
    _request_body_byte_limit,
    _response_identity_map,
    _response_item_count_map,
)
from ferricstore.protocol_constants import ProtocolResponse
from ferricstore.protocol_framing import ResponseIdentity
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_INFLIGHT_REQUESTS,
    DEFAULT_MAX_PENDING_REQUEST_BYTES,
    PendingRequestBudget,
    PendingRequestCapacityError,
    SyncDeadlineScheduler,
)


class SyncPendingRequestHost(Protocol):
    compression: str
    max_inflight_requests: int | None
    max_pending_request_bytes: int | None
    _pending_lock: threading.Lock
    _pending_budget: PendingRequestBudget
    _deadline_scheduler: SyncDeadlineScheduler
    _pending: dict[int, Future[ProtocolResponse]]
    _pending_traces: dict[int, dict[str, Any]]
    _pending_transport_bindings: dict[
        int,
        tuple[int, socket.socket | ssl.SSLSocket | None],
    ]
    _transport_state_lock: threading.Lock
    _transport_generation: int
    _sock: socket.socket | ssl.SSLSocket | None

    def _notify_idle_if_needed(self) -> None: ...

    def _require_socket(self) -> socket.socket | ssl.SSLSocket: ...


class SyncPendingRequestRegistry:
    """Own pending-request capacity, deadlines, and transport bindings."""

    def __init__(self, owner: SyncPendingRequestHost) -> None:
        self.owner = owner

    def state_lock(self) -> threading.Lock:
        lock = getattr(self.owner, "_pending_lock", None)
        if lock is None:
            lock = threading.Lock()
            self.owner._pending_lock = lock
        return cast(threading.Lock, lock)

    def request_budget(self) -> PendingRequestBudget:
        budget = getattr(self.owner, "_pending_budget", None)
        if budget is None:
            budget = PendingRequestBudget(
                max_requests=getattr(
                    self.owner,
                    "max_inflight_requests",
                    DEFAULT_MAX_INFLIGHT_REQUESTS,
                ),
                max_bytes=getattr(
                    self.owner,
                    "max_pending_request_bytes",
                    DEFAULT_MAX_PENDING_REQUEST_BYTES,
                ),
            )
            self.owner._pending_budget = budget
        return cast(PendingRequestBudget, budget)

    def deadline_scheduler(self) -> SyncDeadlineScheduler:
        scheduler = getattr(self.owner, "_deadline_scheduler", None)
        if scheduler is None:
            scheduler = SyncDeadlineScheduler(
                self.expire,
                thread_name="ferricstore-protocol-deadlines",
            )
            self.owner._deadline_scheduler = scheduler
        return cast(SyncDeadlineScheduler, scheduler)

    def release_locked(self, request_id: int) -> None:
        self.request_budget().release(request_id)
        scheduler = getattr(self.owner, "_deadline_scheduler", None)
        if scheduler is not None:
            scheduler.cancel(request_id)

    def set_size(self, request_id: int, size: int) -> None:
        with self.state_lock():
            try:
                self.request_budget().set_size(request_id, size)
            except PendingRequestCapacityError as exc:
                raise FerricStoreError(str(exc)) from exc
            except KeyError as exc:
                raise FerricStoreError("protocol request timed out") from exc

    def body_limit(self, request_id: int) -> int | None:
        with self.state_lock():
            return _request_body_byte_limit(self.request_budget(), request_id)

    def encode_body(
        self,
        request_id: int,
        payload: dict[str, Any] | bytes,
        *,
        compression: str | None = None,
    ) -> tuple[bytes, bool]:
        budget = self.request_budget()
        try:
            return _encode_request_body(
                payload,
                compression=self.owner.compression if compression is None else compression,
                max_body_bytes=self.body_limit(request_id),
                pending_limit=getattr(
                    self.owner,
                    "max_pending_request_bytes",
                    budget.max_bytes,
                ),
            )
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc

    def expire(self, request_id: int) -> None:
        future, _trace = self.discard(request_id)
        if future is None:
            return
        try_set_future_exception(future, FerricStoreError("protocol request timed out"))
        self.owner._notify_idle_if_needed()

    def register(
        self,
        request_id: int,
        future: Future[ProtocolResponse],
        *,
        expected_collection_items: int | None = None,
        client_trace: dict[str, Any] | None = None,
        binding: tuple[int, socket.socket | ssl.SSLSocket | None] | None = None,
        expires_at: float | None = None,
        response_identity: ResponseIdentity | None = None,
    ) -> tuple[int, socket.socket | ssl.SSLSocket | None]:
        transport_lock = getattr(self.owner, "_transport_state_lock", None)
        if transport_lock is None:
            transport_lock = threading.Lock()
            self.owner._transport_state_lock = transport_lock

        def reserve(
            selected_binding: tuple[int, socket.socket | ssl.SSLSocket | None],
        ) -> tuple[int, socket.socket | ssl.SSLSocket | None]:
            with self.state_lock():
                budget = self.request_budget()
                try:
                    budget.reserve(request_id)
                except PendingRequestCapacityError as exc:
                    raise FerricStoreError(str(exc)) from exc
                try:
                    self.owner._pending[request_id] = future
                    bindings = getattr(self.owner, "_pending_transport_bindings", None)
                    if bindings is None:
                        bindings = {}
                        self.owner._pending_transport_bindings = bindings
                    bindings[request_id] = selected_binding
                    if response_identity is not None:
                        _response_identity_map(self.owner)[request_id] = response_identity
                    if expected_collection_items is not None:
                        _response_item_count_map(self.owner)[request_id] = expected_collection_items
                    if client_trace is not None:
                        self.owner._pending_traces[request_id] = client_trace
                    if expires_at is not None:
                        self.deadline_scheduler().schedule(request_id, expires_at)
                except BaseException:
                    self.owner._pending.pop(request_id, None)
                    bindings = getattr(self.owner, "_pending_transport_bindings", None)
                    if bindings is not None:
                        bindings.pop(request_id, None)
                    _response_identity_map(self.owner).pop(request_id, None)
                    _response_item_count_map(self.owner).pop(request_id, None)
                    self.owner._pending_traces.pop(request_id, None)
                    budget.release(request_id)
                    raise
            return selected_binding

        if binding is not None:
            return reserve(binding)
        with transport_lock:
            selected = (
                int(getattr(self.owner, "_transport_generation", 0)),
                cast(
                    socket.socket | ssl.SSLSocket | None,
                    getattr(self.owner, "_sock", None),
                ),
            )
            return reserve(selected)

    def discard(
        self,
        request_id: int,
        *,
        expected_future: Future[ProtocolResponse] | None = None,
    ) -> tuple[Future[ProtocolResponse] | None, dict[str, Any] | None]:
        with self.state_lock():
            future = self.owner._pending.get(request_id)
            if expected_future is not None and future is not expected_future:
                return None, None
            future = self.owner._pending.pop(request_id, None)
            trace = self.owner._pending_traces.pop(request_id, None)
            _response_item_count_map(self.owner).pop(request_id, None)
            _response_identity_map(self.owner).pop(request_id, None)
            bindings = getattr(self.owner, "_pending_transport_bindings", None)
            if bindings is not None:
                bindings.pop(request_id, None)
            self.release_locked(request_id)
            return future, trace

    def take_response(
        self,
        request_id: int,
        sock: socket.socket | ssl.SSLSocket | None,
    ) -> tuple[Future[ProtocolResponse] | None, dict[str, Any] | None]:
        with self.state_lock():
            bindings = getattr(self.owner, "_pending_transport_bindings", None)
            binding = None if bindings is None else bindings.get(request_id)
            if binding is not None and binding[1] is not None and binding[1] is not sock:
                return None, None
            future = self.owner._pending.pop(request_id, None)
            trace = self.owner._pending_traces.pop(request_id, None)
            if bindings is not None:
                bindings.pop(request_id, None)
            _response_identity_map(self.owner).pop(request_id, None)
            self.release_locked(request_id)
            return future, trace

    def pending_socket(self, request_id: int) -> socket.socket | ssl.SSLSocket:
        with self.state_lock():
            bindings = getattr(self.owner, "_pending_transport_bindings", None)
            binding = None if bindings is None else bindings.get(request_id)
            if binding is not None and binding[1] is not None:
                return cast(socket.socket | ssl.SSLSocket, binding[1])
        return self.owner._require_socket()
