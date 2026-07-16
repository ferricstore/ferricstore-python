from __future__ import annotations

import socket
import ssl
import threading
import time
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from typing import TYPE_CHECKING, Any, cast

from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import (
    raise_primary_with_cleanup,
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.protocol_common import (
    _response_identity_map,
    _response_item_count_map,
    _sync_adapter_deadline,
    _timeout_with_deadline,
    _validate_pending_response_identity,
)
from ferricstore.protocol_constants import (
    _FLAG_COMPRESSED,
    _FLAG_MORE_CHUNKS,
    _FLAG_TRACE,
    _HEADER,
    _MAGIC,
    _OP_AUTH,
    _OP_STARTUP,
    _OP_SUBSCRIBE_EVENTS,
    _OPCODES,
    _REQUEST_VERSION,
    _RESPONSE_VERSION,
    _USE_ADAPTER_TIMEOUT,
    ProtocolResponse,
)
from ferricstore.protocol_framing import (
    ResponseBodyAccumulator,
    ResponseFrameBudget,
    ResponseIdentity,
    frame_parts,
    validate_response_identity,
)
from ferricstore.protocol_framing import send_frames as _send_frames
from ferricstore.protocol_pipeline_codec import (
    _expected_payload_collection_items,
)
from ferricstore.protocol_responses import _decode_protocol_response
from ferricstore.protocol_sync_state import _SyncProtocolStateMixin


class SyncProtocolTransportMixin(_SyncProtocolStateMixin):
    """Socket lifecycle, request multiplexing, and response framing."""

    if TYPE_CHECKING:

        def _response_value(self, response: ProtocolResponse) -> Any: ...

        def _attach_client_trace(
            self,
            response: ProtocolResponse,
            client_trace: dict[str, Any] | None,
        ) -> ProtocolResponse: ...

    def _connect(self) -> None:
        try:
            self._connect_without_idle_notification()
        finally:
            self._notify_idle_if_needed()

    def _connect_without_idle_notification(self) -> None:
        last_error: BaseException | None = None
        deadline = _sync_adapter_deadline(self)

        for attempt in range(3):
            try:
                self._connect_once()
                if self.password is not None:
                    self._response_value(
                        self._request(
                            _OP_AUTH,
                            0,
                            {"username": self.username, "password": self.password},
                        )
                    )
                with self._subscription_lock:
                    subscriptions = list(self._flow_wake_subscriptions)
                for payload in subscriptions:
                    self._response_value(self._request(_OP_SUBSCRIBE_EVENTS, 0, payload))
                with self._events_cv:
                    self._event_error = None
                break
            except (ConnectionError, OSError, TimeoutError, FerricStoreError) as exc:
                self._close_transport(exc, mark_closed=False)
                if not self._startup_retryable(exc):
                    raise
                last_error = exc
                if attempt == 2:
                    raise
                delay = 0.02 * (attempt + 1)
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise FutureTimeoutError from exc
                    delay = min(delay, remaining)
                time.sleep(delay)
        else:
            if last_error is not None:
                raise last_error

        try:
            self._start_heartbeat()
        except BaseException as start_error:
            try:
                self._close_transport(
                    FerricStoreError("protocol heartbeat thread failed to start", raw=start_error),
                    mark_closed=False,
                )
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(
                    start_error,
                    start_error.__traceback__,
                    cleanup_error,
                )
            raise

    @staticmethod
    def _startup_retryable(exc: BaseException) -> bool:
        if isinstance(exc, (ConnectionError, OSError, TimeoutError)):
            return True
        if isinstance(exc, FerricStoreError):
            message = str(exc).lower()
            return "timed out" in message or "closed" in message or "reset" in message
        return False

    def _connect_once(self) -> None:
        deadline = _sync_adapter_deadline(self)
        connect_timeout = _timeout_with_deadline(self.timeout, deadline)
        raw_sock = socket.create_connection((self.host, self.port), timeout=connect_timeout)
        raw_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        connected_sock: socket.socket | ssl.SSLSocket
        if self.tls:
            context = self._tls_context()
            if context is None:
                raw_sock.close()
                raise FerricStoreError("TLS context is unavailable")
            try:
                raw_sock.settimeout(_timeout_with_deadline(self.timeout, deadline))
                connected_sock = context.wrap_socket(raw_sock, server_hostname=self.host)
            except BaseException:
                raw_sock.close()
                raise
            connected_sock.settimeout(None)
        else:
            raw_sock.settimeout(None)
            connected_sock = raw_sock
        with self._transport_state_lock:
            rejected = self._closed
            self._transport_generation = int(getattr(self, "_transport_generation", 0)) + 1
            self._sock = connected_sock
            self._connection_ready = False
        if rejected:
            error = FerricStoreError("protocol connection is closed")
            try:
                self._close_transport(error, mark_closed=True, expected_sock=connected_sock)
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(error, error.__traceback__, cleanup_error)
            raise error
        reader_thread = threading.Thread(
            target=self._reader_loop,
            args=(connected_sock,),
            daemon=True,
        )
        self._reader_thread = reader_thread
        try:
            reader_thread.start()
        except BaseException as start_error:
            if reader_thread.ident is None:
                self._reader_thread = None
            try:
                self._close_transport(
                    FerricStoreError("protocol reader thread failed to start", raw=start_error),
                    mark_closed=False,
                    expected_sock=connected_sock,
                )
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(
                    start_error,
                    start_error.__traceback__,
                    cleanup_error,
                )
            raise

        startup: dict[str, Any] = {
            "compression": self.compression,
            "compact_flow_responses": True,
        }
        if self.client_name is not None:
            startup["client_name"] = self.client_name
            startup["driver_name"] = self.client_name
        self._response_value(self._request(_OP_STARTUP, 0, startup))

    def _start_heartbeat(self) -> None:
        if self.heartbeat_interval is None or self.heartbeat_interval <= 0:
            return

        previous_stop = getattr(self, "_heartbeat_stop", None)
        if previous_stop is not None:
            previous_stop.set()
        heartbeat_stop = threading.Event()
        self._heartbeat_stop = heartbeat_stop
        heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(self._sock, heartbeat_stop),
            name="ferricstore-protocol-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread = heartbeat_thread
        try:
            heartbeat_thread.start()
        except BaseException:
            heartbeat_stop.set()
            if heartbeat_thread.ident is None:
                self._heartbeat_thread = None
                self._heartbeat_stop = None
            raise

    def _heartbeat_loop(
        self,
        sock: socket.socket | ssl.SSLSocket | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        if sock is None:
            sock = self._sock
        if stop_event is None:
            stop_event = threading.Event()
        interval = float(self.heartbeat_interval or 0)
        timeout = self.heartbeat_timeout
        while interval > 0 and self._sock is sock and sock is not None:
            if stop_event.wait(interval):
                return
            if self._sock is not sock:
                return
            if getattr(self, "_heartbeat_pause_count", 0) > 0:
                continue
            if time.monotonic() - self._last_activity < interval:
                continue

            request_id: int | None = None
            try:
                request_id, future = self._submit_request(_OPCODES["PING"], 0, {})
                if timeout is None:
                    future.result()
                else:
                    future.result(timeout=timeout)
            except Exception as exc:
                if request_id is not None:
                    self._discard_pending_request(request_id)
                self._close_transport(
                    FerricStoreError("protocol heartbeat failed", raw=exc),
                    mark_closed=False,
                    expected_sock=sock,
                )
                return

    def _next_request_id(self) -> int:
        self._request_id = (self._request_id + 1) & 0xFFFFFFFFFFFFFFFF
        if self._request_id == 0:
            self._request_id = 1
        return self._request_id

    def _next_lane_id(self, lane_id: int) -> int:
        if lane_id == 0:
            return lane_id
        fixed_lane_id = cast(int | None, getattr(self, "_fixed_lane_id", None))
        if fixed_lane_id is not None:
            return fixed_lane_id
        if self.lanes == 1:
            return lane_id
        self._lane_cursor = (self._lane_cursor % self.lanes) + 1
        return self._lane_cursor

    def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> dict[str, Any] | None:
        sock = self._pending_socket(request_id)
        trace_enabled = bool(extra_flags & _FLAG_TRACE)
        encode_started_ns = time.perf_counter_ns() if trace_enabled else 0
        body, compressed = self._encode_pending_request_body(
            request_id,
            payload,
            # Compression is negotiated by STARTUP itself.  KV correctly
            # rejects compressed frames until that request succeeds.
            compression="none" if opcode == _OP_STARTUP else None,
        )
        flags = extra_flags
        if compressed:
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        self._set_pending_request_size(request_id, len(header) + len(body))
        encode_done_ns = time.perf_counter_ns() if trace_enabled else 0
        write_started_ns = encode_done_ns
        write_timeout = _timeout_with_deadline(
            self.timeout,
            _sync_adapter_deadline(self),
        )
        try:
            _send_frames(sock, frame_parts(header, body), timeout=write_timeout)
        except BaseException as write_error:
            try:
                self._close_transport(
                    FerricStoreError("protocol write failed", raw=write_error),
                    mark_closed=False,
                    expected_sock=sock,
                )
            except BaseException as cleanup_error:
                raise_primary_with_cleanup(
                    write_error,
                    write_error.__traceback__,
                    cleanup_error,
                )
            raise
        self._last_activity = time.monotonic()
        if not trace_enabled:
            return None
        write_done_ns = time.perf_counter_ns()
        return {
            "encode_us": (encode_done_ns - encode_started_ns) // 1000,
            "socket_write_us": (write_done_ns - write_started_ns) // 1000,
        }

    def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse:
        trace_enabled = bool(flags & _FLAG_TRACE)
        request_started_ns = time.perf_counter_ns() if trace_enabled else 0
        effective_timeout = self.timeout if timeout is _USE_ADAPTER_TIMEOUT else timeout
        effective_timeout = cast(float | None, effective_timeout)
        deadline = None if effective_timeout is None else time.monotonic() + effective_timeout
        parent_deadline = _sync_adapter_deadline(self)
        if parent_deadline is not None:
            deadline = parent_deadline if deadline is None else min(deadline, parent_deadline)
        request_id: int | None = None

        try:
            request_id, future = self._submit_request(
                opcode,
                lane_id,
                payload,
                flags,
                exact_lane=exact_lane,
                expected_collection_items=expected_collection_items,
                _deadline=deadline,
                _expire_at_adapter_timeout=False,
            )
            wait_started_ns = time.perf_counter_ns() if trace_enabled else 0
            if deadline is None:
                response = future.result()
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise FutureTimeoutError
                response = future.result(timeout=remaining)
            if trace_enabled and response.trace is not None:
                wait_done_ns = time.perf_counter_ns()
                client_trace = response.trace.setdefault("client", {})
                client_trace["future_wait_us"] = (wait_done_ns - wait_started_ns) // 1000
                client_trace["request_total_us"] = (wait_done_ns - request_started_ns) // 1000
            return response
        except (FutureTimeoutError, TimeoutError) as exc:
            if request_id is not None:
                self._discard_pending_request(request_id)
                self._notify_idle_if_needed()
            raise FerricStoreError("protocol request timed out") from exc

    def _request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse:
        return self._request(
            opcode,
            lane_id,
            payload,
            flags,
            timeout=timeout,
            exact_lane=True,
            expected_collection_items=expected_collection_items,
        )

    def _submit_request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
        _deadline: float | None = None,
        _expire_at_adapter_timeout: bool = True,
    ) -> tuple[int, Future[ProtocolResponse]]:
        trace_enabled = bool(flags & _FLAG_TRACE)
        submit_started_ns = time.perf_counter_ns() if trace_enabled else 0
        deadline_context = getattr(self, "_deadline_context", None)
        if deadline_context is None:
            deadline_context = threading.local()
            self._deadline_context = deadline_context
        had_deadline = hasattr(deadline_context, "deadline")
        previous_deadline = getattr(deadline_context, "deadline", None)
        deadline_context.deadline = _deadline
        lock_acquired = False
        try:
            self._ensure_connected()
            if _deadline is None:
                self._lock.acquire()
                lock_acquired = True
            else:
                remaining = _deadline - time.monotonic()
                if remaining <= 0 or not self._lock.acquire(timeout=remaining):
                    raise FutureTimeoutError
                lock_acquired = True
            lock_acquired_ns = time.perf_counter_ns() if trace_enabled else 0
            request_id = self._next_request_id()
            if not exact_lane:
                lane_id = self._next_lane_id(lane_id)
            future: Future[ProtocolResponse] = Future()
            if expected_collection_items is None:
                expected_collection_items = _expected_payload_collection_items(opcode, payload)
            client_trace: dict[str, Any] | None = None
            if trace_enabled:
                client_trace = {
                    "request_lock_wait_us": (lock_acquired_ns - submit_started_ns) // 1000
                }
            response_timeout: float | None = getattr(self, "timeout", None)
            self._register_pending_request(
                request_id,
                future,
                expected_collection_items=expected_collection_items,
                client_trace=client_trace,
                expires_at=(
                    None
                    if (
                        not _expire_at_adapter_timeout
                        or _deadline is not None
                        or response_timeout is None
                    )
                    else time.monotonic() + max(0.0, response_timeout)
                ),
                response_identity=ResponseIdentity(
                    lane_id=lane_id,
                    opcode=opcode,
                    request_id=request_id,
                ),
            )
            try:
                trace = self._send(opcode, lane_id, request_id, payload, flags)
                if trace_enabled and client_trace is not None:
                    submit_done_ns = time.perf_counter_ns()
                    if trace is not None:
                        client_trace.update(trace)
                    client_trace["submit_locked_us"] = (submit_done_ns - lock_acquired_ns) // 1000
                    client_trace["submit_total_us"] = (submit_done_ns - submit_started_ns) // 1000
            except Exception:
                self._discard_pending_request(request_id, expected_future=future)
                self._notify_idle_if_needed()
                raise
            return request_id, future
        finally:
            if lock_acquired:
                self._lock.release()
            if had_deadline:
                deadline_context.deadline = previous_deadline
            else:
                del deadline_context.deadline

    def _submit_request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        expected_collection_items: int | None = None,
        _expire_at_adapter_timeout: bool = True,
    ) -> tuple[int, Future[ProtocolResponse]]:
        return self._submit_request(
            opcode,
            lane_id,
            payload,
            flags,
            exact_lane=True,
            expected_collection_items=expected_collection_items,
            _expire_at_adapter_timeout=_expire_at_adapter_timeout,
        )

    def _reader_loop(self, sock: socket.socket | ssl.SSLSocket | None = None) -> None:
        if sock is None:
            sock = self._sock
        try:
            while self._sock is sock and sock is not None:
                response = self._recv_response(sock)
                self._last_activity = time.monotonic()
                if response.request_id == 0:
                    self._enqueue_event(response.value)
                    continue
                future, client_trace = self._take_pending_response(response.request_id, sock)
                if future is not None:
                    response = self._attach_client_trace(
                        response,
                        client_trace,
                    )
                    try_set_future_result(future, response)
                self._notify_idle_if_needed()
        except Exception as exc:
            self._close_transport(exc, mark_closed=False, expected_sock=sock)

    def _fail_pending(
        self,
        exc: BaseException,
        *,
        transport_generation: int | None = None,
    ) -> None:
        pending: list[Future[ProtocolResponse]] = []
        with self._pending_state_lock():
            bindings = getattr(self, "_pending_transport_bindings", None)
            for request_id, future in list(self._pending.items()):
                binding = None if bindings is None else bindings.get(request_id)
                if (
                    transport_generation is not None
                    and binding is not None
                    and binding[0] != transport_generation
                ):
                    continue
                pending.append(future)
                self._pending.pop(request_id, None)
                self._pending_traces.pop(request_id, None)
                _response_item_count_map(self).pop(request_id, None)
                _response_identity_map(self).pop(request_id, None)
                if bindings is not None:
                    bindings.pop(request_id, None)
                self._release_pending_lifecycle_locked(request_id)
        for future in pending:
            try_set_future_exception(future, exc)
        self._notify_idle_if_needed()

    def _recv_matching(self, request_id: int) -> ProtocolResponse:
        while True:
            response = self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                self._enqueue_event(response.value)
                continue
            raise FerricStoreError(
                "protocol response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    def _recv_response(
        self,
        sock: socket.socket | ssl.SSLSocket | None = None,
    ) -> ProtocolResponse:
        read_started_ns = time.perf_counter_ns()
        header = self._recv_exact(_HEADER.size, sock)
        magic, version, flags, lane_id, opcode, request_id, body_len = _HEADER.unpack(header)
        if magic != _MAGIC or version != _RESPONSE_VERSION:
            raise FerricStoreError("invalid protocol response frame header")
        _validate_pending_response_identity(
            self,
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
        )

        self._check_response_size(body_len)
        body = self._recv_exact(body_len, sock)
        chunks = ResponseBodyAccumulator(body)
        final_flags = flags
        budget: ResponseFrameBudget | None = None
        if final_flags & _FLAG_MORE_CHUNKS:
            budget = ResponseFrameBudget(
                max_body_bytes=self.max_response_bytes,
                max_chunks=self.max_response_chunks,
            )
            budget.add_chunk(body_len)
        while final_flags & _FLAG_MORE_CHUNKS:
            next_header = self._recv_exact(_HEADER.size, sock)
            (
                next_magic,
                next_version,
                next_flags,
                next_lane_id,
                next_opcode,
                next_request_id,
                next_body_len,
            ) = _HEADER.unpack(next_header)
            if next_magic != _MAGIC or next_version != _RESPONSE_VERSION:
                raise FerricStoreError("invalid protocol chunk continuation")
            validate_response_identity(
                ResponseIdentity(lane_id, opcode, request_id),
                lane_id=next_lane_id,
                opcode=next_opcode,
                request_id=next_request_id,
                message="invalid protocol chunk continuation",
            )
            if budget is not None:
                budget.add_chunk(next_body_len)
            chunks.append(self._recv_exact(next_body_len, sock))
            final_flags = next_flags

        body = chunks.finish()
        read_done_ns = time.perf_counter_ns()
        return _decode_protocol_response(
            self,
            lane_id=lane_id,
            opcode=opcode,
            request_id=request_id,
            flags=final_flags,
            body=body,
            read_started_ns=read_started_ns,
            read_done_ns=read_done_ns,
        )

    def _recv_exact(self, size: int, sock: socket.socket | ssl.SSLSocket | None = None) -> bytes:
        if sock is None:
            sock = self._require_socket()
        if size == 0:
            return b""
        chunks: ResponseBodyAccumulator | None = None
        received = 0
        while received < size:
            try:
                chunk = sock.recv(size - received)
            except TimeoutError:
                if self._sock is not sock:
                    raise FerricStoreError("protocol connection closed") from None
                continue
            if not chunk:
                raise FerricStoreError("protocol connection closed")
            if chunks is None:
                chunks = ResponseBodyAccumulator(chunk)
            else:
                chunks.append(chunk)
            received += len(chunk)
        if chunks is None:
            raise FerricStoreError("protocol connection closed")
        return chunks.finish()

    def _check_response_size(self, size: int) -> None:
        limit = self.max_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_response_bytes")

    def _check_decompressed_response_size(self, size: int) -> None:
        limit = self.max_decompressed_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_decompressed_response_bytes")

    def _require_socket(self) -> socket.socket | ssl.SSLSocket:
        if self._sock is None:
            raise FerricStoreError("protocol connection is closed")
        return self._sock

    def _ensure_connected(self) -> None:
        connecting = bool(getattr(self, "_connecting", False))
        connection_ready = bool(getattr(self, "_connection_ready", not connecting))
        if self._sock is not None and connection_ready:
            return
        if self._closed:
            raise FerricStoreError("protocol connection is closed")
        current_thread_id = threading.get_ident()
        if (
            self._sock is not None
            and connecting
            and getattr(self, "_connecting_thread_id", None) == current_thread_id
        ):
            return
        deadline = _sync_adapter_deadline(self)
        connect_lock_acquired = False
        try:
            if deadline is None:
                self._connect_lock.acquire()
                connect_lock_acquired = True
            else:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._connect_lock.acquire(timeout=remaining):
                    raise FutureTimeoutError
                connect_lock_acquired = True
            connecting = bool(getattr(self, "_connecting", False))
            connection_ready = bool(getattr(self, "_connection_ready", not connecting))
            if self._sock is not None and connection_ready:
                return
            if self._closed:
                raise FerricStoreError("protocol connection is closed")
            self._connecting = True
            self._connecting_thread_id = current_thread_id
            self._connection_ready = False
            try:
                self._connect()
                if self._closed:
                    error = FerricStoreError("protocol connection is closed")
                    self._close_transport(error, mark_closed=True)
                    raise error
                if self._sock is not None:
                    transport_state_lock = getattr(self, "_transport_state_lock", None)
                    if transport_state_lock is None:
                        self._connection_ready = True
                    else:
                        with transport_state_lock:
                            self._connection_ready = self._sock is not None and not self._closed
            finally:
                self._connecting = False
                self._connecting_thread_id = None
                self._notify_idle_if_needed()
        finally:
            if connect_lock_acquired:
                self._connect_lock.release()


__all__ = ["SyncProtocolTransportMixin"]
