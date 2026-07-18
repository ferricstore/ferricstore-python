from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import replace
from typing import Any, cast

from ferricstore.errors import (
    FerricStoreError,
    InvalidCommandError,
)
from ferricstore.lifecycle_core import (
    consume_async_future_exception,
    raise_primary_with_cleanup,
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.protocol_async_batch import AsyncProtocolBatchMixin
from ferricstore.protocol_async_state import _AsyncProtocolStateMixin
from ferricstore.protocol_commands import (
    _compact_flow_many_payloads_from_raw,
    build_protocol_command,  # noqa: F401 - historical monkeypatch seam
)
from ferricstore.protocol_common import (
    _encode_request_body,
    _next_protocol_lane,
    _notify_event_listeners,
    _request_body_byte_limit,
    _response_identity_map,
    _response_item_count_map,
    _validate_pending_response_identity,
)
from ferricstore.protocol_constants import (
    _FLAG_COMPRESSED,
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
    ProtocolCommand,
    ProtocolResponse,
)
from ferricstore.protocol_framing import (
    ResponseFrameAssembler,
    ResponseIdentity,
)
from ferricstore.protocol_lifecycle import PendingRequestCapacityError
from ferricstore.protocol_negotiation import (
    apply_hello_negotiation,
    mark_authenticated,
    validate_unauthenticated_request_size,
)
from ferricstore.protocol_pipeline_codec import _expected_payload_collection_items
from ferricstore.protocol_pipelines import AsyncProtocolPipeline
from ferricstore.protocol_planning import PreparedCommand
from ferricstore.protocol_responses import (
    _batch_item_value,
    _decode_protocol_response,
    _response_value,
)
from ferricstore.protocol_retry import request_outcome_error
from ferricstore.protocol_subscriptions import AsyncProtocolSubscriptionMixin
from ferricstore.protocol_transport_commands import (
    adapter_from_url,
    build_adapter_protocol_command,
    compact_flow_many_for_adapter,
    prepare_adapter_protocol_command,
)


class AsyncProtocolAdapter(
    AsyncProtocolBatchMixin,
    AsyncProtocolSubscriptionMixin,
    _AsyncProtocolStateMixin,
):
    """FerricStore protocol TCP adapter for the async SDK."""

    client: AsyncProtocolAdapter
    requires_explicit_session = True
    supports_concurrent_fanout = True
    _request_ensures_connection = True

    def _build_protocol_command(self, *args: Any) -> ProtocolCommand:
        return build_adapter_protocol_command(self, args)

    def _prepare_protocol_command(self, args: tuple[Any, ...]) -> PreparedCommand:
        return prepare_adapter_protocol_command(self, args)

    def _compact_flow_many_payloads(
        self,
        commands: list[tuple[Any, ...]],
        protocol_commands: list[ProtocolCommand] | None,
    ) -> list[tuple[int, bytes, int]] | None:
        return compact_flow_many_for_adapter(
            self,
            _compact_flow_many_payloads_from_raw,
            commands,
            protocol_commands,
        )

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncProtocolAdapter:
        return adapter_from_url(cls, url, kwargs)

    def pipeline(self, transaction: bool = False) -> AsyncProtocolPipeline:
        if transaction:
            raise InvalidCommandError("protocol pipeline does not support transaction mode")
        return AsyncProtocolPipeline(self)

    async def execute_command(self, *args: Any) -> Any:
        return await self.execute_prepared_command(self._prepare_protocol_command(args))

    async def execute_prepared_command(self, prepared: PreparedCommand) -> Any:
        command = prepared.command
        response = await self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return self._response_value(response)

    async def execute_command_on_lane(self, args: tuple[Any, ...], lane_id: int) -> Any:
        """Execute a routed command on the exact topology lane."""
        return await self.execute_prepared_command_on_lane(
            self._prepare_protocol_command(args),
            lane_id,
        )

    async def execute_prepared_command_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> Any:
        command = prepared.command
        response = await self._request_on_lane(
            command.opcode,
            lane_id,
            command.payload,
            command.flags,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return self._response_value(response)

    async def execute_command_with_trace(self, *args: Any) -> dict[str, Any]:
        return await self.execute_prepared_command_with_trace(self._prepare_protocol_command(args))

    async def execute_prepared_command_with_trace(
        self,
        prepared: PreparedCommand,
    ) -> dict[str, Any]:
        command = prepared.command
        response = await self._request(
            command.opcode,
            command.lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    async def execute_command_with_trace_on_lane(
        self,
        args: tuple[Any, ...],
        lane_id: int,
    ) -> dict[str, Any]:
        return await self.execute_prepared_command_with_trace_on_lane(
            self._prepare_protocol_command(args),
            lane_id,
        )

    async def execute_prepared_command_with_trace_on_lane(
        self,
        prepared: PreparedCommand,
        lane_id: int,
    ) -> dict[str, Any]:
        command = prepared.command
        response = await self._request_on_lane(
            command.opcode,
            lane_id,
            command.payload,
            command.flags | _FLAG_TRACE,
            timeout=None if prepared.blocks_forever else _USE_ADAPTER_TIMEOUT,
            expected_collection_items=prepared.expected_collection_items,
        )
        return {"value": self._response_value(response), "trace": response.trace or {}}

    async def _ensure_connected(self) -> None:
        try:
            await self._ensure_connected_without_idle_notification()
        finally:
            self._notify_idle_if_needed()

    async def _ensure_connected_without_idle_notification(self) -> None:
        connection_ready = bool(getattr(self, "_connection_ready", self._writer is not None))
        if self._writer is not None and not self._writer.is_closing() and connection_ready:
            return
        if getattr(self, "_closed", False):
            raise FerricStoreError("protocol connection is closed")

        connect = self._ensure_connected_without_timeout()
        if self.timeout is None:
            await connect
            return
        try:
            await asyncio.wait_for(connect, self.timeout)
        except asyncio.TimeoutError as exc:
            error = FerricStoreError("protocol connection timed out")
            await self._publish_event_error(error)
            raise error from exc

    async def _publish_event_error(self, error: BaseException) -> None:
        async with self._events_cv:
            self._event_error = error
            self._events_cv.notify_all()
        _notify_event_listeners(list(self._event_listeners))

    async def _ensure_connected_without_timeout(self) -> None:
        async with self._connect_lock:
            connection_ready = bool(getattr(self, "_connection_ready", self._writer is not None))
            if self._writer is not None and not self._writer.is_closing() and connection_ready:
                return
            if self._closed:
                raise FerricStoreError("protocol connection is closed")
            if self._writer is not None:
                await self._close_transport(mark_closed=False)

            reader: asyncio.StreamReader | None = None
            writer: asyncio.StreamWriter | None = None
            self._connection_ready = False
            try:
                for startup_attempt in range(2):
                    reader = None
                    writer = None
                    context = self._tls_context()
                    reader, writer = await asyncio.open_connection(
                        self.host,
                        self.port,
                        ssl=context,
                        server_hostname=self.host if self.tls else None,
                    )
                    if self._closed:
                        writer.close()
                        with contextlib.suppress(Exception):
                            await writer.wait_closed()
                        raise FerricStoreError("protocol connection is closed")
                    self._reader, self._writer = reader, writer
                    self._reader_task = asyncio.create_task(self._reader_loop(reader, writer))

                    hello: dict[str, Any] = {"compression": self.compression}
                    if self.client_name is not None:
                        hello["client_name"] = self.client_name
                    hello_value = self._response_value(
                        await self._request(
                            _OPCODES["HELLO"],
                            0,
                            hello,
                            _skip_connect=True,
                        )
                    )
                    apply_hello_negotiation(self, hello_value)

                    startup: dict[str, Any] = {
                        "compression": self.compression,
                        "compact_flow_responses": True,
                    }
                    if self.client_name is not None:
                        startup["client_name"] = self.client_name
                        startup["driver_name"] = self.client_name

                    startup_value = self._response_value(
                        await self._request(
                            _OP_STARTUP,
                            0,
                            startup,
                            _skip_connect=True,
                        )
                    )
                    apply_hello_negotiation(self, startup_value)

                    if self.password is not None:
                        self._response_value(
                            await self._request(
                                _OP_AUTH,
                                0,
                                {"username": self.username, "password": self.password},
                                _skip_connect=True,
                            )
                        )
                        mark_authenticated(self)
                    for payload in list(self._flow_wake_subscriptions):
                        self._response_value(
                            await self._request(
                                _OP_SUBSCRIBE_EVENTS,
                                0,
                                payload,
                                _skip_connect=True,
                            )
                        )
                    if self._closed:
                        raise FerricStoreError("protocol connection is closed")
                    async with self._events_cv:
                        self._event_error = None
                    if self._writer is writer and not writer.is_closing():
                        self._connection_ready = True
                        self._start_heartbeat()
                        return
                    if startup_attempt == 0:
                        await self._close_transport(
                            FerricStoreError("protocol connection closed during startup"),
                            mark_closed=False,
                            expected_reader=reader,
                            expected_writer=writer,
                        )
                        continue
                    raise FerricStoreError("protocol connection is closed")
            except BaseException as exc:
                published_error = (
                    FerricStoreError("protocol connection attempt cancelled", raw=exc)
                    if isinstance(exc, asyncio.CancelledError)
                    else exc
                )
                if isinstance(exc, asyncio.CancelledError):
                    cleanup_task = asyncio.create_task(
                        self._close_transport(
                            published_error,
                            mark_closed=False,
                            expected_reader=reader,
                            expected_writer=writer,
                        )
                    )
                    cleanup_task.add_done_callback(consume_async_future_exception)
                    raise
                await self._close_transport(
                    published_error,
                    mark_closed=False,
                    expected_reader=reader,
                    expected_writer=writer,
                )
                raise

    def _start_heartbeat(self) -> None:
        if self.heartbeat_interval is None or self.heartbeat_interval <= 0:
            return
        old_task = self._heartbeat_task
        if old_task is not None and old_task is not asyncio.current_task() and not old_task.done():
            old_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(self._writer))

    async def _heartbeat_loop(self, writer: asyncio.StreamWriter | None = None) -> None:
        if writer is None:
            writer = self._writer
        interval = float(self.heartbeat_interval or 0)
        timeout = self.heartbeat_timeout
        try:
            while interval > 0 and self._writer is writer and writer is not None:
                await asyncio.sleep(interval)
                if self._writer is not writer:
                    return
                if getattr(self, "_heartbeat_pause_count", 0) > 0:
                    continue
                if time.monotonic() - self._last_activity < interval:
                    continue
                try:
                    request = self._request(_OPCODES["PING"], 0, {})
                    if timeout is None:
                        await request
                    else:
                        await asyncio.wait_for(request, timeout=timeout)
                except Exception as exc:
                    await self._close_transport(
                        FerricStoreError("protocol heartbeat failed", raw=exc),
                        mark_closed=False,
                        expected_writer=writer,
                    )
                    return
        except asyncio.CancelledError:
            raise

    def _next_request_id(self) -> int:
        self._request_id = (self._request_id + 1) & 0xFFFFFFFFFFFFFFFF
        if self._request_id == 0:
            self._request_id = 1
        return self._request_id

    def _next_lane_id(self, lane_id: int) -> int:
        return _next_protocol_lane(self, lane_id)

    async def _send(
        self,
        opcode: int,
        lane_id: int,
        request_id: int,
        payload: dict[str, Any] | bytes,
        extra_flags: int = 0,
    ) -> dict[str, Any] | None:
        writer = self._require_writer()
        trace_enabled = bool(extra_flags & _FLAG_TRACE)
        encode_started_ns = time.perf_counter_ns() if trace_enabled else 0
        pending_budget = self._pending_request_budget()
        body, compressed = _encode_request_body(
            payload,
            # Compression is negotiated by STARTUP itself.  KV correctly
            # rejects compressed frames until that request succeeds.
            compression=(
                "none"
                if opcode in {_OPCODES["HELLO"], _OP_STARTUP}
                or (
                    getattr(self, "_auth_required", False)
                    and not getattr(self, "_authenticated", False)
                )
                else self.compression
            ),
            max_body_bytes=_request_body_byte_limit(pending_budget, request_id),
            pending_limit=getattr(
                self,
                "max_pending_request_bytes",
                pending_budget.max_bytes,
            ),
        )
        validate_unauthenticated_request_size(self, len(body))
        flags = extra_flags
        if compressed:
            flags |= _FLAG_COMPRESSED
        header = _HEADER.pack(
            _MAGIC, _REQUEST_VERSION, flags, lane_id, opcode, request_id, len(body)
        )
        self._set_pending_request_size(request_id, len(header) + len(body))
        writer.write(header)
        if body:
            writer.write(body)
        self._queued_write_bytes += len(header) + len(body)
        self._last_activity = time.monotonic()
        encode_done_ns = time.perf_counter_ns() if trace_enabled else 0
        write_started_ns = encode_done_ns
        if self.write_drain_bytes == 0 or self._queued_write_bytes >= self.write_drain_bytes:
            self._queued_write_bytes = 0
            await writer.drain()
        if not trace_enabled:
            return None
        write_done_ns = time.perf_counter_ns()
        return {
            "encode_us": (encode_done_ns - encode_started_ns) // 1000,
            "socket_write_us": (write_done_ns - write_started_ns) // 1000,
        }

    async def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
        _skip_connect: bool = False,
    ) -> ProtocolResponse:
        request = self._request_without_timeout(
            opcode,
            lane_id,
            payload,
            flags,
            exact_lane=exact_lane,
            expected_collection_items=expected_collection_items,
            _skip_connect=_skip_connect,
        )
        effective_timeout = self.timeout if timeout is _USE_ADAPTER_TIMEOUT else timeout
        effective_timeout = cast(float | None, effective_timeout)
        if effective_timeout is None:
            return await request
        try:
            return await asyncio.wait_for(request, effective_timeout)
        except asyncio.TimeoutError as exc:
            raise request_outcome_error(
                opcode,
                exc,
                message="protocol request timed out",
            ) from exc

    async def _request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = _USE_ADAPTER_TIMEOUT,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse:
        return await self._request(
            opcode,
            lane_id,
            payload,
            flags,
            timeout=timeout,
            exact_lane=True,
            expected_collection_items=expected_collection_items,
        )

    async def _request_without_timeout(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
        _skip_connect: bool = False,
    ) -> ProtocolResponse:
        if not _skip_connect and hasattr(self, "_connect_lock"):
            await self._ensure_connected()
        loop = asyncio.get_running_loop()
        request_id: int | None = None
        future: asyncio.Future[ProtocolResponse] | None = None
        try:
            async with self._write_lock:
                request_id = self._next_request_id()
                if not exact_lane:
                    lane_id = self._next_lane_id(lane_id)
                future = loop.create_future()
                self._reserve_pending_request(request_id)
                self._pending[request_id] = future
                _response_identity_map(self)[request_id] = ResponseIdentity(
                    lane_id=lane_id,
                    opcode=opcode,
                    request_id=request_id,
                )
                if expected_collection_items is None:
                    expected_collection_items = _expected_payload_collection_items(opcode, payload)
                if expected_collection_items is not None:
                    _response_item_count_map(self)[request_id] = expected_collection_items
                trace_enabled = bool(flags & _FLAG_TRACE)
                if trace_enabled:
                    self._pending_traces[request_id] = {}
                writer = getattr(self, "_writer", None)
                try:
                    trace = await self._send(opcode, lane_id, request_id, payload, flags)
                except PendingRequestCapacityError as capacity_error:
                    raise FerricStoreError(str(capacity_error)) from capacity_error
                except BaseException as write_error:
                    if isinstance(write_error, asyncio.CancelledError):
                        if writer is not None:
                            cleanup_task = asyncio.create_task(
                                self._close_transport(
                                    FerricStoreError(
                                        "protocol write cancelled",
                                        raw=write_error,
                                    ),
                                    mark_closed=False,
                                    expected_writer=writer,
                                )
                            )
                            cleanup_task.add_done_callback(consume_async_future_exception)
                        raise
                    cleanup_error: BaseException | None = None
                    if writer is not None:
                        try:
                            await self._close_transport(
                                FerricStoreError("protocol write failed", raw=write_error),
                                mark_closed=False,
                                expected_writer=writer,
                            )
                        except BaseException as exc:
                            cleanup_error = exc
                    if future.done() and not future.cancelled():
                        future.exception()
                    if cleanup_error is not None:
                        outcome_error = request_outcome_error(
                            opcode,
                            write_error,
                            message="protocol write failed",
                        )
                        raise_primary_with_cleanup(
                            outcome_error,
                            outcome_error.__traceback__,
                            cleanup_error,
                        )
                    raise request_outcome_error(
                        opcode,
                        write_error,
                        message="protocol write failed",
                    ) from write_error
                if trace_enabled and trace is not None:
                    pending_trace = self._pending_traces.get(request_id)
                    if pending_trace is not None:
                        pending_trace.update(trace)
            return await future
        finally:
            if request_id is not None:
                self._pending.pop(request_id, None)
                self._pending_traces.pop(request_id, None)
                _response_item_count_map(self).pop(request_id, None)
                _response_identity_map(self).pop(request_id, None)
                self._release_pending_request(request_id)
                self._notify_idle_if_needed()

    async def _reader_loop(
        self,
        reader: asyncio.StreamReader | None = None,
        writer: asyncio.StreamWriter | None = None,
    ) -> None:
        if reader is None:
            reader = self._reader
        if writer is None:
            writer = self._writer
        try:
            while self._reader is reader and reader is not None:
                response = await self._recv_response(reader)
                self._last_activity = time.monotonic()
                if response.request_id == 0:
                    await self._enqueue_event(response.value)
                    continue
                future = self._pending.pop(response.request_id, None)
                client_trace = self._pending_traces.pop(response.request_id, None)
                _response_identity_map(self).pop(response.request_id, None)
                self._release_pending_request(response.request_id)
                if future is not None:
                    response = self._attach_client_trace(
                        response,
                        client_trace,
                    )
                    try_set_future_result(future, response)
                self._notify_idle_if_needed()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._close_transport(
                exc,
                mark_closed=False,
                expected_reader=reader,
                expected_writer=writer,
            )

    def _fail_pending(self, exc: BaseException) -> None:
        identities = _response_identity_map(self)
        pending = [
            (future, identities.get(request_id)) for request_id, future in self._pending.items()
        ]
        self._pending.clear()
        self._pending_traces.clear()
        _response_item_count_map(self).clear()
        _response_identity_map(self).clear()
        self._pending_request_budget().clear()
        for future, identity in pending:
            pending_error = (
                exc
                if identity is None
                else request_outcome_error(
                    identity.opcode,
                    exc,
                    message="protocol connection closed",
                )
            )
            try_set_future_exception(future, pending_error)
        self._notify_idle_if_needed()

    async def _recv_matching(self, request_id: int) -> ProtocolResponse:
        while True:
            response = await self._recv_response()
            if response.request_id == request_id:
                return response
            if response.request_id == 0:
                await self._enqueue_event(response.value)
                continue
            raise FerricStoreError(
                "protocol response request_id mismatch: "
                f"expected {request_id}, got {response.request_id}",
                raw=response,
            )

    async def _recv_response(self, reader: asyncio.StreamReader | None = None) -> ProtocolResponse:
        assembler = getattr(self, "_response_frame_assembler", None)
        if assembler is None:
            assembler = ResponseFrameAssembler(
                max_body_bytes=self.max_response_bytes,
                max_chunks=self.max_response_chunks,
            )
            self._response_frame_assembler = assembler
        while True:
            frame_started_ns = time.perf_counter_ns()
            header = await self._recv_exact(_HEADER.size, reader)
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
            assembled = assembler.add(
                ResponseIdentity(lane_id, opcode, request_id),
                flags,
                await self._recv_exact(body_len, reader),
                read_started_ns=frame_started_ns,
            )
            if assembled is None:
                continue
            read_done_ns = time.perf_counter_ns()
            return _decode_protocol_response(
                self,
                lane_id=assembled.identity.lane_id,
                opcode=assembled.identity.opcode,
                request_id=assembled.identity.request_id,
                flags=assembled.flags,
                body=assembled.body,
                read_started_ns=assembled.read_started_ns,
                read_done_ns=read_done_ns,
            )

    async def _recv_exact(self, size: int, reader: asyncio.StreamReader | None = None) -> bytes:
        if reader is None:
            reader = self._require_reader()
        try:
            return await reader.readexactly(size)
        except asyncio.IncompleteReadError as exc:
            raise FerricStoreError("protocol connection closed") from exc

    def _check_response_size(self, size: int) -> None:
        limit = self.max_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_response_bytes")

    def _check_decompressed_response_size(self, size: int) -> None:
        limit = self.max_decompressed_response_bytes
        if limit is not None and size > limit:
            raise FerricStoreError("protocol response exceeds max_decompressed_response_bytes")

    def _require_reader(self) -> asyncio.StreamReader:
        if self._reader is None:
            raise FerricStoreError("protocol connection is closed")
        return self._reader

    def _require_writer(self) -> asyncio.StreamWriter:
        if self._writer is None:
            raise FerricStoreError("protocol connection is closed")
        return self._writer

    def _response_value(self, response: ProtocolResponse) -> Any:
        value = _response_value(response)
        if getattr(response, "opcode", None) == _OP_AUTH:
            mark_authenticated(self)
        return value

    def _batch_item_value(self, item: Any) -> Any:
        return _batch_item_value(item)

    def _attach_client_trace(
        self, response: ProtocolResponse, client_trace: dict[str, Any] | None
    ) -> ProtocolResponse:
        if not client_trace:
            return response
        trace = dict(response.trace or {})
        client = dict(trace.get("client") or {})
        client.update(client_trace)
        trace["client"] = client
        return replace(response, trace=trace)


__all__ = [
    "AsyncProtocolAdapter",
    "AsyncProtocolPipeline",
]
