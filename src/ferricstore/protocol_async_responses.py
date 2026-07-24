from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import try_set_future_exception, try_set_future_result
from ferricstore.protocol_common import (
    _response_identity_map,
    _response_item_count_map,
    _validate_pending_response_identity,
)
from ferricstore.protocol_constants import (
    _HEADER,
    _MAGIC,
    _OP_AUTH,
    _RESPONSE_VERSION,
    ProtocolResponse,
)
from ferricstore.protocol_framing import ResponseFrameAssembler, ResponseIdentity
from ferricstore.protocol_negotiation import mark_authenticated
from ferricstore.protocol_responses import (
    _batch_item_value,
    _decode_protocol_response,
    _response_value,
)
from ferricstore.protocol_retry import request_outcome_error


class AsyncProtocolResponseMixin:
    """Frame receive, response dispatch, and pending-failure policy."""

    if TYPE_CHECKING:
        _last_activity: float
        _pending: dict[int, asyncio.Future[ProtocolResponse]]
        _pending_traces: dict[int, dict[str, Any]]
        _reader: asyncio.StreamReader | None
        _response_frame_assembler: ResponseFrameAssembler
        _writer: asyncio.StreamWriter | None
        max_decompressed_response_bytes: int | None
        max_response_bytes: int | None
        max_response_chunks: int | None

        async def _close_transport(
            self,
            exc: BaseException | None = None,
            *,
            mark_closed: bool = False,
            expected_reader: asyncio.StreamReader | None = None,
            expected_writer: asyncio.StreamWriter | None = None,
        ) -> None: ...

        async def _enqueue_event(self, event: Any) -> None: ...

        def _notify_idle_if_needed(self) -> None: ...

        def _pending_request_budget(self) -> Any: ...

        def _release_pending_request(self, request_id: int) -> None: ...

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
                    response = self._attach_client_trace(response, client_trace)
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
                    may_mutate=identity.may_mutate,
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


__all__ = ["AsyncProtocolResponseMixin"]
