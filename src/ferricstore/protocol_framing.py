from __future__ import annotations

import io
import os
import socket
import struct
import sys
import time
import zlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

from ferricstore.config_validation import (
    validate_optional_nonnegative_int as validated_optional_nonnegative_int,
)
from ferricstore.config_validation import (
    validate_optional_positive_int as validated_optional_positive_int,
)
from ferricstore.errors import FerricStoreError

_FRAME_BODY_SPLIT_BYTES = 64 * 1024
_SOCKET_TIMEOUT_OPTION_SIZE = struct.calcsize("@I" if os.name == "nt" else "@ll")


@dataclass(frozen=True, slots=True)
class ResponseIdentity:
    """Transport-neutral identity of one logical protocol response."""

    lane_id: int
    opcode: int
    request_id: int


def validate_response_identity(
    expected: ResponseIdentity,
    *,
    lane_id: int,
    opcode: int,
    request_id: int,
    message: str = "protocol response identity mismatch",
) -> None:
    actual = ResponseIdentity(lane_id=lane_id, opcode=opcode, request_id=request_id)
    if actual != expected:
        raise FerricStoreError(
            message,
            raw={"expected": expected, "actual": actual},
        )


def validated_response_chunk_limit(value: Any) -> int | None:
    """Validate the bound on frame-count overhead for one logical response."""
    return validated_optional_positive_int(value, name="max_response_chunks")


def frame_parts(header: bytes, body: bytes) -> tuple[bytes, ...]:
    """Use one write for small frames and avoid copying large frame bodies."""
    if not body:
        return (header,)
    if len(body) < _FRAME_BODY_SPLIT_BYTES:
        return (header + body,)
    return header, body


class ResponseBodyAccumulator:
    """Assemble continuation chunks without retaining a second full chunk list."""

    __slots__ = ("_first", "_stream")

    def __init__(self, first: bytes) -> None:
        self._first = first
        self._stream: io.BytesIO | None = None

    def append(self, chunk: bytes) -> None:
        stream = self._stream
        if stream is None:
            stream = io.BytesIO()
            stream.write(self._first)
            self._first = b""
            self._stream = stream
        stream.write(chunk)

    def finish(self) -> bytes:
        stream = self._stream
        return self._first if stream is None else stream.getvalue()


class ResponseFrameBudget:
    """Bound response body bytes and continuation-frame overhead in one state machine."""

    __slots__ = ("body_bytes", "chunks", "max_body_bytes", "max_chunks")

    def __init__(
        self,
        *,
        max_body_bytes: int | None,
        max_chunks: int | None,
    ) -> None:
        self.max_body_bytes = validated_optional_nonnegative_int(
            max_body_bytes,
            name="max_response_bytes",
        )
        self.max_chunks = validated_optional_positive_int(
            max_chunks,
            name="max_response_chunks",
        )
        self.body_bytes = 0
        self.chunks = 0

    def add_chunk(self, body_bytes: int) -> None:
        self.chunks += 1
        if self.max_chunks is not None and self.chunks > self.max_chunks:
            raise FerricStoreError("protocol response exceeds max_response_chunks")
        self.body_bytes += body_bytes
        if self.max_body_bytes is not None and self.body_bytes > self.max_body_bytes:
            raise FerricStoreError("protocol response exceeds max_response_bytes")


@dataclass(frozen=True, slots=True)
class AssembledResponseFrame:
    identity: ResponseIdentity
    flags: int
    body: bytes
    read_started_ns: int


@dataclass(slots=True)
class _PendingResponseFrames:
    accumulator: ResponseBodyAccumulator
    budget: ResponseFrameBudget
    flags: int
    read_started_ns: int


class ResponseFrameAssembler:
    """Reassemble independently interleaved response streams by full identity."""

    __slots__ = ("_max_body_bytes", "_max_chunks", "_pending")

    def __init__(self, *, max_body_bytes: int | None, max_chunks: int | None) -> None:
        self._max_body_bytes = validated_optional_nonnegative_int(
            max_body_bytes,
            name="max_response_bytes",
        )
        self._max_chunks = validated_response_chunk_limit(max_chunks)
        self._pending: dict[ResponseIdentity, _PendingResponseFrames] = {}

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    def clear(self) -> None:
        self._pending.clear()

    def reconfigure(self, *, max_body_bytes: int | None, max_chunks: int | None) -> None:
        """Apply connection-negotiated limits when no response is mid-assembly."""
        if self._pending:
            raise FerricStoreError("cannot change response limits while chunks are pending")
        self._max_body_bytes = validated_optional_nonnegative_int(
            max_body_bytes,
            name="max_response_bytes",
        )
        self._max_chunks = validated_response_chunk_limit(max_chunks)

    def add(
        self,
        identity: ResponseIdentity,
        flags: int,
        body: bytes,
        *,
        read_started_ns: int,
    ) -> AssembledResponseFrame | None:
        from ferricstore.protocol_constants import _FLAG_MORE_CHUNKS

        more = bool(flags & _FLAG_MORE_CHUNKS)
        logical_flags = flags & ~_FLAG_MORE_CHUNKS
        pending = self._pending.get(identity)
        if pending is None and not more:
            self._check_single_frame(len(body))
            return AssembledResponseFrame(identity, logical_flags, body, read_started_ns)

        if pending is None:
            budget = ResponseFrameBudget(
                max_body_bytes=self._max_body_bytes,
                max_chunks=self._max_chunks,
            )
            budget.add_chunk(len(body))
            self._pending[identity] = _PendingResponseFrames(
                accumulator=ResponseBodyAccumulator(body),
                budget=budget,
                flags=logical_flags,
                read_started_ns=read_started_ns,
            )
            return None

        try:
            pending.budget.add_chunk(len(body))
            pending.accumulator.append(body)
            pending.flags |= logical_flags
        except BaseException:
            self._pending.pop(identity, None)
            raise
        if more:
            return None

        self._pending.pop(identity, None)
        return AssembledResponseFrame(
            identity,
            pending.flags,
            pending.accumulator.finish(),
            pending.read_started_ns,
        )

    def _check_single_frame(self, body_bytes: int) -> None:
        budget = ResponseFrameBudget(
            max_body_bytes=self._max_body_bytes,
            max_chunks=self._max_chunks,
        )
        budget.add_chunk(body_bytes)


def decompress_response(body: bytes, limit: int | None) -> bytes:
    limit = validated_optional_nonnegative_int(
        limit,
        name="max_decompressed_response_bytes",
    )
    try:
        decompressor = zlib.decompressobj()
        if limit is None:
            decoded = decompressor.decompress(body)
            if not decompressor.eof or decompressor.unused_data or decompressor.unconsumed_tail:
                raise FerricStoreError("protocol response has invalid compressed data")
            return decoded + decompressor.flush()

        output = bytearray()
        compressed = body
        while True:
            remaining = limit - len(output)
            output.extend(decompressor.decompress(compressed, min(remaining + 1, sys.maxsize)))
            if len(output) > limit:
                raise FerricStoreError("protocol response exceeds max_decompressed_response_bytes")
            if not decompressor.unconsumed_tail:
                break
            compressed = decompressor.unconsumed_tail

        if not decompressor.eof or decompressor.unused_data or decompressor.unconsumed_tail:
            raise FerricStoreError("protocol response has invalid compressed data")
        flush_limit = limit - len(output) + 1
        output.extend(
            decompressor.flush(flush_limit) if flush_limit <= sys.maxsize else decompressor.flush()
        )
        if len(output) > limit:
            raise FerricStoreError("protocol response exceeds max_decompressed_response_bytes")
        return bytes(output)
    except zlib.error as exc:
        raise FerricStoreError(
            "protocol response has invalid compressed data",
            raw=exc,
        ) from exc


def send_frames(
    sock: Any,
    frames: Sequence[bytes],
    *,
    timeout: float | None,
    manage_timeout: bool = True,
) -> None:
    """Write frames against one deadline without changing the socket's read timeout."""
    getsockopt = cast(Callable[..., Any] | None, getattr(sock, "getsockopt", None))
    setsockopt = cast(Callable[..., Any] | None, getattr(sock, "setsockopt", None))
    apply_write_timeout = (
        manage_timeout
        and timeout is not None
        and getsockopt is not None
        and setsockopt is not None
        and hasattr(socket, "SO_SNDTIMEO")
    )
    deadline = time.monotonic() + timeout if apply_write_timeout and timeout is not None else None
    previous_timeout: bytes | None = None
    if apply_write_timeout and getsockopt is not None:
        previous_timeout = cast(
            bytes,
            getsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, _SOCKET_TIMEOUT_OPTION_SIZE),
        )

    def set_write_timeout(remaining: float) -> None:
        if setsockopt is None:
            return
        if remaining <= 0:
            raise TimeoutError("protocol batch write timed out")
        if os.name == "nt":
            timeout_value = struct.pack("@I", max(1, int(remaining * 1_000)))
        else:
            seconds = int(remaining)
            microseconds = max(1, int((remaining - seconds) * 1_000_000))
            timeout_value = struct.pack("@ll", seconds, microseconds)
        setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, timeout_value)

    def send_frame(frame: bytes) -> None:
        send = cast(Callable[[Any], int] | None, getattr(sock, "send", None))
        if send is None:
            if deadline is not None:
                set_write_timeout(deadline - time.monotonic())
            sock.sendall(frame)
            return
        view = memoryview(frame)
        offset = 0
        while offset < len(view):
            if deadline is not None:
                set_write_timeout(deadline - time.monotonic())
            sent = send(view[offset:])
            if sent <= 0:
                raise ConnectionError("protocol socket write made no progress")
            offset += sent

    try:
        for frame in frames:
            send_frame(frame)
    except BaseException as write_error:
        if previous_timeout is not None and setsockopt is not None:
            try:
                setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, previous_timeout)
            except BaseException as restore_error:
                raise write_error.with_traceback(write_error.__traceback__) from restore_error
        raise
    else:
        if previous_timeout is not None and setsockopt is not None:
            try:
                setsockopt(socket.SOL_SOCKET, socket.SO_SNDTIMEO, previous_timeout)
            except OSError:
                # The reader owns connection shutdown and may retire the socket
                # immediately after receiving the response.  A completed write
                # must not become an ambiguous failure solely because restoring
                # timeout state raced that close.
                fileno = getattr(sock, "fileno", None)
                if not callable(fileno) or fileno() >= 0:
                    raise
