from __future__ import annotations

import io
import time
import zlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, cast

from ferricstore.errors import FerricStoreError

_FRAME_BODY_SPLIT_BYTES = 64 * 1024


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


def validated_optional_nonnegative_int(value: Any, *, name: str) -> int | None:
    """Validate a byte/item/count limit without coercing lossy input types."""
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer or None")
    return value


def validated_nonnegative_int(value: Any, *, name: str) -> int:
    validated = validated_optional_nonnegative_int(value, name=name)
    if validated is None:
        raise ValueError(f"{name} must be a non-negative integer")
    return validated


def validated_optional_positive_int(value: Any, *, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer or None")
    return value


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


def decompress_response(body: bytes, limit: int | None) -> bytes:
    limit = validated_optional_nonnegative_int(
        limit,
        name="max_decompressed_response_bytes",
    )
    try:
        if limit is None:
            return zlib.decompress(body)

        decompressor = zlib.decompressobj()
        output = bytearray()
        compressed = body
        while True:
            remaining = limit - len(output)
            output.extend(decompressor.decompress(compressed, remaining + 1))
            if len(output) > limit:
                raise FerricStoreError("protocol response exceeds max_decompressed_response_bytes")
            if not decompressor.unconsumed_tail:
                break
            compressed = decompressor.unconsumed_tail

        if not decompressor.eof:
            raise FerricStoreError("protocol response has invalid compressed data")
        output.extend(decompressor.flush(limit - len(output) + 1))
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
    """Write a frame sequence against one deadline instead of one timeout per frame."""
    gettimeout = cast(Callable[[], float | None] | None, getattr(sock, "gettimeout", None))
    settimeout = cast(
        Callable[[float | None], None] | None,
        getattr(sock, "settimeout", None),
    )
    previous_timeout: float | None = None
    apply_write_timeout = (
        manage_timeout and timeout is not None and gettimeout is not None and settimeout is not None
    )
    deadline = time.monotonic() + timeout if apply_write_timeout and timeout is not None else None
    if apply_write_timeout and gettimeout is not None and settimeout is not None:
        previous_timeout = gettimeout()
    try:
        for index, frame in enumerate(frames):
            if deadline is not None and settimeout is not None:
                remaining = timeout if index == 0 else deadline - time.monotonic()
                assert remaining is not None
                if remaining <= 0:
                    raise TimeoutError("protocol batch write timed out")
                settimeout(remaining)
            sock.sendall(frame)
    except BaseException as write_error:
        if apply_write_timeout and settimeout is not None:
            try:
                settimeout(previous_timeout)
            except BaseException as restore_error:
                raise write_error.with_traceback(write_error.__traceback__) from restore_error
        raise
    else:
        if apply_write_timeout and settimeout is not None:
            settimeout(previous_timeout)
