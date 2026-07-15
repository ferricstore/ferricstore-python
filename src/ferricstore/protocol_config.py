from __future__ import annotations

from dataclasses import dataclass

from ferricstore.config_validation import (
    validate_bool,
    validate_optional_positive_int,
    validate_optional_thread_wait_seconds,
)
from ferricstore.config_validation import (
    validate_optional_nonnegative_int as validated_optional_nonnegative_int,
)
from ferricstore.protocol_common import _protocol_collection_limit, _protocol_lane_count
from ferricstore.protocol_framing import validated_response_chunk_limit


@dataclass(frozen=True, slots=True)
class ProtocolRuntimeConfig:
    """Validated transport-independent protocol limits and timing."""

    timeout: float | None
    heartbeat_interval: float | None
    heartbeat_timeout: float | None
    lanes: int
    max_response_bytes: int | None
    max_response_chunks: int | None
    max_decompressed_response_bytes: int | None
    max_event_queue_size: int | None
    max_decoded_collection_items: int | None
    max_inflight_requests: int | None
    max_pending_request_bytes: int | None
    max_batch_items: int | None
    tls: bool

    @classmethod
    def build(
        cls,
        *,
        timeout: float | None,
        heartbeat_interval: float | None,
        heartbeat_timeout: float | None,
        lanes: int,
        max_response_bytes: int | None,
        max_response_chunks: int | None,
        max_decompressed_response_bytes: int | None,
        max_event_queue_size: int | None,
        max_decoded_collection_items: int | None,
        max_inflight_requests: int | None,
        max_pending_request_bytes: int | None,
        max_batch_items: int | None,
        tls: bool,
    ) -> ProtocolRuntimeConfig:
        return cls(
            timeout=validate_optional_thread_wait_seconds(timeout, name="timeout"),
            heartbeat_interval=validate_optional_thread_wait_seconds(
                heartbeat_interval,
                name="heartbeat_interval",
            ),
            heartbeat_timeout=validate_optional_thread_wait_seconds(
                heartbeat_timeout,
                name="heartbeat_timeout",
            ),
            lanes=_protocol_lane_count(lanes),
            max_response_bytes=validated_optional_nonnegative_int(
                max_response_bytes,
                name="max_response_bytes",
            ),
            max_response_chunks=validated_response_chunk_limit(max_response_chunks),
            max_decompressed_response_bytes=validated_optional_nonnegative_int(
                max_decompressed_response_bytes,
                name="max_decompressed_response_bytes",
            ),
            max_event_queue_size=validated_optional_nonnegative_int(
                max_event_queue_size,
                name="max_event_queue_size",
            ),
            max_decoded_collection_items=_protocol_collection_limit(max_decoded_collection_items),
            max_inflight_requests=validate_optional_positive_int(
                max_inflight_requests,
                name="max_inflight_requests",
            ),
            max_pending_request_bytes=validate_optional_positive_int(
                max_pending_request_bytes,
                name="max_pending_request_bytes",
            ),
            max_batch_items=validate_optional_positive_int(
                max_batch_items,
                name="max_batch_items",
            ),
            tls=validate_bool(tls, name="tls"),
        )
