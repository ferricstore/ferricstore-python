from __future__ import annotations

import socket
import ssl
import threading
import time
from concurrent.futures import Future
from functools import partial
from typing import TYPE_CHECKING, Any, Protocol

from ferricstore.batch_core import require_batch_items
from ferricstore.errors import FerricStoreError
from ferricstore.lifecycle_core import (
    DeferredCallbackFuture,
    raise_primary_with_cleanup,
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.protocol_commands import (
    _compact_flow_many_payloads_from_raw,  # noqa: F401 - historical monkeypatch seam
    _compact_pipeline_payload_from_raw,
)
from ferricstore.protocol_common import _compact_payload_budget, _set_wire_future_sources
from ferricstore.protocol_constants import (
    _FLAG_COMPRESSED,
    _FLAG_CUSTOM_PAYLOAD,
    _HEADER,
    _MAGIC,
    _OP_PIPELINE,
    _REQUEST_VERSION,
    ProtocolCommand,
    ProtocolResponse,
)
from ferricstore.protocol_framing import ResponseIdentity, frame_parts
from ferricstore.protocol_framing import send_frames as _send_frames
from ferricstore.protocol_lifecycle import (
    DEFAULT_MAX_BATCH_ITEMS,
    DEFAULT_MAX_PENDING_REQUEST_BYTES,
    PendingRequestCapacityError,
    check_batch_item_limit,
)
from ferricstore.protocol_pipeline_codec import (
    _blocks_forever,
    _compact_pipeline_payload,
    _expected_command_collection_items,
    _pipeline_frame_supported,
)
from ferricstore.protocol_planning import PreparedCommand
from ferricstore.protocol_responses import (
    _flow_many_group_values,
    _ok_scalar,
    _pipeline_pair_list,
)


class SyncProtocolBatchHost(Protocol):
    """Typed transport contract consumed by batch orchestration."""

    compression: str
    max_batch_items: int | None
    max_pending_request_bytes: int | None
    timeout: float | None
    _last_activity: float
    _lock: threading.Lock

    def _check_batch_item_count(self, count: int) -> None: ...

    def execute_command(self, *args: Any) -> Any: ...

    def _build_protocol_command(self, *args: Any) -> ProtocolCommand: ...

    def _compact_flow_many_payloads(
        self,
        commands: list[tuple[Any, ...]],
        protocol_commands: list[ProtocolCommand] | None,
    ) -> list[tuple[int, bytes, int]] | None: ...

    def _ensure_connected(self) -> None: ...

    def _current_transport_binding(
        self,
    ) -> tuple[int, socket.socket | ssl.SSLSocket | None]: ...

    def _next_request_id(self) -> int: ...

    def _next_lane_id(self, lane_id: int) -> int: ...

    def _register_pending_request(
        self,
        request_id: int,
        future: Future[ProtocolResponse],
        *,
        expected_collection_items: int | None = None,
        client_trace: dict[str, Any] | None = None,
        binding: tuple[int, socket.socket | ssl.SSLSocket | None] | None = None,
        expires_at: float | None = None,
        response_identity: ResponseIdentity | None = None,
    ) -> tuple[int, socket.socket | ssl.SSLSocket | None]: ...

    def _encode_pending_request_body(
        self,
        request_id: int,
        payload: dict[str, Any] | bytes,
        *,
        compression: str | None = None,
    ) -> tuple[bytes, bool]: ...

    def _set_pending_request_size(self, request_id: int, size: int) -> None: ...

    def _require_socket(self) -> socket.socket | ssl.SSLSocket: ...

    def _close_transport(
        self,
        exc: BaseException | None = None,
        *,
        mark_closed: bool = False,
        expected_sock: socket.socket | ssl.SSLSocket | None = None,
    ) -> None: ...

    def _discard_pending_request(
        self,
        request_id: int,
        *,
        expected_future: Future[ProtocolResponse] | None = None,
    ) -> tuple[Future[ProtocolResponse] | None, dict[str, Any] | None]: ...

    def _request(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = ...,
        exact_lane: bool = False,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse: ...

    def _request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        timeout: float | None | object = ...,
        expected_collection_items: int | None = None,
    ) -> ProtocolResponse: ...

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
    ) -> tuple[int, Future[ProtocolResponse]]: ...

    def _submit_request_on_lane(
        self,
        opcode: int,
        lane_id: int,
        payload: dict[str, Any] | bytes,
        flags: int = 0,
        *,
        expected_collection_items: int | None = None,
        _expire_at_adapter_timeout: bool = True,
    ) -> tuple[int, Future[ProtocolResponse]]: ...

    def _response_value(self, response: ProtocolResponse) -> Any: ...

    def _batch_item_value(self, item: Any) -> Any: ...

    def _submit_pipeline_payload(
        self,
        payload: bytes,
        expected_count: int,
        *,
        routed_lane: int | None = None,
    ) -> Future[ProtocolResponse]: ...

    def _pipeline_item_futures(
        self,
        response_future: Future[ProtocolResponse],
        count: int,
    ) -> list[Future[Any]]: ...

    def _submit_pipeline(
        self,
        commands: list[ProtocolCommand],
        *,
        raw_commands: list[tuple[Any, ...]] | None = None,
        compact: bool = True,
        routed_lane: int | None = None,
    ) -> list[Future[Any]]: ...

    def _build_compact_raw_pipeline(
        self,
        commands: list[tuple[Any, ...]],
        *,
        values_only: bool,
        protocol_commands: list[ProtocolCommand] | None = None,
    ) -> bytes | None: ...

    def _value_future(self, response_future: Future[ProtocolResponse]) -> Future[Any]: ...


if TYPE_CHECKING:

    class _SyncProtocolBatchBase(SyncProtocolBatchHost):
        pass

else:

    class _SyncProtocolBatchBase:
        pass


class SyncProtocolBatchMixin(_SyncProtocolBatchBase):
    """Batch submission and pipeline completion for a sync transport host."""

    def _check_batch_item_count(self, count: int) -> None:
        try:
            check_batch_item_limit(
                count,
                getattr(self, "max_batch_items", DEFAULT_MAX_BATCH_ITEMS),
            )
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc

    def _build_compact_raw_pipeline(
        self,
        commands: list[tuple[Any, ...]],
        *,
        values_only: bool,
        protocol_commands: list[ProtocolCommand] | None = None,
    ) -> bytes | None:
        pending_limit = getattr(
            self,
            "max_pending_request_bytes",
            DEFAULT_MAX_PENDING_REQUEST_BYTES,
        )
        enabled, max_payload_bytes = _compact_payload_budget(
            pending_limit,
            getattr(self, "compression", "none"),
        )
        if not enabled:
            return None
        try:
            return _compact_pipeline_payload_from_raw(
                commands,
                values_only=values_only,
                protocol_commands=protocol_commands,
                max_payload_bytes=max_payload_bytes,
                pending_limit=pending_limit,
            )
        except PendingRequestCapacityError as exc:
            raise FerricStoreError(str(exc)) from exc

    def submit_commands(self, commands: list[tuple[Any, ...]]) -> list[Future[Any]]:
        return self._submit_commands(commands, routed_lane=None)

    def submit_commands_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Future[Any]]:
        return self._submit_commands(commands, routed_lane=lane_id)

    def submit_prepared_commands_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
    ) -> list[Future[Any]]:
        self._check_batch_item_count(len(prepared_commands))
        return self._submit_commands(
            [prepared.args for prepared in prepared_commands],
            routed_lane=lane_id,
            prepared_commands=prepared_commands,
        )

    def _submit_commands(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
        prepared_commands: list[PreparedCommand] | None = None,
    ) -> list[Future[Any]]:
        return _submit_commands_on_host(
            self,
            commands,
            routed_lane=routed_lane,
            prepared_commands=prepared_commands,
        )

    def submit_batch(self, commands: list[tuple[Any, ...]]) -> Future[list[Any]]:
        return self._submit_batch(commands, routed_lane=None)

    def submit_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> Future[list[Any]]:
        return self._submit_batch(commands, routed_lane=lane_id)

    def submit_prepared_batch_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
    ) -> Future[list[Any]]:
        self._check_batch_item_count(len(prepared_commands))
        return self._submit_batch(
            [prepared.args for prepared in prepared_commands],
            routed_lane=lane_id,
            prepared_commands=prepared_commands,
        )

    def _submit_batch(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
        prepared_commands: list[PreparedCommand] | None = None,
    ) -> Future[list[Any]]:
        future: Future[list[Any]] = Future()
        if not commands:
            future.set_result([])
            return future
        self._check_batch_item_count(len(commands))
        future.set_running_or_notify_cancel()

        protocol_commands = (
            [prepared.command for prepared in prepared_commands]
            if prepared_commands is not None
            else None
        )
        compact_payload = self._build_compact_raw_pipeline(
            commands,
            values_only=True,
            protocol_commands=protocol_commands,
        )
        if compact_payload is not None:
            response_future = self._submit_pipeline_payload(
                compact_payload,
                len(commands),
                routed_lane=routed_lane,
            )
            self._complete_batch_future(response_future, len(commands), future)
            return future

        flow_many_payloads = self._compact_flow_many_payloads(commands, protocol_commands)
        if flow_many_payloads is not None:
            self._submit_flow_many_batch(
                flow_many_payloads,
                len(commands),
                future,
                routed_lane=routed_lane,
            )
            return future

        if protocol_commands is None:
            protocol_commands = [self._build_protocol_command(*command) for command in commands]
        if not _pipeline_frame_supported(protocol_commands):
            item_futures = self._submit_commands(
                commands,
                routed_lane=routed_lane,
                prepared_commands=prepared_commands,
            )
            _set_wire_future_sources(
                future,
                [
                    source
                    for item in item_futures
                    for source in getattr(item, "_ferricstore_sources", (item,))
                ],
            )
            lock = threading.Lock()
            results: list[Any] = [None] * len(item_futures)
            remaining = len(item_futures)

            def complete_items(index: int, item_future: Future[Any]) -> None:
                nonlocal remaining
                try:
                    value = item_future.result()
                except Exception as exc:
                    try_set_future_exception(future, exc)
                    return

                with lock:
                    if future.done():
                        return
                    results[index] = value
                    remaining -= 1
                    if remaining == 0:
                        try_set_future_result(future, results)

            for index, item in enumerate(item_futures):
                item.add_done_callback(partial(complete_items, index))
            return future

        response_future = self._submit_pipeline_request(
            protocol_commands,
            compact=False,
            values_only=True,
            routed_lane=routed_lane,
        )
        self._complete_batch_future(response_future, len(protocol_commands), future)
        return future

    def _submit_flow_many_batch(
        self,
        payloads: list[tuple[int, bytes, int]],
        expected_count: int,
        future: Future[list[Any]],
        *,
        routed_lane: int | None = None,
    ) -> None:
        pending: list[tuple[Future[ProtocolResponse], int]] = []
        try:
            for opcode, payload, count in payloads:
                submit = (
                    self._submit_request if routed_lane is None else self._submit_request_on_lane
                )
                _request_id, response_future = submit(
                    opcode,
                    1 if routed_lane is None else routed_lane,
                    payload,
                    _FLAG_CUSTOM_PAYLOAD,
                )
                pending.append((response_future, count))
        except Exception as exc:
            _set_wire_future_sources(
                future,
                [response_future for response_future, _count in pending],
            )
            try_set_future_exception(future, exc)
            return

        _set_wire_future_sources(
            future,
            [response_future for response_future, _count in pending],
        )

        results: list[list[Any] | None] = [None] * len(pending)
        remaining = len(pending)
        lock = threading.Lock()

        def complete(index: int, response_future: Future[ProtocolResponse], count: int) -> None:
            nonlocal remaining
            try:
                value = self._response_value(response_future.result())
                group_values = _flow_many_group_values(value, count)
            except Exception as exc:
                try_set_future_exception(future, exc)
                return

            with lock:
                if future.done():
                    return
                results[index] = group_values
                remaining -= 1
                if remaining == 0:
                    merged = [item for group in results if group is not None for item in group]
                    if len(merged) != expected_count:
                        try_set_future_exception(
                            future,
                            FerricStoreError(
                                "protocol Flow many returned invalid result", raw=merged
                            ),
                        )
                    else:
                        try_set_future_result(future, merged)

        for index, (response_future, count) in enumerate(pending):
            response_future.add_done_callback(partial(complete, index, count=count))

    def _complete_batch_future(
        self,
        response_future: Future[ProtocolResponse],
        expected_count: int,
        future: Future[list[Any]],
        *,
        allow_scalar_ok: bool = False,
    ) -> None:

        _set_wire_future_sources(future, [response_future])

        def complete(source_future: Future[ProtocolResponse]) -> None:
            try:
                value = self._response_value(source_future.result())
                if allow_scalar_ok and _ok_scalar(value):
                    try_set_future_result(future, [value] * expected_count)
                    return
                if not isinstance(value, list) or len(value) != expected_count:
                    raise FerricStoreError("protocol PIPELINE returned invalid result", raw=value)
                if _pipeline_pair_list(value):
                    result = [self._batch_item_value(item) for item in value]
                else:
                    result = value
                try_set_future_result(future, result)
            except Exception as exc:
                try_set_future_exception(future, exc)

        response_future.add_done_callback(complete)

    def _submit_pipeline_payload(
        self,
        payload: bytes,
        _expected_count: int,
        *,
        routed_lane: int | None = None,
    ) -> Future[ProtocolResponse]:
        submit = self._submit_request if routed_lane is None else self._submit_request_on_lane
        _request_id, response_future = submit(
            _OP_PIPELINE,
            1 if routed_lane is None else routed_lane,
            payload,
            _FLAG_CUSTOM_PAYLOAD,
        )
        return response_future

    def _submit_pipeline(
        self,
        commands: list[ProtocolCommand],
        *,
        raw_commands: list[tuple[Any, ...]] | None = None,
        compact: bool = True,
        routed_lane: int | None = None,
    ) -> list[Future[Any]]:
        response_future = self._submit_pipeline_request(
            commands,
            raw_commands=raw_commands,
            compact=compact,
            routed_lane=routed_lane,
        )
        return self._pipeline_item_futures(response_future, len(commands))

    def _pipeline_item_futures(
        self,
        response_future: Future[ProtocolResponse],
        count: int,
    ) -> list[Future[Any]]:
        futures: list[Future[Any]] = [DeferredCallbackFuture() for _ in range(count)]
        for future in futures:
            future.set_running_or_notify_cancel()

        def complete(source_future: Future[ProtocolResponse]) -> None:
            deferred = [future for future in futures if isinstance(future, DeferredCallbackFuture)]
            for future in deferred:
                future.defer_callbacks()
            try:
                value = self._response_value(source_future.result())
                if not isinstance(value, list) or len(value) != len(futures):
                    raise FerricStoreError("protocol PIPELINE returned invalid result", raw=value)
                decoded = [self._batch_item_value(item) for item in value]
                for target, item in zip(futures, decoded, strict=True):
                    try_set_future_result(target, item)
            except Exception as exc:
                for target in futures:
                    try_set_future_exception(target, exc)
            finally:
                for future in deferred:
                    future.release_callbacks()

        response_future.add_done_callback(complete)
        return futures

    def _submit_pipeline_request(
        self,
        commands: list[ProtocolCommand],
        *,
        raw_commands: list[tuple[Any, ...]] | None = None,
        compact: bool = True,
        values_only: bool = False,
        routed_lane: int | None = None,
    ) -> Future[ProtocolResponse]:
        if not compact:
            compact_payload = None
        elif raw_commands is not None:
            compact_payload = self._build_compact_raw_pipeline(
                raw_commands,
                values_only=values_only,
            )
        else:
            compact_payload = _compact_pipeline_payload(commands, values_only=values_only)
        flags = _FLAG_CUSTOM_PAYLOAD if compact_payload is not None else 0
        payload: dict[str, Any] | bytes

        if compact_payload is not None:
            payload = compact_payload
        else:
            pipeline_commands = [
                {
                    "opcode": command.opcode,
                    "lane_id": command.lane_id if routed_lane is None else routed_lane,
                    "request_id": idx + 1,
                    "body": command.payload,
                }
                for idx, command in enumerate(commands)
            ]
            payload = {"atomicity": "none", "commands": pipeline_commands, "return": "compact"}

        submit = self._submit_request if routed_lane is None else self._submit_request_on_lane
        _request_id, response_future = submit(
            _OP_PIPELINE,
            1 if routed_lane is None else routed_lane,
            payload,
            flags,
        )
        return response_future

    def _value_future(self, response_future: Future[ProtocolResponse]) -> Future[Any]:
        value_future: Future[Any] = Future()
        value_future.set_running_or_notify_cancel()
        _set_wire_future_sources(value_future, [response_future])

        def complete(source: Future[ProtocolResponse]) -> None:
            try:
                value = self._response_value(source.result())
            except Exception as exc:
                try_set_future_exception(value_future, exc)
            else:
                try_set_future_result(value_future, value)

        response_future.add_done_callback(complete)
        return value_future

    def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
        return self._execute_batch(commands, routed_lane=None)

    def execute_batch_on_lane(
        self,
        commands: list[tuple[Any, ...]],
        lane_id: int,
    ) -> list[Any]:
        return self._execute_batch(commands, routed_lane=lane_id)

    def execute_prepared_batch_on_lane(
        self,
        prepared_commands: list[PreparedCommand],
        lane_id: int,
    ) -> list[Any]:
        self._check_batch_item_count(len(prepared_commands))
        return self._execute_batch(
            [prepared.args for prepared in prepared_commands],
            routed_lane=lane_id,
            prepared_commands=prepared_commands,
        )

    def _execute_batch(
        self,
        commands: list[tuple[Any, ...]],
        *,
        routed_lane: int | None,
        prepared_commands: list[PreparedCommand] | None = None,
    ) -> list[Any]:
        if not commands:
            return []
        self._check_batch_item_count(len(commands))

        lane_id = 1 if routed_lane is None else routed_lane
        request = self._request if routed_lane is None else self._request_on_lane

        protocol_commands = (
            [prepared.command for prepared in prepared_commands]
            if prepared_commands is not None
            else None
        )
        flow_many_payloads = self._compact_flow_many_payloads(commands, protocol_commands)
        if flow_many_payloads is not None:
            values: list[Any] = []
            for opcode, payload, count in flow_many_payloads:
                response = request(
                    opcode,
                    lane_id,
                    payload,
                    _FLAG_CUSTOM_PAYLOAD,
                )
                group_values = self._response_value(response)
                values.extend(_flow_many_group_values(group_values, count))
            return values

        compact_payload = self._build_compact_raw_pipeline(
            commands,
            values_only=True,
            protocol_commands=protocol_commands,
        )
        if compact_payload is not None:
            response = request(
                _OP_PIPELINE,
                lane_id,
                compact_payload,
                _FLAG_CUSTOM_PAYLOAD,
            )
            values = require_batch_items(
                self._response_value(response),
                len(commands),
                operation="protocol PIPELINE",
            )
            if _pipeline_pair_list(values):
                return [self._batch_item_value(item) for item in values]
            return values

        if protocol_commands is None:
            protocol_commands = [self._build_protocol_command(*command) for command in commands]
        if not _pipeline_frame_supported(protocol_commands):
            if routed_lane is None:
                return [self.execute_command(*command) for command in commands]
            values = []
            for index, (raw_command, command) in enumerate(
                zip(commands, protocol_commands, strict=True)
            ):
                command_lane = command.lane_id if routed_lane is None else routed_lane
                blocks_forever = (
                    prepared_commands[index].blocks_forever
                    if prepared_commands is not None
                    else _blocks_forever(raw_command)
                )
                if blocks_forever:
                    response = request(
                        command.opcode,
                        command_lane,
                        command.payload,
                        command.flags,
                        timeout=None,
                    )
                else:
                    response = request(
                        command.opcode,
                        command_lane,
                        command.payload,
                        command.flags,
                    )
                values.append(self._response_value(response))
            return values

        batch_commands = [
            {
                "opcode": command.opcode,
                "lane_id": command.lane_id if routed_lane is None else routed_lane,
                "request_id": idx + 1,
                "body": command.payload,
            }
            for idx, command in enumerate(protocol_commands)
        ]
        response = request(
            _OP_PIPELINE,
            lane_id,
            {"atomicity": "none", "commands": batch_commands, "return": "compact"},
        )

        values = require_batch_items(
            self._response_value(response),
            len(commands),
            operation="protocol PIPELINE",
        )

        return [self._batch_item_value(item) for item in values]


def _submit_commands_on_host(
    host: SyncProtocolBatchHost,
    commands: list[tuple[Any, ...]],
    *,
    routed_lane: int | None,
    prepared_commands: list[PreparedCommand] | None = None,
) -> list[Future[Any]]:
    """Build and submit an ordered non-atomic batch on one transport host."""
    if not commands:
        return []
    host._check_batch_item_count(len(commands))

    protocol_commands = (
        [prepared.command for prepared in prepared_commands]
        if prepared_commands is not None
        else None
    )
    compact_payload = host._build_compact_raw_pipeline(
        commands,
        values_only=False,
        protocol_commands=protocol_commands,
    )
    if compact_payload is not None:
        compact_response_future = host._submit_pipeline_payload(
            compact_payload,
            len(commands),
            routed_lane=routed_lane,
        )
        return host._pipeline_item_futures(compact_response_future, len(commands))

    if protocol_commands is None:
        protocol_commands = [host._build_protocol_command(*command) for command in commands]
    if _pipeline_frame_supported(protocol_commands):
        return host._submit_pipeline(
            protocol_commands,
            compact=False,
            routed_lane=routed_lane,
        )

    pending: list[tuple[int, Future[ProtocolResponse]]] = []
    frames: list[bytes] = []
    host._ensure_connected()
    with host._lock:
        expires_at = None if host.timeout is None else time.monotonic() + max(0.0, host.timeout)
        binding = host._current_transport_binding()
        try:
            for index, (raw_command, command) in enumerate(
                zip(commands, protocol_commands, strict=True)
            ):
                request_id = host._next_request_id()
                lane_id = (
                    host._next_lane_id(command.lane_id) if routed_lane is None else routed_lane
                )
                response_future: Future[ProtocolResponse] = Future()
                prepared = prepared_commands[index] if prepared_commands is not None else None
                blocks_forever = (
                    prepared.blocks_forever
                    if prepared is not None
                    else _blocks_forever(raw_command)
                )
                expected_collection_items = (
                    prepared.expected_collection_items
                    if prepared is not None
                    else _expected_command_collection_items(raw_command)
                )
                host._register_pending_request(
                    request_id,
                    response_future,
                    expected_collection_items=expected_collection_items,
                    binding=binding,
                    expires_at=None if blocks_forever else expires_at,
                    response_identity=ResponseIdentity(
                        lane_id=lane_id,
                        opcode=command.opcode,
                        request_id=request_id,
                    ),
                )
                pending.append((request_id, response_future))

                body, compressed = host._encode_pending_request_body(
                    request_id,
                    command.payload,
                )
                flags = command.flags | (_FLAG_COMPRESSED if compressed else 0)
                header = _HEADER.pack(
                    _MAGIC,
                    _REQUEST_VERSION,
                    flags,
                    lane_id,
                    command.opcode,
                    request_id,
                    len(body),
                )
                host._set_pending_request_size(request_id, len(header) + len(body))
                frames.extend(frame_parts(header, body))

            sock = binding[1] or host._require_socket()
            try:
                _send_frames(sock, frames, timeout=host.timeout)
            except BaseException as write_error:
                try:
                    host._close_transport(
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
            host._last_activity = time.monotonic()
        except BaseException as exc:
            for request_id, response_future in pending:
                host._discard_pending_request(
                    request_id,
                    expected_future=response_future,
                )
                try_set_future_exception(response_future, exc)
            raise

    return [host._value_future(response_future) for _request_id, response_future in pending]


__all__ = ["SyncProtocolBatchMixin"]
