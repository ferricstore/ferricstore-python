from __future__ import annotations

import asyncio
import base64
import json
import math
import threading
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, cast

import ferricstore.http_command_policy as _http_command_policy
from ferricstore.command_grammar import split_flow_value_mget
from ferricstore.errors import (
    FerricStoreError,
    HttpError,
    InvalidCommandError,
    OverloadedError,
    classify_server_error,
)
from ferricstore.http_coalescing import CommandCoalescer
from ferricstore.http_transport import JsonHttpTransport, _HttpDeadline
from ferricstore.protocol_commands import build_protocol_command

_command_name = _http_command_policy.command_name
_command_values = _http_command_policy.command_values
_effective_command_values = _http_command_policy.effective_command_values
_effective_timeout = _http_command_policy.effective_timeout
_is_blocking_command = _http_command_policy.is_blocking_command
_require_http_command = _http_command_policy.require_http_command
_unwrapped_command_values = _http_command_policy.unwrapped_command_values

_HTTP_STRUCTURED_FLOW_COMMANDS = frozenset(
    {
        "FLOW.QUERY",
        "FLOW.VALUE.MGET",
        "FLOW.STEP_CONTINUE",
        "FLOW.START_AND_CLAIM",
        "FLOW.RUN_STEPS_MANY",
        "FLOW.SCHEDULE.CREATE",
        "FLOW.SCHEDULE.GET",
        "FLOW.SCHEDULE.DELETE",
        "FLOW.SCHEDULE.FIRE_DUE",
        "FLOW.SCHEDULE.LIST",
        "FLOW.SCHEDULE.FIRE",
        "FLOW.SCHEDULE.PAUSE",
        "FLOW.SCHEDULE.RESUME",
        "FLOW.EFFECT.RESERVE",
        "FLOW.EFFECT.CONFIRM",
        "FLOW.EFFECT.FAIL",
        "FLOW.EFFECT.COMPENSATE",
        "FLOW.EFFECT.GET",
        "FLOW.GOVERNANCE.LEDGER",
        "FLOW.GOVERNANCE.OVERVIEW",
        "FLOW.APPROVAL.REQUEST",
        "FLOW.APPROVAL.APPROVE",
        "FLOW.APPROVAL.REJECT",
        "FLOW.APPROVAL.GET",
        "FLOW.APPROVAL.LIST",
        "FLOW.CIRCUIT.OPEN",
        "FLOW.CIRCUIT.CLOSE",
        "FLOW.CIRCUIT.GET",
        "FLOW.BUDGET.RESERVE",
        "FLOW.BUDGET.COMMIT",
        "FLOW.BUDGET.RELEASE",
        "FLOW.BUDGET.GET",
        "FLOW.BUDGET.LIST",
        "FLOW.LIMIT.LEASE",
        "FLOW.LIMIT.SPEND",
        "FLOW.LIMIT.RELEASE",
        "FLOW.LIMIT.GET",
        "FLOW.LIMIT.LIST",
    }
)

_BINARY_ENCODING = "ferricstore-json-v1"
_COMPACT_ENCODING = "ferricstore-msgpack-v1"
_BYTES_TAG = "$ferricstore_bytes"
_MAP_TAG = "$ferricstore_map"


class HttpAdapter:
    """Sync command adapter for a FerricStore HTTP command endpoint.

    The adapter keeps the SDK command surface unchanged and replaces only the
    native socket transport. One SDK pipeline maps to one ``POST /v1/commands``
    request.
    """

    client: HttpAdapter
    requires_explicit_session = True
    supports_concurrent_fanout = True
    _supports_native_flow_query_options = False

    def __init__(
        self,
        url: str,
        *,
        bearer_token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = 30.0,
        max_connections: int = 1,
        max_concurrent_requests: int | None = None,
        http2: bool = False,
        compact: bool = False,
        coalesce_window_ms: float = 0,
        coalesce_max_items: int | None = None,
        max_request_bytes: int = 1024 * 1024,
        max_response_bytes: int = 16 * 1024 * 1024,
        max_batch_items: int = 1_000,
        ssl_context: Any = None,
    ) -> None:
        self.max_connections = _positive_int(max_connections, name="max_connections")
        if not isinstance(http2, bool):
            raise TypeError("http2 must be a boolean")
        if not isinstance(compact, bool):
            raise TypeError("compact must be a boolean")
        self.compact = compact
        default_concurrency = 100 if http2 else self.max_connections
        self.max_concurrent_requests = _positive_int(
            default_concurrency if max_concurrent_requests is None else max_concurrent_requests,
            name="max_concurrent_requests",
        )
        self.max_batch_items = _positive_int(max_batch_items, name="max_batch_items")
        self.coalesce_window_ms = _nonnegative_number(
            coalesce_window_ms,
            name="coalesce_window_ms",
        )
        self.coalesce_max_items = _positive_int(
            min(64, self.max_batch_items) if coalesce_max_items is None else coalesce_max_items,
            name="coalesce_max_items",
        )
        if self.coalesce_max_items > self.max_batch_items:
            raise ValueError("coalesce_max_items cannot exceed max_batch_items")
        self._transport = JsonHttpTransport(
            url,
            bearer_token=bearer_token,
            username=username,
            password=password,
            headers=headers,
            timeout=timeout,
            max_request_bytes=max_request_bytes,
            max_response_bytes=max_response_bytes,
            max_connections=self.max_connections,
            http2=http2,
            messagepack=compact,
            ssl_context=ssl_context,
        )
        self._slots = threading.BoundedSemaphore(self.max_concurrent_requests)
        self._request_slots_redundant = (
            not http2 and self.max_concurrent_requests >= self.max_connections
        )
        self._closed = False
        self._state_lock = threading.Lock()
        self.client = self
        self._coalescer = (
            CommandCoalescer(
                self,
                window_ms=self.coalesce_window_ms,
                max_items=self.coalesce_max_items,
                max_bytes=max_request_bytes,
                command_result=_command_result,
            )
            if self.coalesce_window_ms > 0 and self.coalesce_max_items > 1
            else None
        )

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> HttpAdapter:
        return cls(url, **kwargs)

    @property
    def backpressure_scope(self) -> tuple[str, str]:
        return ("http", self._transport.base_url)

    def execute_command(self, *args: Any) -> Any:
        return self._execute_command_with_deadline(args, self._command_deadline([args]))

    def _execute_command_with_deadline(
        self,
        command: tuple[Any, ...],
        deadline: _HttpDeadline,
        cancelled: threading.Event | None = None,
    ) -> Any:
        if self._coalescer is not None and not _is_blocking_command(command):
            return self._coalescer.execute(command, deadline, cancelled)
        return self._execute_batch_with_deadline([command], deadline)[0]

    def execute_batch(self, commands: Sequence[Sequence[Any]]) -> list[Any]:
        command_list = list(commands)
        return self._execute_batch_with_deadline(
            command_list,
            self._command_deadline(command_list),
        )

    def _execute_batch_with_deadline(
        self,
        commands: Sequence[Sequence[Any]],
        deadline: _HttpDeadline,
    ) -> list[Any]:
        results, binary = self._request_batch_with_deadline(commands, deadline)
        return [_command_result(result, binary=binary) for result in results]

    def _request_batch_with_deadline(
        self,
        commands: Sequence[Sequence[Any]],
        deadline: _HttpDeadline,
    ) -> tuple[list[Any], bool]:
        command_list = list(commands)
        if not command_list:
            return [], False
        if len(command_list) > self.max_batch_items:
            raise FerricStoreError(
                f"HTTP command batch exceeds max_batch_items={self.max_batch_items}"
            )
        encoded = [
            self._encode_command(command, index) for index, command in enumerate(command_list)
        ]
        request_body = self._request_body(encoded)
        acquired_slot = False
        if not self._request_slots_redundant:
            if not self._acquire_slot(deadline):
                raise _capacity_timeout_error()
            acquired_slot = True
        try:
            self._require_open()
            if self.compact:
                _status, envelope = self._transport.request_messagepack(
                    "POST",
                    "/v1/commands",
                    body=request_body,
                    _deadline=deadline,
                )
            else:
                _status, envelope = self._transport.request_json(
                    "POST",
                    "/v1/commands",
                    body=request_body,
                    _deadline=deadline,
                )
        finally:
            if acquired_slot:
                self._slots.release()
        results = envelope.get("results")
        if not isinstance(results, list):
            raise HttpError(
                "FerricStore HTTP endpoint response is missing results",
                status_code=200,
                error_code="invalid_response",
                raw=envelope,
                retryable=False,
                safe_to_retry=False,
            )
        if len(results) != len(command_list):
            raise HttpError(
                f"FerricStore HTTP endpoint returned {len(results)} results; "
                f"expected {len(command_list)}",
                status_code=200,
                error_code="invalid_response",
                raw=envelope,
                retryable=False,
                safe_to_retry=False,
            )
        response_encoding = envelope.get("encoding")
        expected_encoding = _COMPACT_ENCODING if self.compact else _BINARY_ENCODING
        if response_encoding not in {None, expected_encoding}:
            raise HttpError(
                "FerricStore HTTP endpoint returned an unknown command encoding",
                status_code=200,
                error_code="invalid_response",
                raw=envelope,
                retryable=False,
                safe_to_retry=False,
            )
        return results, not self.compact and response_encoding == _BINARY_ENCODING

    def _encode_command(self, command: Sequence[Any], index: int) -> Any:
        values = _command_values(command, index)
        name = _command_name(values[0], index).upper()
        effective_values = _effective_command_values(values, index)
        effective_name = _command_name(effective_values[0], index).upper()
        _require_http_command(effective_name)
        if name == "COMMAND_EXEC":
            return _structured_command_exec(values, index, compact=self.compact)
        if name in _HTTP_STRUCTURED_FLOW_COMMANDS:
            return _structured_flow_command(values, index, compact=self.compact)
        if self.compact:
            return _compact_command(values, index)
        return _json_command(values, index)

    def _request_body(self, encoded: list[Any]) -> dict[str, Any]:
        return {
            "commands": encoded,
            "encoding": _COMPACT_ENCODING if self.compact else _BINARY_ENCODING,
        }

    def _request_body_size(self, encoded: list[Any]) -> int:
        body = self._request_body(encoded)
        if self.compact:
            return self._transport.messagepack_size(body)
        return len(_encode_json_bytes(body))

    def execute_batch_ordered(self, commands: Sequence[Sequence[Any]]) -> list[Any]:
        return self.execute_batch(commands)

    def close(self) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        self._transport.close()

    def invalidate(self) -> None:
        # HTTP requests do not retain command-affine server state.
        return None

    def _require_open(self) -> None:
        with self._state_lock:
            if self._closed:
                raise FerricStoreError("HTTP adapter is closed")

    def _acquire_slot(self, deadline: _HttpDeadline) -> bool:
        try:
            remaining = deadline.remaining()
        except TimeoutError:
            return False
        if remaining is None:
            return self._slots.acquire()
        return self._slots.acquire(timeout=remaining)

    def _command_deadline(self, commands: Sequence[Sequence[Any]]) -> _HttpDeadline:
        return _HttpDeadline(_effective_timeout(commands, self._transport.timeout))


class AsyncHttpAdapter:
    """Async command adapter backed by bounded worker-thread HTTP requests."""

    client: AsyncHttpAdapter
    requires_explicit_session = True
    supports_concurrent_fanout = True
    _supports_native_flow_query_options = False

    def __init__(self, url: str, **kwargs: Any) -> None:
        self._sync = HttpAdapter(url, **kwargs)
        self._slots = asyncio.Semaphore(self._sync.max_concurrent_requests)
        self._executor = ThreadPoolExecutor(
            max_workers=self._sync.max_concurrent_requests,
            thread_name_prefix="ferricstore-http",
        )
        self._submission_lock = threading.Lock()
        self._close_lock = asyncio.Lock()
        self._closed = False
        self._shutdown_complete = False
        self.client = self

    @classmethod
    def from_url(cls, url: str, **kwargs: Any) -> AsyncHttpAdapter:
        return cls(url, **kwargs)

    @property
    def backpressure_scope(self) -> tuple[str, str]:
        return ("async-http", self._sync._transport.base_url)

    async def execute_command(self, *args: Any) -> Any:
        if self._sync._coalescer is None or _is_blocking_command(args):
            return (await self.execute_batch([args]))[0]
        deadline = self._sync._command_deadline([args])
        cancelled = threading.Event()
        self._require_open()
        await self._acquire_slot(deadline)
        return await self._submit(
            self._sync._execute_command_with_deadline,
            args,
            deadline,
            cancelled,
            _cancelled=cancelled,
        )

    async def execute_batch(self, commands: Sequence[Sequence[Any]]) -> list[Any]:
        command_list = list(commands)
        if not command_list:
            return []
        deadline = self._sync._command_deadline(command_list)
        self._require_open()
        await self._acquire_slot(deadline)
        return cast(
            list[Any],
            await self._submit(
                self._sync._execute_batch_with_deadline,
                command_list,
                deadline,
            ),
        )

    async def execute_batch_ordered(self, commands: Sequence[Sequence[Any]]) -> list[Any]:
        return await self.execute_batch(commands)

    async def close(self) -> None:
        async with self._close_lock:
            if self._shutdown_complete:
                return
            with self._submission_lock:
                self._closed = True

            shutdown = asyncio.create_task(
                asyncio.to_thread(self._executor.shutdown, wait=True, cancel_futures=False)
            )
            cancellation: asyncio.CancelledError | None = None
            try:
                await asyncio.shield(shutdown)
            except asyncio.CancelledError as exc:
                cancellation = exc
                await shutdown
            finally:
                self._sync.close()
                self._shutdown_complete = True
            if cancellation is not None:
                raise cancellation

    async def invalidate(self) -> None:
        self._sync.invalidate()

    async def _acquire_slot(self, deadline: _HttpDeadline) -> None:
        try:
            remaining = deadline.remaining()
            if remaining is None:
                await self._slots.acquire()
            else:
                await asyncio.wait_for(self._slots.acquire(), timeout=remaining)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise _capacity_timeout_error() from exc

    async def _submit(
        self,
        operation: Callable[..., Any],
        *args: Any,
        _cancelled: threading.Event | None = None,
    ) -> Any:
        release_slot = True
        try:
            with self._submission_lock:
                self._require_open()
                future = self._executor.submit(partial(operation, *args))
            try:
                return await asyncio.wrap_future(future)
            except asyncio.CancelledError:
                if _cancelled is not None:
                    _cancelled.set()
                if not future.done():
                    release_slot = False
                    loop = asyncio.get_running_loop()
                    future.add_done_callback(
                        lambda _future: loop.call_soon_threadsafe(self._slots.release)
                    )
                raise
        finally:
            if release_slot:
                self._slots.release()

    def _require_open(self) -> None:
        if self._closed:
            raise FerricStoreError("HTTP adapter is closed")


def _capacity_timeout_error() -> HttpError:
    return HttpError(
        "FerricStore HTTP request timed out waiting for client capacity",
        error_code="transport_timeout",
        retryable=True,
        safe_to_retry=True,
    )


def _positive_int(value: int, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_number(value: float, *, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError(f"{name} must be a non-negative number")
    return float(value)


def _encode_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _json_command(command: Sequence[Any], index: int) -> list[Any]:
    values = _command_values(command, index)
    encoded = [_command_name(values[0], index)] + [_json_value(value) for value in values[1:]]
    if not isinstance(encoded[0], str) or not encoded[0]:
        raise TypeError(f"HTTP command {index} name must be text")
    _require_http_command(encoded[0].upper())
    return encoded


def _compact_command(command: Sequence[Any], index: int) -> list[Any]:
    values = _command_values(command, index)
    encoded = [_command_name(values[0], index)] + [_compact_value(value) for value in values[1:]]
    _require_http_command(encoded[0].upper())
    return encoded


def _structured_command_exec(values: list[Any], index: int, *, compact: bool) -> dict[str, Any]:
    unwrapped = _unwrapped_command_values(values, index)
    protocol_command = build_protocol_command("COMMAND_EXEC", *unwrapped)
    if not isinstance(protocol_command.payload, Mapping):
        raise InvalidCommandError("COMMAND_EXEC requires a structured native payload over HTTP")
    encode = _compact_value if compact else _json_value
    return {
        "command": "COMMAND_EXEC",
        "opcode": protocol_command.opcode,
        "payload": encode(protocol_command.payload),
    }


def _structured_flow_command(values: list[Any], index: int, *, compact: bool) -> dict[str, Any]:
    name = _command_name(values[0], index).upper()
    if name == "FLOW.VALUE.MGET":
        refs, max_bytes = split_flow_value_mget(values[1:])
        payload: Mapping[str, Any] = {"refs": list(refs)}
        if max_bytes is not None:
            payload = {**payload, "max_bytes": max_bytes}
        opcode = build_protocol_command(name, *values[1:]).opcode
    else:
        protocol_command = build_protocol_command(name, *values[1:])
        if not isinstance(protocol_command.payload, Mapping):
            raise InvalidCommandError(f"{name} cannot use a compact native payload over HTTP")
        payload = protocol_command.payload
        opcode = protocol_command.opcode
    encode = _compact_value if compact else _json_value
    return {
        "command": name,
        "opcode": opcode,
        "payload": encode(payload),
    }


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("HTTP command floats must be finite")
        return value
    if isinstance(value, (bytes, bytearray)):
        return {_BYTES_TAG: base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, Mapping):
        return {_MAP_TAG: [[_json_value(key), _json_value(item)] for key, item in value.items()]}
    raise TypeError(f"HTTP command value is not JSON-compatible: {type(value).__name__}")


def _compact_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bytes, bool, int)):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("HTTP command floats must be finite")
        return value
    if isinstance(value, (list, tuple)):
        return [_compact_value(item) for item in value]
    if isinstance(value, Mapping):
        decoded: dict[Any, Any] = {}
        for key, item in value.items():
            compact_key = _compact_value(key)
            try:
                hash(compact_key)
            except TypeError as exc:
                raise TypeError("compact HTTP map keys must remain hashable") from exc
            decoded[compact_key] = _compact_value(item)
        return decoded
    raise TypeError(f"HTTP command value is not MessagePack-compatible: {type(value).__name__}")


def _native_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, list):
        return [_native_value(item) for item in value]
    if isinstance(value, dict):
        return {
            (key.encode() if isinstance(key, str) else key): _native_value(item)
            for key, item in value.items()
        }
    return value


def _command_result(result: Any, *, binary: bool = False) -> Any:
    if not isinstance(result, dict):
        raise HttpError(
            "FerricStore HTTP endpoint command result is not an object",
            status_code=200,
            error_code="invalid_response",
            raw=result,
            retryable=False,
            safe_to_retry=False,
        )
    status = result.get("status")
    if status == "ok":
        value = result.get("value")
        if binary:
            try:
                value = _decode_binary_value(value)
            except (TypeError, ValueError) as exc:
                raise HttpError(
                    "FerricStore HTTP endpoint returned a malformed binary value",
                    status_code=200,
                    error_code="invalid_response",
                    raw=result,
                    retryable=False,
                    safe_to_retry=False,
                ) from exc
        return _native_value(value)
    error = result.get("error")
    details = error if isinstance(error, dict) else {}
    code_value = details.get("code")
    code = code_value if isinstance(code_value, str) else "upstream_error"
    message_value = details.get("message")
    message = message_value if isinstance(message_value, str) else code.replace("_", " ")
    retry_after_value = details.get("retry_after_ms")
    retry_after_ms = (
        retry_after_value if isinstance(retry_after_value, int) and retry_after_value >= 0 else None
    )
    retryable = details.get("retryable") is True
    safe_to_retry = details.get("safe_to_retry") is True
    if code in {"overload", "overloaded"}:
        raise OverloadedError(
            message,
            raw=result,
            retry_after_ms=retry_after_ms,
            reason=code,
            retryable=True,
            safe_to_retry=True,
        )
    if status == "forbidden" or code == "forbidden":
        raise HttpError(
            message,
            status_code=200,
            error_code=code,
            raw=result,
            retryable=False,
            safe_to_retry=False,
        )
    raise classify_server_error(
        message,
        raw=result,
        retryable=retryable,
        safe_to_retry=safe_to_retry,
        retry_after_ms=retry_after_ms,
    )


def _decode_binary_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_binary_value(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {_BYTES_TAG}:
            encoded = value[_BYTES_TAG]
            if not isinstance(encoded, str):
                raise TypeError("binary marker payload must be text")
            return base64.b64decode(encoded, validate=True)
        if set(value) == {_MAP_TAG}:
            pairs = value[_MAP_TAG]
            if not isinstance(pairs, list):
                raise TypeError("map marker payload must be a list")
            decoded: dict[Any, Any] = {}
            for pair in pairs:
                if not isinstance(pair, list) or len(pair) != 2:
                    raise TypeError("map marker entries must be key/value pairs")
                key = _decode_binary_value(pair[0])
                decoded[key] = _decode_binary_value(pair[1])
            return decoded
        return {key: _decode_binary_value(item) for key, item in value.items()}
    return value


__all__ = ["AsyncHttpAdapter", "HttpAdapter"]
