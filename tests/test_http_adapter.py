from __future__ import annotations

import asyncio
import base64
import json
import socket
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import msgpack
import pytest

import ferricstore.http_adapter as http_adapter_module
import ferricstore.http_transport as http_transport_module
from ferricstore import AsyncFlowClient, AsyncHttpAdapter, FlowClient, HttpAdapter
from ferricstore.errors import (
    FerricStoreError,
    FlowAlreadyExistsError,
    HttpError,
    InvalidCommandError,
    OverloadedError,
)
from ferricstore.http_connection_pool import _KeepAlivePool

Response = tuple[int, Any, dict[str, str]]


class _ProxyState:
    def __init__(self, responder: Callable[[dict[str, Any]], Response]) -> None:
        self.responder = responder
        self.requests: list[dict[str, Any]] = []
        self.headers: list[dict[str, str]] = []
        self.connection_ports: list[int] = []
        self.raw_request_sizes: list[int] = []
        self.connection_closed = threading.Event()


@contextmanager
def proxy_server(
    responder: Callable[[dict[str, Any]], Response],
) -> Iterator[tuple[str, _ProxyState]]:
    state = _ProxyState(responder)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            raw_request = self.rfile.read(length)
            content_type = self.headers.get("Content-Type", "")
            if content_type == "application/vnd.ferricstore.commands+msgpack":
                envelope = msgpack.unpackb(raw_request, raw=False, strict_map_key=False)
            else:
                envelope = json.loads(raw_request)
            state.requests.append(envelope)
            state.raw_request_sizes.append(len(raw_request))
            state.headers.append({name.lower(): value for name, value in self.headers.items()})
            state.connection_ports.append(self.client_address[1])
            status, body, response_headers = state.responder(envelope)
            response_headers = dict(response_headers)
            drop_response = response_headers.pop("X-Test-Drop-Response", None) is not None
            silent_close = response_headers.pop("X-Test-Silent-Close", None) is not None
            if drop_response:
                self.close_connection = True
                with suppress(OSError):
                    self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if isinstance(body, bytes):
                encoded = body
            elif content_type == "application/vnd.ferricstore.commands+msgpack":
                encoded = msgpack.packb(body, use_bin_type=True)
            else:
                encoded = json.dumps(body, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            for name, value in response_headers.items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(encoded)
            self.wfile.flush()
            if silent_close:
                self.close_connection = True

        def finish(self) -> None:
            try:
                super().finish()
            finally:
                state.connection_closed.set()

        def log_message(self, _format: str, *_args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def command_responder(envelope: dict[str, Any]) -> Response:
    binary = envelope.get("encoding") == "ferricstore-json-v1"
    compact = envelope.get("encoding") == "ferricstore-msgpack-v1"
    commands = _decode_wire(envelope["commands"]) if binary else envelope["commands"]
    results: list[dict[str, Any]] = []
    for command in commands:
        if command[0] == "SET":
            results.append({"status": "ok", "value": "OK"})
        elif command[0] == "GET":
            results.append({"status": "ok", "value": "value"})
        elif command[0] == "FLOW.GET":
            results.append(
                {
                    "status": "ok",
                    "value": {"id": "flow-1", "state": "queued", "attempt": 0},
                }
            )
        else:
            results.append({"status": "ok", "value": command})
    response: dict[str, Any] = {"results": results}
    if binary:
        response["encoding"] = "ferricstore-json-v1"
        for result in results:
            if result.get("status") == "ok":
                result["value"] = _encode_wire(result.get("value"))
    elif compact:
        response["encoding"] = "ferricstore-msgpack-v1"
    return 200, response, {}


def _decode_wire(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_wire(item) for item in value]
    if isinstance(value, dict) and set(value) == {"$ferricstore_bytes"}:
        return base64.b64decode(value["$ferricstore_bytes"], validate=True)
    if isinstance(value, dict) and set(value) == {"$ferricstore_map"}:
        return {_decode_wire(key): _decode_wire(item) for key, item in value["$ferricstore_map"]}
    if isinstance(value, dict):
        return {key: _decode_wire(item) for key, item in value.items()}
    return value


def _encode_wire(value: Any) -> Any:
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, bytes):
        return {"$ferricstore_bytes": base64.b64encode(value).decode("ascii")}
    if isinstance(value, list):
        return [_encode_wire(item) for item in value]
    if isinstance(value, dict):
        return {
            "$ferricstore_map": [
                [_encode_wire(key), _encode_wire(item)] for key, item in value.items()
            ]
        }
    return value


def test_flow_client_selects_http_transport_without_changing_commands() -> None:
    with proxy_server(command_responder) as (url, state):
        client = FlowClient.from_url(url, bearer_token="secret", max_connections=2)
        try:
            assert client.kv_set("key", "value") == b"OK"
            assert client.kv_get("key") == b"value"
            assert client.command("FLOW.GET", "flow-1") == {
                b"id": b"flow-1",
                b"state": b"queued",
                b"attempt": 0,
            }
        finally:
            client.close()

    assert state.requests[0]["encoding"] == "ferricstore-json-v1"
    assert _decode_wire(state.requests[0]["commands"]) == [["SET", "key", b"value"]]
    assert state.requests[1:] == [
        {"commands": [["GET", "key"]], "encoding": "ferricstore-json-v1"},
        {"commands": [["FLOW.GET", "flow-1"]], "encoding": "ferricstore-json-v1"},
    ]
    assert all(headers["authorization"] == "Bearer secret" for headers in state.headers)


@pytest.mark.parametrize("compact", [False, True])
def test_structured_flow_commands_reuse_native_payload_builders(compact: bool) -> None:
    adapter = HttpAdapter("http://127.0.0.1:1", compact=compact)
    command = (
        "FLOW.START_AND_CLAIM",
        "flow-1",
        "TYPE",
        "checkout",
        "INITIAL_STATE",
        "queued",
        "WORKER",
        "worker-1",
        "LEASE_MS",
        30_000,
        "NOW",
        100,
    )

    try:
        encoded = adapter._encode_command(command, 0)
    finally:
        adapter.close()

    assert _decode_wire(encoded) == {
        "command": "FLOW.START_AND_CLAIM",
        "opcode": 0x0223,
        "payload": {
            "id": "flow-1",
            "type": "checkout",
            "initial_state": "queued",
            "worker": "worker-1",
            "lease_ms": 30_000,
            "now_ms": 100,
        },
    }


@pytest.mark.parametrize("compact", [False, True])
def test_flow_value_mget_uses_its_existing_native_opcode_over_http(compact: bool) -> None:
    adapter = HttpAdapter("http://127.0.0.1:1", compact=compact)

    try:
        encoded = adapter._encode_command(
            ("FLOW.VALUE.MGET", "ref-a", "MAX_BYTES", "ref-b", "MAX_BYTES", 4_096),
            0,
        )
    finally:
        adapter.close()

    assert _decode_wire(encoded) == {
        "command": "FLOW.VALUE.MGET",
        "opcode": 0x020C,
        "payload": {
            "refs": ["ref-a", "MAX_BYTES", "ref-b"],
            "max_bytes": 4_096,
        },
    }


@pytest.mark.parametrize("compact", [False, True])
def test_flow_query_uses_its_validated_native_payload_over_http(compact: bool) -> None:
    adapter = HttpAdapter("http://127.0.0.1:1", compact=compact)
    query = (
        "FROM runs WHERE partition_key = @partition_key AND type = @type "
        "ORDER BY updated_at_ms ASC LIMIT 10 RETURN RECORDS"
    )

    try:
        encoded = adapter._encode_command(
            ("FLOW.QUERY", "FQL1", query, "partition_key", "tenant-a", "type", "workflow"),
            0,
        )
    finally:
        adapter.close()

    assert _decode_wire(encoded) == {
        "command": "FLOW.QUERY",
        "opcode": 0x0231,
        "payload": {
            "version": "FQL1",
            "query": query,
            "params": {"partition_key": "tenant-a", "type": "workflow"},
        },
    }


def test_http_keep_alive_reuses_one_connection_for_sequential_commands() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url, max_connections=2)
        try:
            assert adapter.execute_command("PING") == [b"PING"]
            assert adapter.execute_command("PING") == [b"PING"]
            assert adapter.execute_command("PING") == [b"PING"]
        finally:
            adapter.close()

    assert len(state.connection_ports) == 3
    assert len(set(state.connection_ports)) == 1
    assert all(headers.get("connection", "").lower() != "close" for headers in state.headers)


@pytest.mark.parametrize("http2", [False, True])
def test_http_keep_alive_preserves_standard_redirect_handling(http2: bool) -> None:
    methods: list[str] = []
    connection_ports: list[int] = []
    authorizations: list[str | None] = []

    class RedirectHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            methods.append("POST")
            connection_ports.append(self.client_address[1])
            authorizations.append(self.headers.get("Authorization"))
            self.send_response(302)
            self.send_header("Location", "/redirected")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            methods.append("GET")
            connection_ports.append(self.client_address[1])
            authorizations.append(self.headers.get("Authorization"))
            encoded = json.dumps(
                {"results": [{"status": "ok", "value": "redirected"}]},
                separators=(",", ":"),
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        adapter = HttpAdapter(
            f"http://{host}:{port}",
            bearer_token="secret",
            http2=http2,
        )
        try:
            assert adapter.execute_command("PING") == b"redirected"
        finally:
            adapter.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    assert methods == ["POST", "GET"]
    assert len(set(connection_ports)) == 1
    assert authorizations == ["Bearer secret", "Bearer secret"]


@pytest.mark.parametrize("http2", [False, True])
def test_http_redirects_preserve_authorization_across_origins(http2: bool) -> None:
    target_authorizations: list[str | None] = []

    class TargetHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            target_authorizations.append(self.headers.get("Authorization"))
            encoded = json.dumps(
                {"results": [{"status": "ok", "value": "redirected"}]},
                separators=(",", ":"),
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: Any) -> None:
            return None

    target = ThreadingHTTPServer(("127.0.0.1", 0), TargetHandler)
    target.daemon_threads = True
    target_thread = threading.Thread(target=target.serve_forever, daemon=True)
    target_thread.start()
    target_host, target_port = target.server_address

    class RedirectHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            self.rfile.read(length)
            self.send_response(302)
            self.send_header(
                "Location",
                f"http://{target_host}:{target_port}/redirected",
            )
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, _format: str, *_args: Any) -> None:
            return None

    redirect = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    redirect.daemon_threads = True
    redirect_thread = threading.Thread(target=redirect.serve_forever, daemon=True)
    redirect_thread.start()
    redirect_host, redirect_port = redirect.server_address

    try:
        adapter = HttpAdapter(
            f"http://{redirect_host}:{redirect_port}",
            bearer_token="secret",
            http2=http2,
        )
        try:
            assert adapter.execute_command("PING") == b"redirected"
        finally:
            adapter.close()
    finally:
        redirect.shutdown()
        redirect.server_close()
        redirect_thread.join()
        target.shutdown()
        target.server_close()
        target_thread.join()

    assert target_authorizations == ["Bearer secret"]


@pytest.mark.parametrize(
    ("status", "redirected_method"),
    [(301, "GET"), (302, "GET"), (303, "GET"), (307, "POST"), (308, "POST")],
)
def test_http1_direct_pool_preserves_redirect_method_semantics(
    status: int,
    redirected_method: str,
) -> None:
    methods: list[str] = []

    class RedirectHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            methods.append("POST")
            if self.path == "/redirected":
                self.respond()
            else:
                self.send_response(status)
                self.send_header("Location", "/redirected")
                self.send_header("Content-Length", "0")
                self.end_headers()

        def do_GET(self) -> None:
            methods.append("GET")
            self.respond()

        def respond(self) -> None:
            encoded = b'{"results":[{"status":"ok","value":"redirected"}]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        adapter = HttpAdapter(f"http://{host}:{port}", bearer_token="secret")
        try:
            assert adapter.execute_command("PING") == b"redirected"
        finally:
            adapter.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    assert methods == ["POST", redirected_method]


def test_http2_separates_physical_connections_from_concurrent_requests() -> None:
    adapter = HttpAdapter(
        "https://proxy.example.com",
        http2=True,
        max_connections=1,
        max_concurrent_requests=8,
    )
    try:
        assert adapter.max_connections == 1
        assert adapter.max_concurrent_requests == 8
        assert adapter._transport.http2_enabled is True

        acquired = [adapter._slots.acquire(blocking=False) for _index in range(8)]
        assert acquired == [True] * 8
        assert adapter._slots.acquire(blocking=False) is False
        for _index in range(8):
            adapter._slots.release()
    finally:
        adapter.close()


def test_http2_is_an_optional_packaging_extra() -> None:
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text()
    assert "http2 = [" in pyproject
    assert "httpx[http2]" in pyproject


def test_concurrent_single_commands_can_be_coalesced_into_one_ordered_batch() -> None:
    call_count = 12
    barrier = threading.Barrier(call_count)

    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(
            url,
            max_connections=2,
            coalesce_window_ms=30,
            coalesce_max_items=call_count,
        )

        def execute(index: int) -> Any:
            barrier.wait()
            return adapter.execute_command("ECHO", str(index))

        try:
            with ThreadPoolExecutor(max_workers=call_count) as executor:
                results = list(executor.map(execute, range(call_count)))
        finally:
            adapter.close()

    assert results == [[b"ECHO", str(index).encode()] for index in range(call_count)]
    assert len(state.requests) == 1
    assert len(state.requests[0]["commands"]) == call_count


def test_coalescing_isolates_per_command_errors() -> None:
    barrier = threading.Barrier(2)

    def responder(envelope: dict[str, Any]) -> Response:
        commands = _decode_wire(envelope["commands"])
        results = []
        for command in commands:
            if command[1] == "bad":
                results.append(
                    {
                        "status": "error",
                        "error": {"code": "upstream_error", "message": "ERR bad command"},
                    }
                )
            else:
                results.append({"status": "ok", "value": _encode_wire(command[1])})
        return 200, {"results": results, "encoding": "ferricstore-json-v1"}, {}

    with proxy_server(responder) as (url, state):
        adapter = HttpAdapter(url, coalesce_window_ms=30, coalesce_max_items=2)

        def execute(value: str) -> Any:
            barrier.wait()
            return adapter.execute_command("ECHO", value)

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                good = executor.submit(execute, "good")
                bad = executor.submit(execute, "bad")
                assert good.result() == b"good"
                with pytest.raises(FerricStoreError, match="bad command"):
                    bad.result()
        finally:
            adapter.close()

    assert len(state.requests) == 1
    assert len(state.requests[0]["commands"]) == 2


def test_coalescing_never_exceeds_the_configured_batch_limit() -> None:
    call_count = 6
    barrier = threading.Barrier(call_count)

    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(
            url,
            max_connections=3,
            coalesce_window_ms=30,
            coalesce_max_items=2,
        )

        def execute(index: int) -> Any:
            barrier.wait()
            return adapter.execute_command("ECHO", index)

        try:
            with ThreadPoolExecutor(max_workers=call_count) as executor:
                assert list(executor.map(execute, range(call_count))) == [
                    [b"ECHO", index] for index in range(call_count)
                ]
        finally:
            adapter.close()

    assert len(state.requests) == 3
    assert all(len(request["commands"]) == 2 for request in state.requests)


def test_coalescing_does_not_create_an_oversized_request() -> None:
    barrier = threading.Barrier(2)

    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(
            url,
            max_connections=2,
            max_request_bytes=220,
            coalesce_window_ms=30,
            coalesce_max_items=2,
        )

        def execute(value: bytes) -> Any:
            barrier.wait()
            return adapter.execute_command("ECHO", value)

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                values = [b"a" * 60, b"b" * 60]
                assert list(executor.map(execute, values)) == [[b"ECHO", value] for value in values]
        finally:
            adapter.close()

    assert len(state.requests) == 2
    assert all(len(request["commands"]) == 1 for request in state.requests)


def test_async_single_commands_use_the_same_coalescing_contract() -> None:
    async def run(url: str) -> list[Any]:
        adapter = AsyncHttpAdapter(
            url,
            max_connections=1,
            max_concurrent_requests=8,
            coalesce_window_ms=30,
            coalesce_max_items=8,
        )
        ready = asyncio.Event()

        async def execute(index: int) -> Any:
            await ready.wait()
            return await adapter.execute_command("ECHO", index)

        try:
            tasks = [asyncio.create_task(execute(index)) for index in range(8)]
            ready.set()
            return await asyncio.gather(*tasks)
        finally:
            await adapter.close()

    with proxy_server(command_responder) as (url, state):
        assert asyncio.run(run(url)) == [[b"ECHO", index] for index in range(8)]

    assert len(state.requests) == 1
    assert len(state.requests[0]["commands"]) == 8


@pytest.mark.parametrize("http2", [False, True])
def test_compact_envelope_round_trips_arbitrary_bytes_and_binary_map_keys(
    http2: bool,
) -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url, compact=True, http2=http2)
        try:
            result = adapter.execute_command(
                "ECHO",
                b"\x00\xff",
                {b"\xff-key": [b"\x80-value", 1, True, None]},
            )
        finally:
            adapter.close()

    assert result == [
        b"ECHO",
        b"\x00\xff",
        {b"\xff-key": [b"\x80-value", 1, True, None]},
    ]
    assert state.requests == [
        {
            "encoding": "ferricstore-msgpack-v1",
            "commands": [["ECHO", b"\x00\xff", {b"\xff-key": [b"\x80-value", 1, True, None]}]],
        }
    ]
    assert state.headers[0]["content-type"] == ("application/vnd.ferricstore.commands+msgpack")


def test_compact_binary_payload_is_smaller_than_the_json_binary_envelope() -> None:
    value = bytes(range(256)) * 4
    sizes = []

    for compact in (False, True):
        with proxy_server(command_responder) as (url, state):
            adapter = HttpAdapter(url, compact=compact)
            try:
                assert adapter.execute_command("ECHO", value) == [b"ECHO", value]
            finally:
                adapter.close()
        sizes.append(state.raw_request_sizes[0])

    json_size, compact_size = sizes
    assert compact_size < json_size * 0.8


def test_compact_is_an_optional_packaging_extra() -> None:
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text()
    assert "compact = [" in pyproject
    assert "msgpack>=" in pyproject


def test_compact_response_rejects_malformed_messagepack() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 200, b"\xc1", {}

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url, compact=True)
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("PING")

    assert exc_info.value.error_code == "invalid_response"
    assert exc_info.value.safe_to_retry is False


def test_compact_http_errors_keep_retry_metadata() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return (
            429,
            {"error": {"code": "overloaded", "message": "busy"}},
            {"Retry-After": "0.125"},
        )

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url, compact=True)
        with pytest.raises(OverloadedError) as exc_info:
            adapter.execute_command("PING")

    assert exc_info.value.retry_after_ms == 125
    assert exc_info.value.safe_to_retry is True


def test_http_keep_alive_pool_enforces_the_real_connection_limit() -> None:
    lock = threading.Lock()
    active = 0
    max_active = 0

    def responder(envelope: dict[str, Any]) -> Response:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        with lock:
            active -= 1
        return command_responder(envelope)

    with proxy_server(responder) as (url, state):
        adapter = HttpAdapter(url, max_connections=2)
        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(
                    executor.map(lambda _index: adapter.execute_command("PING"), range(4))
                )
        finally:
            adapter.close()

    assert results == [[b"PING"]] * 4
    assert max_active == 2
    assert len(set(state.connection_ports)) == 2


def test_http_timeout_includes_waiting_for_client_capacity() -> None:
    adapter = HttpAdapter(
        "http://127.0.0.1:1",
        timeout=0.05,
        max_connections=2,
        max_concurrent_requests=1,
    )
    assert adapter._slots.acquire()
    started = time.monotonic()
    try:
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("SET", "key", "value")
    finally:
        adapter._slots.release()
        adapter.close()

    assert time.monotonic() - started < 0.2
    assert exc_info.value.error_code == "transport_timeout"
    assert exc_info.value.safe_to_retry is True


def test_http_timeout_is_one_deadline_across_redirects() -> None:
    methods: list[str] = []

    class SlowRedirectHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            methods.append("POST")
            time.sleep(0.1)
            self.send_response(302)
            self.send_header("Location", "/redirected")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            methods.append("GET")
            time.sleep(0.1)
            encoded = b'{"results":[{"status":"ok","value":"late"}]}'
            with suppress(OSError):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), SlowRedirectHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    started = time.monotonic()
    try:
        host, port = server.server_address
        adapter = HttpAdapter(f"http://{host}:{port}", timeout=0.15)
        try:
            with pytest.raises(HttpError) as exc_info:
                adapter.execute_command("SET", "key", "value")
        finally:
            adapter.close()
        elapsed = time.monotonic() - started
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    assert elapsed < 0.35
    assert methods == ["POST", "GET"]
    assert exc_info.value.error_code == "transport_timeout"
    assert exc_info.value.safe_to_retry is False


def test_http_timeout_is_enforced_while_response_body_trickles() -> None:
    class SlowBodyHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            body = b'{"results":[{"status":"ok","value":"late"}]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            for byte in body:
                try:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                except OSError:
                    break
                time.sleep(0.01)

        def log_message(self, _format: str, *_args: Any) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), SlowBodyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    started = time.monotonic()
    try:
        host, port = server.server_address
        adapter = HttpAdapter(f"http://{host}:{port}", timeout=0.12)
        try:
            with pytest.raises(HttpError) as exc_info:
                adapter.execute_command("SET", "key", "value")
        finally:
            adapter.close()
        elapsed = time.monotonic() - started
    finally:
        server.shutdown()
        server.server_close()
        thread.join()

    assert elapsed < 0.35
    assert exc_info.value.error_code == "transport_timeout"
    assert exc_info.value.safe_to_retry is False


def test_http_keep_alive_discards_a_silently_closed_idle_connection() -> None:
    calls = 0

    def responder(envelope: dict[str, Any]) -> Response:
        nonlocal calls
        calls += 1
        headers = {"X-Test-Silent-Close": "1"} if calls == 1 else {}
        status, body, _headers = command_responder(envelope)
        return status, body, headers

    with proxy_server(responder) as (url, state):
        adapter = HttpAdapter(url)
        try:
            assert adapter.execute_command("PING") == [b"PING"]
            assert state.connection_closed.wait(1)
            assert adapter.execute_command("PING") == [b"PING"]
        finally:
            adapter.close()

    assert len(state.requests) == 2
    assert len(set(state.connection_ports)) == 2


def test_http_transport_does_not_replay_an_uncertain_post() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 200, {}, {"X-Test-Drop-Response": "1"}

    with proxy_server(responder) as (url, state):
        adapter = HttpAdapter(url)
        try:
            with pytest.raises(HttpError) as exc_info:
                adapter.execute_command("SET", "key", "value")
        finally:
            adapter.close()

    assert exc_info.value.error_code == "transport_error"
    assert exc_info.value.safe_to_retry is False
    assert state.requests == [
        {
            "commands": [["SET", "key", "value"]],
            "encoding": "ferricstore-json-v1",
        }
    ]


def test_http_adapter_close_closes_idle_keep_alive_connections() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url)
        assert adapter.execute_command("PING") == [b"PING"]
        adapter.close()
        assert state.connection_closed.wait(1)


def test_http_transport_builds_basic_auth_from_existing_username_password_options() -> None:
    transport = http_transport_module.JsonHttpTransport(
        "https://proxy.example.com",
        username="worker",
        password="secret:with:colons",
    )

    encoded = base64.b64encode(b"worker:secret:with:colons").decode()
    assert transport.headers["Authorization"] == f"Basic {encoded}"


def test_http_transport_uses_default_user_for_password_only_authentication() -> None:
    transport = http_transport_module.JsonHttpTransport(
        "https://proxy.example.com", password="secret"
    )

    encoded = base64.b64encode(b"default:secret").decode()
    assert transport.headers["Authorization"] == f"Basic {encoded}"


def test_http_pipeline_uses_one_proxy_batch_request() -> None:
    with proxy_server(command_responder) as (url, state):
        client = FlowClient.from_url(url)
        try:
            values = (
                client.pipeline().command("SET", "key", b"value").command("GET", "key").execute()
            )
        finally:
            client.close()

    assert values == [b"OK", b"value"]
    assert state.requests[0]["encoding"] == "ferricstore-json-v1"
    assert _decode_wire(state.requests[0]["commands"]) == [
        ["SET", "key", b"value"],
        ["GET", "key"],
    ]


def test_async_flow_client_uses_the_same_http_command_transport() -> None:
    async def run(url: str) -> None:
        client = AsyncFlowClient.from_url(url, max_connections=2)
        try:
            assert await client.kv_set("key", "value") == b"OK"
            assert await client.kv_get("key") == b"value"
            values = await client.pipeline().command("GET", "a").command("GET", "b").execute()
            assert values == [b"value", b"value"]
        finally:
            await client.close()

    with proxy_server(command_responder) as (url, state):
        asyncio.run(run(url))

    assert state.requests[-1] == {
        "commands": [["GET", "a"], ["GET", "b"]],
        "encoding": "ferricstore-json-v1",
    }
    assert len(set(state.connection_ports)) == 1


def test_async_http_uses_dedicated_bounded_workers_instead_of_default_executor() -> None:
    lock = threading.Lock()
    active = 0
    max_active = 0

    def responder(envelope: dict[str, Any]) -> Response:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.1)
        with lock:
            active -= 1
        return command_responder(envelope)

    async def run(url: str) -> None:
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        adapter = AsyncHttpAdapter(url, max_connections=3)
        try:
            results = await asyncio.gather(
                adapter.execute_command("PING"),
                adapter.execute_command("PING"),
                adapter.execute_command("PING"),
            )
            assert results == [[b"PING"]] * 3
        finally:
            await adapter.close()

    with proxy_server(responder) as (url, state):
        asyncio.run(run(url))

    assert max_active == 3
    assert len(set(state.connection_ports)) == 3


def test_async_http_timeout_includes_waiting_for_async_capacity() -> None:
    async def run() -> None:
        adapter = AsyncHttpAdapter("http://127.0.0.1:1", timeout=0.05)
        await adapter._slots.acquire()
        started = time.monotonic()
        try:
            with pytest.raises(HttpError) as exc_info:
                await adapter.execute_command("SET", "key", "value")
        finally:
            adapter._slots.release()
            await adapter.close()

        assert time.monotonic() - started < 0.2
        assert exc_info.value.error_code == "transport_timeout"
        assert exc_info.value.safe_to_retry is True

    asyncio.run(run())


def test_http_command_errors_keep_sdk_domain_classification() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return (
            200,
            {
                "results": [
                    {
                        "status": "error",
                        "error": {
                            "code": "upstream_error",
                            "message": "ERR flow already exists",
                        },
                    }
                ]
            },
            {},
        )

    with proxy_server(responder) as (url, _state):
        client = FlowClient.from_url(url)
        try:
            with pytest.raises(FlowAlreadyExistsError):
                client.command("FLOW.CREATE", "flow-1")
        finally:
            client.close()


def test_http_proxy_forbidden_result_is_an_http_error() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return (
            200,
            {
                "results": [
                    {
                        "status": "forbidden",
                        "error": {"code": "forbidden", "message": "not authorized"},
                    }
                ]
            },
            {},
        )

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url)
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("GET", "secret")

    assert exc_info.value.status_code == 200
    assert exc_info.value.error_code == "forbidden"


def test_http_request_level_overload_preserves_retry_after() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 503, {"error": {"code": "overload", "message": "busy"}}, {"Retry-After": "2"}

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url)
        with pytest.raises(OverloadedError) as exc_info:
            adapter.execute_command("GET", "key")

    assert exc_info.value.retry_after_ms == 2_000
    assert exc_info.value.safe_to_retry is True


def test_http_request_level_error_preserves_status_and_proxy_code() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 401, {"error": {"code": "unauthorized", "message": "bad token"}}, {}

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url)
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("PING")

    assert str(exc_info.value) == "bad token"
    assert exc_info.value.status_code == 401
    assert exc_info.value.error_code == "unauthorized"
    assert exc_info.value.retryable is False
    assert exc_info.value.safe_to_retry is False


@pytest.mark.parametrize("body", [b"not-json", ["not", "an", "object"]])
def test_http_adapter_rejects_invalid_response_envelopes(body: Any) -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 200, body, {}

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url)
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("PING")

    assert exc_info.value.error_code == "invalid_response"
    assert exc_info.value.safe_to_retry is False


@pytest.mark.parametrize(
    "results",
    [
        None,
        [],
        [{"status": "ok", "value": "PONG"}, {"status": "ok", "value": "extra"}],
        ["not-an-object"],
    ],
)
def test_http_adapter_rejects_malformed_command_results(results: Any) -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 200, {"results": results}, {}

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url)
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("PING")

    assert exc_info.value.error_code == "invalid_response"


def test_http_transport_round_trips_non_utf8_command_bytes() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url)
        value = adapter.execute_command("ECHO", b"\x00\xff")

    assert value == [b"ECHO", b"\x00\xff"]
    assert state.requests[0]["encoding"] == "ferricstore-json-v1"
    assert _decode_wire(state.requests[0]["commands"]) == [["ECHO", b"\x00\xff"]]


def test_http_transport_encodes_nested_json_command_values() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url)
        value = adapter.execute_command(
            "ECHO",
            {b"key": [b"value", 1, True, None, 1.5]},
        )

    assert state.requests[0]["encoding"] == "ferricstore-json-v1"
    assert _decode_wire(state.requests[0]["commands"]) == [
        ["ECHO", {b"key": [b"value", 1, True, None, 1.5]}]
    ]
    assert value == [b"ECHO", {b"key": [b"value", 1, True, None, 1.5]}]


@pytest.mark.parametrize(
    ("value", "error_type", "message"),
    [
        (float("nan"), ValueError, "finite"),
        (object(), TypeError, "not JSON-compatible"),
    ],
)
def test_http_transport_rejects_non_json_command_values(
    value: Any,
    error_type: type[Exception],
    message: str,
) -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url)
        with pytest.raises(error_type, match=message):
            adapter.execute_command("SET", "key", value)

    assert state.requests == []


def test_http_adapter_enforces_batch_and_response_limits() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 200, {"results": [{"status": "ok", "value": "x" * 1_000}]}, {}

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url, max_batch_items=1, max_response_bytes=100)
        with pytest.raises(FerricStoreError, match="max_batch_items"):
            adapter.execute_batch([("PING",), ("PING",)])
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("PING")

    assert exc_info.value.error_code == "response_too_large"


def test_http_adapter_enforces_request_limit_before_network_io() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url, max_request_bytes=64)
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("SET", "key", "x" * 100)

    assert exc_info.value.error_code == "request_too_large"
    assert state.requests == []


def test_http_adapter_rejects_connection_affine_commands() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url)
        with pytest.raises(FerricStoreError, match="connection-affine native transport"):
            adapter.execute_command("SUBSCRIBE", "events")

    assert state.requests == []


@pytest.mark.parametrize("command", ["AUTH", "MULTI", "WATCH", "PSUBSCRIBE"])
def test_http_adapter_rejects_every_connection_affine_command_family(command: str) -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url)
        with pytest.raises(InvalidCommandError):
            adapter.execute_command(command)

    assert state.requests == []


def test_http_adapter_lifecycle_and_url_validation() -> None:
    with pytest.raises(ValueError, match="http:// or https://"):
        HttpAdapter("ferric://127.0.0.1:6388")
    with pytest.raises(ValueError, match="explicit options"):
        HttpAdapter("https://user:secret@example.com")

    with proxy_server(command_responder) as (url, _state):
        adapter = HttpAdapter(url)
        adapter.close()
        with pytest.raises(FerricStoreError, match="closed"):
            adapter.execute_command("PING")


def test_http_adapter_validates_security_and_resource_options() -> None:
    with pytest.raises(ValueError, match="query or fragment"):
        HttpAdapter("https://proxy.example.com?token=secret")
    with pytest.raises(ValueError, match="mutually exclusive"):
        HttpAdapter(
            "https://proxy.example.com",
            bearer_token="secret",
            headers={"authorization": "Bearer other"},
        )
    with pytest.raises(ValueError, match="https://"):
        HttpAdapter("http://proxy.example.com", username="worker", password="secret")
    with pytest.raises(ValueError, match="requires password"):
        HttpAdapter("https://proxy.example.com", username="worker")
    with pytest.raises(ValueError, match="mutually exclusive"):
        HttpAdapter(
            "https://proxy.example.com",
            bearer_token="token",
            username="worker",
            password="secret",
        )
    with pytest.raises(ValueError, match="cannot contain ':'"):
        HttpAdapter("https://proxy.example.com", username="bad:user", password="secret")
    with pytest.raises(ValueError, match="newlines"):
        HttpAdapter("https://proxy.example.com", headers={"x-test": "unsafe\nvalue"})
    with pytest.raises(ValueError, match="timeout"):
        HttpAdapter("https://proxy.example.com", timeout=0)
    with pytest.raises(ValueError, match="max_connections"):
        HttpAdapter("https://proxy.example.com", max_connections=0)


def test_http_adapter_empty_batch_is_a_noop() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url)
        assert adapter.execute_batch([]) == []

    assert state.requests == []


def test_async_http_adapter_lifecycle_matches_sync_adapter() -> None:
    async def run(url: str) -> None:
        adapter = AsyncHttpAdapter(url)
        await adapter.close()
        with pytest.raises(FerricStoreError, match="closed"):
            await adapter.execute_command("PING")

    with proxy_server(command_responder) as (url, state):
        asyncio.run(run(url))

    assert state.requests == []


def test_http_adapters_are_publicly_constructible() -> None:
    assert HttpAdapter.__name__ == "HttpAdapter"
    assert AsyncHttpAdapter.__name__ == "AsyncHttpAdapter"


def test_custom_urllib_opener_keeps_pooling_and_http_error_contracts() -> None:
    calls = 0

    def responder(envelope: dict[str, Any]) -> Response:
        nonlocal calls
        calls += 1
        if calls == 2:
            return 503, {"error": {"code": "overload", "message": "busy"}}, {}
        return command_responder(envelope)

    pool = _KeepAlivePool(1)
    opener = http_transport_module.build_opener(http_transport_module._PooledHTTPHandler(pool))
    try:
        with proxy_server(responder) as (url, state):
            transport = http_transport_module.JsonHttpTransport(url, _opener=opener)
            status, payload = transport.request_json(
                "POST",
                "/v1/commands",
                body={"commands": [["PING"]]},
            )
            assert status == 200
            assert payload["results"][0]["status"] == "ok"
            with pytest.raises(OverloadedError):
                transport.request_json(
                    "POST",
                    "/v1/commands",
                    body={"commands": [["PING"]]},
                )
    finally:
        pool.close()

    assert calls == 2
    assert len(set(state.connection_ports)) == 1


def test_transport_helpers_enforce_bounded_http2_responses() -> None:
    class Response:
        def __init__(self, headers: dict[str, str], chunks: list[bytes]) -> None:
            self.headers = headers
            self._chunks = chunks

        def iter_bytes(self) -> Iterator[bytes]:
            yield from self._chunks

    assert (
        http_transport_module._read_http2_bounded(
            Response({"Content-Length": "invalid"}, [b"ab", b"cd"]),
            4,
        )
        == b"abcd"
    )

    with pytest.raises(HttpError, match="max_response_bytes"):
        http_transport_module._read_http2_bounded(
            Response({"Content-Length": "5"}, []),
            4,
        )
    with pytest.raises(HttpError, match="max_response_bytes"):
        http_transport_module._read_http2_bounded(Response({}, [b"abc", b"de"]), 4)


def test_http_deadline_and_transport_error_classification() -> None:
    assert http_transport_module._HttpDeadline(None).remaining() is None
    expired = http_transport_module._HttpDeadline(1)
    expired._expires_at = 0
    with pytest.raises(TimeoutError, match="deadline exceeded"):
        expired.remaining()

    timeout = http_transport_module._http_transport_error("GET", TimeoutError("late"))
    assert timeout.error_code == "transport_timeout"
    assert timeout.safe_to_retry is True
    failure = http_transport_module._http_transport_error("POST", OSError("closed"))
    assert failure.error_code == "transport_error"
    assert failure.safe_to_retry is False


def test_http2_backend_classifies_optional_dependency_and_client_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = http_transport_module.importlib.import_module

    def missing_httpx(name: str) -> Any:
        if name == "httpx":
            raise ImportError("missing")
        return original_import(name)

    monkeypatch.setattr(http_transport_module.importlib, "import_module", missing_httpx)
    with pytest.raises(ImportError, match="optional dependency"):
        http_transport_module._Http2Backend(max_connections=1, ssl_context=None)

    class FakeTimeout(Exception):
        pass

    class FakeHttpError(Exception):
        pass

    class FakeHttpx:
        TimeoutException = FakeTimeout
        HTTPError = FakeHttpError

    class Client:
        def __init__(self, error: Exception) -> None:
            self.error = error

        def build_request(self, *_args: Any, **_kwargs: Any) -> object:
            return object()

        def send(self, _request: object, *, stream: bool) -> Any:
            assert stream is True
            raise self.error

    for error, timed_out in ((FakeTimeout("late"), True), (FakeHttpError("closed"), False)):
        backend = http_transport_module._Http2Backend.__new__(http_transport_module._Http2Backend)
        backend._httpx = FakeHttpx
        backend._client = Client(error)
        with pytest.raises(http_transport_module._Http2BackendError) as exc_info:
            backend.request(
                "POST",
                "https://example.com/v1/commands",
                headers={},
                data=b"{}",
                deadline=http_transport_module._HttpDeadline(1),
                max_response_bytes=100,
            )
        assert exc_info.value.timed_out is timed_out


def test_transport_direct_api_edges(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = http_transport_module.JsonHttpTransport("http://127.0.0.1:1")
    try:
        with pytest.raises(RuntimeError, match="MessagePack transport was not enabled"):
            transport.request_messagepack("POST", "/v1/commands", body={})
        with pytest.raises(RuntimeError, match="MessagePack transport was not enabled"):
            transport.messagepack_size({})
        with pytest.raises(ValueError, match="JSON-compatible"):
            transport.request_json("POST", "/v1/commands", body={"bad": object()})

        monkeypatch.setattr(
            transport,
            "_request_raw",
            lambda *_args, **_kwargs: (200, {}, b"binary"),
        )
        assert transport.request_bytes("/artifact", headers={"X-Test": "yes"}) == b"binary"
        monkeypatch.setattr(
            transport,
            "_request_raw",
            lambda *_args, **_kwargs: (404, {}, b"not-json"),
        )
        with pytest.raises(HttpError) as exc_info:
            transport.request_bytes("/missing")
        assert exc_info.value.status_code == 404
    finally:
        transport.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"http2": "yes"},
        {"messagepack": "yes"},
    ],
)
def test_transport_rejects_non_boolean_protocol_flags(kwargs: dict[str, Any]) -> None:
    with pytest.raises(TypeError, match="must be a boolean"):
        http_transport_module.JsonHttpTransport("https://example.com", **kwargs)


def test_transport_rejects_http2_with_a_custom_opener() -> None:
    opener = http_transport_module.build_opener()
    with pytest.raises(ValueError, match="custom urllib opener"):
        http_transport_module.JsonHttpTransport("https://example.com", http2=True, _opener=opener)


def test_error_response_fallbacks_and_retry_after_validation() -> None:
    fallback = http_transport_module._decode_error_object(b"not-json", status_code=502)
    assert fallback["error"]["code"] == "http_error"
    assert fallback["raw_body"] == "not-json"

    overloaded = http_transport_module._response_error(
        "POST",
        500,
        {"error": {"code": "overloaded"}, "retry_after_ms": 25},
        retry_after_ms=None,
    )
    assert isinstance(overloaded, OverloadedError)
    assert overloaded.retry_after_ms == 25
    assert http_transport_module._retry_after_ms({"Retry-After": "invalid"}) is None
    assert http_transport_module._retry_after_ms({"Retry-After": "-1"}) is None
    assert http_transport_module._retry_after_ms(None) is None


@pytest.mark.parametrize("flag", ["http2", "compact"])
def test_http_adapter_rejects_non_boolean_protocol_flags(flag: str) -> None:
    with pytest.raises(TypeError, match="must be a boolean"):
        HttpAdapter("https://example.com", **{flag: "yes"})


def test_http_adapter_rejects_invalid_coalescing_limits() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        HttpAdapter(
            "https://example.com",
            max_batch_items=1,
            coalesce_max_items=2,
        )
    with pytest.raises(ValueError, match="non-negative"):
        HttpAdapter("https://example.com", coalesce_window_ms=-1)


def test_http_adapter_rejects_unknown_response_encoding() -> None:
    def responder(_envelope: dict[str, Any]) -> Response:
        return 200, {"encoding": "future-v2", "results": [{"status": "ok"}]}, {}

    with proxy_server(responder) as (url, _state):
        adapter = HttpAdapter(url)
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("PING")

    assert exc_info.value.error_code == "invalid_response"


def test_http_adapter_ordered_lifecycle_and_unbounded_slot_paths() -> None:
    with proxy_server(command_responder) as (url, state):
        adapter = HttpAdapter(url, timeout=None, max_concurrent_requests=1)
        assert adapter._acquire_slot(http_transport_module._HttpDeadline(None)) is True
        adapter._slots.release()
        assert adapter.execute_batch_ordered([("PING",)]) == [[b"PING"]]
        assert adapter.invalidate() is None
        adapter.close()
        adapter.close()

    assert len(state.requests) == 1


def test_coalesced_transport_failure_is_shared_without_replaying_post() -> None:
    adapter = HttpAdapter(
        "http://127.0.0.1:1",
        timeout=0.2,
        coalesce_window_ms=1,
        coalesce_max_items=2,
    )
    try:
        with pytest.raises(HttpError) as exc_info:
            adapter.execute_command("SET", "key", "value")
    finally:
        adapter.close()

    assert exc_info.value.error_code == "transport_error"
    assert exc_info.value.safe_to_retry is False


def test_compact_command_value_and_name_edge_contracts() -> None:
    assert http_adapter_module._compact_value(bytearray(b"value")) == b"value"
    with pytest.raises(ValueError, match="finite"):
        http_adapter_module._compact_value(float("nan"))
    with pytest.raises(TypeError, match="hashable"):
        http_adapter_module._compact_value({("key",): "value", "bad": {}} | {("x",): []})
    with pytest.raises(TypeError, match="MessagePack-compatible"):
        http_adapter_module._compact_value(object())

    assert http_adapter_module._command_name(b"PING", 0) == "PING"
    with pytest.raises(TypeError, match="UTF-8"):
        http_adapter_module._command_name(b"\xff", 0)
    with pytest.raises(TypeError, match="must be text"):
        http_adapter_module._command_name(b"", 0)
    with pytest.raises(TypeError, match="sequence"):
        http_adapter_module._compact_command("PING", 0)
    with pytest.raises(ValueError, match="cannot be empty"):
        http_adapter_module._compact_command([], 0)
    with pytest.raises(InvalidCommandError):
        http_adapter_module._compact_command(["AUTH"], 0)


@pytest.mark.parametrize(
    "value",
    [
        {"$ferricstore_bytes": 123},
        {"$ferricstore_map": "bad"},
        {"$ferricstore_map": [["missing-value"]]},
    ],
)
def test_malformed_binary_result_markers_are_rejected(value: Any) -> None:
    with pytest.raises(HttpError) as exc_info:
        http_adapter_module._command_result(
            {"status": "ok", "value": value},
            binary=True,
        )
    assert exc_info.value.error_code == "invalid_response"


def test_command_result_overload_preserves_command_retry_metadata() -> None:
    with pytest.raises(OverloadedError) as exc_info:
        http_adapter_module._command_result(
            {
                "status": "error",
                "error": {
                    "code": "overload",
                    "message": "busy",
                    "retry_after_ms": 10,
                },
            }
        )
    assert exc_info.value.retry_after_ms == 10


def test_command_result_preserves_structured_query_diagnostic_and_retry_contract() -> None:
    details = {
        "code": "query_projection_changed",
        "message": "ERR Flow visibility projection changed during the query",
        "retryable": True,
        "safe_to_retry": True,
        "retry_after_ms": 0,
    }

    result = {"status": "error", "error": details}

    with pytest.raises(FerricStoreError) as exc_info:
        http_adapter_module._command_result(
            result,
        )

    assert exc_info.value.raw == result
    assert exc_info.value.retryable is True
    assert exc_info.value.safe_to_retry is True
    assert exc_info.value.retry_after_ms == 0


def test_async_http_adapter_empty_batch_invalidation_and_idempotent_close() -> None:
    async def run() -> None:
        adapter = AsyncHttpAdapter("http://127.0.0.1:1", timeout=None)
        assert await adapter.execute_batch([]) == []
        await adapter.invalidate()
        await adapter._acquire_slot(http_transport_module._HttpDeadline(None))
        adapter._slots.release()
        await adapter.close()
        await adapter.close()

    asyncio.run(run())
