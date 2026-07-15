from __future__ import annotations

import asyncio
import gc
import random
import socket
import struct
import subprocess
import sys
import threading
import time
import tracemalloc
import zlib
from concurrent.futures import Future
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pytest

import ferricstore.flow_options as flow_options_module
import ferricstore.protocol as protocol_module
import ferricstore.protocol_sync_batch as protocol_sync_batch_module
from ferricstore.async_client import AsyncFlowClient
from ferricstore.client import FlowClient
from ferricstore.errors import FerricStoreError, InvalidCommandError
from ferricstore.flow_routing import flow_command_route_keys
from ferricstore.lifecycle_core import RetryableResourceSet
from ferricstore.protocol import (
    AsyncProtocolAdapter,
    AsyncProtocolAdapterPool,
    ProtocolAdapter,
    ProtocolAdapterPool,
    ProtocolCommand,
    ProtocolResponse,
    RoutingTopology,
    TopologyProtocolAdapterPool,
    build_protocol_command,
)
from ferricstore.protocol_codec import encode_value
from ferricstore.protocol_common import _endpoint_adapter_is_idle, _endpoint_from_url
from ferricstore.protocol_framing import decompress_response, send_frames
from ferricstore.topology_lifecycle import EndpointAdapterLifecycle, SyncSingleFlight


def _single_shard_topology(
    host: str = "leader.local",
    port: int = 6391,
    *,
    route_epoch: int = 1,
) -> dict[str, Any]:
    return {
        "route_epoch": route_epoch,
        "shard_count": 1,
        "ranges": [
            {
                "first_slot": 0,
                "last_slot": 1023,
                "shard": 0,
                "lane_id": 1,
                "endpoint": {
                    "node": f"{host}@cluster",
                    "host": host,
                    "native_port": port,
                },
            }
        ],
    }


def _routing_topology(epoch: int, host: str) -> RoutingTopology:
    payload = _single_shard_topology(host)
    payload["route_epoch"] = epoch
    return RoutingTopology.build(payload)


def _bare_topology_pool(pool_type: type[Any]) -> Any:
    pool = object.__new__(pool_type)
    pool._seed_endpoint_keys = set()
    pool._tls = False
    pool._endpoint_lifecycle = EndpointAdapterLifecycle(is_idle=_endpoint_adapter_is_idle)
    pool._cleanup_adapters = RetryableResourceSet(())
    pool._topology_generation = 0
    pool.topology = RoutingTopology.empty()
    return pool


@pytest.mark.parametrize(
    "option",
    ["timeout", "heartbeat_interval", "heartbeat_timeout"],
)
@pytest.mark.parametrize(
    "invalid",
    [
        float("nan"),
        float("inf"),
        float("-inf"),
        -1.0,
        True,
        threading.TIMEOUT_MAX + 1.0,
    ],
)
def test_protocol_timing_rejects_unsafe_wait_values(
    monkeypatch: pytest.MonkeyPatch,
    option: str,
    invalid: float,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_ensure_connected", lambda self: None)

    with pytest.raises(ValueError, match=option):
        ProtocolAdapter(**{option: invalid})
    with pytest.raises(ValueError, match=option):
        AsyncProtocolAdapter(**{option: invalid})


@pytest.mark.parametrize("url", ["ferric://store.example:0", "ferrics://store.example:0"])
def test_protocol_urls_reject_explicit_zero_port(url: str) -> None:
    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        ProtocolAdapter.from_url(url)
    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        AsyncProtocolAdapter.from_url(url)
    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        _endpoint_from_url(url)


@pytest.mark.parametrize(
    "pool_type",
    [TopologyProtocolAdapterPool, protocol_module.AsyncTopologyProtocolAdapterPool],
)
def test_topology_install_rejects_stale_route_epoch(pool_type: type[Any]) -> None:
    pool = _bare_topology_pool(pool_type)
    pool._install_topology(_routing_topology(20, "new-leader.example"))
    generation = pool._topology_generation

    with pytest.raises(FerricStoreError, match=r"stale.*route_epoch"):
        pool._install_topology(_routing_topology(19, "old-leader.example"))

    assert pool.topology.route_epoch == 20
    assert pool.topology.slots[0]["endpoint"]["host"] == "new-leader.example"
    assert pool._topology_generation == generation


@pytest.mark.parametrize(
    "pool_type",
    [TopologyProtocolAdapterPool, protocol_module.AsyncTopologyProtocolAdapterPool],
)
def test_topology_install_rejects_conflicting_same_epoch(pool_type: type[Any]) -> None:
    pool = _bare_topology_pool(pool_type)
    topology = _routing_topology(20, "leader-a.example")
    pool._install_topology(topology)
    generation = pool._topology_generation

    assert pool._install_topology(topology) == []
    with pytest.raises(FerricStoreError, match=r"conflicting.*route_epoch"):
        pool._install_topology(_routing_topology(20, "leader-b.example"))

    assert pool.topology == topology
    assert pool._topology_generation == generation


def test_tls_topology_keeps_distinct_physical_destinations() -> None:
    topology = RoutingTopology.build(
        {
            "route_epoch": 1,
            "shard_count": 2,
            "ranges": [
                {
                    "first_slot": 0,
                    "last_slot": 511,
                    "shard": 0,
                    "lane_id": 1,
                    "endpoint": {
                        "node": "node-a",
                        "host": "shared.example",
                        "native_port": 6388,
                        "native_tls_port": 6389,
                    },
                },
                {
                    "first_slot": 512,
                    "last_slot": 1023,
                    "shard": 1,
                    "lane_id": 1,
                    "endpoint": {
                        "node": "node-b",
                        "host": "shared.example",
                        "native_port": 6388,
                        "native_tls_port": 6390,
                    },
                },
            ],
        }
    )

    assert len(topology.endpoints) == 2
    assert len(topology.route_destinations) == 2
    assert {route["endpoint"]["native_tls_port"] for route in topology.route_destinations} == {
        6389,
        6390,
    }


def test_shared_protocol_adapter_requires_transaction_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # This test characterizes session ownership only; it must not depend on a
    # FerricStore process happening to listen on the default local port.
    monkeypatch.setattr(ProtocolAdapter, "_ensure_connected", lambda _self: None)
    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    client = FlowClient(adapter)
    try:
        with pytest.raises(InvalidCommandError, match=r"client\.transaction"):
            client.multi()

        session = adapter.acquire_session()
        try:
            assert adapter.requires_explicit_session is True
            assert session.requires_explicit_session is False
        finally:
            session.close()
    finally:
        client.close()


def test_shared_async_protocol_adapter_requires_transaction_session() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(timeout=None, heartbeat_interval=None)
        client = AsyncFlowClient(adapter)
        try:
            with pytest.raises(InvalidCommandError, match=r"client\.transaction"):
                await client.multi()

            session = await adapter.acquire_session()
            try:
                assert adapter.requires_explicit_session is True
                assert session.requires_explicit_session is False
            finally:
                await session.close()
        finally:
            await client.close()

    asyncio.run(run())


def test_flow_partitions_negative_count_is_rejected_without_hanging() -> None:
    script = """
from ferricstore.errors import InvalidCommandError
from ferricstore.protocol import build_protocol_command
try:
    build_protocol_command('FLOW.CLAIM_DUE', 'jobs', 'PARTITIONS', -2)
except InvalidCommandError:
    raise SystemExit(0)
raise SystemExit(1)
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=str(Path(__file__).resolve().parents[1]),
        timeout=2,
        check=False,
    )
    assert completed.returncode == 0


@pytest.mark.parametrize("count", [True, 5])
def test_flow_partitions_requires_exact_nonnegative_count(count: Any) -> None:
    with pytest.raises(InvalidCommandError, match="PARTITIONS"):
        build_protocol_command("FLOW.CLAIM_DUE", "jobs", "PARTITIONS", count, "only-one")


def test_flow_items_ext_requires_declared_item_and_nested_counts() -> None:
    one_item = ("flow-1", "-", b"payload", 0, 0)
    with pytest.raises(InvalidCommandError, match="ITEMS_EXT"):
        build_protocol_command("FLOW.CREATE_MANY", "AUTO", "ITEMS_EXT", 2, *one_item)

    with pytest.raises(InvalidCommandError, match="ITEMS_EXT"):
        build_protocol_command(
            "FLOW.CREATE_MANY",
            "AUTO",
            "ITEMS_EXT",
            1,
            "flow-1",
            "-",
            b"payload",
            -1,
            0,
        )


def test_flow_option_routing_tokenization_is_linear(monkeypatch: Any) -> None:
    token_calls = 0
    original_token = flow_options_module._token

    def counted_token(value: Any) -> str:
        nonlocal token_calls
        token_calls += 1
        return original_token(value)

    monkeypatch.setattr(flow_options_module, "_token", counted_token)
    args = ("state",) + (b"PAYLOAD",) * 512

    assert flow_command_route_keys("FLOW.SEARCH", args) == ()
    assert token_calls <= len(args) * 4


def _effective_url_credentials(url: str, kwargs: dict[str, Any]) -> tuple[str | None, str | None]:
    parsed = urlparse(url)
    username = kwargs.get("username")
    password = kwargs.get("password")
    if "username" not in kwargs and parsed.username is not None:
        username = unquote(parsed.username)
    if "password" not in kwargs and parsed.password is not None:
        password = unquote(parsed.password)
    return username, password


def test_topology_scopes_url_credentials_to_each_seed_and_not_learned_endpoints(
    monkeypatch: Any,
) -> None:
    captured: dict[str, tuple[str | None, str | None]] = {}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology("seed-a.example", 6388)
            return b"OK"

        def close(self) -> None:
            pass

    def from_url(url: str, **kwargs: Any) -> FakeAdapter:
        captured[url] = _effective_url_credentials(url, kwargs)
        return FakeAdapter(url)

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    seeds = [
        "ferric://alice:first@seed-a.example:6388",
        "ferric://bob:second@seed-b.example:6388",
    ]
    pool = TopologyProtocolAdapterPool(seeds, endpoint_policy="any")
    try:
        pool._adapter_for_endpoint({"host": "seed-b.example", "native_port": 6388})
        pool._adapter_for_endpoint({"host": "learned.example", "native_port": 6388})
    finally:
        pool.close()

    assert captured == {
        seeds[0]: ("alice", "first"),
        seeds[1]: ("bob", "second"),
        "ferric://learned.example:6388": (None, None),
    }


def test_async_topology_scopes_url_credentials_to_each_seed_and_not_learned_endpoints(
    monkeypatch: Any,
) -> None:
    captured: dict[str, tuple[str | None, str | None]] = {}

    class FakeAdapter:
        def add_event_listener(self, _listener: Any) -> None:
            pass

        def register_flow_wake_subscription(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        async def close(self) -> None:
            pass

    def from_url(url: str, **kwargs: Any) -> FakeAdapter:
        captured[url] = _effective_url_credentials(url, kwargs)
        return FakeAdapter()

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", from_url)
    seeds = [
        "ferric://alice:first@seed-a.example:6388",
        "ferric://bob:second@seed-b.example:6388",
    ]

    async def run() -> None:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(seeds, endpoint_policy="any")
        pool._adapter_for_url(seeds[0])
        pool._adapter_for_endpoint({"host": "seed-b.example", "native_port": 6388})
        pool._adapter_for_endpoint({"host": "learned.example", "native_port": 6388})
        await pool.close()

    asyncio.run(run())
    assert captured == {
        seeds[0]: ("alice", "first"),
        seeds[1]: ("bob", "second"),
        "ferric://learned.example:6388": (None, None),
    }


@pytest.mark.parametrize("async_pool", [False, True])
def test_topology_rejects_mixed_plaintext_and_tls_seeds_before_connecting(
    monkeypatch: Any,
    async_pool: bool,
) -> None:
    def unexpected_adapter(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("invalid topology configuration attempted to create an adapter")

    monkeypatch.setattr(ProtocolAdapter, "from_url", unexpected_adapter)
    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", unexpected_adapter)
    pool_type = (
        protocol_module.AsyncTopologyProtocolAdapterPool
        if async_pool
        else TopologyProtocolAdapterPool
    )

    with pytest.raises(ValueError, match="cannot mix ferric:// and ferrics://"):
        pool_type(
            [
                "ferric://127.0.0.1:6388",
                "ferrics://127.0.0.1:6389",
            ]
        )


@pytest.mark.parametrize("async_pool", [False, True])
def test_topology_rejects_invalid_endpoint_policy_before_connecting(
    monkeypatch: Any,
    async_pool: bool,
) -> None:
    def unexpected_adapter(*_args: Any, **_kwargs: Any) -> Any:
        pytest.fail("invalid topology configuration attempted to create an adapter")

    monkeypatch.setattr(ProtocolAdapter, "from_url", unexpected_adapter)
    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", unexpected_adapter)
    pool_type = (
        protocol_module.AsyncTopologyProtocolAdapterPool
        if async_pool
        else TopologyProtocolAdapterPool
    )

    with pytest.raises(ValueError, match="invalid endpoint_policy"):
        pool_type(["ferric://127.0.0.1:6388"], endpoint_policy="typo")


@pytest.mark.parametrize("async_pool", [False, True])
def test_topology_endpoint_policy_none_rejects_every_non_seed_endpoint(
    monkeypatch: Any,
    async_pool: bool,
) -> None:
    endpoint = {"host": "attacker.example", "native_port": 6388}
    if async_pool:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.example:6388"],
            endpoint_policy="none",
            trusted_hosts=["attacker.example"],
        )
    else:
        pool = object.__new__(TopologyProtocolAdapterPool)
        pool.endpoint_policy = "none"
        pool.endpoint_validator = None
        pool._tls = False
        pool._seed_endpoint_keys = {("seed.example", 6388)}
        pool._trusted_hosts = {"attacker.example"}

    with pytest.raises(FerricStoreError, match="unsafe learned endpoint"):
        pool._validate_endpoint(endpoint)
    pool._validate_endpoint({"host": "SEED.EXAMPLE", "native_port": 6388})


@pytest.mark.parametrize("async_pool", [False, True])
def test_topology_endpoint_validator_none_result_is_allowed(async_pool: bool) -> None:
    endpoint = {"host": "learned.example", "native_port": 6388}
    if async_pool:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.example:6388"],
            endpoint_policy="any",
            endpoint_validator=lambda _endpoint: None,
        )
    else:
        pool = object.__new__(TopologyProtocolAdapterPool)
        pool.endpoint_policy = "any"
        pool.endpoint_validator = lambda _endpoint: None
        pool._tls = False
        pool._seed_endpoint_keys = {("seed.example", 6388)}
        pool._trusted_hosts = set()

    pool._validate_endpoint(endpoint)


@pytest.mark.parametrize("limit", [None, 1024])
def test_response_decompression_normalizes_corrupt_zlib_errors(limit: int | None) -> None:
    with pytest.raises(FerricStoreError, match="invalid compressed data") as exc_info:
        decompress_response(b"not-a-zlib-stream", limit)

    assert isinstance(exc_info.value.raw, zlib.error)


@pytest.mark.parametrize("limit", [None, 1024])
def test_response_decompression_rejects_trailing_compressed_data(limit: int | None) -> None:
    body = zlib.compress(b"valid-response") + b"trailing-junk"

    with pytest.raises(FerricStoreError, match="invalid compressed data"):
        decompress_response(body, limit)


@pytest.mark.parametrize(
    "value",
    [
        {"same": b"text", b"same": b"binary"},
        {b"same": b"binary", "same": b"text"},
    ],
)
def test_generic_map_encoding_rejects_duplicate_wire_keys(value: dict[Any, bytes]) -> None:
    with pytest.raises(FerricStoreError, match="duplicate protocol map key"):
        encode_value(value)


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_generic_encoding_normalizes_out_of_range_integers(value: int) -> None:
    with pytest.raises(FerricStoreError, match="signed 64-bit"):
        encode_value(value)


def test_generic_encoding_rejects_unsupported_value_types() -> None:
    with pytest.raises(FerricStoreError, match="unsupported protocol value type"):
        encode_value(object())


def test_generic_map_encoding_rejects_unsupported_key_types() -> None:
    with pytest.raises(FerricStoreError, match="protocol map key"):
        encode_value({1: b"value"})


def test_generic_map_encoding_uses_near_linear_peak_memory() -> None:
    value = {f"key-{index}": b"x" * (128 * 1024) for index in range(24)}
    gc.collect()
    tracemalloc.start()
    try:
        encoded = encode_value(value)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert peak < len(encoded) * 1.7


def test_pending_request_budget_stops_generic_encoding_before_late_values(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class Socket:
        def sendall(self, _frame: bytes) -> None:
            raise AssertionError("an over-budget request must not be written")

        def shutdown(self, *_args: Any) -> None:
            return None

        def close(self) -> None:
            return None

    class MustNotEncode:
        def __str__(self) -> str:
            raise AssertionError("encoding must stop as soon as the byte budget is exceeded")

    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=64,
    )
    adapter._sock = Socket()  # type: ignore[assignment]
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    try:
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            adapter._submit_request_on_lane(
                protocol_module._OP_SET,
                1,
                {"large": b"x" * 128, "late": MustNotEncode()},
            )
    finally:
        adapter.close()


def test_compact_pipeline_respects_pending_budget_before_large_allocation(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class Socket:
        def sendall(self, _frame: bytes) -> None:
            raise AssertionError("an over-budget compact pipeline must not be written")

        def shutdown(self, *_args: Any) -> None:
            return None

        def close(self) -> None:
            return None

    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=64,
    )
    adapter._sock = Socket()  # type: ignore[assignment]
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    value = b"x" * (512 * 1024)
    commands = [("SET", f"key-{index}", value) for index in range(4)]

    gc.collect()
    tracemalloc.start()
    try:
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            adapter.submit_commands(commands)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        adapter.close()

    assert peak < 512 * 1024
    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0


def test_async_compact_pipeline_respects_pending_budget_before_large_allocation() -> None:
    class Writer:
        def write(self, _frame: bytes) -> None:
            raise AssertionError("an over-budget compact pipeline must not be written")

        async def drain(self) -> None:
            return None

    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            max_pending_request_bytes=64,
        )
        adapter._writer = Writer()  # type: ignore[assignment]

        async def connected() -> None:
            return None

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        value = b"x" * (512 * 1024)
        commands = [("SET", f"key-{index}", value) for index in range(4)]

        gc.collect()
        tracemalloc.start()
        try:
            with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
                await adapter.execute_batch(commands)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
            adapter._writer = None
            await adapter.close()

        assert peak < 512 * 1024
        assert adapter.pending_request_count == 0
        assert adapter.pending_request_bytes == 0

    asyncio.run(run())


def test_sync_batch_budget_rejects_before_protocol_command_allocation(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=64,
    )
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    commands = [("PING",)] * 100_000

    gc.collect()
    tracemalloc.start()
    try:
        with pytest.raises(FerricStoreError, match="max_batch_items"):
            adapter.submit_commands(commands)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        adapter.close()

    assert peak < 1_000_000
    assert adapter.pending_request_count == 0


def test_async_compressed_batch_budget_rejects_before_command_allocation() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            compression="zlib",
            max_pending_request_bytes=64,
        )

        async def connected() -> None:
            return None

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        commands = [("GET", "key")] * 50_000

        gc.collect()
        tracemalloc.start()
        try:
            with pytest.raises(FerricStoreError, match="max_batch_items"):
                await adapter.execute_batch(commands)
            _current, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
            await adapter.close()

        assert peak < 1_000_000
        assert adapter.pending_request_count == 0

    asyncio.run(run())


def test_sync_topology_batch_budget_rejects_before_command_planning(monkeypatch: Any) -> None:
    class FakeAdapter:
        def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[bytes]:
            return [b"OK"] * len(commands)

        def close(self) -> None:
            return None

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    pool = TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"],
        endpoint_policy="any",
        max_batch_items=10,
    )
    commands = [("PING",)] * 100_000

    gc.collect()
    tracemalloc.start()
    try:
        with pytest.raises(FerricStoreError, match="max_batch_items"):
            pool.execute_batch(commands)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        pool.close()

    assert peak < 1_000_000


def test_async_topology_batch_budget_rejects_before_command_planning(monkeypatch: Any) -> None:
    class FakeAdapter:
        async def execute_command(self, *args: Any) -> Any:
            return _single_shard_topology() if args[0] == "SHARDS" else b"OK"

        async def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[bytes]:
            return [b"OK"] * len(commands)

        async def close(self) -> None:
            return None

    monkeypatch.setattr(
        AsyncProtocolAdapter,
        "from_url",
        lambda *_args, **_kwargs: FakeAdapter(),
    )
    commands = [("PING",)] * 100_000

    async def run() -> int:
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"],
            endpoint_policy="any",
            max_batch_items=10,
        )
        await pool.refresh_topology()
        gc.collect()
        tracemalloc.start()
        try:
            with pytest.raises(FerricStoreError, match="max_batch_items"):
                await pool.execute_batch(commands)
            _current, peak = tracemalloc.get_traced_memory()
            return peak
        finally:
            tracemalloc.stop()
            await pool.close()

    assert asyncio.run(run()) < 1_000_000


def test_flow_compact_pipeline_counts_utf8_bytes_before_encoding(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=512 * 1024,
    )
    flow_id = "🚀" * 150_000

    gc.collect()
    tracemalloc.start()
    try:
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            adapter.submit_commands([("FLOW.GET", flow_id)])
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
        adapter.close()

    assert peak < 256 * 1024
    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0


def test_flow_compact_pipeline_stops_at_aggregate_byte_budget(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=256 * 1024,
    )
    flow_id = "x" * (64 * 1024)
    templates = [
        ("FLOW.GET", flow_id, "PARTITION", "tenant"),
        ("FLOW.HISTORY", flow_id, "COUNT", 10, "PARTITION", "tenant"),
        (
            "FLOW.SIGNAL",
            flow_id,
            "SIGNAL",
            "ready",
            "IF_STATE",
            "queued",
            "TRANSITION_TO",
            "running",
            "NOW",
            123,
        ),
    ]

    try:
        for command in templates:
            gc.collect()
            tracemalloc.start()
            try:
                with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
                    adapter.submit_commands([command] * 16)
                _current, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()

            assert peak < 512 * 1024
            assert adapter.pending_request_count == 0
            assert adapter.pending_request_bytes == 0
    finally:
        adapter.close()


def test_streamed_zlib_request_body_round_trips_generic_codec_values() -> None:
    payload = {
        "name": "order",
        "items": [b"one", b"two", {"count": 3, "ready": True}],
    }

    body, compressed = protocol_module._encode_request_body(
        payload,
        compression="zlib",
        max_body_bytes=None,
        pending_limit=None,
    )

    assert compressed is True
    assert zlib.decompress(body) == encode_value(payload)


def test_compressed_request_budget_stops_before_late_payload_values(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class Socket:
        def sendall(self, _frame: bytes) -> None:
            raise AssertionError("an over-budget request must not be written")

        def shutdown(self, *_args: Any) -> None:
            return None

        def close(self) -> None:
            return None

    class MustNotEncode:
        def __str__(self) -> str:
            raise AssertionError("compressed encoding must stop at its wire-byte budget")

    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        compression="zlib",
        max_pending_request_bytes=128,
    )
    adapter._sock = Socket()  # type: ignore[assignment]
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    try:
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            adapter._submit_request_on_lane(
                protocol_module._OP_SET,
                1,
                {
                    "large": random.Random(0).randbytes(256 * 1024),
                    "late": MustNotEncode(),
                },
            )
    finally:
        adapter.close()


def test_sync_response_identity_is_validated_before_body_decode(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    adapter._ensure_connected = lambda: None  # type: ignore[method-assign]
    adapter._send = lambda *_args, **_kwargs: None  # type: ignore[method-assign]
    request_id, future = adapter._submit_request_on_lane(
        protocol_module._OP_GET,
        3,
        {"key": "one"},
    )
    header = protocol_module._HEADER.pack(
        protocol_module._MAGIC,
        protocol_module._RESPONSE_VERSION,
        0,
        4,
        protocol_module._OP_GET,
        request_id,
        1_000_000,
    )
    reads = iter([header])
    adapter._recv_exact = lambda _size, _sock=None: next(reads)  # type: ignore[method-assign]

    try:
        with pytest.raises(FerricStoreError, match="response identity mismatch"):
            adapter._recv_response()
        assert future.done() is False
    finally:
        adapter._discard_pending_request(request_id, expected_future=future)
        adapter.close()


def test_async_response_identity_is_validated_before_body_decode() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(timeout=None, heartbeat_interval=None)
        sent = asyncio.Event()

        async def connected() -> None:
            return None

        async def send(*_args: Any, **_kwargs: Any) -> None:
            sent.set()

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        adapter._send = send  # type: ignore[method-assign]
        request = asyncio.create_task(
            adapter._request_without_timeout(
                protocol_module._OP_GET,
                3,
                {"key": "one"},
                exact_lane=True,
            )
        )
        await sent.wait()
        request_id = next(iter(adapter._pending))
        header = protocol_module._HEADER.pack(
            protocol_module._MAGIC,
            protocol_module._RESPONSE_VERSION,
            0,
            3,
            protocol_module._OP_SET,
            request_id,
            1_000_000,
        )
        reads = iter([header])

        async def recv_exact(_size: int, _reader: Any = None) -> bytes:
            return next(reads)

        adapter._recv_exact = recv_exact  # type: ignore[method-assign]
        try:
            with pytest.raises(FerricStoreError, match="response identity mismatch"):
                await adapter._recv_response()
            assert request.done() is False
        finally:
            request.cancel()
            await asyncio.gather(request, return_exceptions=True)
            await adapter.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    ("commands", "mode"),
    [
        (
            [
                ("FLOW.HISTORY", "f1", "COUNT", 10, "PARTITION", "p1"),
                ("FLOW.HISTORY", "f2", "COUNT", 10, "PARTITION", "p2"),
            ],
            10,
        ),
        (
            [
                (
                    "FLOW.SIGNAL",
                    "f1",
                    "SIGNAL",
                    "ready",
                    "PARTITION",
                    "p1",
                    "IF_STATE",
                    "queued",
                    "TRANSITION_TO",
                    "next",
                    "NOW",
                    123,
                ),
                (
                    "FLOW.SIGNAL",
                    "f2",
                    "SIGNAL",
                    "ready",
                    "PARTITION",
                    "p2",
                    "IF_STATE",
                    "queued",
                    "TRANSITION_TO",
                    "next",
                    "NOW",
                    124,
                ),
            ],
            11,
        ),
    ],
)
def test_submit_commands_uses_authoritative_raw_compact_pipeline_registry(
    commands: list[tuple[Any, ...]],
    mode: int,
) -> None:
    adapter = object.__new__(ProtocolAdapter)
    captured: dict[str, Any] = {}

    def submit(
        opcode: int,
        lane_id: int,
        payload: Any,
        flags: int = 0,
        **_kwargs: Any,
    ) -> tuple[int, Future[ProtocolResponse]]:
        captured.update(opcode=opcode, lane_id=lane_id, payload=payload, flags=flags)
        source: Future[ProtocolResponse] = Future()
        source.set_result(
            ProtocolResponse(
                lane_id,
                opcode,
                1,
                flags,
                0,
                [[b"ok", b"one"], [b"ok", b"two"]],
            )
        )
        return 1, source

    adapter._submit_request = submit  # type: ignore[method-assign]
    futures = adapter.submit_commands(commands)

    assert captured["opcode"] == protocol_module._OP_PIPELINE
    assert captured["flags"] == protocol_module._FLAG_CUSTOM_PAYLOAD
    assert captured["payload"][:6] == struct.pack(
        ">BBI",
        protocol_module._COMPACT_PIPELINE_REQUEST,
        mode,
        2,
    )
    assert [future.result() for future in futures] == [b"one", b"two"]


def test_submit_commands_compacts_history_before_structured_command_allocation(
    monkeypatch: Any,
) -> None:
    adapter = object.__new__(ProtocolAdapter)
    captured: dict[str, Any] = {}

    def submit(
        opcode: int,
        lane_id: int,
        payload: Any,
        flags: int = 0,
        **_kwargs: Any,
    ) -> tuple[int, Future[ProtocolResponse]]:
        captured.update(payload=payload, flags=flags)
        source: Future[ProtocolResponse] = Future()
        source.set_result(ProtocolResponse(lane_id, opcode, 1, flags, 0, [[b"ok", b"history"]]))
        return 1, source

    adapter._submit_request = submit  # type: ignore[method-assign]
    monkeypatch.setattr(
        protocol_module,
        "build_protocol_command",
        lambda *_args, **_kwargs: pytest.fail(
            "the raw compact path must not allocate structured commands"
        ),
    )

    futures = adapter.submit_commands([("FLOW.HISTORY", "f1", "COUNT", 10, "PARTITION", "p1")])

    assert captured["flags"] == protocol_module._FLAG_CUSTOM_PAYLOAD
    assert futures[0].result() == b"history"


def test_sync_protocol_reader_thread_start_failure_closes_published_socket(
    monkeypatch: Any,
) -> None:
    class Socket:
        def __init__(self) -> None:
            self.closed = False

        def setsockopt(self, *_args: Any) -> None:
            return None

        def settimeout(self, _timeout: float | None) -> None:
            return None

        def shutdown(self, *_args: Any) -> None:
            return None

        def close(self) -> None:
            self.closed = True

    sock = Socket()
    original_start = threading.Thread.start

    def fail_reader_start(thread: threading.Thread) -> None:
        target = getattr(thread, "_target", None)
        if getattr(target, "__name__", None) == "_reader_loop":
            raise RuntimeError("reader thread start failed")
        original_start(thread)

    monkeypatch.setattr(protocol_module.socket, "create_connection", lambda *_args, **_kwargs: sock)
    monkeypatch.setattr(threading.Thread, "start", fail_reader_start)

    with pytest.raises(RuntimeError, match="reader thread start failed"):
        ProtocolAdapter(timeout=None, heartbeat_interval=None)

    assert sock.closed is True


def test_sync_protocol_heartbeat_thread_start_failure_rolls_back_state(
    monkeypatch: Any,
) -> None:
    adapter = object.__new__(ProtocolAdapter)
    adapter.heartbeat_interval = 1.0
    adapter._heartbeat_stop = None
    adapter._heartbeat_thread = None
    adapter._sock = object()  # type: ignore[assignment]
    original_start = threading.Thread.start

    def fail_heartbeat_start(thread: threading.Thread) -> None:
        if thread.name == "ferricstore-protocol-heartbeat":
            raise RuntimeError("heartbeat thread start failed")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_heartbeat_start)

    with pytest.raises(RuntimeError, match="heartbeat thread start failed"):
        adapter._start_heartbeat()

    assert adapter._heartbeat_thread is None
    assert adapter._heartbeat_stop is None


def test_sync_singleflight_never_overlaps_refresh_operations() -> None:
    singleflight = SyncSingleFlight[int]()
    callers = 32
    start = threading.Barrier(callers + 1)
    state_lock = threading.Lock()
    active = 0
    maximum_active = 0

    def refresh() -> int:
        nonlocal active, maximum_active
        with state_lock:
            active += 1
            maximum_active = max(maximum_active, active)
        time.sleep(0)
        with state_lock:
            active -= 1
        return 1

    def call_repeatedly() -> None:
        start.wait()
        for _ in range(500):
            assert singleflight.run(refresh) == 1

    threads = [threading.Thread(target=call_repeatedly) for _ in range(callers)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(5)

    assert not any(thread.is_alive() for thread in threads)
    assert maximum_active == 1


def test_sync_protocol_request_timeout_bounds_write_lock_wait(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.02, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_ensure_connected", lambda: None)
    monkeypatch.setattr(adapter, "_send", lambda *_args, **_kwargs: None)

    finished = threading.Event()
    outcome: dict[str, Any] = {}
    adapter._lock.acquire()
    started = time.monotonic()

    def request() -> None:
        try:
            adapter._request(0x0101, 1, {"key": "a"})
        except BaseException as exc:
            outcome["error"] = exc
        finally:
            outcome["elapsed"] = time.monotonic() - started
            finished.set()

    thread = threading.Thread(target=request)
    thread.start()
    completed_in_budget = finished.wait(0.5)
    adapter._lock.release()
    thread.join(1)
    adapter.close()

    assert completed_in_budget is True
    assert isinstance(outcome["error"], FerricStoreError)
    assert "timed out" in str(outcome["error"])
    assert outcome["elapsed"] < 0.25


def test_async_protocol_request_timeout_bounds_connect_lock_wait() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(timeout=0.02, heartbeat_interval=None)
        await adapter._connect_lock.acquire()
        started = time.monotonic()
        task = asyncio.create_task(adapter.execute_command("PING"))
        error: BaseException | None = None
        try:
            await asyncio.wait_for(task, timeout=0.5)
        except BaseException as exc:
            error = exc
        finally:
            adapter._connect_lock.release()

        assert isinstance(error, FerricStoreError)
        assert "timed out" in str(error)
        assert time.monotonic() - started < 0.25

    asyncio.run(run())


def test_sync_protocol_event_waiters_loop_until_each_has_an_event(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(heartbeat_interval=None)
    results: list[str | None] = []
    results_lock = threading.Lock()

    def wait() -> None:
        value = adapter.wait_event(timeout=1)
        with results_lock:
            results.append(value)

    threads = [threading.Thread(target=wait) for _ in range(2)]
    for thread in threads:
        thread.start()
    time.sleep(0.02)

    adapter._enqueue_event("first")
    time.sleep(0.02)
    with results_lock:
        first_count = len(results)
    adapter._enqueue_event("second")
    for thread in threads:
        thread.join(1)
    adapter.close()

    assert first_count == 1
    assert sorted(results) == ["first", "second"]


def test_async_protocol_event_waiters_loop_until_each_has_an_event() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        waiters = [asyncio.create_task(adapter.wait_event(timeout=1)) for _ in range(2)]
        await asyncio.sleep(0)

        await adapter._enqueue_event("first")
        done, _pending = await asyncio.wait(
            waiters,
            timeout=1,
            return_when=asyncio.FIRST_COMPLETED,
        )
        assert len(done) == 1
        assert sum(waiter.done() for waiter in waiters) == 1

        await adapter._enqueue_event("second")
        assert sorted(await asyncio.gather(*waiters)) == ["first", "second"]
        await adapter.close()

    asyncio.run(run())


def test_async_connect_failure_is_published_to_event_waiters(monkeypatch: Any) -> None:
    async def fail_connect(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("connection refused")

    async def run() -> None:
        monkeypatch.setattr(protocol_module.asyncio, "open_connection", fail_connect)
        adapter = AsyncProtocolAdapter(timeout=0.02, heartbeat_interval=None)
        adapter._event_error = FerricStoreError("previous connection failed")

        with pytest.raises(OSError, match="connection refused"):
            await adapter._ensure_connected()
        with pytest.raises(OSError, match="connection refused"):
            await adapter.wait_event(timeout=0.01)

    asyncio.run(run())


def test_sync_protocol_pool_close_retries_only_failed_adapters() -> None:
    class Adapter:
        def __init__(self, *, fail_once: bool = False) -> None:
            self.fail_once = fail_once
            self.close_calls = 0

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("transient close failure")

    failed = Adapter(fail_once=True)
    succeeded = Adapter()
    pool = ProtocolAdapterPool([failed, succeeded])

    with pytest.raises(RuntimeError, match="transient close failure"):
        pool.close()
    pool.close()

    assert [failed.close_calls, succeeded.close_calls] == [2, 1]


def test_async_protocol_pool_close_retries_only_failed_adapters() -> None:
    class Adapter:
        def __init__(self, *, fail_once: bool = False) -> None:
            self.fail_once = fail_once
            self.close_calls = 0

        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        async def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("transient close failure")

    async def run() -> None:
        failed = Adapter(fail_once=True)
        succeeded = Adapter()
        pool = AsyncProtocolAdapterPool([failed, succeeded])

        with pytest.raises(RuntimeError, match="transient close failure"):
            await pool.close()
        await pool.close()

        assert [failed.close_calls, succeeded.close_calls] == [2, 1]

    asyncio.run(run())


def test_sync_write_failure_retires_transport_and_fails_all_pending(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailingSocket:
        def __init__(self) -> None:
            self.closed = False

        def sendall(self, _data: bytes) -> None:
            raise OSError("partial write")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    sock = FailingSocket()
    adapter._sock = sock  # type: ignore[assignment]
    unrelated: Future[Any] = Future()
    adapter._pending[99] = unrelated

    with pytest.raises(OSError, match="partial write"):
        adapter._submit_request(0x0101, 1, {"key": "a"})

    assert adapter._sock is None
    assert sock.closed is True
    assert unrelated.done() is True
    assert isinstance(unrelated.exception(), FerricStoreError)


def test_sync_old_transport_close_does_not_fail_replacement_requests(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def setsockopt(self, *_args: Any) -> None:
            pass

        def settimeout(self, _timeout: float | None) -> None:
            pass

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    old_socket = FakeSocket()
    new_socket = FakeSocket()
    adapter._sock = old_socket  # type: ignore[assignment]
    close_paused = threading.Event()
    allow_close = threading.Event()
    close_errors: list[BaseException] = []

    def pause_old_close() -> None:
        close_paused.set()
        assert allow_close.wait(timeout=1)

    adapter.add_event_listener(pause_old_close)

    def close_old_transport() -> None:
        try:
            adapter._close_transport(
                FerricStoreError("old transport failed"),
                expected_sock=old_socket,  # type: ignore[arg-type]
            )
        except BaseException as exc:
            close_errors.append(exc)

    close_thread = threading.Thread(target=close_old_transport)
    close_thread.start()
    assert close_paused.wait(timeout=1)

    monkeypatch.setattr(protocol_module.socket, "create_connection", lambda *_a, **_k: new_socket)
    monkeypatch.setattr(adapter, "_reader_loop", lambda *_args: None)
    monkeypatch.setattr(adapter, "_request", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(adapter, "_response_value", lambda _response: None)
    adapter._connect_once()
    monkeypatch.setattr(adapter, "_send", lambda *_args, **_kwargs: None)
    _request_id, replacement_future = adapter._submit_request(0x0101, 1, {"key": "new"})

    allow_close.set()
    close_thread.join(timeout=1)
    replacement_was_failed = replacement_future.done()
    adapter.remove_event_listener(pause_old_close)
    adapter.close()

    assert close_errors == []
    assert close_thread.is_alive() is False
    assert replacement_was_failed is False
    assert old_socket.closed is True


def test_sync_pending_completion_race_cannot_abort_socket_cleanup(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FakeSocket:
        def __init__(self) -> None:
            self.closed = False

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    completion_started = threading.Event()
    completion_finished = threading.Event()

    class RacingFuture(Future[Any]):
        def set_exception(self, exception: BaseException) -> None:
            completion_started.set()
            assert completion_finished.wait(timeout=1)
            super().set_exception(exception)

    adapter = ProtocolAdapter(heartbeat_interval=None)
    sock = FakeSocket()
    adapter._sock = sock  # type: ignore[assignment]
    pending = RacingFuture()
    adapter._pending[1] = pending

    def complete_response() -> None:
        assert completion_started.wait(timeout=1)
        pending.set_result("response")
        completion_finished.set()

    completion_thread = threading.Thread(target=complete_response)
    completion_thread.start()
    adapter._close_transport(FerricStoreError("reader failed"))
    completion_thread.join(timeout=1)

    assert completion_thread.is_alive() is False
    assert pending.result() == "response"
    assert sock.closed is True


def test_sync_multiframe_write_failure_retires_transport(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    monkeypatch.setattr(
        protocol_sync_batch_module,
        "_pipeline_frame_supported",
        lambda _commands: False,
    )

    class FailingSocket:
        def __init__(self) -> None:
            self.closed = False

        def sendall(self, _data: bytes) -> None:
            raise OSError("batch write failed")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=None, heartbeat_interval=None)
    sock = FailingSocket()
    adapter._sock = sock  # type: ignore[assignment]

    with pytest.raises(OSError, match="batch write failed"):
        adapter.submit_commands([("GET", "a"), ("GET", "b")])

    assert adapter._sock is None
    assert sock.closed is True
    assert adapter._pending == {}


def test_sync_write_preserves_primary_error_when_timeout_restore_fails(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailingSocket:
        def __init__(self) -> None:
            self.setsockopt_calls = 0
            self.closed = False

        def getsockopt(self, *_args: Any) -> bytes:
            return b"previous"

        def setsockopt(self, *_args: Any) -> None:
            self.setsockopt_calls += 1
            if self.setsockopt_calls == 2:
                raise RuntimeError("timeout restore failed")

        def settimeout(self, _value: float | None) -> None:
            pytest.fail("a write deadline must not change the shared read timeout")

        def sendall(self, _data: bytes) -> None:
            raise OSError("partial write")

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.closed = True

    adapter = ProtocolAdapter(timeout=0.1, heartbeat_interval=None)
    sock = FailingSocket()
    adapter._sock = sock  # type: ignore[assignment]

    with pytest.raises(OSError, match="partial write") as raised:
        adapter._submit_request(0x0101, 1, {"key": "a"})

    assert isinstance(raised.value.__cause__, RuntimeError)
    assert "timeout restore failed" in str(raised.value.__cause__)
    assert adapter._sock is None
    assert sock.closed is True


def test_successful_write_ignores_timeout_restore_after_peer_closed_socket() -> None:
    class ClosedAfterWriteSocket:
        def __init__(self) -> None:
            self.setsockopt_calls = 0
            self.writes: list[bytes] = []

        def getsockopt(self, *_args: Any) -> bytes:
            return b"previous"

        def setsockopt(self, *_args: Any) -> None:
            self.setsockopt_calls += 1
            if self.setsockopt_calls == 2:
                raise OSError(9, "Bad file descriptor")

        def settimeout(self, _value: float | None) -> None:
            pytest.fail("a write deadline must not change the shared read timeout")

        def fileno(self) -> int:
            return -1

        def sendall(self, data: bytes) -> None:
            self.writes.append(data)

    sock = ClosedAfterWriteSocket()

    send_frames(sock, [b"complete request"], timeout=0.1)

    assert sock.writes == [b"complete request"]


def test_send_frames_never_mutates_the_shared_read_timeout() -> None:
    class Socket:
        def __init__(self) -> None:
            self.writes: list[bytes] = []
            self.timeout_options: list[bytes] = []

        def getsockopt(self, *_args: Any) -> bytes:
            return b"previous"

        def setsockopt(self, _level: int, _option: int, value: bytes) -> None:
            self.timeout_options.append(value)

        def settimeout(self, _value: float | None) -> None:
            raise AssertionError("a write deadline must not mutate the reader timeout")

        def sendall(self, data: bytes) -> None:
            self.writes.append(data)

    sock = Socket()

    send_frames(sock, [b"request"], timeout=0.1)

    assert sock.writes == [b"request"]
    assert len(sock.timeout_options) == 2


def test_sync_write_deadline_does_not_timeout_a_concurrent_reader() -> None:
    left, right = socket.socketpair()
    left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4_096)
    left.settimeout(None)
    reader_finished = threading.Event()
    reader_errors: list[BaseException] = []

    def read() -> None:
        try:
            left.recv(1)
        except BaseException as exc:
            reader_errors.append(exc)
        finally:
            reader_finished.set()

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    try:
        with pytest.raises((TimeoutError, OSError)):
            send_frames(left, [b"x" * (4 * 1024 * 1024)], timeout=0.05)

        assert not reader_finished.wait(0.02)
        assert reader_errors == []
        assert left.gettimeout() is None
    finally:
        left.close()
        right.close()
        reader.join(timeout=1)


def test_submit_commands_starts_response_deadline_after_connection(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    monkeypatch.setattr(
        protocol_sync_batch_module,
        "_pipeline_frame_supported",
        lambda _commands: False,
    )

    class RecordingSocket:
        def __init__(self) -> None:
            self.timeout: float | None = None
            self.writes: list[bytes] = []

        def gettimeout(self) -> float | None:
            return self.timeout

        def settimeout(self, value: float | None) -> None:
            self.timeout = value

        def sendall(self, data: bytes) -> None:
            self.writes.append(data)

    adapter = ProtocolAdapter(timeout=0.1, heartbeat_interval=None)
    sock = RecordingSocket()
    adapter._sock = sock  # type: ignore[assignment]
    connected_at = 0.0

    def slow_connect() -> None:
        nonlocal connected_at
        time.sleep(0.03)
        connected_at = time.monotonic()

    adapter._ensure_connected = slow_connect  # type: ignore[method-assign]
    registered_deadlines: list[float] = []
    register = adapter._register_pending_request

    def record_deadline(*args: Any, **kwargs: Any) -> Any:
        expires_at = kwargs.get("expires_at")
        if expires_at is not None:
            registered_deadlines.append(expires_at)
        return register(*args, **kwargs)

    adapter._register_pending_request = record_deadline  # type: ignore[method-assign]
    futures = adapter.submit_commands([("PING",), ("MULTI",)])
    try:
        assert len(futures) == 2
        assert len(sock.writes) == 2
        assert len(registered_deadlines) == 2
        assert all(deadline >= connected_at + 0.09 for deadline in registered_deadlines)
    finally:
        for request_id, future in list(adapter._pending.items()):
            adapter._discard_pending_request(request_id, expected_future=future)
        adapter._sock = None
        adapter.close()


def test_flow_option_analysis_does_not_decode_opaque_binary_payload() -> None:
    class OpaquePayload(bytes):
        def decode(self, *_args: Any, **_kwargs: Any) -> str:
            raise AssertionError("opaque payload must not be decoded as command grammar")

    payload = OpaquePayload(b"\xff" * 1024)

    command = build_protocol_command(
        "FLOW.CREATE",
        "flow-1",
        "TYPE",
        "job",
        "PAYLOAD",
        payload,
    )

    assert command.payload["payload"] is payload


def test_flow_option_analysis_bounds_binary_payload_peak_memory() -> None:
    payload = b"\xff" * (1024 * 1024)
    gc.collect()
    tracemalloc.start()
    try:
        command = build_protocol_command(
            "FLOW.CREATE",
            "flow-1",
            "TYPE",
            "job",
            "PAYLOAD",
            payload,
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert command.payload["payload"] is payload
    assert peak < 2 * 1024 * 1024


def test_flow_routing_bounds_large_partition_key_peak_memory() -> None:
    partition_key = b"x" * (8 * 1024 * 1024)

    gc.collect()
    tracemalloc.start()
    try:
        route_keys = flow_command_route_keys(
            "FLOW.CLAIM_DUE",
            ("job", "PARTITION", partition_key),
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert len(route_keys) == 1
    assert peak < 512 * 1024


def test_flow_routing_streams_large_text_keys() -> None:
    routing_value = "🚀" * (1024 * 1024)
    encoded = routing_value.encode()
    expected_partition_route = flow_command_route_keys(
        "FLOW.CLAIM_DUE",
        ("job", "PARTITION", encoded),
    )
    expected_id_route = flow_command_route_keys("FLOW.GET", (encoded,))
    del encoded

    gc.collect()
    tracemalloc.start()
    try:
        partition_route = flow_command_route_keys(
            "FLOW.CLAIM_DUE",
            ("job", "PARTITION", routing_value),
        )
        id_route = flow_command_route_keys("FLOW.GET", (routing_value,))
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert partition_route == expected_partition_route
    assert id_route == expected_id_route
    assert peak < 512 * 1024


def test_async_drain_failure_retires_transport_and_fails_all_pending() -> None:
    class FailingWriter:
        def __init__(self) -> None:
            self.closed = False

        def is_closing(self) -> bool:
            return self.closed

        def write(self, _part: bytes) -> None:
            pass

        async def drain(self) -> None:
            raise OSError("drain failed")

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            pass

    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            write_drain_bytes=0,
        )
        writer = FailingWriter()
        adapter._writer = writer  # type: ignore[assignment]
        adapter._connection_ready = True
        unrelated = asyncio.get_running_loop().create_future()
        adapter._pending[99] = unrelated

        with pytest.raises(OSError, match="drain failed"):
            await adapter._request_without_timeout(0x0101, 1, {"key": "a"})

        assert adapter._writer is None
        assert writer.closed is True
        assert unrelated.done() is True
        with pytest.raises(FerricStoreError):
            await unrelated

    asyncio.run(run())


def test_async_request_timeout_does_not_wait_for_blocked_transport_cleanup() -> None:
    class BlockingWriter:
        def __init__(self) -> None:
            self.drain_entered = asyncio.Event()
            self.wait_closed_entered = asyncio.Event()
            self.release_close = asyncio.Event()
            self.closed = False

        def is_closing(self) -> bool:
            return self.closed

        def write(self, _frame: bytes) -> None:
            return None

        async def drain(self) -> None:
            self.drain_entered.set()
            await asyncio.Event().wait()

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            self.wait_closed_entered.set()
            await self.release_close.wait()

    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=0.01,
            heartbeat_interval=None,
            write_drain_bytes=0,
        )
        writer = BlockingWriter()
        adapter._writer = writer  # type: ignore[assignment]

        async def connected() -> None:
            return None

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        request = asyncio.create_task(adapter.execute_command("GET", "key"))
        await asyncio.wait_for(writer.drain_entered.wait(), timeout=1)

        with pytest.raises(FerricStoreError, match="protocol request timed out"):
            await asyncio.wait_for(request, timeout=0.1)

        await asyncio.wait_for(writer.wait_closed_entered.wait(), timeout=1)
        assert not writer.release_close.is_set()
        writer.release_close.set()
        await adapter.close()

    asyncio.run(run())


def test_sync_close_failure_still_publishes_state_and_retains_socket_for_retry(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)

    class FailOnceSocket:
        def __init__(self) -> None:
            self.close_calls = 0

        def shutdown(self, *_args: Any) -> None:
            pass

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("transient socket close failure")

    adapter = ProtocolAdapter(heartbeat_interval=None)
    sock = FailOnceSocket()
    adapter._sock = sock  # type: ignore[assignment]
    pending: Future[Any] = Future()
    adapter._pending[1] = pending

    with pytest.raises(RuntimeError, match="transient socket close failure"):
        adapter._close_transport(FerricStoreError("reader failed"))

    assert adapter._sock is None
    assert pending.done() is True
    with pytest.raises(FerricStoreError, match="reader failed"):
        adapter.wait_event(timeout=0)

    adapter.close()
    assert sock.close_calls == 2


def test_async_close_failure_still_publishes_state_and_retains_writer_for_retry() -> None:
    class FailOnceWriter:
        def __init__(self) -> None:
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("transient writer close failure")

        async def wait_closed(self) -> None:
            pass

    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        writer = FailOnceWriter()
        adapter._writer = writer  # type: ignore[assignment]
        pending = asyncio.get_running_loop().create_future()
        adapter._pending[1] = pending

        with pytest.raises(RuntimeError, match="transient writer close failure"):
            await adapter.close()

        assert adapter._writer is None
        with pytest.raises(FerricStoreError, match="closed"):
            await pending
        with pytest.raises(FerricStoreError, match="closed"):
            await adapter.wait_event(timeout=0)

        await adapter.close()
        assert writer.close_calls == 2

    asyncio.run(run())


def test_async_close_notifies_event_waiters_before_writer_cleanup_finishes() -> None:
    class BlockingWriter:
        def __init__(self) -> None:
            self.close_called = asyncio.Event()
            self.release = asyncio.Event()

        def close(self) -> None:
            self.close_called.set()

        async def wait_closed(self) -> None:
            await self.release.wait()

    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        writer = BlockingWriter()
        adapter._writer = writer  # type: ignore[assignment]
        event_waiter = asyncio.create_task(adapter.wait_event())
        await asyncio.sleep(0)
        close_task = asyncio.create_task(adapter.close())
        await asyncio.wait_for(writer.close_called.wait(), timeout=1)

        try:
            with pytest.raises(FerricStoreError, match="closed"):
                await asyncio.wait_for(event_waiter, timeout=0.1)
            with pytest.raises(FerricStoreError, match="closed"):
                await adapter.wait_event(timeout=0)
            assert close_task.done() is False
        finally:
            writer.release.set()
            await close_task

    asyncio.run(run())


def test_async_invalidate_cancellation_cannot_interrupt_notification_then_cleanup() -> None:
    class Writer:
        def __init__(self) -> None:
            self.closed = False
            self.finished = asyncio.Event()

        def close(self) -> None:
            self.closed = True

        async def wait_closed(self) -> None:
            self.finished.set()

    async def run() -> None:
        adapter = AsyncProtocolAdapter(heartbeat_interval=None)
        writer = Writer()
        adapter._writer = writer  # type: ignore[assignment]
        notification_started = asyncio.Event()
        publish_event_error = adapter._publish_event_error

        async def publish(error: BaseException) -> None:
            notification_started.set()
            await publish_event_error(error)

        adapter._publish_event_error = publish  # type: ignore[method-assign]
        await adapter._events_cv.acquire()
        invalidate_task = asyncio.create_task(adapter.invalidate())
        try:
            await asyncio.wait_for(notification_started.wait(), timeout=1)
            assert adapter._writer is None
            invalidate_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await invalidate_task
        finally:
            adapter._events_cv.release()

        await asyncio.wait_for(writer.finished.wait(), timeout=1)
        assert writer.closed is True

    asyncio.run(run())


def test_sync_pool_event_poll_reserves_adapter_against_session_lease() -> None:
    poll_entered = threading.Event()
    release_poll = threading.Event()

    class Adapter:
        def add_event_listener(self, _listener: Any) -> None:
            pass

        def remove_event_listener(self, _listener: Any) -> None:
            pass

        def wait_event(self, *, timeout: float | None = None) -> str:
            assert timeout == 0.0
            poll_entered.set()
            assert release_poll.wait(timeout=1)
            return "event"

        def close(self) -> None:
            pass

    pool = ProtocolAdapterPool([Adapter()])  # type: ignore[list-item]
    event_result: list[Any] = []
    session_result: list[Any] = []
    session_acquired = threading.Event()

    poll_thread = threading.Thread(target=lambda: event_result.append(pool.wait_event(timeout=1)))

    def acquire_session() -> None:
        session_result.append(pool.acquire_session())
        session_acquired.set()

    session_thread = threading.Thread(target=acquire_session)
    poll_thread.start()
    assert poll_entered.wait(timeout=1)
    session_thread.start()
    leased_during_poll = session_acquired.wait(timeout=0.05)
    release_poll.set()
    poll_thread.join(timeout=1)
    session_thread.join(timeout=1)
    session_result[0].close()
    pool.close()

    assert leased_during_poll is False
    assert event_result == ["event"]


def test_async_pool_event_poll_reserves_adapter_against_session_lease() -> None:
    async def run() -> None:
        poll_entered = asyncio.Event()
        release_poll = asyncio.Event()

        class Adapter:
            def add_event_listener(self, _listener: Any) -> None:
                pass

            def remove_event_listener(self, _listener: Any) -> None:
                pass

            async def wait_event(self, *, timeout: float | None = None) -> str:
                assert timeout == 0.0
                poll_entered.set()
                await release_poll.wait()
                return "event"

            async def close(self) -> None:
                pass

        pool = AsyncProtocolAdapterPool([Adapter()])  # type: ignore[list-item]
        poll_task = asyncio.create_task(pool.wait_event(timeout=1))
        await asyncio.wait_for(poll_entered.wait(), timeout=1)
        session_task = asyncio.create_task(pool.acquire_session())
        await asyncio.sleep(0)
        leased_during_poll = session_task.done()
        release_poll.set()
        assert await poll_task == "event"
        session = await asyncio.wait_for(session_task, timeout=1)
        await session.close()
        await pool.close()

        assert leased_during_poll is False

    asyncio.run(run())


@pytest.mark.parametrize("limit", [0, -1])
def test_protocol_rejects_nonpositive_response_chunk_limit(limit: int, monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    with pytest.raises(ValueError, match="max_response_chunks"):
        ProtocolAdapter(max_response_chunks=limit)


def test_sync_chunked_response_has_bounded_frame_count(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    header = struct.Struct(">4sBBIHQI")
    frames = [
        header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
        header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
        header.pack(b"FSNP", 0x81, 0, 1, 0x0101, 1, 0),
    ]
    adapter = ProtocolAdapter(max_response_chunks=2, heartbeat_interval=None)
    reads = iter(frames)

    def recv_exact(size: int, _sock: Any = None) -> bytes:
        return b"" if size == 0 else next(reads)

    adapter._recv_exact = recv_exact  # type: ignore[method-assign]

    with pytest.raises(FerricStoreError, match="max_response_chunks"):
        adapter._recv_response()

    adapter.close()


def test_async_chunked_response_has_bounded_frame_count() -> None:
    async def run() -> None:
        header = struct.Struct(">4sBBIHQI")
        frames = iter(
            [
                header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
                header.pack(b"FSNP", 0x81, 0x20, 1, 0x0101, 1, 0),
                header.pack(b"FSNP", 0x81, 0, 1, 0x0101, 1, 0),
            ]
        )
        adapter = AsyncProtocolAdapter(max_response_chunks=2, heartbeat_interval=None)

        async def recv_exact(size: int, _reader: Any = None) -> bytes:
            return b"" if size == 0 else next(frames)

        adapter._recv_exact = recv_exact  # type: ignore[method-assign]
        with pytest.raises(FerricStoreError, match="max_response_chunks"):
            await adapter._recv_response()
        await adapter.close()

    asyncio.run(run())


def test_sync_topology_refresh_does_not_hold_routing_lock_and_is_singleflight(
    monkeypatch: Any,
) -> None:
    block = False
    entered = threading.Event()
    release = threading.Event()
    calls = 0

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            nonlocal calls
            if args[0] == "SHARDS":
                calls += 1
                if block:
                    entered.set()
                    release.wait(1)
                return _single_shard_topology()
            return b"OK"

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    initial_calls = calls
    block = True
    errors: list[BaseException] = []

    def refresh() -> None:
        try:
            pool.refresh_topology()
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=refresh) for _ in range(2)]
    for thread in threads:
        thread.start()
    assert entered.wait(1)
    lock_available = pool._lock.acquire(timeout=0.05)
    if lock_available:
        pool._lock.release()
    release.set()
    for thread in threads:
        thread.join(1)

    assert lock_available is True
    assert errors == []
    assert calls - initial_calls == 1
    pool.close()


def test_sync_topology_refresh_cannot_close_adapter_selected_by_inflight_command(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
    selected = threading.Event()
    proceed = threading.Event()
    selected_adapters: list[Any] = []

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            if self.closed:
                raise RuntimeError("used closed adapter")
            return b"OK"

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    execute_selected = pool._execute_protocol_command

    def pause_after_selection(
        adapter: Any,
        prepared: Any,
        lane_id: int,
    ) -> Any:
        selected_adapters.append(adapter)
        selected.set()
        assert proceed.wait(1)
        return execute_selected(adapter, prepared, lane_id)

    pool._execute_protocol_command = pause_after_selection  # type: ignore[method-assign]
    values: list[Any] = []
    errors: list[BaseException] = []

    def execute() -> None:
        try:
            values.append(pool.execute_command("GET", "key-1"))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=execute)
    thread.start()
    assert selected.wait(1)
    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    assert selected_adapters[0].closed is False
    proceed.set()
    thread.join(1)

    assert errors == []
    assert values == [b"OK"]
    assert selected_adapters[0].closed is True
    pool.close()


def test_async_topology_refresh_cannot_close_adapter_selected_by_inflight_command(
    monkeypatch: Any,
) -> None:
    async def run() -> None:
        current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
        selected = asyncio.Event()
        proceed = asyncio.Event()
        selected_adapters: list[Any] = []

        class FakeAdapter:
            def __init__(self, url: str) -> None:
                self.url = url
                self.closed = False

            async def execute_command(self, *args: Any) -> Any:
                if args[0] == "SHARDS":
                    return _single_shard_topology(
                        current["host"], current["port"], route_epoch=current["epoch"]
                    )
                if self.closed:
                    raise RuntimeError("used closed adapter")
                return b"OK"

            async def close(self) -> None:
                self.closed = True

        monkeypatch.setattr(
            AsyncProtocolAdapter,
            "from_url",
            lambda url, **_kwargs: FakeAdapter(url),
        )
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        execute_selected = pool._execute_protocol_command

        async def pause_after_selection(
            adapter: Any,
            prepared: Any,
            lane_id: int,
        ) -> Any:
            selected_adapters.append(adapter)
            selected.set()
            await asyncio.wait_for(proceed.wait(), 1)
            return await execute_selected(adapter, prepared, lane_id)

        pool._execute_protocol_command = pause_after_selection  # type: ignore[method-assign]
        task = asyncio.create_task(pool.execute_command("GET", "key-1"))
        await asyncio.wait_for(selected.wait(), 1)
        current.update(host="leader-2.local", port=6392, epoch=2)
        await pool.refresh_topology()
        assert selected_adapters[0].closed is False
        proceed.set()

        assert await asyncio.wait_for(task, 1) == b"OK"
        await asyncio.sleep(0)
        assert selected_adapters[0].closed is True
        await pool.close()

    asyncio.run(run())


def test_sync_topology_reselects_route_when_generation_changes_before_lease(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
    calls: list[str] = []

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            if self.closed:
                raise RuntimeError("used closed adapter")
            calls.append(self.url)
            return b"OK"

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    lease_adapter = pool._leased_adapter_for_endpoint
    refreshed = False

    def refresh_before_first_lease(endpoint: Any, *, generation: int) -> Any:
        nonlocal refreshed
        if not refreshed:
            refreshed = True
            current.update(host="leader-2.local", port=6392, epoch=2)
            pool.refresh_topology()
        return lease_adapter(endpoint, generation=generation)

    pool._leased_adapter_for_endpoint = refresh_before_first_lease  # type: ignore[method-assign]
    try:
        assert pool.execute_command("GET", "key-1") == b"OK"
    finally:
        pool.close()

    assert calls == ["ferric://leader-2.local:6392"]


def test_sync_topology_batch_holds_endpoint_lease_through_execution(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
    created: dict[str, Any] = {}
    planned = threading.Event()
    proceed = threading.Event()

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            if self.closed:
                raise RuntimeError("used closed adapter")
            return b"OK"

        def execute_batch(self, commands: list[tuple[Any, ...]]) -> list[Any]:
            if self.closed:
                raise RuntimeError("used closed adapter")
            return [b"OK"] * len(commands)

        def close(self) -> None:
            self.closed = True

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        adapter = FakeAdapter(url)
        created[url] = adapter
        return adapter

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    run_groups = pool._batch_fanout.run

    def pause_after_plan(*args: Any, **kwargs: Any) -> Any:
        planned.set()
        assert proceed.wait(1)
        return run_groups(*args, **kwargs)

    pool._batch_fanout.run = pause_after_plan  # type: ignore[method-assign]
    values: list[Any] = []
    errors: list[BaseException] = []

    def execute() -> None:
        try:
            values.extend(pool.execute_batch([("GET", "key-1"), ("GET", "key-2")]))
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=execute)
    thread.start()
    assert planned.wait(1)
    leader = created["ferric://leader-1.local:6391"]
    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    assert leader.closed is False
    proceed.set()
    thread.join(1)

    assert errors == []
    assert values == [b"OK", b"OK"]
    assert leader.closed is True
    pool.close()


def test_sync_topology_event_snapshots_hold_the_registry_lock() -> None:
    pool = object.__new__(TopologyProtocolAdapterPool)
    lock = threading.Lock()

    class LockCheckedAdapters:
        def values(self) -> list[Any]:
            if lock.acquire(blocking=False):
                lock.release()
                raise AssertionError("adapter registry read without its lock")
            return []

    pool._lock = lock  # type: ignore[assignment]
    pool._adapters = LockCheckedAdapters()  # type: ignore[assignment]

    assert pool.events == []
    assert pool._take_event() is None


def test_sync_topology_endpoint_connect_does_not_hold_lock_and_is_singleflight(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}
    entered = threading.Event()
    release = threading.Event()
    created: list[str] = []

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            return b"OK"

        def close(self) -> None:
            pass

    def from_url(url: str, **_kwargs: Any) -> FakeAdapter:
        created.append(url)
        if "leader-2.local" in url:
            entered.set()
            release.wait(1)
        return FakeAdapter(url)

    monkeypatch.setattr(ProtocolAdapter, "from_url", from_url)
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    endpoint = next(iter(pool.topology.endpoints.values()))
    results: list[Any] = []

    threads = [
        threading.Thread(target=lambda: results.append(pool._adapter_for_endpoint(endpoint)))
        for _ in range(2)
    ]
    for thread in threads:
        thread.start()
    assert entered.wait(1)
    lock_available = pool._lock.acquire(timeout=0.05)
    if lock_available:
        pool._lock.release()
    release.set()
    for thread in threads:
        thread.join(1)

    assert lock_available is True
    assert len(results) == 2
    assert results[0] is results[1]
    assert created.count("ferric://leader-2.local:6392") == 1
    pool.close()


def test_async_topology_refresh_does_not_hold_routing_lock_and_is_singleflight(
    monkeypatch: Any,
) -> None:
    async def run() -> None:
        block = False
        entered = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        class FakeAdapter:
            def __init__(self, url: str) -> None:
                self.url = url

            async def execute_command(self, *args: Any) -> Any:
                nonlocal calls
                if args[0] == "SHARDS":
                    calls += 1
                    if block:
                        entered.set()
                        await release.wait()
                    return _single_shard_topology()
                return b"OK"

            async def close(self) -> None:
                pass

        monkeypatch.setattr(
            AsyncProtocolAdapter,
            "from_url",
            lambda url, **_kwargs: FakeAdapter(url),
        )
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        initial_calls = calls
        block = True
        refreshes = [asyncio.create_task(pool.refresh_topology()) for _ in range(2)]
        await asyncio.wait_for(entered.wait(), 1)
        lock_available = True
        try:
            await asyncio.wait_for(pool._lock.acquire(), 0.05)
        except asyncio.TimeoutError:
            lock_available = False
        else:
            pool._lock.release()
        release.set()
        await asyncio.gather(*refreshes)

        assert lock_available is True
        assert calls - initial_calls == 1
        await pool.close()

    asyncio.run(run())


def test_sync_retired_endpoint_closes_when_it_becomes_idle_without_refresh(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.closed = False
            self._active = [0]
            self._idle_listeners: list[Any] = []

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            return b"OK"

        def add_idle_listener(self, listener: Any) -> None:
            self._idle_listeners.append(listener)

        def remove_idle_listener(self, listener: Any) -> None:
            if listener in self._idle_listeners:
                self._idle_listeners.remove(listener)

        def become_idle(self) -> None:
            self._active[0] = 0
            for listener in tuple(self._idle_listeners):
                listener()

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
    retired._active[0] = 1

    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    assert retired.closed is False

    retired.become_idle()
    assert retired.closed is True
    pool.close()


def test_sync_failed_retired_cleanup_retries_on_next_topology_refresh(
    monkeypatch: Any,
) -> None:
    current = {"host": "leader-1.local", "port": 6391, "epoch": 1}

    class FakeAdapter:
        def __init__(self, url: str) -> None:
            self.url = url
            self.close_calls = 0
            self.fail_once = "leader-1.local" in url

        def execute_command(self, *args: Any) -> Any:
            if args[0] == "SHARDS":
                return _single_shard_topology(
                    current["host"], current["port"], route_epoch=current["epoch"]
                )
            return b"OK"

        def close(self) -> None:
            self.close_calls += 1
            if self.fail_once and self.close_calls == 1:
                raise RuntimeError("retired cleanup failed")

    monkeypatch.setattr(
        ProtocolAdapter,
        "from_url",
        lambda url, **_kwargs: FakeAdapter(url),
    )
    pool = protocol_module.TopologyProtocolAdapterPool(
        ["ferric://seed.local:6388"], endpoint_policy="any"
    )
    retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

    current.update(host="leader-2.local", port=6392, epoch=2)
    pool.refresh_topology()
    current.update(host="leader-3.local", port=6393, epoch=3)
    pool.refresh_topology()
    calls_before_pool_close = retired.close_calls
    pool.close()

    assert calls_before_pool_close == 2
    assert retired.close_calls == 2


def test_async_retired_endpoint_closes_when_it_becomes_idle_without_refresh(
    monkeypatch: Any,
) -> None:
    async def run() -> None:
        current = {"host": "leader-1.local", "port": 6391, "epoch": 1}

        class FakeAdapter:
            def __init__(self, url: str) -> None:
                self.url = url
                self.closed = False
                self._active = [0]
                self._idle_listeners: list[Any] = []

            async def execute_command(self, *args: Any) -> Any:
                if args[0] == "SHARDS":
                    return _single_shard_topology(
                        current["host"], current["port"], route_epoch=current["epoch"]
                    )
                return b"OK"

            def add_idle_listener(self, listener: Any) -> None:
                self._idle_listeners.append(listener)

            def remove_idle_listener(self, listener: Any) -> None:
                if listener in self._idle_listeners:
                    self._idle_listeners.remove(listener)

            def become_idle(self) -> None:
                self._active[0] = 0
                for listener in tuple(self._idle_listeners):
                    listener()

            async def close(self) -> None:
                self.closed = True

        monkeypatch.setattr(
            AsyncProtocolAdapter,
            "from_url",
            lambda url, **_kwargs: FakeAdapter(url),
        )
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))
        retired._active[0] = 1

        current.update(host="leader-2.local", port=6392, epoch=2)
        await pool.refresh_topology()
        assert retired.closed is False

        retired.become_idle()
        await asyncio.sleep(0)
        assert retired.closed is True
        await pool.close()

    asyncio.run(run())


def test_async_failed_retired_cleanup_retries_on_next_topology_refresh(
    monkeypatch: Any,
) -> None:
    async def run() -> None:
        current = {"host": "leader-1.local", "port": 6391, "epoch": 1}

        class FakeAdapter:
            def __init__(self, url: str) -> None:
                self.url = url
                self.close_calls = 0
                self.fail_once = "leader-1.local" in url

            async def execute_command(self, *args: Any) -> Any:
                if args[0] == "SHARDS":
                    return _single_shard_topology(
                        current["host"], current["port"], route_epoch=current["epoch"]
                    )
                return b"OK"

            async def close(self) -> None:
                self.close_calls += 1
                if self.fail_once and self.close_calls == 1:
                    raise RuntimeError("retired cleanup failed")

        monkeypatch.setattr(
            AsyncProtocolAdapter,
            "from_url",
            lambda url, **_kwargs: FakeAdapter(url),
        )
        pool = protocol_module.AsyncTopologyProtocolAdapterPool(
            ["ferric://seed.local:6388"], endpoint_policy="any"
        )
        await pool.refresh_topology()
        retired = pool._adapter_for_endpoint(next(iter(pool.topology.endpoints.values())))

        current.update(host="leader-2.local", port=6392, epoch=2)
        await pool.refresh_topology()
        current.update(host="leader-3.local", port=6393, epoch=3)
        await pool.refresh_topology()
        while pool._cleanup_tasks:
            await asyncio.gather(*tuple(pool._cleanup_tasks))
        calls_before_pool_close = retired.close_calls
        await pool.close()

        assert calls_before_pool_close == 2
        assert retired.close_calls == 2

    asyncio.run(run())


def test_sync_topology_subscription_updates_cannot_finish_stale() -> None:
    first_entered = threading.Event()
    release_first = threading.Event()
    second_applied = threading.Event()
    applied: list[str] = []

    class Adapter:
        def subscribe_flow_wake(self, name: str) -> str:
            if name == "A":
                first_entered.set()
                assert release_first.wait(timeout=1)
            else:
                second_applied.set()
            applied.append(name)
            return name

    pool = object.__new__(TopologyProtocolAdapterPool)
    pool._lock = threading.RLock()
    pool._subscription_update_lock = threading.Lock()
    pool._closed = False
    pool._subscription_registry = protocol_module.FlowWakeSubscriptionRegistry()
    pool._subscription_generation = 0
    pool._adapters = {"node": Adapter()}
    errors: list[BaseException] = []

    def subscribe(name: str) -> None:
        try:
            pool.subscribe_flow_wake(name)
        except BaseException as exc:
            errors.append(exc)

    first = threading.Thread(target=subscribe, args=("A",))
    second = threading.Thread(target=subscribe, args=("B",))
    first.start()
    assert first_entered.wait(timeout=1)
    second.start()
    second_applied.wait(timeout=0.05)
    release_first.set()
    first.join(timeout=1)
    second.join(timeout=1)

    desired = pool._subscription_registry.snapshot()[0].args[0]
    assert errors == []
    assert first.is_alive() is False
    assert second.is_alive() is False
    assert desired == "B"
    assert applied[-1] == "B"


def test_async_topology_subscription_updates_cannot_finish_stale() -> None:
    async def run() -> None:
        first_entered = asyncio.Event()
        release_first = asyncio.Event()
        applied: list[str] = []

        class Adapter:
            async def subscribe_flow_wake(self, name: str) -> str:
                if name == "A":
                    first_entered.set()
                    await release_first.wait()
                applied.append(name)
                return name

        pool = object.__new__(protocol_module.AsyncTopologyProtocolAdapterPool)
        pool._closed = False
        pool._subscription_update_lock = asyncio.Lock()
        pool._subscription_registry = protocol_module.FlowWakeSubscriptionRegistry()
        pool._subscription_generation = 0
        pool._adapters = {"node": Adapter()}

        first = asyncio.create_task(pool.subscribe_flow_wake("A"))
        await asyncio.wait_for(first_entered.wait(), timeout=1)
        second = asyncio.create_task(pool.subscribe_flow_wake("B"))
        await asyncio.sleep(0)
        release_first.set()
        await asyncio.gather(first, second)

        desired = pool._subscription_registry.snapshot()[0].args[0]
        assert desired == "B"
        assert applied[-1] == "B"

    asyncio.run(run())


def test_pipeline_targets_are_running_after_wire_submission() -> None:
    adapter = object.__new__(ProtocolAdapter)
    source: Future[ProtocolResponse] = Future()
    adapter._submit_pipeline_request = lambda *args, **kwargs: source
    decode_entered = threading.Event()
    release_decode = threading.Event()

    def decode(item: Any) -> Any:
        if item == b"first":
            decode_entered.set()
            assert release_decode.wait(timeout=1)
        return item

    adapter._batch_item_value = decode
    targets = adapter._submit_pipeline([ProtocolCommand(1, {}), ProtocolCommand(2, {})])

    completion = threading.Thread(
        target=source.set_result,
        args=(ProtocolResponse(1, 1, 1, 0, 0, [b"first", b"second"]),),
    )
    completion.start()
    assert decode_entered.wait(timeout=1)
    assert targets[0].cancel() is False
    release_decode.set()
    completion.join(timeout=1)

    assert completion.is_alive() is False
    assert targets[0].result(timeout=1) == b"first"
    assert targets[1].result(timeout=1) == b"second"


def test_pipeline_completion_publishes_all_siblings_before_callbacks() -> None:
    adapter = object.__new__(ProtocolAdapter)
    source: Future[ProtocolResponse] = Future()
    adapter._submit_pipeline_request = lambda *args, **kwargs: source
    targets = adapter._submit_pipeline([ProtocolCommand(1, {}), ProtocolCommand(2, {})])
    observations: list[tuple[bool, Any]] = []

    def inspect_sibling(_future: Future[Any]) -> None:
        observations.append(
            (
                targets[1].done(),
                targets[1].result(timeout=0.05) if targets[1].done() else None,
            )
        )

    targets[0].add_done_callback(inspect_sibling)
    source.set_result(
        ProtocolResponse(
            1,
            1,
            1,
            0,
            0,
            [[b"ok", b"first"], [b"ok", b"second"]],
        )
    )

    assert observations == [(True, b"second")]


class _SyncEventAdapter:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self._listeners: list[Any] = []

    def add_event_listener(self, listener: Any) -> None:
        self._listeners.append(listener)

    def remove_event_listener(self, listener: Any) -> None:
        self._listeners.remove(listener)

    def emit(self, event: Any) -> None:
        self.events.append(event)
        for listener in tuple(self._listeners):
            listener()

    def wait_event(self, timeout: float | None = None) -> Any | None:
        del timeout
        return self.events.pop(0) if self.events else None

    def close(self) -> None:
        return None


def test_sync_pool_rechecks_buffered_events_after_session_waiter_is_served() -> None:
    adapters = [_SyncEventAdapter(), _SyncEventAdapter()]
    pool = ProtocolAdapterPool(adapters)  # type: ignore[arg-type]
    first_index, _ = pool._acquire_adapter()
    second_index, _ = pool._acquire_adapter()
    acquired_session: list[Any] = []
    session_ready = threading.Event()

    def acquire_session() -> None:
        acquired_session.append(pool.acquire_session())
        session_ready.set()

    session_thread = threading.Thread(target=acquire_session, daemon=True)
    session_thread.start()
    deadline = time.monotonic() + 1
    while pool._session_waiters != 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert pool._session_waiters == 1

    adapters[second_index].emit("ready")
    event_result: list[Any] = []
    event_ready = threading.Event()

    def wait_event() -> None:
        event_result.append(pool.wait_event(timeout=1))
        event_ready.set()

    event_thread = threading.Thread(target=wait_event, daemon=True)
    event_thread.start()
    deadline = time.monotonic() + 1
    while pool._event_ready.is_set() and time.monotonic() < deadline:
        time.sleep(0.001)
    pool._release_adapter(first_index)

    try:
        assert session_ready.wait(timeout=1)
        assert event_ready.wait(timeout=0.2)
        assert event_result == ["ready"]
    finally:
        if acquired_session:
            acquired_session[0].close()
        pool._release_adapter(second_index)
        pool.close()
        session_thread.join(timeout=1)
        event_thread.join(timeout=1)


class _AsyncEventAdapter:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self._listeners: list[Any] = []

    def add_event_listener(self, listener: Any) -> None:
        self._listeners.append(listener)

    def remove_event_listener(self, listener: Any) -> None:
        self._listeners.remove(listener)

    def emit(self, event: Any) -> None:
        self.events.append(event)
        for listener in tuple(self._listeners):
            listener()

    async def wait_event(self, timeout: float | None = None) -> Any | None:
        del timeout
        return self.events.pop(0) if self.events else None

    async def close(self) -> None:
        return None


def test_async_pool_rechecks_buffered_events_after_session_waiter_is_served() -> None:
    async def run() -> None:
        adapters = [_AsyncEventAdapter(), _AsyncEventAdapter()]
        pool = AsyncProtocolAdapterPool(adapters)  # type: ignore[arg-type]
        first_index, _ = await pool._acquire_adapter()
        second_index, _ = await pool._acquire_adapter()
        session_task = asyncio.create_task(pool.acquire_session())
        await asyncio.sleep(0)
        assert pool._session_waiters == 1

        adapters[second_index].emit("ready")
        event_task = asyncio.create_task(pool.wait_event(timeout=1))
        await asyncio.sleep(0)
        assert pool._event_ready.is_set() is False
        await pool._release_adapter(first_index)

        session = await asyncio.wait_for(session_task, timeout=1)
        try:
            assert await asyncio.wait_for(event_task, timeout=0.2) == "ready"
        finally:
            await session.close()
            await pool._release_adapter(second_index)
            await pool.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "limit_name",
    ["max_inflight_requests", "max_pending_request_bytes", "max_batch_items"],
)
@pytest.mark.parametrize("invalid", [0, -1, True])
def test_protocol_pending_limits_require_positive_integers(
    monkeypatch: Any,
    limit_name: str,
    invalid: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    with pytest.raises(ValueError, match=limit_name):
        ProtocolAdapter(heartbeat_interval=None, **{limit_name: invalid})
    with pytest.raises(ValueError, match=limit_name):
        AsyncProtocolAdapter(heartbeat_interval=None, **{limit_name: invalid})


def test_sync_protocol_bounds_inflight_request_count(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_inflight_requests=2,
    )
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)

    first_id, first = adapter._submit_request(protocol_module._OP_GET, 1, b"a")
    second_id, second = adapter._submit_request(protocol_module._OP_GET, 1, b"b")
    assert adapter.pending_request_count == 2
    with pytest.raises(FerricStoreError, match="max_inflight_requests"):
        adapter._submit_request(protocol_module._OP_GET, 1, b"c")

    adapter._discard_pending_request(first_id, expected_future=first)
    third_id, third = adapter._submit_request(protocol_module._OP_GET, 1, b"c")
    assert adapter.pending_request_count == 2

    adapter._discard_pending_request(second_id, expected_future=second)
    adapter._discard_pending_request(third_id, expected_future=third)
    adapter.close()


def test_sync_protocol_bounds_total_pending_request_bytes(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(
        timeout=None,
        heartbeat_interval=None,
        max_pending_request_bytes=64,
    )
    adapter._sock = object()  # type: ignore[assignment]
    sent: list[Any] = []
    monkeypatch.setattr(
        protocol_sync_batch_module,
        "_send_frames",
        lambda *args, **kwargs: sent.append(args),
    )

    with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
        adapter._submit_request(protocol_module._OP_SET, 1, b"x" * 65)

    assert sent == []
    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0
    assert adapter._sock is not None
    adapter._sock = None
    adapter.close()


def test_sync_submitted_request_expires_at_adapter_timeout(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.02, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)

    _request_id, response = adapter._submit_request(protocol_module._OP_GET, 1, b"key")
    with pytest.raises(FerricStoreError, match="protocol request timed out"):
        response.result(timeout=1)

    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0
    adapter.close()


def test_sync_explicit_unbounded_blocking_request_does_not_gain_submit_deadline(
    monkeypatch: Any,
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=0.02, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)
    errors: list[BaseException] = []

    def request() -> None:
        try:
            adapter._request(protocol_module._OP_GET, 1, b"key", timeout=None)
        except BaseException as exc:
            errors.append(exc)

    caller = threading.Thread(target=request)
    caller.start()
    deadline = time.monotonic() + 1
    while adapter.pending_request_count != 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert adapter.pending_request_count == 1

    time.sleep(0.05)
    try:
        assert caller.is_alive()
        assert errors == []
    finally:
        adapter._fail_pending(FerricStoreError("test cleanup"))
        caller.join(timeout=1)
        adapter.close()


def test_sync_deadline_scheduler_compacts_completed_request_churn(monkeypatch: Any) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda self: None)
    adapter = ProtocolAdapter(timeout=60, heartbeat_interval=None)
    monkeypatch.setattr(adapter, "_send", lambda *args, **kwargs: None)

    for _ in range(5_000):
        request_id, response = adapter._submit_request(protocol_module._OP_GET, 1, b"key")
        adapter._discard_pending_request(request_id, expected_future=response)

    assert adapter.pending_request_count == 0
    assert adapter.pending_request_bytes == 0
    assert len(adapter._deadline_scheduler._heap) < 64
    adapter.close()


def test_async_protocol_bounds_inflight_request_count() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            max_inflight_requests=2,
        )

        async def connected() -> None:
            return None

        async def sent(*_args: Any, **_kwargs: Any) -> None:
            return None

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        adapter._send = sent  # type: ignore[method-assign]
        first = asyncio.create_task(
            adapter._request_without_timeout(protocol_module._OP_GET, 1, b"a")
        )
        second = asyncio.create_task(
            adapter._request_without_timeout(protocol_module._OP_GET, 1, b"b")
        )
        await asyncio.sleep(0)
        assert adapter.pending_request_count == 2

        with pytest.raises(FerricStoreError, match="max_inflight_requests"):
            await adapter._request_without_timeout(protocol_module._OP_GET, 1, b"c")

        adapter._fail_pending(FerricStoreError("test cleanup"))
        await asyncio.gather(first, second, return_exceptions=True)
        assert adapter.pending_request_count == 0
        assert adapter.pending_request_bytes == 0
        await adapter.close()

    asyncio.run(run())


def test_async_protocol_bounds_total_pending_request_bytes() -> None:
    async def run() -> None:
        adapter = AsyncProtocolAdapter(
            timeout=None,
            heartbeat_interval=None,
            max_pending_request_bytes=64,
        )

        class Writer:
            def __init__(self) -> None:
                self.writes: list[Any] = []

            def write(self, frame: bytes) -> None:
                self.writes.append(frame)

            async def drain(self) -> None:
                return None

        writer = Writer()
        adapter._writer = writer  # type: ignore[assignment]

        async def connected() -> None:
            return None

        adapter._ensure_connected = connected  # type: ignore[method-assign]
        with pytest.raises(FerricStoreError, match="max_pending_request_bytes"):
            await adapter._request_without_timeout(protocol_module._OP_SET, 1, b"x" * 65)

        assert writer.writes == []
        assert adapter.pending_request_count == 0
        assert adapter.pending_request_bytes == 0
        assert adapter._writer is writer
        adapter._writer = None
        await adapter.close()

    asyncio.run(run())
