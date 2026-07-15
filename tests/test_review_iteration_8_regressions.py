from __future__ import annotations

import asyncio
import zlib
from concurrent.futures import Future

import pytest

import ferricstore.protocol_codec as protocol_codec
import ferricstore.protocol_common as protocol_common
import ferricstore.topology_core as topology_core
from ferricstore import (
    AsyncFlowClient,
    AsyncQueueFlowWorker,
    FlowClient,
    QueueFlowWorker,
)
from ferricstore.protocol_async import AsyncProtocolAdapter
from ferricstore.protocol_codec import EncodedValueLimitError, encode_value
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_constants import (
    _FLAG_COMPRESSED,
    _HEADER,
    _OP_STARTUP,
    _OPCODES,
    ProtocolResponse,
)
from ferricstore.protocol_framing import ResponseIdentity
from ferricstore.protocol_sync import ProtocolAdapter
from ferricstore.topology_core import FlowWakeSubscriptionRegistry, RouteKind
from ferricstore.types import CreateItem
from ferricstore.workflow import Workflow, complete, state


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (
            ("FLOW.CREATE", "flow-1", "TYPE", "jobs", "MAX_ACTIVE_MS", 30_000),
            {"id": "flow-1", "type": "jobs", "max_active_ms": 30_000},
        ),
        (
            ("FLOW.CREATE", "flow-1", "TYPE", "jobs", "PAYLOAD_REF", "payload:1"),
            {"id": "flow-1", "type": "jobs", "payload_ref": "payload:1"},
        ),
        (
            ("FLOW.CREATE", "flow-1", "TYPE", "jobs", "RETENTION_TTL", 60_000),
            {"id": "flow-1", "type": "jobs", "retention_ttl_ms": 60_000},
        ),
        (
            ("FLOW.GET", "flow-1", "PAYLOAD", "MAXBYTES", 4_096),
            {"id": "flow-1", "payload": True, "payload_max_bytes": 4_096},
        ),
    ],
)
def test_native_flow_builder_accepts_current_kv_option_aliases(
    args: tuple[object, ...],
    expected: dict[str, object],
) -> None:
    command = build_protocol_command(*args)

    assert command.payload == expected


def test_oversized_non_ascii_string_rejects_before_character_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_character_scan(*_args: object) -> object:
        raise AssertionError("character scan ran after length already proved overflow")

    monkeypatch.setattr(protocol_codec, "range", unexpected_character_scan, raising=False)

    with pytest.raises(EncodedValueLimitError, match="exceeds max_bytes"):
        encode_value("éé", max_bytes=6)


def _frame_flags_and_body(frame: bytes) -> tuple[int, bytes]:
    _magic, _version, flags, _lane, _opcode, _request_id, body_size = _HEADER.unpack_from(frame)
    body = frame[_HEADER.size :]
    assert len(body) == body_size
    return flags, body


def test_sync_zlib_negotiation_starts_uncompressed_then_compresses_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RecordingSocket:
        def __init__(self) -> None:
            self.frame = bytearray()

        def send(self, value: object) -> int:
            chunk = bytes(value)
            self.frame.extend(chunk)
            return len(chunk)

        def shutdown(self, *_args: object) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(ProtocolAdapter, "_connect", lambda _self: None)
    adapter = ProtocolAdapter(
        compression="zlib",
        timeout=None,
        heartbeat_interval=None,
    )
    sock = RecordingSocket()
    adapter._sock = sock  # type: ignore[assignment]
    adapter._connection_ready = True
    try:
        for request_id, opcode, payload, expect_compressed in (
            (1, _OP_STARTUP, {"compression": "zlib"}, False),
            (2, _OPCODES["PING"], {}, True),
        ):
            future: Future[ProtocolResponse] = Future()
            adapter._register_pending_request(
                request_id,
                future,
                binding=(adapter._transport_generation, sock),  # type: ignore[arg-type]
                response_identity=ResponseIdentity(0, opcode, request_id),
            )
            adapter._send(opcode, 0, request_id, payload)
            flags, body = _frame_flags_and_body(bytes(sock.frame))
            assert bool(flags & _FLAG_COMPRESSED) is expect_compressed
            if expect_compressed:
                assert zlib.decompress(body) == encode_value(payload)
            else:
                assert body == encode_value(payload)
            adapter._discard_pending_request(request_id, expected_future=future)
            sock.frame.clear()
    finally:
        adapter.close()


def test_async_zlib_negotiation_starts_uncompressed_then_compresses_requests() -> None:
    class RecordingWriter:
        def __init__(self) -> None:
            self.frame = bytearray()

        def write(self, value: bytes) -> None:
            self.frame.extend(value)

        async def drain(self) -> None:
            return None

    async def exercise() -> None:
        adapter = AsyncProtocolAdapter(
            compression="zlib",
            timeout=None,
            heartbeat_interval=None,
            write_drain_bytes=0,
        )
        writer = RecordingWriter()
        adapter._writer = writer  # type: ignore[assignment]
        adapter._connection_ready = True
        try:
            for request_id, opcode, payload, expect_compressed in (
                (1, _OP_STARTUP, {"compression": "zlib"}, False),
                (2, _OPCODES["PING"], {}, True),
            ):
                adapter._reserve_pending_request(request_id)
                await adapter._send(opcode, 0, request_id, payload)
                flags, body = _frame_flags_and_body(bytes(writer.frame))
                assert bool(flags & _FLAG_COMPRESSED) is expect_compressed
                if expect_compressed:
                    assert zlib.decompress(body) == encode_value(payload)
                else:
                    assert body == encode_value(payload)
                adapter._release_pending_request(request_id)
                writer.frame.clear()
        finally:
            adapter._writer = None
            await adapter.close()

    asyncio.run(exercise())


def test_topology_subscription_registry_snapshots_mutable_filters() -> None:
    class ReconnectingAdapter:
        def __init__(self) -> None:
            self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

        def register_flow_wake_subscription(
            self,
            *args: object,
            **kwargs: object,
        ) -> None:
            self.calls.append((args, kwargs))

    states = ["queued"]
    partition_keys = ["tenant-a"]
    registry = FlowWakeSubscriptionRegistry()
    registry.remember(
        ("jobs",),
        {"states": states, "partition_keys": partition_keys},
    )

    states.append("mutated-after-subscribe")
    partition_keys.clear()
    adapter = ReconnectingAdapter()
    registry.register_for_reconnect(adapter)

    assert adapter.calls == [
        (
            ("jobs",),
            {"states": ["queued"], "partition_keys": ["tenant-a"]},
        )
    ]


def test_class_workflow_rejects_duplicate_state_handlers() -> None:
    class DuplicateStateWorkflow(Workflow):
        type = "jobs"

        @state("queued")
        def first(self, _ctx: object) -> object:
            return complete()

        @state("queued")
        def second(self, _ctx: object) -> object:
            return complete()

    with pytest.raises(ValueError, match=r"duplicate workflow state: 'queued'"):
        DuplicateStateWorkflow(object())  # type: ignore[arg-type]


def test_class_workflow_discovery_does_not_evaluate_unrelated_descriptors() -> None:
    class DescriptorWorkflow(Workflow):
        type = "jobs"

        @property
        def unrelated(self) -> object:
            raise AssertionError("workflow discovery evaluated an unrelated property")

        @state("queued")
        def queued(self, _ctx: object) -> object:
            return complete()

    workflow = DescriptorWorkflow(object())  # type: ignore[arg-type]

    assert set(workflow._states) == {"queued"}


def test_flow_priority_rejects_values_kv_cannot_index_before_sync_io() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def execute_command(self, *args: object) -> bytes:
            self.calls.append(args)
            return b"OK"

    executor = Executor()
    client = FlowClient(executor)  # type: ignore[arg-type]
    operations = (
        lambda: client.create("flow", type="jobs", priority=3, now_ms=1),
        lambda: client.claim_flows("jobs", worker="worker", priority=3),
        lambda: client.transition(
            "flow",
            from_state="queued",
            to_state="done",
            lease_token=b"lease",
            fencing_token=1,
            priority=3,
            now_ms=1,
        ),
        lambda: client.signal("flow", signal="ready", priority=3, now_ms=1),
        lambda: client.create_many(
            None,
            [CreateItem("flow")],
            type="jobs",
            priority=3,
            now_ms=1,
        ),
    )

    for operation in operations:
        with pytest.raises(ValueError, match="priority cannot exceed 2"):
            operation()

    assert executor.calls == []


def test_flow_priority_rejects_values_kv_cannot_index_before_async_io() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        async def execute_command(self, *args: object) -> bytes:
            self.calls.append(args)
            return b"OK"

    async def exercise() -> None:
        executor = Executor()
        client = AsyncFlowClient(executor)  # type: ignore[arg-type]
        operations = (
            lambda: client.create("flow", type="jobs", priority=3, now_ms=1),
            lambda: client.claim_flows("jobs", worker="worker", priority=3),
            lambda: client.transition(
                "flow",
                from_state="queued",
                to_state="done",
                lease_token=b"lease",
                fencing_token=1,
                priority=3,
                now_ms=1,
            ),
            lambda: client.signal("flow", signal="ready", priority=3, now_ms=1),
        )

        for operation in operations:
            with pytest.raises(ValueError, match="priority cannot exceed 2"):
                await operation()

        assert executor.calls == []

    asyncio.run(exercise())


def test_queue_workers_reject_priorities_kv_cannot_claim() -> None:
    class SyncExecutor:
        def execute_command(self, *_args: object) -> bytes:
            return b"OK"

    class AsyncExecutor:
        async def execute_command(self, *_args: object) -> bytes:
            return b"OK"

    with pytest.raises(ValueError, match="priority cannot exceed 2"):
        QueueFlowWorker(FlowClient(SyncExecutor()), type="jobs", priority=3)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="priority cannot exceed 2"):
        AsyncQueueFlowWorker(AsyncFlowClient(AsyncExecutor()), type="jobs", priority=3)  # type: ignore[arg-type]


def test_single_shard_routing_avoids_intermediate_tuple_materialization(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_tuple(*_args: object) -> object:
        raise AssertionError("single-shard routing materialized an intermediate tuple")

    monkeypatch.setattr(topology_core, "tuple", unexpected_tuple, raising=False)

    decision = topology_core.route_for_keys(
        ["key-a", object(), "key-b"],
        slot_for_key=lambda _key: 7,
    )

    assert decision.kind is RouteKind.SINGLE_SHARD
    assert decision.key == "key-a"
    assert decision.slots == (7,)


def test_empty_topology_does_not_copy_a_full_placeholder_slot_list(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_tuple(*_args: object) -> object:
        raise AssertionError("empty topology copied a full placeholder list")

    monkeypatch.setattr(protocol_common, "tuple", unexpected_tuple, raising=False)

    topology = protocol_common.RoutingTopology.empty()

    assert len(topology.slots) == protocol_common._ROUTE_SLOT_COUNT
    assert topology.slots[0] is None
    assert topology.slots[-1] is None
