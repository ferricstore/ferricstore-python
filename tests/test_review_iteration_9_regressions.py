from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace
from typing import Any, ClassVar

import pytest

import ferricstore.protocol_commands as protocol_commands_module
import ferricstore.protocol_compact_budget as compact_budget_module
import ferricstore.protocol_flow_codec as flow_codec_module
import ferricstore.protocol_pipeline_raw as pipeline_raw_module
from ferricstore import AsyncFlowClient, FlowClient, RawCodec
from ferricstore.async_client_sessions import AsyncTransactionSession
from ferricstore.async_workflow_context import AsyncWorkflowEffect
from ferricstore.async_workflow_execution import handle_claimed_batch
from ferricstore.client_autobatch import AutobatchFlowClient
from ferricstore.client_sessions import TransactionSession
from ferricstore.commands import DataCommandsMixin
from ferricstore.errors import EffectAlreadyReservedError, InvalidCommandError
from ferricstore.legacy_worker import Worker
from ferricstore.lifecycle_core import RetryableResourceSet
from ferricstore.protocol_async import AsyncProtocolAdapter
from ferricstore.protocol_async_topology import AsyncTopologyProtocolAdapterPool
from ferricstore.protocol_commands import build_protocol_command
from ferricstore.protocol_constants import (
    _COMPACT_FLOW_RETRY_MANY_REQUEST,
    _FLAG_CUSTOM_PAYLOAD,
)
from ferricstore.protocol_lifecycle import PendingRequestCapacityError
from ferricstore.protocol_pipeline_codec import _expected_command_collection_items
from ferricstore.protocol_response_contracts import validate_response_cardinality
from ferricstore.protocol_sync import ProtocolAdapter
from ferricstore.protocol_sync_topology import TopologyProtocolAdapterPool
from ferricstore.topology_core import RouteKind, route_for_keys
from ferricstore.types import BudgetPolicy, ClaimedFlow, EffectResult
from ferricstore.workflow_models import WorkflowEffect
from ferricstore.workflow_types import complete


def test_retry_many_default_compact_request_uses_retry_marker() -> None:
    command = build_protocol_command(
        "FLOW.RETRY_MANY",
        "MIXED",
        "NOW",
        123,
        "RUN_AT",
        456,
        "ITEMS",
        "flow-1",
        "tenant-a",
        b"lease",
        7,
    )

    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)
    assert command.payload[0] == _COMPACT_FLOW_RETRY_MANY_REQUEST


@pytest.mark.parametrize("value", [None, b"old-value", "old-value"])
def test_set_scalar_success_responses_do_not_apply_collection_cardinality(
    value: object,
) -> None:
    args = ("SET", "key", b"value", "GET")
    command = build_protocol_command(*args)

    assert _expected_command_collection_items(args) is None
    validate_response_cardinality(command.opcode, value, None)


@pytest.mark.parametrize(
    ("options", "expected"),
    [
        (("EX", 3), {"ttl": 3_000}),
        (("PX", 7), {"ttl": 7}),
        (("EXAT", 11), {"exat": 11}),
        (("PXAT", 13), {"pxat": 13}),
        (("NX",), {"nx": True}),
        (("XX", "GET"), {"xx": True, "get": True}),
        (("KEEPTTL",), {"keepttl": True}),
    ],
)
def test_native_set_supports_validated_option_contract(
    options: tuple[object, ...], expected: dict[str, object]
) -> None:
    command = build_protocol_command("SET", "key", b"value", *options)

    assert command.payload == {"key": "key", "value": b"value", **expected}


@pytest.mark.parametrize(
    "options",
    [
        ("EX", 0),
        ("PX", -1),
        ("EX", 1.5),
        ("PX", True),
        ("NX", "XX"),
        ("EX", 1, "PX", 2),
        ("EXAT", 1, "KEEPTTL"),
        ("NX", "NX"),
    ],
)
def test_native_set_rejects_invalid_or_conflicting_options(options: tuple[object, ...]) -> None:
    with pytest.raises(InvalidCommandError):
        build_protocol_command("SET", "key", b"value", *options)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"ex": 0},
        {"px": 1.5},
        {"ex": 1, "px": 2},
        {"nx": True, "xx": True},
        {"exat": 1, "keepttl": True},
    ],
)
def test_public_set_rejects_invalid_options_before_io(kwargs: dict[str, Any]) -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def execute_command(self, *args: object) -> bytes:
            self.calls.append(args)
            return b"OK"

    executor = Executor()
    client = FlowClient(executor)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        client.set("key", b"value", **kwargs)

    assert executor.calls == []


@pytest.mark.parametrize(
    "args",
    [
        ("OPTIONS", "extra"),
        ("GET", "key", "extra"),
        ("LOCK", "key", "owner", 100, "extra"),
        ("HGETALL", "key", "extra"),
        ("LRANGE", "key", 0, 1, "extra"),
        ("ZSCORE", "key", "member", "extra"),
        ("CLUSTER.KEYSLOT", "key", "extra"),
    ],
)
def test_specialized_native_commands_reject_surplus_arguments(
    args: tuple[object, ...],
) -> None:
    with pytest.raises(InvalidCommandError, match="wrong number of arguments"):
        build_protocol_command(*args)


@pytest.mark.parametrize(
    "args",
    [
        ("MGET",),
        ("DEL",),
        ("MSET",),
        ("HSET", "hash"),
        ("SADD", "set"),
        ("ZADD", "zset"),
    ],
)
def test_specialized_native_commands_reject_missing_values(args: tuple[object, ...]) -> None:
    with pytest.raises(InvalidCommandError):
        build_protocol_command(*args)


def test_variable_arity_commands_skip_redundant_schema_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def redundant_validation(_name: str, _argument_count: int) -> None:
        raise AssertionError("variable-arity builder already validates its required arguments")

    monkeypatch.setattr(
        protocol_commands_module,
        "validate_specialized_command_arity",
        redundant_validation,
    )

    assert build_protocol_command("SET", "key", b"value").payload == {
        "key": "key",
        "value": b"value",
    }


@pytest.mark.parametrize(
    "args",
    [
        ("LRANGE", "key", 1.9, 2),
        ("LRANGE", "key", float("inf"), 2),
        ("LPOP", "key", True),
        ("ZADD", "key", float("nan"), "member"),
        ("ZADD", "key", float("inf"), "member"),
        ("HSET", "key", [], "value"),
        ("MGET", "\ud800"),
    ],
)
def test_invalid_native_values_raise_sdk_command_errors(args: tuple[object, ...]) -> None:
    with pytest.raises(InvalidCommandError):
        build_protocol_command(*args)


@pytest.mark.parametrize(
    ("adapter_type", "url", "tls"),
    [
        (ProtocolAdapter, "ferrics://store.example", False),
        (ProtocolAdapter, "ferric://store.example", True),
        (AsyncProtocolAdapter, "ferrics://store.example", False),
        (AsyncProtocolAdapter, "ferric://store.example", True),
    ],
)
def test_protocol_url_rejects_tls_override_conflicting_with_scheme(
    adapter_type: type[ProtocolAdapter] | type[AsyncProtocolAdapter],
    url: str,
    tls: bool,
) -> None:
    with pytest.raises(ValueError, match=r"TLS.*scheme|scheme.*TLS"):
        adapter_type.from_url(url, tls=tls)


class _XAddProbe(DataCommandsMixin):
    codec = RawCodec()

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def command(self, *args: object) -> bytes:
        self.calls.append(args)
        return b"stream-id"


def test_xadd_emits_nomkstream_and_approximate_trim_options() -> None:
    client = _XAddProbe()

    client.xadd(
        "stream",
        {"field": "value"},
        nomkstream=True,
        maxlen=100,
        approximate=True,
    )

    assert client.calls == [
        ("XADD", "stream", "NOMKSTREAM", "MAXLEN", "~", 100, "*", "field", b"value")
    ]


def test_xadd_rejects_unknown_and_conflicting_options() -> None:
    client = _XAddProbe()

    with pytest.raises(TypeError, match="unexpected"):
        client.xadd("stream", {"field": "value"}, typo=True)
    with pytest.raises(ValueError, match=r"maxlen.*minid|exactly one"):
        client.xadd("stream", {"field": "value"}, maxlen=10, minid="1-0")

    assert client.calls == []


def test_workflow_effect_does_not_replay_an_existing_reservation() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def effect_reserve(self, *_args: object, **_kwargs: object) -> EffectResult:
            self.calls.append("reserve")
            return EffectResult(
                status="confirmed",
                decision="already_reserved",
                external_id="charge-1",
            )

        def effect_confirm(self, *_args: object, **_kwargs: object) -> EffectResult:
            self.calls.append("confirm")
            return EffectResult(status="confirmed")

    client = Client()
    context = SimpleNamespace(
        client=client,
        id="flow-1",
        partition_key="tenant-a",
        lease_token=b"lease",
        fencing_token=1,
    )
    external_calls: list[str] = []
    effect = WorkflowEffect(
        context,  # type: ignore[arg-type]
        "charge",
        "payment.charge",
        operation_digest="charge:v1",
    )

    with pytest.raises(EffectAlreadyReservedError) as raised:
        effect.call(lambda: external_calls.append("called"))

    assert raised.value.reservation.external_id == "charge-1"
    assert external_calls == []
    assert client.calls == ["reserve"]


def test_workflow_effect_can_resolve_an_existing_reservation_without_replay() -> None:
    class Client:
        def effect_reserve(self, *_args: object, **_kwargs: object) -> EffectResult:
            return EffectResult(
                status="confirmed",
                decision="already_reserved",
                external_id="charge-1",
            )

    context = SimpleNamespace(
        client=Client(),
        id="flow-1",
        partition_key="tenant-a",
        lease_token=b"lease",
        fencing_token=1,
    )
    external_calls: list[str] = []
    effect = WorkflowEffect(
        context,  # type: ignore[arg-type]
        "charge",
        "payment.charge",
        operation_digest="charge:v1",
        replay=lambda reservation: reservation.external_id,
    )

    assert effect.call(lambda: external_calls.append("called")) == "charge-1"
    assert external_calls == []


def test_async_workflow_effect_does_not_replay_an_existing_reservation() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls: list[str] = []

        async def effect_reserve(self, *_args: object, **_kwargs: object) -> EffectResult:
            self.calls.append("reserve")
            return EffectResult(status="reserved", decision="already_reserved")

        async def effect_confirm(self, *_args: object, **_kwargs: object) -> EffectResult:
            self.calls.append("confirm")
            return EffectResult(status="confirmed")

    async def exercise() -> None:
        client = Client()
        context = SimpleNamespace(
            client=client,
            id="flow-1",
            partition_key="tenant-a",
            lease_token=b"lease",
            fencing_token=1,
        )
        external_calls: list[str] = []
        effect = AsyncWorkflowEffect(
            context,  # type: ignore[arg-type]
            "charge",
            "payment.charge",
            operation_digest="charge:v1",
        )

        with pytest.raises(EffectAlreadyReservedError):
            await effect.call(lambda: external_calls.append("called"))

        assert external_calls == []
        assert client.calls == ["reserve"]

    asyncio.run(exercise())


def test_async_budget_scope_failure_uses_configured_retry_policy() -> None:
    class Host:
        client = object()
        handlers: ClassVar[dict[str, Any]] = {"queued": lambda _ctx: complete(result=b"ok")}
        error_modes: ClassVar[dict[str, str]] = {}
        on_error = "retry"
        budget_policies: ClassVar[dict[str, BudgetPolicy]] = {
            "queued": BudgetPolicy(
                scope=lambda _ctx: (_ for _ in ()).throw(ValueError("scope failed")),
                amount=1,
            )
        }
        concurrency = 1
        state_modes: ClassVar[dict[str, str]] = {}

        def __init__(self) -> None:
            self.applied: list[object] = []

        @staticmethod
        def _merge_governance_attributes(value: object, _attributes: object) -> object:
            return value

        async def _apply_uniform(
            self, _state: str, _jobs: list[ClaimedFlow], outcome: object
        ) -> None:
            self.applied.append(outcome)

        @staticmethod
        def _uniform_partition_key(_jobs: list[ClaimedFlow]) -> None:
            return None

        @staticmethod
        def _job_mutation(_job: object, _outcome: object) -> object:
            raise AssertionError("single uniform outcome should use _apply_uniform")

    async def exercise() -> None:
        host = Host()
        job = ClaimedFlow(
            "flow-1",
            b"lease",
            1,
            type="jobs",
            state="queued",
            partition_key="tenant-a",
        )

        assert await handle_claimed_batch(host, "queued", [job]) == 1  # type: ignore[arg-type]
        assert len(host.applied) == 1
        assert host.applied[0].error == "scope failed"  # type: ignore[union-attr]

    asyncio.run(exercise())


def test_legacy_worker_stop_interrupts_long_idle_wait() -> None:
    class Workflow:
        _states: ClassVar[dict[str, object]] = {"queued": object()}

        def __init__(self) -> None:
            self.called = threading.Event()

        def run_once(self, *_args: object, **_kwargs: object) -> list[object]:
            self.called.set()
            return []

    workflow = Workflow()
    worker = Worker(
        workflow,  # type: ignore[arg-type]
        worker="worker-1",
        idle_sleep_s=10,
        max_idle_sleep_s=10,
    )
    thread = threading.Thread(target=worker.run_forever)
    thread.start()
    assert workflow.called.wait(1)

    started = time.monotonic()
    worker.stop()
    thread.join(0.25)

    assert not thread.is_alive()
    assert time.monotonic() - started < 0.25


def test_legacy_worker_rotates_states_when_every_batch_is_full() -> None:
    class Workflow:
        _states: ClassVar[dict[str, object]] = {"first": object(), "second": object()}

        def __init__(self) -> None:
            self.calls: list[str] = []

        def run_once(self, state: str, **_kwargs: object) -> list[object]:
            self.calls.append(state)
            return [object()]

    workflow = Workflow()
    worker = Worker(
        workflow,  # type: ignore[arg-type]
        worker="worker-1",
        states=["first", "second"],
        limit=1,
    )

    assert worker.run_once() == 2
    assert workflow.calls == ["first", "second"]


def test_autobatch_async_fallback_returns_future_before_io_completes() -> None:
    class BlockingClient:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()

        def create(self, *_args: object, **_kwargs: object) -> bytes:
            self.started.set()
            assert self.release.wait(1)
            return b"record"

    base = BlockingClient()
    client = AutobatchFlowClient(base, max_delay_ms=0)  # type: ignore[arg-type]
    try:
        started = time.monotonic()
        future = client.create_async("flow-1", type="jobs", return_record=True)
        elapsed = time.monotonic() - started

        assert elapsed < 0.1
        assert base.started.wait(1)
        assert not future.done()
        base.release.set()
        assert future.result(timeout=1) == b"record"
    finally:
        base.release.set()
        client.close()


def test_transaction_session_rejects_execute_or_discard_outside_active_context() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        def execute_command(self, *args: object) -> bytes:
            self.calls.append(args)
            return b"OK"

    executor = Executor()
    transaction = TransactionSession(FlowClient(executor))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="not active"):
        transaction.execute()
    with pytest.raises(RuntimeError, match="not active"):
        transaction.discard()

    assert executor.calls == []


def test_async_transaction_session_rejects_execute_or_discard_outside_active_context() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        async def execute_command(self, *args: object) -> bytes:
            self.calls.append(args)
            return b"OK"

    async def exercise() -> None:
        executor = Executor()
        transaction = AsyncTransactionSession(AsyncFlowClient(executor))  # type: ignore[arg-type]

        with pytest.raises(RuntimeError, match="not active"):
            await transaction.execute()
        with pytest.raises(RuntimeError, match="not active"):
            await transaction.discard()

        assert executor.calls == []

    asyncio.run(exercise())


class _UnexpectedEagerEncode(str):
    def encode(self, *_args: object, **_kwargs: object) -> bytes:
        raise AssertionError("oversized compact input was encoded before admission")


@pytest.mark.parametrize("adapter_type", [ProtocolAdapter, AsyncProtocolAdapter])
@pytest.mark.parametrize("command_name", ["MGET", "FLOW.VALUE.MGET"])
def test_direct_compact_commands_preflight_pending_budget_before_utf8_allocation(
    adapter_type: type[ProtocolAdapter] | type[AsyncProtocolAdapter],
    command_name: str,
) -> None:
    adapter = object.__new__(adapter_type)
    adapter.max_pending_request_bytes = 128
    adapter.compression = "none"
    oversized = _UnexpectedEagerEncode("x" * 1_024)

    with pytest.raises(PendingRequestCapacityError):
        adapter._build_protocol_command(command_name, oversized)


@pytest.mark.parametrize("adapter_type", [ProtocolAdapter, AsyncProtocolAdapter])
def test_direct_compact_commands_keep_fast_path_when_admitted(
    adapter_type: type[ProtocolAdapter] | type[AsyncProtocolAdapter],
) -> None:
    adapter = object.__new__(adapter_type)
    adapter.max_pending_request_bytes = 1_024
    adapter.compression = "none"

    command = adapter._build_protocol_command("MGET", "key-a", "key-b")

    assert command.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(command.payload, bytes)


@pytest.mark.parametrize(
    "args",
    [
        ("MGET", "key-a", "kéy-b"),
        ("MSET", "key-a", b"value-a", "key-b", "value-b"),
        (
            "FLOW.CREATE_MANY",
            "MIXED",
            "TYPE",
            "jobs",
            "STATE",
            "queued",
            "NOW",
            1,
            "RUN_AT",
            2,
            "INDEPENDENT",
            "true",
            "ITEMS",
            "flow-1",
            "tenant-a",
            b"payload",
        ),
        (
            "FLOW.CLAIM_DUE",
            "jobs",
            "WORKER",
            "worker-1",
            "LEASE_MS",
            30_000,
            "LIMIT",
            10,
            "PARTITIONS",
            2,
            "tenant-a",
            "tenant-b",
        ),
        (
            "FLOW.COMPLETE_MANY",
            "MIXED",
            "NOW",
            3,
            "ITEMS",
            "flow-1",
            "tenant-a",
            b"lease",
            7,
        ),
        (
            "FLOW.RETRY_MANY",
            "MIXED",
            "NOW",
            3,
            "RUN_AT",
            4,
            "ITEMS",
            "flow-1",
            "tenant-a",
            b"lease",
            7,
        ),
        (
            "FLOW.FAIL_MANY",
            "MIXED",
            "NOW",
            3,
            "ITEMS",
            "flow-1",
            "tenant-a",
            b"lease",
            7,
        ),
        (
            "FLOW.CANCEL_MANY",
            "MIXED",
            "NOW",
            3,
            "ITEMS",
            "flow-1",
            "tenant-a",
            7,
        ),
        (
            "FLOW.TRANSITION_MANY",
            "MIXED",
            "queued",
            "running",
            "NOW",
            3,
            "RUN_AT",
            4,
            "ITEMS",
            "flow-1",
            "tenant-a",
            7,
            None,
        ),
        ("FLOW.VALUE.MGET", "ref-a", "réf-b", "MAX_BYTES", 100),
        ("FLOW.LIST", "jobs", "COUNT", 10, "RETURN", "META"),
    ],
    ids=[
        "mget",
        "mset",
        "flow-create-many",
        "flow-claim-due",
        "flow-complete-many",
        "flow-retry-many",
        "flow-fail-many",
        "flow-cancel-many",
        "flow-transition-many",
        "flow-value-mget",
        "flow-list",
    ],
)
def test_compact_command_budget_matches_exact_wire_size(args: tuple[object, ...]) -> None:
    unbounded = build_protocol_command(*args)
    assert unbounded.flags == _FLAG_CUSTOM_PAYLOAD
    assert isinstance(unbounded.payload, bytes)
    wire_size = len(unbounded.payload)

    with compact_budget_module.compact_encoding_policy(
        enabled=True,
        max_payload_bytes=wire_size,
        pending_limit=wire_size,
    ):
        exact = build_protocol_command(*args)

    assert exact.payload == unbounded.payload
    with (
        compact_budget_module.compact_encoding_policy(
            enabled=True,
            max_payload_bytes=wire_size - 1,
            pending_limit=wire_size - 1,
        ),
        pytest.raises(PendingRequestCapacityError),
    ):
        build_protocol_command(*args)


@pytest.mark.parametrize(
    ("mode", "items"),
    [
        (7, [{"value": "välue", "now_ms": 1}]),
        (
            8,
            [
                {
                    "value": b"value",
                    "owner_flow_id": "flow-1",
                    "name": "result",
                    "partition_key": None,
                    "now_ms": 1,
                }
            ],
        ),
    ],
    ids=["anonymous-value", "owned-value"],
)
def test_compact_flow_value_put_budget_matches_exact_wire_size(
    mode: int,
    items: list[dict[str, object]],
) -> None:
    unbounded = flow_codec_module._compact_flow_value_put_payload(mode, items)
    assert unbounded is not None
    wire_size = len(unbounded)

    with compact_budget_module.compact_encoding_policy(
        enabled=True,
        max_payload_bytes=wire_size,
        pending_limit=wire_size,
    ):
        exact = flow_codec_module._compact_flow_value_put_payload(mode, items)

    assert exact == unbounded
    with (
        compact_budget_module.compact_encoding_policy(
            enabled=True,
            max_payload_bytes=wire_size - 1,
            pending_limit=wire_size - 1,
        ),
        pytest.raises(PendingRequestCapacityError),
    ):
        flow_codec_module._compact_flow_value_put_payload(mode, items)


def test_compact_same_value_budget_matches_exact_wire_size() -> None:
    keys = ["key-a", "kéy-b", b"key-c"]
    value = "välue"
    unbounded = pipeline_raw_module._compact_kv_set_keys_value_payload(keys, value)
    assert unbounded is not None
    wire_size = len(unbounded)

    with compact_budget_module.compact_encoding_policy(
        enabled=True,
        max_payload_bytes=wire_size,
        pending_limit=wire_size,
    ):
        exact = pipeline_raw_module._compact_kv_set_keys_value_payload(keys, value)

    assert exact == unbounded
    with (
        compact_budget_module.compact_encoding_policy(
            enabled=True,
            max_payload_bytes=wire_size - 1,
            pending_limit=wire_size - 1,
        ),
        pytest.raises(PendingRequestCapacityError),
    ):
        pipeline_raw_module._compact_kv_set_keys_value_payload(keys, value)


@pytest.mark.parametrize("encoder", ["keys", "pairs"])
def test_compact_kv_admission_scans_each_value_at_most_once(
    encoder: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = compact_budget_module._binary_wire_size
    calls = 0

    def tracked(value: object) -> int | None:
        nonlocal calls
        calls += 1
        return original(value)

    monkeypatch.setattr(compact_budget_module, "_binary_wire_size", tracked)
    monkeypatch.setattr(pipeline_raw_module, "_binary_wire_size", tracked, raising=False)
    values = tuple(f"value-{index}" for index in range(32))
    with compact_budget_module.compact_encoding_policy(
        enabled=True,
        max_payload_bytes=16_384,
        pending_limit=16_400,
    ):
        if encoder == "keys":
            payload = pipeline_raw_module._compact_kv_keys_payload(values, 2)
        else:
            payload = pipeline_raw_module._compact_kv_set_pairs_payload(values)

    assert payload is not None
    assert calls <= len(values)


@pytest.mark.parametrize("adapter_type", [ProtocolAdapter, AsyncProtocolAdapter])
def test_direct_zlib_commands_avoid_eager_uncompressed_compact_payloads(
    adapter_type: type[ProtocolAdapter] | type[AsyncProtocolAdapter],
) -> None:
    adapter = object.__new__(adapter_type)
    adapter.max_pending_request_bytes = 1_024
    adapter.compression = "zlib"

    command = adapter._build_protocol_command("MGET", "key-a", "key-b")

    assert command.flags == 0
    assert command.payload == {"keys": ["key-a", "key-b"]}


def test_cross_shard_routing_stops_at_the_first_mismatch() -> None:
    visited: list[str] = []

    def slot_for_key(key: str | bytes) -> int:
        assert isinstance(key, str)
        visited.append(key)
        return 0 if key == "first" else 1

    decision = route_for_keys(
        ["first", "second", *(f"unused-{index}" for index in range(10_000))],
        slot_for_key=slot_for_key,
    )

    assert decision.kind is RouteKind.CROSS_SHARD
    assert decision.slots == (0, 1)
    assert visited == ["first", "second"]


@pytest.mark.parametrize("adapter_type", [ProtocolAdapter, AsyncProtocolAdapter])
def test_default_tls_context_is_cached_across_reconnects(
    adapter_type: type[ProtocolAdapter] | type[AsyncProtocolAdapter],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = object()
    calls = 0

    def create_default_context() -> object:
        nonlocal calls
        calls += 1
        return sentinel

    monkeypatch.setattr("ssl.create_default_context", create_default_context)
    adapter = object.__new__(adapter_type)
    adapter.tls = True
    adapter.ssl_context = None
    adapter._default_ssl_context = None

    assert adapter._tls_context() is sentinel
    assert adapter._tls_context() is sentinel
    assert calls == 1


@pytest.mark.parametrize(
    "pool_type",
    [TopologyProtocolAdapterPool, AsyncTopologyProtocolAdapterPool],
)
def test_topology_command_planning_honors_endpoint_request_budget(
    pool_type: type[TopologyProtocolAdapterPool] | type[AsyncTopologyProtocolAdapterPool],
) -> None:
    pool = object.__new__(pool_type)
    pool._adapter_kwargs = {
        "max_pending_request_bytes": 128,
        "compression": "none",
    }
    oversized = _UnexpectedEagerEncode("x" * 1_024)

    with pytest.raises(PendingRequestCapacityError):
        pool._prepare_routed_command(("MGET", oversized))


@pytest.mark.parametrize("adapter_type", [ProtocolAdapter, AsyncProtocolAdapter])
def test_flow_many_batch_compaction_honors_pending_request_budget(
    adapter_type: type[ProtocolAdapter] | type[AsyncProtocolAdapter],
) -> None:
    adapter = object.__new__(adapter_type)
    adapter.max_pending_request_bytes = 128
    adapter.compression = "none"
    oversized = _UnexpectedEagerEncode("x" * 1_024)
    commands = [
        (
            "FLOW.CREATE",
            "flow-1",
            "TYPE",
            "jobs",
            "STATE",
            "queued",
            "NOW",
            1,
            "RUN_AT",
            1,
            "PAYLOAD",
            oversized,
        )
    ]

    with pytest.raises(PendingRequestCapacityError):
        adapter._compact_flow_many_payloads(commands, None)


@pytest.mark.parametrize("method", ["submit_mget", "submit_mset_same_value"])
def test_direct_bulk_submit_helpers_honor_pending_request_budget(method: str) -> None:
    adapter = object.__new__(ProtocolAdapter)
    adapter.max_pending_request_bytes = 128
    adapter.compression = "none"
    oversized = _UnexpectedEagerEncode("x" * 1_024)

    with pytest.raises(PendingRequestCapacityError):
        if method == "submit_mget":
            adapter.submit_mget([oversized])
        else:
            adapter.submit_mset_same_value([oversized], b"value")


def test_sync_retired_cleanup_retries_transient_failure_without_refresh() -> None:
    closed = threading.Event()

    class Adapter:
        def __init__(self) -> None:
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("transient close failure")
            closed.set()

    adapter = Adapter()
    pool = object.__new__(TopologyProtocolAdapterPool)
    pool._cleanup_adapters = RetryableResourceSet([adapter])
    pool._adapter_cleanup_lock = threading.Lock()
    pool._lock = threading.RLock()
    pool._close_adapters_snapshot = None
    pool._event_listener = lambda: None
    pool._closed = False

    try:
        pool._cleanup_retired_adapters([adapter])

        assert closed.wait(1)
        assert adapter.close_calls == 2
        assert not pool._cleanup_adapters.contains(adapter)
    finally:
        scheduler = getattr(pool, "_cleanup_retry_scheduler", None)
        if scheduler is not None:
            scheduler.close()


def test_async_retired_cleanup_retries_transient_failure_without_refresh() -> None:
    async def exercise() -> None:
        closed = asyncio.Event()

        class Adapter:
            def __init__(self) -> None:
                self.close_calls = 0

            async def close(self) -> None:
                self.close_calls += 1
                if self.close_calls == 1:
                    raise RuntimeError("transient close failure")
                closed.set()

        adapter = Adapter()
        pool = object.__new__(AsyncTopologyProtocolAdapterPool)
        pool._cleanup_adapters = RetryableResourceSet(())
        pool._cleanup_tasks = set()
        pool._cleanup_tasks_by_adapter = {}
        pool._cleanup_retry_requested = set()
        pool._event_listener = lambda: None
        pool._closed = False

        pool._schedule_adapter_cleanup(adapter)

        await asyncio.wait_for(closed.wait(), 1)
        await asyncio.gather(*tuple(pool._cleanup_tasks))
        assert adapter.close_calls == 2
        assert not pool._cleanup_adapters.contains(adapter)

    asyncio.run(exercise())
