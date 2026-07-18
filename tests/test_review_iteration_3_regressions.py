from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, BudgetPolicy, FlowClient, FlowStatePolicy, WorkerConfig
from ferricstore.async_producer import AsyncProducerLoop
from ferricstore.async_workflow_budget import AsyncWorkflowBudget
from ferricstore.protocol_async import AsyncProtocolAdapter
from ferricstore.protocol_async_pool import AsyncProtocolAdapterPool
from ferricstore.protocol_sync_pool import ProtocolAdapterPool
from ferricstore.workflow_models import WorkflowBudget


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"workers": 0}, "workers"),
        ({"concurrency": True}, "concurrency"),
        ({"command_connections": -1}, "command_connections"),
        ({"claim_connections": 1.5}, "claim_connections"),
        ({"batch_size": 0}, "batch_size"),
        ({"lease_ms": 0}, "lease_ms"),
        ({"priority": -1}, "priority"),
        ({"reclaim_expired": 1}, "reclaim_expired"),
        ({"reclaim_ratio": 101}, "reclaim_ratio"),
        ({"claim_values": "profile"}, "claim_values"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
        ({"block_ms": -1}, "block_ms"),
        ({"claim_scan_block_ms": -1}, "claim_scan_block_ms"),
        ({"idle_sleep_s": float("nan")}, "idle_sleep_s"),
        ({"max_idle_sleep_s": -1.0}, "max_idle_sleep_s"),
        ({"exception_policy": "ignore"}, "exception_policy"),
        ({"complete_independent": 1}, "complete_independent"),
        ({"claim_partition_batch_size": 0}, "claim_partition_batch_size"),
        ({"claim_drain_batches": 0}, "claim_drain_batches"),
        ({"claim_prefetch": -1}, "claim_prefetch"),
        ({"protocol_wake_hints": "true"}, "protocol_wake_hints"),
        ({"scan_before_blocking": 1}, "scan_before_blocking"),
        ({"complete_async_depth": -1}, "complete_async_depth"),
        ({"fuse_complete_claim": 1}, "fuse_complete_claim"),
        ({"apply_async_depth": -1}, "apply_async_depth"),
        ({"server_shards": 0}, "server_shards"),
        ({"server_shards": 1_025}, "server_shards"),
        ({"producer_loop_thread": 1}, "producer_loop_thread"),
        ({"empty_claim_cooldown_s": float("inf")}, "empty_claim_cooldown_s"),
        ({"partial_claim_cooldown_s": -0.1}, "partial_claim_cooldown_s"),
    ],
)
def test_worker_config_rejects_invalid_values_at_its_public_boundary(
    kwargs: dict[str, Any], field: str
) -> None:
    with pytest.raises(ValueError, match=field):
        WorkerConfig(**kwargs)


def test_worker_config_snapshots_mutable_claim_values() -> None:
    claim_values = ["profile"]

    config = WorkerConfig(claim_values=claim_values)
    claim_values.append("limits")

    assert config.claim_values == ("profile",)


@pytest.mark.parametrize("mode", ["serial", "", 1, True])
def test_flow_state_policy_rejects_invalid_modes_when_declared(mode: Any) -> None:
    with pytest.raises(ValueError, match="mode"):
        FlowStatePolicy(mode=mode)


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"scope": "", "amount": 1}, "scope"),
        ({"scope": 3, "amount": 1}, "scope"),
        ({"scope": "tenant-a", "amount": True}, "amount"),
        ({"scope": "tenant-a", "amount": 0}, "amount"),
        ({"scope": "tenant-a", "amount": 1, "limit": 0}, "limit"),
        ({"scope": "tenant-a", "amount": 1, "window_ms": 0}, "window_ms"),
        ({"scope": "tenant-a", "amount": 1, "usage_key": ""}, "usage_key"),
        ({"scope": "tenant-a", "amount": 1, "attribute_prefix": ""}, "attribute_prefix"),
    ],
)
def test_budget_policy_rejects_values_outside_the_kv_contract(
    kwargs: dict[str, Any], field: str
) -> None:
    with pytest.raises(ValueError, match=field):
        BudgetPolicy(**kwargs)


def test_async_pool_constructor_rolls_back_adapters_after_partial_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[Any] = []

    class Adapter:
        def __init__(self) -> None:
            self.closed = 0

        async def close(self) -> None:
            self.closed += 1

    def from_url(_url: str, **_kwargs: Any) -> Any:
        if created:
            raise RuntimeError("second adapter failed")
        adapter = Adapter()
        created.append(adapter)
        return adapter

    monkeypatch.setattr(AsyncProtocolAdapter, "from_url", staticmethod(from_url))

    with pytest.raises(RuntimeError, match="second adapter failed"):
        AsyncProtocolAdapterPool.from_url("ferric://localhost:6388", max_connections=2)

    assert created[0].closed == 1


@pytest.mark.parametrize("pool_type", [ProtocolAdapterPool, AsyncProtocolAdapterPool])
def test_pool_constructor_rolls_back_partial_event_listener_registration(
    pool_type: type[Any],
) -> None:
    class Adapter:
        def __init__(self, *, fail: bool = False) -> None:
            self.fail = fail
            self.listeners: list[Any] = []

        def add_event_listener(self, listener: Any) -> None:
            if self.fail:
                raise RuntimeError("listener registration failed")
            self.listeners.append(listener)

        def remove_event_listener(self, listener: Any) -> None:
            self.listeners.remove(listener)

    first = Adapter()
    second = Adapter(fail=True)

    with pytest.raises(RuntimeError, match="listener registration failed"):
        pool_type([first, second])

    assert first.listeners == []


def test_async_producer_startup_survives_caller_cancellation() -> None:
    async def run() -> None:
        factory_started = threading.Event()
        release_factory = threading.Event()
        clients: list[Any] = []

        class Client:
            def __init__(self) -> None:
                self.closed = 0

            async def close(self) -> None:
                self.closed += 1

        def client_factory(_url: str, **_kwargs: Any) -> Client:
            factory_started.set()
            if not release_factory.wait(timeout=1):
                raise RuntimeError("test factory was not released")
            client = Client()
            clients.append(client)
            return client

        producer = AsyncProducerLoop(
            "ferric://localhost:6388",
            client_kwargs={},
            client_factory=client_factory,
        )

        async def send(_client: Any) -> None:
            raise AssertionError("cancelled startup must not run the send callback")

        operation = asyncio.create_task(producer.run(send))
        assert await asyncio.to_thread(factory_started.wait, 1)
        operation.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation

        release_factory.set()
        await asyncio.wait_for(producer.close(), timeout=1)

        assert len(clients) == 1
        assert clients[0].closed == 1

    asyncio.run(run())


def test_limit_release_sends_exact_spend_reservation_ids() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args: Any) -> dict[str, Any]:
            self.calls.append(args)
            return {"released": 2}

    executor = Executor()
    client = FlowClient(executor)
    reservation_ids = ["flr1:1:batch:1", "flr1:1:batch:2"]
    try:
        assert client.limit_release(
            "tenant-a",
            shard_id=0,
            reservation_ids=reservation_ids,
        ) == {"released": 2}
    finally:
        client.close()

    reservation_ids.append("mutated-after-call")
    assert executor.calls == [
        (
            "FLOW.LIMIT.RELEASE",
            "tenant-a",
            "SHARD_ID",
            0,
            "RESERVATION_IDS",
            ["flr1:1:batch:1", "flr1:1:batch:2"],
        )
    ]


def test_limit_release_rejects_tokenless_amount_contract() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args: Any) -> dict[str, Any]:
            self.calls.append(args)
            return {"released": 2}

    executor = Executor()
    client = FlowClient(executor)
    try:
        with pytest.raises(TypeError):
            client.limit_release("tenant-a", shard_id=0, amount=2)  # type: ignore[call-arg]
    finally:
        client.close()

    assert executor.calls == []


def test_async_limit_release_sends_exact_spend_reservation_ids() -> None:
    async def run() -> None:
        class Executor:
            def __init__(self) -> None:
                self.calls: list[tuple[Any, ...]] = []

            async def execute_command(self, *args: Any) -> dict[str, Any]:
                self.calls.append(args)
                return {"released": 1}

        executor = Executor()
        client = AsyncFlowClient(executor)
        try:
            assert await client.limit_release(
                "tenant-a",
                shard_id=0,
                reservation_ids=["flr1:1:batch:1"],
            ) == {"released": 1}
        finally:
            await client.close()

        assert executor.calls == [
            (
                "FLOW.LIMIT.RELEASE",
                "tenant-a",
                "SHARD_ID",
                0,
                "RESERVATION_IDS",
                ["flr1:1:batch:1"],
            )
        ]

    asyncio.run(run())


def test_async_limit_release_rejects_tokenless_amount_contract() -> None:
    async def run() -> None:
        class Executor:
            def __init__(self) -> None:
                self.calls: list[tuple[Any, ...]] = []

            async def execute_command(self, *args: Any) -> dict[str, Any]:
                self.calls.append(args)
                return {"released": 2}

        executor = Executor()
        client = AsyncFlowClient(executor)
        try:
            with pytest.raises(TypeError):
                await client.limit_release(  # type: ignore[call-arg]
                    "tenant-a",
                    shard_id=0,
                    amount=2,
                )
        finally:
            await client.close()

        assert executor.calls == []

    asyncio.run(run())


def test_sync_keyed_session_request_falls_back_to_unkeyed_acquisition() -> None:
    class Session:
        def execute_command(self, *_args: Any) -> bytes:
            return b"OK"

        def close(self) -> None:
            pass

    class Executor:
        def __init__(self) -> None:
            self.acquisitions = 0

        def execute_command(self, *_args: Any) -> bytes:
            raise AssertionError("the parent executor must not receive session traffic")

        def acquire_session(self) -> Session:
            self.acquisitions += 1
            return Session()

    executor = Executor()
    client = FlowClient(executor)
    session_client, owned = client._acquire_session_client(["watched-key"])
    try:
        assert owned is True
        assert executor.acquisitions == 1
    finally:
        session_client.close()


def test_async_keyed_session_request_falls_back_to_unkeyed_acquisition() -> None:
    async def run() -> None:
        class Session:
            async def execute_command(self, *_args: Any) -> bytes:
                return b"OK"

            async def close(self) -> None:
                pass

        class Executor:
            def __init__(self) -> None:
                self.acquisitions = 0

            async def execute_command(self, *_args: Any) -> bytes:
                raise AssertionError("the parent executor must not receive session traffic")

            async def acquire_session(self) -> Session:
                self.acquisitions += 1
                return Session()

        executor = Executor()
        client = AsyncFlowClient(executor)
        session_client, owned = await client._acquire_session_client(["watched-key"])
        try:
            assert owned is True
            assert executor.acquisitions == 1
        finally:
            await session_client.close()

    asyncio.run(run())


def test_circuit_open_exposes_the_full_kv_rule_contract() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args: Any) -> dict[str, Any]:
            self.calls.append(args)
            return {"scope": "payment", "status": "open"}

    executor = Executor()
    client = FlowClient(executor)
    try:
        result = client.circuit_open(
            "payment",
            open_ms=10_000,
            failure_threshold=8,
            window_ms=60_000,
            min_calls=12,
            failure_rate_pct=25,
            latency_threshold_ms=500,
            error_classes=["timeout", "io", "timeout"],
            half_open_max_probes=3,
            half_open_success_threshold=2,
        )
    finally:
        client.close()

    assert result.scope == "payment"
    assert executor.calls == [
        (
            "FLOW.CIRCUIT.OPEN",
            "payment",
            "OPEN_MS",
            10_000,
            "FAILURE_THRESHOLD",
            8,
            "WINDOW_MS",
            60_000,
            "MIN_CALLS",
            12,
            "FAILURE_RATE_PCT",
            25,
            "LATENCY_THRESHOLD_MS",
            500,
            "ERROR_CLASSES",
            ["timeout", "io"],
            "HALF_OPEN_MAX_PROBES",
            3,
            "HALF_OPEN_SUCCESS_THRESHOLD",
            2,
        )
    ]


def test_async_circuit_open_exposes_the_full_kv_rule_contract() -> None:
    async def run() -> None:
        class Executor:
            def __init__(self) -> None:
                self.calls: list[tuple[Any, ...]] = []

            async def execute_command(self, *args: Any) -> dict[str, Any]:
                self.calls.append(args)
                return {"scope": "payment", "status": "open"}

        executor = Executor()
        client = AsyncFlowClient(executor)
        try:
            result = await client.circuit_open(
                "payment",
                window_ms=60_000,
                min_calls=12,
                failure_rate_pct=25,
                latency_threshold_ms=500,
                error_classes=["timeout"],
                half_open_max_probes=3,
                half_open_success_threshold=2,
            )
        finally:
            await client.close()

        assert result.scope == "payment"
        assert executor.calls == [
            (
                "FLOW.CIRCUIT.OPEN",
                "payment",
                "WINDOW_MS",
                60_000,
                "MIN_CALLS",
                12,
                "FAILURE_RATE_PCT",
                25,
                "LATENCY_THRESHOLD_MS",
                500,
                "ERROR_CLASSES",
                ["timeout"],
                "HALF_OPEN_MAX_PROBES",
                3,
                "HALF_OPEN_SUCCESS_THRESHOLD",
                2,
            )
        ]

    asyncio.run(run())


_INVALID_GOVERNANCE_CALLS: list[tuple[str, tuple[Any, ...], dict[str, Any], str]] = [
    ("schedule_create", ("",), {"target": {"type": "job"}}, "id"),
    ("schedule_create", ("schedule-1",), {"target": {}}, "target"),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "kind": "weekly"},
        "kind",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "at_ms": -1},
        "at_ms",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "delay_ms": -1},
        "delay_ms",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "start_at_ms": -1},
        "start_at_ms",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "every_ms": 0},
        "every_ms",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "cron": ""},
        "cron",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "kind": "cron", "cron": "* * * * *", "timezone": ""},
        "timezone",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "kind": "interval", "every_ms": 1, "overlap_policy": "wait"},
        "overlap_policy",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "overlap_retry_ms": 0},
        "overlap_retry_ms",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "kind": "interval", "every_ms": 1, "max_fires": 0},
        "max_fires",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "kind": "interval", "every_ms": 1, "end_at_ms": -1},
        "end_at_ms",
    ),
    (
        "schedule_create",
        ("schedule-1",),
        {"target": {"type": "job"}, "now_ms": -1},
        "now_ms",
    ),
    ("schedule_get", ("",), {}, "id"),
    ("schedule_fire", ("schedule-1",), {"now_ms": -1}, "now_ms"),
    ("schedule_pause", ("",), {}, "id"),
    ("schedule_resume", ("",), {}, "id"),
    ("schedule_delete", ("",), {}, "id"),
    ("schedule_fire_due", (), {"now_ms": -1}, "now_ms"),
    ("schedule_fire_due", (), {"worker": ""}, "worker"),
    ("schedule_fire_due", (), {"block_ms": -1}, "block_ms"),
    ("schedule_fire_due", (), {"limit": 0}, "limit"),
    ("schedule_list", (), {"kind": "weekly"}, "kind"),
    ("schedule_list", (), {"state": ""}, "state"),
    ("schedule_list", (), {"timezone": ""}, "timezone"),
    ("schedule_list", (), {"target_type": ""}, "target_type"),
    ("schedule_list", (), {"from_ms": -1}, "from_ms"),
    ("schedule_list", (), {"to_ms": -1}, "to_ms"),
    ("schedule_list", (), {"count": 0}, "count"),
    ("governance_ledger", ("flow-1",), {"limit": 0}, "limit"),
    ("governance_ledger", ("flow-1",), {"from_ms": -1}, "from_ms"),
    ("governance_ledger", ("flow-1",), {"to_ms": -1}, "to_ms"),
    ("approval_request", ("",), {"flow_id": "flow-1", "scope": "tenant"}, "id"),
    ("approval_request", ("a-1",), {"flow_id": "", "scope": "tenant"}, "flow_id"),
    ("approval_request", ("a-1",), {"flow_id": "flow-1", "scope": ""}, "scope"),
    (
        "approval_request",
        ("a-1",),
        {"flow_id": "flow-1", "scope": "tenant", "policy_hash": ""},
        "policy_hash",
    ),
    (
        "approval_request",
        ("a-1",),
        {"flow_id": "flow-1", "scope": "tenant", "policy_version": True},
        "policy_version",
    ),
    (
        "approval_request",
        ("a-1",),
        {"flow_id": "flow-1", "scope": "tenant", "timeout_ms": 0},
        "timeout_ms",
    ),
    (
        "approval_request",
        ("a-1",),
        {"flow_id": "flow-1", "scope": "tenant", "expires_at_ms": -1},
        "expires_at_ms",
    ),
    (
        "approval_request",
        ("a-1",),
        {"flow_id": "flow-1", "scope": "tenant", "now_ms": -1},
        "now_ms",
    ),
    ("approval_approve", ("a-1",), {"approver": ""}, "approver"),
    ("approval_reject", ("",), {"approver": "operator"}, "id"),
    ("approval_list", (), {"status": "expired"}, "status"),
    ("approval_list", (), {"scope": ""}, "scope"),
    ("approval_list", (), {"partition_key": ""}, "partition_key"),
    ("approval_list", (), {"flow_id": ""}, "flow_id"),
    ("approval_list", (), {"limit": 0}, "limit"),
    ("governance_overview", (), {"status": "expired"}, "status"),
    ("governance_overview", (), {"scope": ""}, "scope"),
    ("governance_overview", (), {"partition_key": ""}, "partition_key"),
    ("governance_overview", (), {"flow_id": ""}, "flow_id"),
    ("governance_overview", (), {"limit": 0}, "limit"),
    ("circuit_open", ("",), {}, "scope"),
    ("circuit_open", ("tenant",), {"open_ms": 0}, "open_ms"),
    ("circuit_open", ("tenant",), {"failure_threshold": 0}, "failure_threshold"),
    ("circuit_open", ("tenant",), {"window_ms": 0}, "window_ms"),
    ("circuit_open", ("tenant",), {"min_calls": 65}, "min_calls"),
    ("circuit_open", ("tenant",), {"failure_rate_pct": 101}, "failure_rate_pct"),
    ("circuit_open", ("tenant",), {"failure_threshold": 65}, "failure_threshold"),
    (
        "circuit_open",
        ("tenant",),
        {"failure_threshold": 65, "failure_rate_pct": 20},
        "min_calls",
    ),
    ("circuit_open", ("tenant",), {"latency_threshold_ms": 0}, "latency_threshold_ms"),
    ("circuit_open", ("tenant",), {"error_classes": "timeout"}, "error_classes"),
    ("circuit_open", ("tenant",), {"half_open_max_probes": 0}, "half_open_max_probes"),
    (
        "circuit_open",
        ("tenant",),
        {"half_open_success_threshold": 0},
        "half_open_success_threshold",
    ),
    ("circuit_open", ("tenant",), {"now_ms": -1}, "now_ms"),
    ("circuit_close", ("",), {}, "scope"),
    ("circuit_get", ("",), {}, "scope"),
    ("budget_reserve", ("", 1), {}, "scope"),
    ("budget_reserve", ("tenant", True), {}, "amount"),
    ("budget_reserve", ("tenant", 1), {"limit": 0}, "limit"),
    ("budget_reserve", ("tenant", 1), {"window_ms": 0}, "window_ms"),
    ("budget_reserve", ("tenant", 1), {"reservation_id": ""}, "reservation_id"),
    ("budget_reserve", ("tenant", 1), {"now_ms": -1}, "now_ms"),
    ("budget_commit", ("tenant", "", 1), {}, "reservation_id"),
    ("budget_commit", ("tenant", "r-1", -1), {}, "actual_amount"),
    ("budget_release", ("", "r-1"), {}, "scope"),
    ("budget_release", ("tenant", ""), {}, "reservation_id"),
    ("budget_get", ("",), {}, "scope"),
    ("budget_list", (), {"scope": ""}, "scope"),
    ("budget_list", (), {"partition_key": ""}, "partition_key"),
    ("budget_list", (), {"limit": 0}, "limit"),
    (
        "limit_lease",
        ("tenant",),
        {"shard_id": -1, "amount": 1, "ttl_ms": 1},
        "shard_id",
    ),
    (
        "limit_lease",
        ("tenant",),
        {"shard_id": 0, "amount": 1_001, "ttl_ms": 1},
        "amount",
    ),
    (
        "limit_lease",
        ("tenant",),
        {"shard_id": 0, "amount": 1, "ttl_ms": 0},
        "ttl_ms",
    ),
    (
        "limit_lease",
        ("tenant",),
        {"shard_id": 0, "amount": 1, "ttl_ms": None},
        "ttl_ms",
    ),
    (
        "limit_lease",
        ("tenant",),
        {"shard_id": 0, "amount": 1, "ttl_ms": 1, "limit": -1},
        "limit",
    ),
    ("limit_spend", ("",), {"shard_id": 0, "amount": 1}, "scope"),
    ("limit_spend", ("tenant",), {"shard_id": 0, "amount": 0}, "amount"),
    (
        "limit_release",
        ("tenant",),
        {"shard_id": 0, "reservation_ids": ["r-1", "r-1"]},
        "reservation_ids",
    ),
    ("limit_get", ("",), {}, "scope"),
    ("limit_list", (), {"scope": ""}, "scope"),
    ("limit_list", (), {"partition_key": ""}, "partition_key"),
    ("limit_list", (), {"limit": 0}, "limit"),
    ("limit_list", (), {"now_ms": -1}, "now_ms"),
]


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs", "field"),
    _INVALID_GOVERNANCE_CALLS,
)
def test_sync_governance_methods_reject_kv_invalid_values_before_network_io(
    method_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    field: str,
) -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *command: Any) -> None:
            self.calls.append(command)
            raise AssertionError("invalid governance input reached the executor")

    executor = Executor()
    client = FlowClient(executor)
    try:
        with pytest.raises(ValueError, match=field):
            getattr(client, method_name)(*args, **kwargs)
    finally:
        client.close()

    assert executor.calls == []


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs", "field"),
    _INVALID_GOVERNANCE_CALLS,
)
def test_async_governance_methods_reject_kv_invalid_values_before_network_io(
    method_name: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    field: str,
) -> None:
    async def run() -> None:
        class Executor:
            def __init__(self) -> None:
                self.calls: list[tuple[Any, ...]] = []

            async def execute_command(self, *command: Any) -> None:
                self.calls.append(command)
                raise AssertionError("invalid governance input reached the executor")

        executor = Executor()
        client = AsyncFlowClient(executor)
        try:
            with pytest.raises(ValueError, match=field):
                await getattr(client, method_name)(*args, **kwargs)
        finally:
            await client.close()

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize("budget_type", [WorkflowBudget, AsyncWorkflowBudget])
@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"scope": "", "amount": 1}, "scope"),
        ({"scope": "tenant", "amount": 0}, "amount"),
        ({"scope": "tenant", "amount": 1, "limit": 0}, "limit"),
        ({"scope": "tenant", "amount": 1, "window_ms": 0}, "window_ms"),
        ({"scope": "tenant", "amount": 1, "usage_key": ""}, "usage_key"),
        ({"scope": "tenant", "amount": 1, "attribute_prefix": ""}, "attribute_prefix"),
    ],
)
def test_workflow_budget_helpers_validate_before_context_entry(
    budget_type: type[Any], kwargs: dict[str, Any], field: str
) -> None:
    with pytest.raises(ValueError, match=field):
        budget_type(object(), **kwargs)
