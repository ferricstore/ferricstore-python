from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ferricstore import (
    AsyncFlowClient,
    AsyncQueueFlow,
    AsyncQueueFlowWorker,
    AsyncWorkflow,
    BackpressurePolicy,
    FlowClient,
    QueueFlowWorker,
    RetryPolicy,
    ValueConfig,
    Worker,
    WorkflowWorker,
)
from ferricstore.async_client_sessions import AsyncTransactionSession
from ferricstore.client_sessions import TransactionSession
from ferricstore.protocol_async import AsyncProtocolAdapter
from ferricstore.protocol_async_pool import AsyncProtocolAdapterPool
from ferricstore.protocol_async_topology import AsyncTopologyProtocolAdapterPool
from ferricstore.protocol_sync import ProtocolAdapter
from ferricstore.protocol_sync_pool import ProtocolAdapterPool
from ferricstore.protocol_sync_topology import TopologyProtocolAdapterPool


class _SyncExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> list[Any]:
        self.calls.append(args)
        return []


class _AsyncExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[Any, ...]] = []

    async def execute_command(self, *args: Any) -> list[Any]:
        self.calls.append(args)
        return []


class _SyncTransactionClient:
    def __init__(self) -> None:
        self.acquire_calls = 0
        self.closed = 0

    def _acquire_session_client(self, _keys: Any = ()) -> tuple[Any, bool]:
        self.acquire_calls += 1
        return self, True

    def multi(self) -> None:
        pass

    def transaction_exec(self) -> list[Any]:
        return []

    def discard(self) -> None:
        pass

    def close(self) -> None:
        self.closed += 1


class _AsyncTransactionClient:
    def __init__(self) -> None:
        self.acquire_calls = 0
        self.closed = 0

    async def _acquire_session_client(self, _keys: Any = ()) -> tuple[Any, bool]:
        self.acquire_calls += 1
        return self, True

    async def multi(self) -> None:
        pass

    async def transaction_exec(self) -> list[Any]:
        return []

    async def discard(self) -> None:
        pass

    async def close(self) -> None:
        self.closed += 1


@pytest.mark.parametrize("invalid", [True, 1.5, "2"])
def test_run_steps_many_rejects_non_integer_step_counts(invalid: Any) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="steps"):
        client.run_steps_many(
            ["flow-1"],
            type="email",
            steps=invalid,
            worker="worker-1",
        )

    assert executor.calls == []


@pytest.mark.parametrize("invalid", [True, 1.5, "2"])
def test_async_run_steps_many_rejects_non_integer_step_counts(invalid: Any) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match="steps"):
            await client.run_steps_many(
                ["flow-1"],
                type="email",
                steps=invalid,
                worker="worker-1",
            )

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize("invalid", [0, 1, "false", object()])
def test_boolean_command_options_reject_truthy_non_booleans(invalid: Any) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match="IDEMPOTENT"):
        client.create("flow-1", type="email", idempotent=invalid)

    assert executor.calls == []


@pytest.mark.parametrize("invalid", [0, 1, "false", object()])
def test_async_boolean_command_options_reject_truthy_non_booleans(invalid: Any) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match="IDEMPOTENT"):
            await client.create("flow-1", type="email", idempotent=invalid)

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"limit": True}, "limit"),
        ({"priority": -1}, "priority"),
        ({"now_ms": -1}, "now_ms"),
        ({"block_ms": -1}, "block_ms"),
        ({"reclaim_ratio": 101}, "reclaim_ratio"),
        ({"payload_max_bytes": -1}, "payload_max_bytes"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
        ({"states": "queued"}, "states"),
        ({"partition_keys": "partition-1"}, "partition_keys"),
        ({"values": "profile"}, "values"),
    ],
)
def test_claim_due_rejects_values_outside_the_kv_contract(
    kwargs: dict[str, Any], field: str
) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match=field):
        client.claim_due("email", worker="worker-1", **kwargs)

    assert executor.calls == []


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"limit": True}, "limit"),
        ({"priority": -1}, "priority"),
        ({"now_ms": -1}, "now_ms"),
        ({"block_ms": -1}, "block_ms"),
        ({"reclaim_ratio": 101}, "reclaim_ratio"),
        ({"payload_max_bytes": -1}, "payload_max_bytes"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
        ({"states": "queued"}, "states"),
        ({"partition_keys": "partition-1"}, "partition_keys"),
        ({"values": "profile"}, "values"),
    ],
)
def test_async_claim_due_rejects_values_outside_the_kv_contract(
    kwargs: dict[str, Any], field: str
) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match=field):
            await client.claim_due("email", worker="worker-1", **kwargs)

        assert executor.calls == []

    asyncio.run(run())


def test_transaction_session_rejects_reuse_after_releasing_affine_client() -> None:
    client = _SyncTransactionClient()
    transaction = TransactionSession(client)  # type: ignore[arg-type]

    with transaction:
        pass

    with pytest.raises(RuntimeError, match="cannot be reused"):
        transaction.__enter__()

    assert client.acquire_calls == 1
    assert client.closed == 1


def test_async_transaction_session_rejects_reuse_after_releasing_affine_client() -> None:
    async def run() -> None:
        client = _AsyncTransactionClient()
        transaction = AsyncTransactionSession(client)  # type: ignore[arg-type]

        async with transaction:
            pass

        with pytest.raises(RuntimeError, match="cannot be reused"):
            await transaction.__aenter__()

        assert client.acquire_calls == 1
        assert client.closed == 1

    asyncio.run(run())


@pytest.mark.parametrize("watch", ["tenant:{42}", b"tenant:{42}", ["tenant:{42}", ""]])
def test_transaction_watch_keys_reject_invalid_sequences(watch: Any) -> None:
    with pytest.raises(ValueError, match="watch"):
        TransactionSession(_SyncTransactionClient(), watch=watch)  # type: ignore[arg-type]


@pytest.mark.parametrize("watch", ["tenant:{42}", b"tenant:{42}", ["tenant:{42}", ""]])
def test_async_transaction_watch_keys_reject_invalid_sequences(watch: Any) -> None:
    with pytest.raises(ValueError, match="watch"):
        AsyncTransactionSession(_AsyncTransactionClient(), watch=watch)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"lease_ms": True}, "lease_ms"),
        ({"now_ms": -1}, "now_ms"),
        ({"retention_ttl_ms": 0}, "retention_ttl_ms"),
    ],
)
def test_run_steps_many_rejects_other_values_outside_the_kv_contract(
    kwargs: dict[str, Any], field: str
) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match=field):
        client.run_steps_many(
            ["flow-1"],
            type="email",
            steps=1,
            worker="worker-1",
            **kwargs,
        )

    assert executor.calls == []


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"lease_ms": True}, "lease_ms"),
        ({"now_ms": -1}, "now_ms"),
        ({"retention_ttl_ms": 0}, "retention_ttl_ms"),
    ],
)
def test_async_run_steps_many_rejects_other_values_outside_the_kv_contract(
    kwargs: dict[str, Any], field: str
) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match=field):
            await client.run_steps_many(
                ["flow-1"],
                type="email",
                steps=1,
                worker="worker-1",
                **kwargs,
            )

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"values": "profile"}, "values"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
    ],
)
def test_named_value_reads_reject_invalid_options(kwargs: dict[str, Any], field: str) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match=field):
        client.get("flow-1", **kwargs)

    assert executor.calls == []


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"values": "profile"}, "values"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
    ],
)
def test_async_named_value_reads_reject_invalid_options(kwargs: dict[str, Any], field: str) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match=field):
            await client.get("flow-1", **kwargs)

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"limit": True}, "limit"),
        ({"priority": -1}, "priority"),
        ({"now_ms": -1}, "now_ms"),
        ({"partition_keys": "partition-1"}, "partition_keys"),
        ({"payload": "false"}, "payload"),
        ({"payload_max_bytes": -1}, "payload_max_bytes"),
        ({"include_record": "false"}, "include_record"),
        ({"job_only": 0}, "job_only"),
        ({"include_record": False, "include_attributes": "false"}, "include_attributes"),
    ],
)
def test_reclaim_rejects_values_outside_the_kv_contract(kwargs: dict[str, Any], field: str) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match=field):
        client.reclaim("email", worker="worker-1", **kwargs)

    assert executor.calls == []


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"limit": True}, "limit"),
        ({"priority": -1}, "priority"),
        ({"now_ms": -1}, "now_ms"),
        ({"partition_keys": "partition-1"}, "partition_keys"),
        ({"payload": "false"}, "payload"),
        ({"payload_max_bytes": -1}, "payload_max_bytes"),
        ({"include_record": "false"}, "include_record"),
        ({"job_only": 0}, "job_only"),
        ({"include_record": False, "include_attributes": "false"}, "include_attributes"),
    ],
)
def test_async_reclaim_rejects_values_outside_the_kv_contract(
    kwargs: dict[str, Any], field: str
) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match=field):
            await client.reclaim("email", worker="worker-1", **kwargs)

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"priority": -1}, "priority"),
        ({"reclaim_expired": "false"}, "reclaim_expired"),
        ({"reclaim_ratio": 101}, "reclaim_ratio"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
    ],
)
def test_queue_workers_validate_claim_options_before_allocating_runtime_state(
    kwargs: dict[str, Any], field: str
) -> None:
    client = FlowClient(_SyncExecutor())

    with pytest.raises(ValueError, match=field):
        QueueFlowWorker(client, type="email", **kwargs)


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"lease_ms": 0}, "lease_ms"),
        ({"priority": -1}, "priority"),
        ({"reclaim_expired": "false"}, "reclaim_expired"),
        ({"reclaim_ratio": 101}, "reclaim_ratio"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
    ],
)
def test_async_queue_workers_validate_claim_options_before_allocating_runtime_state(
    kwargs: dict[str, Any], field: str
) -> None:
    client = AsyncFlowClient(_AsyncExecutor())

    with pytest.raises(ValueError, match=field):
        AsyncQueueFlowWorker(client, type="email", **kwargs)


@pytest.mark.parametrize(
    "field",
    [
        "complete_independent",
        "protocol_wake_hints",
        "scan_before_blocking",
        "fuse_complete_claim",
    ],
)
def test_queue_workers_reject_truthy_non_boolean_runtime_flags(field: str) -> None:
    client = FlowClient(_SyncExecutor())

    with pytest.raises(ValueError, match=field):
        QueueFlowWorker(client, type="email", **{field: "false"})


@pytest.mark.parametrize(
    "field",
    [
        "complete_independent",
        "protocol_wake_hints",
        "fuse_complete_claim",
        "auto_partitions",
        "close_client",
    ],
)
def test_async_queue_workers_reject_truthy_non_boolean_runtime_flags(field: str) -> None:
    client = AsyncFlowClient(_AsyncExecutor())

    with pytest.raises(ValueError, match=field):
        AsyncQueueFlowWorker(client, type="email", **{field: "false"})


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"priority": -1}, "priority"),
        ({"reclaim_expired": "false"}, "reclaim_expired"),
        ({"reclaim_ratio": 101}, "reclaim_ratio"),
    ],
)
def test_workflow_worker_validates_claim_options(kwargs: dict[str, Any], field: str) -> None:
    workflow = type("FakeWorkflow", (), {"type": "email", "_states": {"queued": object()}})()

    with pytest.raises(ValueError, match=field):
        WorkflowWorker(workflow, **kwargs)


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"priority": -1}, "priority"),
        ({"reclaim_expired": "false"}, "reclaim_expired"),
        ({"reclaim_ratio": 101}, "reclaim_ratio"),
    ],
)
def test_legacy_worker_validates_claim_options(kwargs: dict[str, Any], field: str) -> None:
    workflow = type("FakeWorkflow", (), {"_states": {"queued": object()}})()

    with pytest.raises(ValueError, match=field):
        Worker(workflow, worker="worker-1", **kwargs)


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"priority": -1}, "priority"),
        ({"value_max_bytes": -1}, "value_max_bytes"),
        ({"producer_loop_thread": "false"}, "producer_loop_thread"),
    ],
)
def test_async_workflow_validates_claim_and_runtime_options(
    kwargs: dict[str, Any], field: str
) -> None:
    client = AsyncFlowClient(_AsyncExecutor())

    with pytest.raises(ValueError, match=field):
        AsyncWorkflow(client, type="email", **kwargs)


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"value_max_bytes": -1}, "value_max_bytes"),
        ({"complete_independent": "false"}, "complete_independent"),
        ({"protocol_wake_hints": "false"}, "protocol_wake_hints"),
        ({"fuse_complete_claim": "false"}, "fuse_complete_claim"),
        ({"producer_loop_thread": "false"}, "producer_loop_thread"),
    ],
)
def test_async_queue_flow_validates_runtime_options(kwargs: dict[str, Any], field: str) -> None:
    client = AsyncFlowClient(_AsyncExecutor())

    with pytest.raises(ValueError, match=field):
        AsyncQueueFlow(client, type="email", **kwargs)


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"max_retries": -1}, "max_retries"),
        ({"max_retries": 1_001}, "max_retries"),
        ({"backoff": "quadratic"}, "backoff"),
        ({"base_ms": -1}, "base_ms"),
        ({"max_ms": 2_592_000_001}, "max_ms"),
        ({"jitter_pct": 101}, "jitter_pct"),
        ({"exhausted_to": ""}, "exhausted_to"),
        ({"exhausted_to": "running"}, "exhausted_to"),
    ],
)
def test_retry_policy_rejects_values_outside_the_kv_contract(
    kwargs: dict[str, Any], field: str
) -> None:
    with pytest.raises(ValueError, match=field):
        RetryPolicy(**kwargs)


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"value_max_bytes": -1}, "value_max_bytes"),
        ({"local_cache": "false"}, "local_cache"),
    ],
)
def test_value_config_rejects_invalid_hydration_options(kwargs: dict[str, Any], field: str) -> None:
    with pytest.raises(ValueError, match=field):
        ValueConfig(**kwargs)


@pytest.mark.parametrize("field", ["enabled", "shared"])
def test_backpressure_policy_rejects_truthy_non_boolean_flags(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        BackpressurePolicy(**{field: "false"})


@pytest.mark.parametrize("adapter", [ProtocolAdapter, AsyncProtocolAdapter])
def test_protocol_adapters_reject_truthy_non_boolean_tls(
    adapter: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ProtocolAdapter, "_ensure_connected", lambda _self: None)
    with pytest.raises(ValueError, match="tls"):
        adapter(tls="false")


@pytest.mark.parametrize("pool", [ProtocolAdapterPool, AsyncProtocolAdapterPool])
def test_protocol_pools_reject_truthy_non_boolean_ha_routing(
    pool: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    sentinel = object()
    monkeypatch.setattr(
        pool,
        "from_urls",
        classmethod(lambda _cls, _urls, **_kwargs: sentinel),
    )

    with pytest.raises(ValueError, match="ha_routing"):
        pool.from_url("ferric://127.0.0.1:6388", ha_routing="false")


@pytest.mark.parametrize("pool", [ProtocolAdapterPool, AsyncProtocolAdapterPool])
def test_protocol_pools_do_not_split_scalar_seed_urls(
    pool: type[Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    sentinel = object()
    monkeypatch.setattr(
        pool,
        "from_urls",
        classmethod(lambda _cls, _urls, **_kwargs: sentinel),
    )

    with pytest.raises(ValueError, match="seeds"):
        pool.from_url("ferric://127.0.0.1:6388", seeds="ferric://seed:6388")


@pytest.mark.parametrize(
    "pool",
    [TopologyProtocolAdapterPool, AsyncTopologyProtocolAdapterPool],
)
@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"warm_connections": "false"}, "warm_connections"),
        ({"tls": "false"}, "tls"),
        ({"trusted_hosts": "seed.example"}, "trusted_hosts"),
        ({"endpoint_validator": object()}, "endpoint_validator"),
    ],
)
def test_topology_pools_validate_security_and_runtime_options(
    pool: type[Any],
    kwargs: dict[str, Any],
    field: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(TopologyProtocolAdapterPool, "refresh_topology", lambda _self: None)
    with pytest.raises(ValueError, match=field):
        pool(["ferric://127.0.0.1:6388"], **kwargs)


@pytest.mark.parametrize(
    ("method", "args", "kwargs", "field"),
    [
        ("set", ("key", "value"), {"nx": "false"}, "NX"),
        ("set", ("key", "value"), {"encode": "false"}, "encode"),
        ("expire", ("key", 30), {"nx": "false"}, "NX"),
    ],
)
def test_kv_command_builders_reject_truthy_non_boolean_flags(
    method: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    field: str,
) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)

    with pytest.raises(ValueError, match=field):
        getattr(client, method)(*args, **kwargs)

    assert executor.calls == []


@pytest.mark.parametrize(
    ("method", "args", "kwargs", "field"),
    [
        ("set", ("key", "value"), {"nx": "false"}, "NX"),
        ("set", ("key", "value"), {"encode": "false"}, "encode"),
        ("expire", ("key", 30), {"nx": "false"}, "NX"),
    ],
)
def test_async_kv_command_builders_reject_truthy_non_boolean_flags(
    method: str,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    field: str,
) -> None:
    async def run() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)

        with pytest.raises(ValueError, match=field):
            await getattr(client, method)(*args, **kwargs)

        assert executor.calls == []

    asyncio.run(run())


@pytest.mark.parametrize("client", [FlowClient, AsyncFlowClient])
def test_clients_do_not_split_scalar_topology_url_collections(client: type[Any]) -> None:
    with pytest.raises(ValueError, match="urls must be a sequence"):
        client.from_urls("ferric://127.0.0.1:6388")
