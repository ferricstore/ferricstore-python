from __future__ import annotations

import asyncio
from concurrent.futures import Future
from typing import Any

from ferricstore import AsyncFlowClient, ClaimedFlow, FlowClient, FlowRecord, JsonCodec

_COMPACT_CLAIM = [[b"flow-1", b"tenant-1", b"lease-1", 7, b"ready", {b"tenant": b"acme"}]]
_RECORD_CLAIM = [
    {
        b"id": b"flow-1",
        b"type": b"order",
        b"state": b"running",
        b"run_state": b"ready",
        b"partition_key": b"tenant-1",
        b"lease_token": b"lease-1",
        b"fencing_token": 7,
        b"version": 2,
        b"payload": b'{"ok":true}',
        b"values": {b"order": b'{"number":42}'},
        b"value_refs": {b"order": {b"ref": b"order-ref"}},
        b"attributes": {b"tenant": b"acme"},
    }
]


def _response_for_args(args: tuple[Any, ...], record_response: Any, compact_response: Any) -> Any:
    if "RETURN" in args and args[args.index("RETURN") + 1] == "RECORDS":
        return record_response
    return compact_response


class _SyncClaimExecutor:
    def __init__(
        self,
        record_response: Any = _RECORD_CLAIM,
        compact_response: Any = _COMPACT_CLAIM,
    ) -> None:
        self.record_response = record_response
        self.compact_response = compact_response
        self.calls: list[tuple[Any, ...]] = []

    def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        return _response_for_args(args, self.record_response, self.compact_response)

    def submit_command(self, *args: Any) -> Future[Any]:
        future: Future[Any] = Future()
        try:
            future.set_result(self.execute_command(*args))
        except Exception as exc:
            future.set_exception(exc)
        return future


class _AsyncClaimExecutor:
    def __init__(
        self,
        record_response: Any = _RECORD_CLAIM,
        compact_response: Any = _COMPACT_CLAIM,
    ) -> None:
        self.record_response = record_response
        self.compact_response = compact_response
        self.calls: list[tuple[Any, ...]] = []

    async def execute_command(self, *args: Any) -> Any:
        self.calls.append(args)
        return _response_for_args(args, self.record_response, self.compact_response)


def test_sync_claim_due_requests_records_and_decodes_full_record() -> None:
    executor = _SyncClaimExecutor()
    client = FlowClient(executor, codec=JsonCodec())

    records = client.claim_due(
        "order",
        worker="worker-1",
        partition_key="tenant-1",
        include_record=True,
        payload=True,
        values=["order"],
        include_attributes=True,
        now_ms=100,
    )

    assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == "RECORDS"
    assert isinstance(records[0], FlowRecord)
    assert records[0].id == "flow-1"
    assert records[0].type == "order"
    assert records[0].state == "running"
    assert records[0].run_state == "ready"
    assert records[0].lease_token == b"lease-1"
    assert records[0].fencing_token == 7
    assert records[0].version == 2
    assert records[0].payload == {"ok": True}
    assert records[0].values == {"order": {"number": 42}}
    assert records[0].value_refs == {"order": {"ref": "order-ref"}}
    assert records[0].attributes == {"tenant": "acme"}


def test_sync_claim_due_decodes_compact_rows_for_claimed_flow_contract() -> None:
    executor = _SyncClaimExecutor()
    client = FlowClient(executor)

    jobs = client.claim_due(
        "order",
        worker="worker-1",
        partition_key="tenant-1",
        include_record=False,
        include_state=True,
        include_attributes=True,
        now_ms=100,
    )

    assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == ("JOBS_COMPACT_STATE_ATTRS")
    assert jobs == [
        ClaimedFlow(
            id="flow-1",
            partition_key="tenant-1",
            lease_token=b"lease-1",
            fencing_token=7,
            run_state="ready",
            attributes={"tenant": "acme"},
        )
    ]


def test_async_claim_due_requests_records_and_decodes_full_record() -> None:
    async def run() -> None:
        executor = _AsyncClaimExecutor()
        client = AsyncFlowClient(executor, codec=JsonCodec())

        records = await client.claim_due(
            "order",
            worker="worker-1",
            partition_key="tenant-1",
            include_record=True,
            payload=True,
            values=["order"],
            include_attributes=True,
            now_ms=100,
        )

        assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == "RECORDS"
        assert isinstance(records[0], FlowRecord)
        assert records[0].id == "flow-1"
        assert records[0].type == "order"
        assert records[0].state == "running"
        assert records[0].run_state == "ready"
        assert records[0].lease_token == b"lease-1"
        assert records[0].fencing_token == 7
        assert records[0].version == 2
        assert records[0].payload == {"ok": True}
        assert records[0].values == {"order": {"number": 42}}
        assert records[0].value_refs == {"order": {"ref": "order-ref"}}
        assert records[0].attributes == {"tenant": "acme"}

    asyncio.run(run())


def test_async_claim_due_decodes_compact_rows_for_claimed_flow_contract() -> None:
    async def run() -> None:
        executor = _AsyncClaimExecutor()
        client = AsyncFlowClient(executor)

        jobs = await client.claim_due(
            "order",
            worker="worker-1",
            partition_key="tenant-1",
            include_record=False,
            include_state=True,
            include_attributes=True,
            now_ms=100,
        )

        assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == (
            "JOBS_COMPACT_STATE_ATTRS"
        )
        assert jobs == [
            ClaimedFlow(
                id="flow-1",
                partition_key="tenant-1",
                lease_token=b"lease-1",
                fencing_token=7,
                run_state="ready",
                attributes={"tenant": "acme"},
            )
        ]

    asyncio.run(run())


def test_sync_reclaim_requests_records_and_decodes_full_record() -> None:
    executor = _SyncClaimExecutor()
    client = FlowClient(executor, codec=JsonCodec())

    records = client.reclaim(
        "order",
        worker="worker-1",
        partition_key="tenant-1",
        include_record=True,
        payload=True,
        values=["order"],
        now_ms=100,
    )

    assert executor.calls[0][0] == "FLOW.RECLAIM"
    assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == "RECORDS"
    assert isinstance(records[0], FlowRecord)
    assert records[0].payload == {"ok": True}
    assert records[0].values == {"order": {"number": 42}}


def test_async_reclaim_requests_records_and_decodes_full_record() -> None:
    async def run() -> None:
        executor = _AsyncClaimExecutor()
        client = AsyncFlowClient(executor, codec=JsonCodec())

        records = await client.reclaim(
            "order",
            worker="worker-1",
            partition_key="tenant-1",
            include_record=True,
            payload=True,
            values=["order"],
            now_ms=100,
        )

        assert executor.calls[0][0] == "FLOW.RECLAIM"
        assert executor.calls[0][executor.calls[0].index("RETURN") + 1] == "RECORDS"
        assert isinstance(records[0], FlowRecord)
        assert records[0].payload == {"ok": True}
        assert records[0].values == {"order": {"number": 42}}

    asyncio.run(run())
