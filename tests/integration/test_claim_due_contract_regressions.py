from __future__ import annotations

import asyncio
import os
import time
import uuid

import pytest

from ferricstore import AsyncFlowClient, FlowClient, FlowRecord, JsonCodec

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
)


@pytest.fixture
def ferricstore_url() -> str:
    return os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")


def test_sync_claim_due_record_contract_includes_lease_and_selected_data(
    ferricstore_url: str,
) -> None:
    suffix = uuid.uuid4().hex
    flow_type = f"py-sdk-claim-record-{suffix}"
    flow_id = f"py-sdk-claim-record-flow-{suffix}"
    partition_key = f"py-sdk-claim-record-partition-{suffix}"
    worker = f"py-sdk-claim-record-worker-{suffix}"
    now_ms = int(time.time() * 1000)
    client = FlowClient.from_url(ferricstore_url, codec=JsonCodec())
    claimed: list[FlowRecord] = []

    try:
        client.create(
            flow_id,
            type=flow_type,
            state="queued",
            payload={"answer": 42},
            partition_key=partition_key,
            attributes={"tenant": "acme", "case": "f02"},
            values={"order": {"number": 42}},
            now_ms=now_ms,
            run_at_ms=now_ms - 1,
            idempotent=True,
        )

        records = client.claim_due(
            flow_type,
            state="queued",
            worker=worker,
            partition_key=partition_key,
            lease_ms=30_000,
            limit=1,
            now_ms=now_ms + 1,
            include_record=True,
            payload=True,
            values=["order"],
            include_attributes=True,
        )

        assert len(records) == 1
        record = records[0]
        claimed.append(record)
        assert isinstance(record, FlowRecord)
        assert record.id == flow_id
        assert record.type == flow_type
        assert record.state == "running"
        assert record.partition_key == partition_key
        assert record.lease_token
        assert record.fencing_token > 0
        assert record.payload == {"answer": 42}
        assert record.values == {"order": {"number": 42}}
        assert record.attributes == {"tenant": "acme", "case": "f02"}

        [reclaimed] = client.reclaim(
            flow_type,
            worker=f"{worker}-reclaimed",
            partition_key=partition_key,
            lease_ms=30_000,
            limit=1,
            now_ms=now_ms + 30_002,
            include_record=True,
            payload=True,
            values=["order"],
        )
        claimed[:] = [reclaimed]
        assert isinstance(reclaimed, FlowRecord)
        assert reclaimed.id == record.id
        assert reclaimed.fencing_token > record.fencing_token
        assert reclaimed.lease_token != record.lease_token
        assert reclaimed.payload == record.payload
        assert reclaimed.values == record.values
        assert reclaimed.attributes == record.attributes
    finally:
        if claimed:
            record = claimed[0]
            client.complete(
                record.id,
                lease_token=record.lease_token,
                fencing_token=record.fencing_token,
                partition_key=record.partition_key,
                return_record=False,
            )
        client.close()


def test_async_claim_due_record_contract_includes_lease_and_selected_data(
    ferricstore_url: str,
) -> None:
    async def run() -> None:
        suffix = uuid.uuid4().hex
        flow_type = f"py-sdk-async-claim-record-{suffix}"
        flow_id = f"py-sdk-async-claim-record-flow-{suffix}"
        partition_key = f"py-sdk-async-claim-record-partition-{suffix}"
        worker = f"py-sdk-async-claim-record-worker-{suffix}"
        now_ms = int(time.time() * 1000)
        client = AsyncFlowClient.from_url(ferricstore_url, codec=JsonCodec())
        claimed: list[FlowRecord] = []

        try:
            await client.create(
                flow_id,
                type=flow_type,
                state="queued",
                payload={"answer": 42},
                partition_key=partition_key,
                attributes={"tenant": "acme", "case": "f02"},
                values={"order": {"number": 42}},
                now_ms=now_ms,
                run_at_ms=now_ms - 1,
                idempotent=True,
            )

            records = await client.claim_due(
                flow_type,
                state="queued",
                worker=worker,
                partition_key=partition_key,
                lease_ms=30_000,
                limit=1,
                now_ms=now_ms + 1,
                include_record=True,
                payload=True,
                values=["order"],
                include_attributes=True,
            )

            assert len(records) == 1
            record = records[0]
            claimed.append(record)
            assert isinstance(record, FlowRecord)
            assert record.id == flow_id
            assert record.type == flow_type
            assert record.state == "running"
            assert record.partition_key == partition_key
            assert record.lease_token
            assert record.fencing_token > 0
            assert record.payload == {"answer": 42}
            assert record.values == {"order": {"number": 42}}
            assert record.attributes == {"tenant": "acme", "case": "f02"}

            [reclaimed] = await client.reclaim(
                flow_type,
                worker=f"{worker}-reclaimed",
                partition_key=partition_key,
                lease_ms=30_000,
                limit=1,
                now_ms=now_ms + 30_002,
                include_record=True,
                payload=True,
                values=["order"],
            )
            claimed[:] = [reclaimed]
            assert isinstance(reclaimed, FlowRecord)
            assert reclaimed.id == record.id
            assert reclaimed.fencing_token > record.fencing_token
            assert reclaimed.lease_token != record.lease_token
            assert reclaimed.payload == record.payload
            assert reclaimed.values == record.values
            assert reclaimed.attributes == record.attributes
        finally:
            if claimed:
                record = claimed[0]
                await client.complete(
                    record.id,
                    lease_token=record.lease_token,
                    fencing_token=record.fencing_token,
                    partition_key=record.partition_key,
                    return_record=False,
                )
            await client.close()

    asyncio.run(run())
