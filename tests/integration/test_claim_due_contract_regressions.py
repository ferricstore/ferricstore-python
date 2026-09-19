from __future__ import annotations

import asyncio
import os
import ssl
import time
import uuid
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, FlowClient, FlowRecord, JsonCodec

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
)


@pytest.fixture
def ferricstore_url() -> str:
    return os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")


def _client_options(url: str) -> dict[str, Any]:
    if not url.startswith(("http://", "https://")):
        return {}
    options: dict[str, Any] = {}
    username = os.environ.get("FERRICSTORE_USERNAME")
    password = os.environ.get("FERRICSTORE_PASSWORD")
    ca_file = os.environ.get("FERRICSTORE_CA_FILE")
    http2 = os.environ.get("FERRICSTORE_HTTP2")
    if username is not None:
        options["username"] = username
    if password is not None:
        options["password"] = password
    if ca_file is not None:
        context = ssl.create_default_context(cafile=ca_file)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        options["ssl_context"] = context
    if http2 is not None:
        options["http2"] = http2.lower() in {"1", "true", "yes"}
    return options


def test_sync_claim_due_record_contract_includes_lease_and_selected_data(
    ferricstore_url: str,
) -> None:
    suffix = uuid.uuid4().hex
    flow_type = f"py-sdk-claim-record-{suffix}"
    flow_id = f"py-sdk-claim-record-flow-{suffix}"
    partition_key = f"py-sdk-claim-record-partition-{suffix}"
    worker = f"py-sdk-claim-record-worker-{suffix}"
    now_ms = int(time.time() * 1000)
    client = FlowClient.from_url(
        ferricstore_url,
        codec=JsonCodec(),
        **_client_options(ferricstore_url),
    )
    claimed: list[FlowRecord] = []

    try:
        client.create(
            flow_id,
            type=flow_type,
            state="queued",
            payload={"answer": 42},
            partition_key=partition_key,
            attributes={"tenant": "acme", "case": "f02"},
            state_meta={"attempt": 1},
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
        assert record.state_meta == {"queued": {"attempt": 1}}

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
        client = AsyncFlowClient.from_url(
            ferricstore_url,
            codec=JsonCodec(),
            **_client_options(ferricstore_url),
        )
        claimed: list[FlowRecord] = []

        try:
            await client.create(
                flow_id,
                type=flow_type,
                state="queued",
                payload={"answer": 42},
                partition_key=partition_key,
                attributes={"tenant": "acme", "case": "f02"},
                state_meta={"attempt": 1},
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
            assert record.state_meta == {"queued": {"attempt": 1}}

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
