from __future__ import annotations

import os
import uuid
from contextlib import suppress

import pytest

from ferricstore import FlowClient, JsonCodec

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_INTEGRATION") != "1",
    reason="set FERRICSTORE_INTEGRATION=1 to run FerricStore integration tests",
)


def test_real_ferricstore_command_and_flow_cycle() -> None:
    client = FlowClient.from_url(
        os.environ.get("FERRICSTORE_URL", "redis://127.0.0.1:6379/0"),
        codec=JsonCodec(),
    )
    suffix = uuid.uuid4().hex
    key = f"py-sdk:kv:{suffix}"
    flow_id = f"py-sdk:flow:{suffix}"
    flow_type = "py-sdk-integration"

    try:
        assert client.command("SET", key, "value") in (True, b"OK", "OK")
        assert client.command("GET", key) in (b"value", "value")

        client.create(
            flow_id,
            type=flow_type,
            state="queued",
            partition_key=flow_id,
            payload={"hello": "world"},
            idempotent=True,
        )

        jobs = client.claim_jobs(
            flow_type,
            state="queued",
            worker="py-sdk-integration-worker",
            partition_key=flow_id,
            limit=1,
            priority=None,
        )

        assert len(jobs) == 1
        job = jobs[0]
        assert job.id == flow_id
        assert job.partition_key == flow_id
        assert job.lease_token
        assert job.fencing_token > 0

        client.complete(
            job.id,
            lease_token=job.lease_token,
            fencing_token=job.fencing_token,
            partition_key=job.partition_key,
            result={"ok": True},
        )

        record = client.get(flow_id, partition_key=flow_id)
        assert record is not None
        assert record.state == "completed"
    finally:
        with suppress(Exception):
            client.command("DEL", key)
        client.close()
