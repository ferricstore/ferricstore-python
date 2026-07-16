from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from ferricstore import AsyncFlowClient, FlowClient, RawCodec

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_COMPATIBILITY") != "1",
    reason="set FERRICSTORE_COMPATIBILITY=1 to run server compatibility tests",
)


def _url() -> str:
    return os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")


def test_sync_sdk_compatibility_smoke() -> None:
    suffix = uuid.uuid4().hex
    keys = [f"py-sdk-compat:{suffix}:{index}" for index in range(3)]
    flow_id = f"py-sdk-compat-flow:{suffix}"
    client = FlowClient.from_url(
        _url(),
        codec=RawCodec(),
        timeout=3.0,
        max_connections=2,
    )
    try:
        assert client.ping() in {b"PONG", "PONG"}
        assert client.set(keys[0], b"one") in {b"OK", "OK", True}
        assert client.kv_get(keys[0], decode=False) == b"one"

        assert client.mset({keys[1]: b"two", keys[2]: b"three"}, encode=False) in {
            b"OK",
            "OK",
            True,
        }
        assert client.mget(*keys, decode=False) == [b"one", b"two", b"three"]

        pipeline = client.pipeline()
        pipeline.command("SET", keys[0], b"pipeline")
        pipeline.command("GET", keys[0])
        assert pipeline.execute() == [b"OK", b"pipeline"]

        created = client.create(
            flow_id,
            type="sdk-compatibility",
            state="queued",
            payload=b"payload",
        )
        assert created in {b"OK", "OK", True}
        record = client.get(flow_id)
        assert record is not None
        assert record.id == flow_id
        assert record.type == "sdk-compatibility"
    finally:
        try:
            client.delete(*keys)
        finally:
            client.close()


def test_async_sdk_compatibility_smoke() -> None:
    async def exercise() -> None:
        suffix = uuid.uuid4().hex
        key = f"py-sdk-compat-async:{suffix}"
        client = AsyncFlowClient.from_url(
            _url(),
            codec=RawCodec(),
            timeout=3.0,
            max_connections=2,
        )
        try:
            assert await client.ping() in {b"PONG", "PONG"}
            assert await client.set(key, b"value") in {b"OK", "OK", True}
            assert await client.kv_get(key, decode=False) == b"value"
        finally:
            try:
                await client.delete(key)
            finally:
                await client.close()

    asyncio.run(exercise())
