from __future__ import annotations

import asyncio
import os
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, FerricStoreError, FlowClient, JsonCodec

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_CLUSTER_INTEGRATION") != "1",
    reason="set FERRICSTORE_CLUSTER_INTEGRATION=1 to run cluster failover tests",
)


def _urls() -> list[str]:
    raw = os.environ.get(
        "FERRICSTORE_URLS",
        "ferric://127.0.0.1:56430,ferric://127.0.0.1:56431,ferric://127.0.0.1:56432",
    )
    return [url.strip() for url in raw.split(",") if url.strip()]


def _compose(*args: str, timeout: int = 90) -> None:
    project = os.environ.get("FERRICSTORE_CLUSTER_COMPOSE_PROJECT", "sdk-cluster")
    compose_file = Path(
        os.environ.get("FERRICSTORE_CLUSTER_COMPOSE_FILE", "docker-compose.cluster.yml")
    )
    subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(compose_file), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _service(route: dict[str, Any]) -> str:
    node = str(route["leader_node"])
    if "@" not in node:
        raise AssertionError(f"unexpected cluster node name: {node!r}")
    service = node.split("@", 1)[1].split(".", 1)[0]
    if service not in {"fs0", "fs1", "fs2"}:
        raise AssertionError(f"unexpected cluster service: {service!r}")
    return service


def _endpoint_url(route: dict[str, Any]) -> str:
    endpoint = route["endpoint"]
    return f"ferric://{endpoint['host']}:{endpoint['native_port']}"


def _wait_native(url: str, timeout: float = 90.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        client: FlowClient | None = None
        try:
            client = FlowClient.from_url(url, timeout=1.0)
            if client.ping() in {b"PONG", "PONG", True}:
                return
        except Exception as exc:
            last_error = exc
        finally:
            if client is not None:
                client.close()
        time.sleep(0.2)
    raise AssertionError(f"FerricStore endpoint did not recover: {last_error!r}")


def _open_sync_topology_client(urls: list[str], timeout: float = 90.0) -> FlowClient:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            return FlowClient.from_urls(
                urls,
                codec=JsonCodec(),
                endpoint_policy="any",
                max_connections=2,
                timeout=1.0,
            )
        except Exception as exc:
            last_error = exc
        time.sleep(0.2)
    raise AssertionError(f"FerricStore cluster did not expose a topology: {last_error!r}")


async def _wait_clients_rerouted(
    sync_client: FlowClient,
    async_client: AsyncFlowClient,
    key: str,
    old_leader: str,
    timeout: float = 90.0,
) -> tuple[dict[str, Any], dict[str, Any]]:
    deadline = time.monotonic() + timeout
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        try:
            sync_client.refresh_topology()
            await async_client.refresh_topology()
            sync_route = sync_client.route(key)
            async_route = await async_client.route(key)
            if (
                str(sync_route["leader_node"]) != old_leader
                and str(async_route["leader_node"]) != old_leader
            ):
                return sync_route, async_route
        except Exception as exc:
            last_error = exc
        await asyncio.sleep(0.2)
    raise AssertionError(f"sync/async clients did not reroute: {last_error!r}")


async def _exercise_sync_and_async_failover() -> None:
    urls = _urls()
    if len(urls) < 3:
        pytest.skip("cluster failover requires at least three seed URLs")

    sync_client = _open_sync_topology_client(urls)
    async_client = AsyncFlowClient.from_urls(
        urls,
        codec=JsonCodec(),
        endpoint_policy="any",
        max_connections=2,
        timeout=1.0,
    )
    key = f"{{py-sdk-dual-failover:{uuid.uuid4().hex}}}:kv"
    stopped_service: str | None = None
    stopped_url: str | None = None
    try:
        await async_client.refresh_topology()
        before = sync_client.route(key)
        old_leader = str(before["leader_node"])
        stopped_service = _service(before)
        stopped_url = _endpoint_url(before)

        assert sync_client.kv_set(key, {"phase": "before"}) in {b"OK", "OK", True}
        assert await async_client.kv_get(key) == {"phase": "before"}

        await asyncio.to_thread(_compose, "stop", "-t", "1", stopped_service)

        with pytest.raises((FerricStoreError, OSError, TimeoutError, ConnectionError)):
            sync_client.kv_set(key, {"phase": "sync-stale"})
        with pytest.raises((FerricStoreError, OSError, TimeoutError, ConnectionError)):
            await async_client.kv_set(key, {"phase": "async-stale"})

        sync_route, async_route = await _wait_clients_rerouted(
            sync_client,
            async_client,
            key,
            old_leader,
        )
        assert str(sync_route["leader_node"]) != old_leader
        assert str(async_route["leader_node"]) != old_leader

        assert sync_client.kv_set(key, {"phase": "sync-after"}) in {b"OK", "OK", True}
        assert await async_client.kv_get(key) == {"phase": "sync-after"}
        assert await async_client.kv_set(key, {"phase": "async-after"}) in {
            b"OK",
            "OK",
            True,
        }
        assert sync_client.kv_get(key) == {"phase": "async-after"}
    finally:
        if stopped_service is not None:
            await asyncio.to_thread(_compose, "start", stopped_service)
        if stopped_url is not None:
            await asyncio.to_thread(_wait_native, stopped_url)
        try:
            sync_client.delete(key)
        finally:
            sync_client.close()
            await async_client.close()


def test_sync_and_async_clients_reroute_after_leader_stop() -> None:
    asyncio.run(_exercise_sync_and_async_failover())
