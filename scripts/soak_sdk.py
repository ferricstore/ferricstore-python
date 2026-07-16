from __future__ import annotations

import asyncio
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from ferricstore import AsyncFlowClient, FlowClient, RawCodec


def _positive_float(name: str, default: str) -> float:
    value = float(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _positive_int(name: str, default: str) -> int:
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _sync_phase(url: str, duration: float, workers: int, prefix: str) -> int:
    client = FlowClient.from_url(
        url,
        codec=RawCodec(),
        timeout=3.0,
        max_connections=min(workers, 8),
    )
    keys = [f"{prefix}:sync:{worker}" for worker in range(workers)]
    deadline = time.monotonic() + duration

    def run(worker: int) -> int:
        key = keys[worker]
        operations = 0
        sequence = 0
        while time.monotonic() < deadline:
            value = f"{worker}:{sequence}".encode()
            client.command("SET", key, value)
            actual = client.command("GET", key)
            if actual != value:
                raise AssertionError(f"sync read-after-write mismatch: {actual!r} != {value!r}")
            operations += 2
            sequence += 1
        return operations

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            return sum(executor.map(run, range(workers)))
    finally:
        try:
            client.delete(*keys)
        finally:
            client.close()


async def _async_phase(url: str, duration: float, workers: int, prefix: str) -> int:
    client = AsyncFlowClient.from_url(
        url,
        codec=RawCodec(),
        timeout=3.0,
        max_connections=min(workers, 8),
    )
    keys = [f"{prefix}:async:{worker}" for worker in range(workers)]
    deadline = time.monotonic() + duration

    async def run(worker: int) -> int:
        key = keys[worker]
        operations = 0
        sequence = 0
        while time.monotonic() < deadline:
            value = f"{worker}:{sequence}".encode()
            await client.command("SET", key, value)
            actual = await client.command("GET", key)
            if actual != value:
                raise AssertionError(f"async read-after-write mismatch: {actual!r} != {value!r}")
            operations += 2
            sequence += 1
        return operations

    try:
        return sum(await asyncio.gather(*(run(worker) for worker in range(workers))))
    finally:
        try:
            await client.delete(*keys)
        finally:
            await client.close()


def main() -> None:
    url = os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:6388")
    duration = _positive_float("FERRICSTORE_SOAK_SECONDS", "60")
    workers = _positive_int("FERRICSTORE_SOAK_CONCURRENCY", "16")
    mode = os.environ.get("FERRICSTORE_SOAK_MODE", "both").lower()
    if mode not in {"sync", "async", "both"}:
        raise ValueError("FERRICSTORE_SOAK_MODE must be sync, async, or both")

    prefix = f"py-sdk-soak:{uuid.uuid4().hex}"
    started = time.monotonic()
    sync_operations = 0
    async_operations = 0
    phase_duration = duration / 2 if mode == "both" else duration
    if mode in {"sync", "both"}:
        sync_operations = _sync_phase(url, phase_duration, workers, prefix)
    if mode in {"async", "both"}:
        async_operations = asyncio.run(_async_phase(url, phase_duration, workers, prefix))

    elapsed = time.monotonic() - started
    total = sync_operations + async_operations
    if total <= 0:
        raise AssertionError("soak completed without any operations")
    print(  # noqa: T201 - CLI summary
        f"soak_ok operations={total} sync={sync_operations} async={async_operations} "
        f"elapsed_s={elapsed:.3f} ops_per_s={total / elapsed:.1f}"
    )


if __name__ == "__main__":
    main()
