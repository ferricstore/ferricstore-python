from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from typing import Any

import pytest

from ferricstore import complete
from ferricstore.async_worker import AsyncQueueFlow, AsyncQueueFlowWorker, AsyncWorkflow
from ferricstore.lifecycle_core import SyncCloseTaskRegistry
from ferricstore.protocol_lifecycle import SyncDeadlineScheduler


def test_sync_close_task_registry_rolls_back_failed_thread_start(monkeypatch: Any) -> None:
    registry = SyncCloseTaskRegistry()
    resource = object()
    close_calls = 0
    failed_once = False
    original_start = threading.Thread.start

    def flaky_start(thread: threading.Thread) -> None:
        nonlocal failed_once
        if thread.name == "ferricstore-close" and not failed_once:
            failed_once = True
            raise RuntimeError("thread start failed")
        original_start(thread)

    def close() -> str:
        nonlocal close_calls
        close_calls += 1
        return "closed"

    monkeypatch.setattr(threading.Thread, "start", flaky_start)

    with pytest.raises(RuntimeError, match="thread start failed"):
        registry.run(resource, close, lambda future: future.result(timeout=0.2))

    assert registry.run(resource, close, lambda future: future.result(timeout=0.2)) == "closed"
    assert close_calls == 1


def test_sync_deadline_scheduler_rolls_back_failed_thread_start(monkeypatch: Any) -> None:
    expired = threading.Event()
    scheduler = SyncDeadlineScheduler(
        lambda _request_id: expired.set(),
        thread_name="deadline-start-rollback-test",
    )
    failed_once = False
    original_start = threading.Thread.start

    def flaky_start(thread: threading.Thread) -> None:
        nonlocal failed_once
        if thread.name == "deadline-start-rollback-test" and not failed_once:
            failed_once = True
            raise RuntimeError("thread start failed")
        original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", flaky_start)

    try:
        with pytest.raises(RuntimeError, match="thread start failed"):
            scheduler.schedule(1, time.monotonic())

        scheduler.schedule(2, time.monotonic())
        assert expired.wait(0.2)
    finally:
        with contextlib.suppress(RuntimeError):
            scheduler.close()


class _BlockingClaimClient:
    def __init__(self) -> None:
        self.claim_started = asyncio.Event()
        self.release_claim = asyncio.Event()
        self.closed = asyncio.Event()
        self.claim_cancelled = False
        self.claim_calls = 0
        self.close_calls = 0

    async def claim_flows(self, *_args: Any, **_kwargs: Any) -> list[Any]:
        self.claim_calls += 1
        self.claim_started.set()
        try:
            await self.release_claim.wait()
        except asyncio.CancelledError:
            self.claim_cancelled = True
            raise
        return []

    async def close(self) -> None:
        self.close_calls += 1
        self.closed.set()


class _BlockingProducerClient(_BlockingClaimClient):
    def __init__(self) -> None:
        super().__init__()
        self.enqueue_started = asyncio.Event()
        self.release_enqueue = asyncio.Event()

    async def enqueue(self, *_args: Any, **_kwargs: Any) -> bytes:
        self.enqueue_started.set()
        await self.release_enqueue.wait()
        return b"OK"


async def _queue_handler(_job: Any) -> None:
    return None


def test_async_queue_worker_close_tracks_standalone_run_once_and_is_terminal() -> None:
    async def run() -> None:
        client = _BlockingClaimClient()
        worker = AsyncQueueFlowWorker(
            client,
            type="jobs",
            close_client=True,
        )
        run_task = asyncio.create_task(worker.run_once(_queue_handler))
        await client.claim_started.wait()

        try:
            with pytest.raises(TimeoutError, match="close timed out"):
                await worker.close(timeout=0.01)
            assert client.closed.is_set() is False
            assert run_task.done() is False
        finally:
            client.release_claim.set()
            await run_task

        await worker.close(timeout=0.2)
        await worker.close(timeout=0.2)

        assert client.close_calls == 1
        with pytest.raises(RuntimeError, match="closed"):
            await worker.run_once(_queue_handler)
        with pytest.raises(RuntimeError, match="closed"):
            worker.start(_queue_handler)

    asyncio.run(run())


def test_async_queue_worker_reuses_timed_out_resource_close_operation() -> None:
    class SlowCloseClient(_BlockingClaimClient):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = asyncio.Event()
            self.release_close = asyncio.Event()

        async def close(self) -> None:
            self.close_calls += 1
            self.close_started.set()
            await self.release_close.wait()
            self.closed.set()

    async def run() -> None:
        client = SlowCloseClient()
        worker = AsyncQueueFlowWorker(client, type="jobs", close_client=True)

        with pytest.raises(TimeoutError, match="close timed out"):
            await worker.close(timeout=0.01)
        assert client.close_calls == 1
        assert client.close_started.is_set() is True

        client.release_close.set()
        await asyncio.sleep(0)
        await worker.close(timeout=0.2)

        assert client.close_calls == 1
        assert client.closed.is_set() is True

    asyncio.run(run())


def test_async_queue_worker_retries_close_that_failed_after_timeout() -> None:
    class FailLateClient(_BlockingClaimClient):
        def __init__(self) -> None:
            super().__init__()
            self.close_started = asyncio.Event()
            self.release_close = asyncio.Event()

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                self.close_started.set()
                await self.release_close.wait()
                raise RuntimeError("late close failure")
            self.closed.set()

    async def run() -> None:
        client = FailLateClient()
        worker = AsyncQueueFlowWorker(client, type="jobs", close_client=True)

        with pytest.raises(TimeoutError, match="close timed out"):
            await worker.close(timeout=0.01)
        client.release_close.set()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        await worker.close(timeout=0.2)

        assert client.close_calls == 2
        assert client.closed.is_set() is True

    asyncio.run(run())


def test_async_queue_flow_close_waits_for_standalone_run_once() -> None:
    async def run() -> None:
        client = _BlockingClaimClient()
        queue = AsyncQueueFlow(client, type="jobs")
        queue._owns_client = True
        run_task = asyncio.create_task(queue.run_once(_queue_handler))
        await client.claim_started.wait()

        close_task = asyncio.create_task(queue.close())
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(close_task), timeout=0.01)
            assert client.closed.is_set() is False
            assert close_task.done() is False
        finally:
            client.release_claim.set()
            await run_task
            await close_task

        with pytest.raises(RuntimeError, match="closed"):
            await queue.run_once(_queue_handler)

    asyncio.run(run())


def test_async_workflow_close_waits_for_standalone_run_once() -> None:
    async def run() -> None:
        client = _BlockingClaimClient()
        workflow = AsyncWorkflow(client, type="orders", states=["queued"])
        workflow._owns_client = True

        @workflow.state("queued")
        async def queued(_ctx: Any) -> Any:
            return complete()

        run_task = asyncio.create_task(workflow.run_once())
        await client.claim_started.wait()

        close_task = asyncio.create_task(workflow.close(timeout=None))
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(close_task), timeout=0.01)
            assert client.closed.is_set() is False
            assert close_task.done() is False
        finally:
            client.release_claim.set()
            await run_task
            await close_task

        with pytest.raises(RuntimeError, match="closed"):
            await workflow.run_once()

    asyncio.run(run())


@pytest.mark.parametrize("runtime", ["queue", "workflow"])
def test_async_runtime_close_waits_for_in_flight_producer(runtime: str) -> None:
    async def run() -> None:
        client = _BlockingProducerClient()
        if runtime == "queue":
            owner: AsyncQueueFlow | AsyncWorkflow = AsyncQueueFlow(client, type="jobs")
        else:
            owner = AsyncWorkflow(client, type="orders", states=["queued"])
        owner._owns_client = True

        enqueue_task = asyncio.create_task(owner.enqueue("job-1"))
        await client.enqueue_started.wait()
        close_task = asyncio.create_task(owner.close())
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(close_task), timeout=0.01)
            assert client.closed.is_set() is False
            assert close_task.done() is False
        finally:
            client.release_enqueue.set()
            assert await enqueue_task == b"OK"
            await close_task

        with pytest.raises(RuntimeError, match="closed"):
            await owner.enqueue("job-2")

    asyncio.run(run())


@pytest.mark.parametrize("runtime", ["queue", "workflow"])
def test_async_runtime_run_cancellation_does_not_cancel_blocking_claim(runtime: str) -> None:
    async def run() -> None:
        client = _BlockingClaimClient()
        if runtime == "queue":
            owner: AsyncQueueFlow | AsyncWorkflow = AsyncQueueFlow(
                client,
                type="jobs",
                block_ms=60_000,
            )
            run_task = asyncio.create_task(owner.run(_queue_handler))
        else:
            workflow = AsyncWorkflow(
                client,
                type="orders",
                states=["queued"],
                block_ms=60_000,
            )

            @workflow.state("queued")
            async def queued(_ctx: Any) -> Any:
                return complete()

            owner = workflow
            run_task = asyncio.create_task(workflow.run())

        await client.claim_started.wait()
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task

        try:
            assert client.claim_cancelled is False
        finally:
            client.release_claim.set()
            await owner.close()

    asyncio.run(run())
