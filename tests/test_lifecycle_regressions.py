from __future__ import annotations

import asyncio
import contextlib
import threading
import time
from typing import Any

import pytest

import ferricstore.async_wake as async_wake_module
import ferricstore.worker_core as worker_core_module
from ferricstore import complete
from ferricstore.async_producer import AsyncProducerLoop
from ferricstore.async_wake import AsyncFlowWakeCoordinator
from ferricstore.async_worker import AsyncQueueFlow, AsyncQueueFlowWorker, AsyncWorkflow
from ferricstore.lifecycle_core import SyncCloseTaskRegistry
from ferricstore.protocol_lifecycle import SyncDeadlineScheduler
from ferricstore.worker_core import CloseDeadline, CloseTimeoutError


def test_async_producer_loop_retries_client_close_before_retiring_thread() -> None:
    class RetryableCloseClient:
        def __init__(self) -> None:
            self.close_calls = 0

        async def ping(self) -> str:
            return "PONG"

        async def close(self) -> None:
            self.close_calls += 1
            if self.close_calls == 1:
                raise RuntimeError("first close failed")

    async def run() -> None:
        client = RetryableCloseClient()
        producer = AsyncProducerLoop(
            "ferric://localhost:6388",
            client_kwargs={},
            client_factory=lambda *_args, **_kwargs: client,
        )

        assert await producer.run(lambda background: background.ping()) == "PONG"
        thread = producer._thread
        assert thread is not None

        with pytest.raises(RuntimeError, match="first close failed"):
            await producer.close()

        assert thread.is_alive()
        await producer.close()

        assert client.close_calls == 2
        assert thread.is_alive() is False

    asyncio.run(run())


def test_async_wake_catches_python_310_asyncio_timeout(monkeypatch: Any) -> None:
    class LegacyAsyncioTimeout(Exception):
        pass

    async def raise_timeout(awaitable: Any, timeout: float) -> None:
        awaitable.close()
        raise LegacyAsyncioTimeout

    async def run() -> None:
        coordinator = AsyncFlowWakeCoordinator(
            object(),
            type="jobs",
            state=None,
            states=None,
            partition_key=None,
            partition_keys=None,
            priority=None,
            limit=1,
            enabled=True,
        )
        coordinator._enabled = True
        monkeypatch.setattr(
            async_wake_module,
            "AsyncioTimeoutError",
            LegacyAsyncioTimeout,
        )
        monkeypatch.setattr(async_wake_module.asyncio, "wait_for", raise_timeout)

        assert await coordinator.wait(0, 0.01) == (False, 0)

    asyncio.run(run())


def test_close_deadline_catches_python_310_future_timeout(monkeypatch: Any) -> None:
    class LegacyFutureTimeout(Exception):
        pass

    class PendingFuture:
        def done(self) -> bool:
            return False

        def result(self, *, timeout: float | None = None) -> None:
            raise LegacyFutureTimeout

    monkeypatch.setattr(
        worker_core_module,
        "FutureTimeoutError",
        LegacyFutureTimeout,
    )

    with pytest.raises(CloseTimeoutError, match="close timed out"):
        CloseDeadline.start(0.1).future_result(PendingFuture(), "close timed out")


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


@pytest.mark.parametrize("runtime", ["worker", "queue", "workflow"])
def test_concurrent_async_close_callers_use_independent_deadlines(runtime: str) -> None:
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
        if runtime == "worker":
            owner: AsyncQueueFlowWorker | AsyncQueueFlow | AsyncWorkflow = AsyncQueueFlowWorker(
                client, type="jobs", close_client=True
            )
        elif runtime == "queue":
            owner = AsyncQueueFlow(client, type="jobs")
            owner._owns_client = True
        else:
            owner = AsyncWorkflow(client, type="orders", states=["queued"])
            owner._owns_client = True

        long_close = asyncio.create_task(owner.close(timeout=0.2))
        await client.close_started.wait()
        started = time.monotonic()
        try:
            with pytest.raises(CloseTimeoutError, match="close timed out"):
                await owner.close(timeout=0.01)
            assert time.monotonic() - started < 0.08
            assert long_close.done() is False
        finally:
            client.release_close.set()
        await long_close
        assert client.close_calls == 1

    asyncio.run(run())


@pytest.mark.parametrize("runtime", ["worker", "queue", "workflow"])
def test_long_close_caller_can_rejoin_cleanup_after_short_caller_times_out(runtime: str) -> None:
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
        if runtime == "worker":
            owner: AsyncQueueFlowWorker | AsyncQueueFlow | AsyncWorkflow = AsyncQueueFlowWorker(
                client, type="jobs", close_client=True
            )
        elif runtime == "queue":
            owner = AsyncQueueFlow(client, type="jobs")
            owner._owns_client = True
        else:
            owner = AsyncWorkflow(client, type="orders", states=["queued"])
            owner._owns_client = True

        short_close = asyncio.create_task(owner.close(timeout=0.01))
        await client.close_started.wait()
        long_close = asyncio.create_task(owner.close(timeout=0.2))
        with pytest.raises(CloseTimeoutError, match="close timed out"):
            await short_close
        assert long_close.done() is False

        client.release_close.set()
        await long_close
        assert client.close_calls == 1

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
