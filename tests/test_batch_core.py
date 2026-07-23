from __future__ import annotations

import asyncio
import base64
import hashlib
import threading
import zlib
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

import pytest

import ferricstore.batch_core as batch_core_module
import ferricstore.flow_routing as flow_routing_module
from ferricstore.batch_core import (
    batch_fingerprint,
    batch_values_equal,
    is_pipeline_status_batch,
    queued_batch_fingerprint,
    require_batch_items,
    run_async_fanout,
    run_sync_fanout,
    run_sync_fanout_on_executor,
)
from ferricstore.command_core import command_route_keys
from ferricstore.errors import FerricStoreError, OverloadedError, StaleLeaseError
from ferricstore.worker_core import CloseDeadline, many_item_error


def _flow_partition_route_key(value: str | bytes) -> str:
    encoded = value if isinstance(value, bytes) else value.encode()
    text = encoded.decode(errors="replace")
    if text.startswith("__flow_auto__:"):
        bucket = text.removeprefix("__flow_auto__:")
        if bucket.isdigit() and str(int(bucket)) == bucket and int(bucket) < 256:
            return f"f:{{fa:{bucket}}}:route"
    digest = base64.urlsafe_b64encode(hashlib.sha256(encoded).digest()).rstrip(b"=").decode()
    return f"f:{{f:{digest}}}:route"


def _flow_id_route_key(value: str | bytes) -> str:
    encoded = value if isinstance(value, bytes) else value.encode()
    return f"f:{{fa:{zlib.crc32(encoded) % 256}}}:route"


def test_flow_partition_digest_cache_does_not_retain_oversized_keys() -> None:
    flow_routing_module._cached_logical_partition_routing_key.cache_clear()

    flow_routing_module.flow_logical_partition_routing_key("tenant:small")
    before = flow_routing_module._cached_logical_partition_routing_key.cache_info()
    flow_routing_module.flow_logical_partition_routing_key("x" * 4_097)
    after = flow_routing_module._cached_logical_partition_routing_key.cache_info()

    assert before.currsize == 1
    assert after.currsize == before.currsize


def test_batch_fingerprint_preserves_encoded_type_distinctions() -> None:
    assert batch_fingerprint(True) != batch_fingerprint(1)
    assert batch_fingerprint(1) != batch_fingerprint(1.0)
    assert batch_fingerprint(0.0) != batch_fingerprint(-0.0)


def test_batch_fingerprint_never_groups_unrecognized_objects_by_repr() -> None:
    class SameRepresentation:
        def __repr__(self) -> str:
            return "same"

    left = SameRepresentation()
    right = SameRepresentation()

    assert batch_fingerprint(left) != batch_fingerprint(right)
    assert not batch_values_equal(left, right)


def test_batch_values_equal_handles_dataclasses_structurally_and_strictly() -> None:
    @dataclass(frozen=True)
    class Outcome:
        value: object

    assert batch_values_equal(Outcome(b"same"), Outcome(b"same"))
    assert not batch_values_equal(Outcome(True), Outcome(1))


def test_batch_fingerprint_uses_identity_for_mutable_values() -> None:
    @dataclass
    class MutableOutcome:
        value: int

    mutable_pairs = [
        ([1], [1]),
        ({"value": 1}, {"value": 1}),
        ({1}, {1}),
        (bytearray(b"value"), bytearray(b"value")),
        (MutableOutcome(1), MutableOutcome(1)),
    ]

    for left, right in mutable_pairs:
        assert queued_batch_fingerprint(left) != queued_batch_fingerprint(right)


def test_batch_fingerprint_keeps_immutable_container_coalescing() -> None:
    @dataclass(frozen=True)
    class FrozenOutcome:
        value: tuple[int, bytes]

    assert queued_batch_fingerprint((1, b"value")) == queued_batch_fingerprint((1, b"value"))
    assert queued_batch_fingerprint(frozenset({1, 2})) == queued_batch_fingerprint(
        frozenset({2, 1})
    )
    assert queued_batch_fingerprint(FrozenOutcome((1, b"value"))) == queued_batch_fingerprint(
        FrozenOutcome((1, b"value"))
    )


def test_batch_values_equal_short_circuits_identical_structural_values() -> None:
    class CountingDict(dict[str, int]):
        def __init__(self) -> None:
            super().__init__((str(index), index) for index in range(1_000))
            self.visited = 0

        def items(self):
            self.visited += len(self)
            return super().items()

    value = CountingDict()

    assert all(batch_values_equal(value, value) for _ in range(1_000))
    assert value.visited == 0


def test_close_deadline_does_not_start_cleanup_after_deadline_expired() -> None:
    async def run() -> None:
        started = False
        reports: list[dict[str, object]] = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: reports.append(context))

        async def cleanup() -> None:
            nonlocal started
            started = True
            raise RuntimeError("orphan cleanup failed")

        try:
            with pytest.raises(TimeoutError, match="expired"):
                await CloseDeadline.start(0).wait_awaitable(cleanup(), "expired")
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

        assert started is False
        assert reports == []

    asyncio.run(run())


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), -1.0, True])
def test_close_deadline_rejects_non_finite_negative_or_boolean_timeout(
    invalid: float,
) -> None:
    with pytest.raises(ValueError, match="close timeout must be non-negative and finite"):
        CloseDeadline.start(invalid)


def test_close_deadline_rejects_timeout_above_platform_wait_limit() -> None:
    with pytest.raises(ValueError, match="close timeout exceeds platform wait limit"):
        CloseDeadline.start(threading.TIMEOUT_MAX + 1.0)


def test_close_deadline_consumes_cleanup_failure_after_waiter_cancellation() -> None:
    async def run() -> None:
        started = asyncio.Event()
        release = asyncio.Event()
        reports: list[dict[str, object]] = []
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: reports.append(context))

        async def cleanup() -> None:
            started.set()
            await release.wait()
            raise RuntimeError("late cleanup failure")

        try:
            caller = asyncio.create_task(
                CloseDeadline.start(None).wait_awaitable(cleanup(), "unused")
            )
            await started.wait()
            caller.cancel()
            with pytest.raises(asyncio.CancelledError):
                await caller

            release.set()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        finally:
            loop.set_exception_handler(previous_handler)

        assert reports == []

    asyncio.run(run())


def test_close_deadline_preserves_operation_timeout_that_wins_wait_race() -> None:
    class OperationTimeoutFuture(Future[Any]):
        def __init__(self) -> None:
            super().__init__()
            self.done_calls = 0

        def done(self) -> bool:
            self.done_calls += 1
            return self.done_calls > 1

        def result(self, timeout: float | None = None) -> Any:
            raise TimeoutError("operation timed out")

    with pytest.raises(TimeoutError, match="operation timed out"):
        CloseDeadline.start(1).future_result(
            OperationTimeoutFuture(),
            "close timed out",
        )


def test_pipeline_status_detection_does_not_confuse_two_item_list_values() -> None:
    assert not is_pipeline_status_batch([[b"first", b"second"]])
    assert not is_pipeline_status_batch([[b"ok", b"application-value"]])
    assert not is_pipeline_status_batch([{b"status": b"ok", b"value": b"application-value"}])
    assert is_pipeline_status_batch([["ok", [b"first", b"second"]]])
    assert is_pipeline_status_batch([{"status": "busy", "value": "retry"}])


def test_require_batch_items_enforces_type_and_cardinality() -> None:
    assert require_batch_items([b"one"], 1, operation="PIPELINE") == [b"one"]
    with pytest.raises(FerricStoreError, match="expected 1"):
        require_batch_items([], 1, operation="PIPELINE")
    with pytest.raises(FerricStoreError, match="must return a list"):
        require_batch_items(b"one", 1, operation="PIPELINE")


def test_async_fanout_preserves_successful_exception_values() -> None:
    application_value = RuntimeError("application value")

    async def operation(value: object) -> object:
        await asyncio.sleep(0)
        return value

    async def run() -> list[object]:
        return await run_async_fanout(
            [application_value, b"ok"],
            operation,
            concurrent=True,
        )

    assert asyncio.run(run()) == [application_value, b"ok"]


def test_sync_fanout_keeps_only_a_bounded_submission_window(monkeypatch) -> None:
    class TrackedFuture(Future[int]):
        def __init__(self, executor: TrackingExecutor) -> None:
            super().__init__()
            self.executor = executor
            self.consumed = False

        def result(self, timeout: float | None = None) -> int:
            if not self.consumed:
                self.consumed = True
                self.executor.scheduled -= 1
            return super().result(timeout)

    class TrackingExecutor:
        instance: TrackingExecutor | None = None

        def __init__(self, max_workers: int) -> None:
            self.max_workers = max_workers
            self.scheduled = 0
            self.max_scheduled = 0
            type(self).instance = self

        def __enter__(self) -> TrackingExecutor:
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

        def submit(self, operation: Any, item: int) -> Future[int]:
            self.scheduled += 1
            self.max_scheduled = max(self.max_scheduled, self.scheduled)
            future = TrackedFuture(self)
            future.set_result(operation(item))
            return future

        def map(self, operation: Any, items: Any) -> Any:
            futures = [self.submit(operation, item) for item in items]
            return (future.result() for future in futures)

    monkeypatch.setattr(batch_core_module, "ThreadPoolExecutor", TrackingExecutor)

    assert run_sync_fanout(
        list(range(100)),
        lambda item: item * 2,
        concurrent=True,
        max_concurrency=4,
    ) == [item * 2 for item in range(100)]
    assert TrackingExecutor.instance is not None
    assert TrackingExecutor.instance.max_scheduled <= 4


def test_reusable_sync_fanout_keeps_one_executor_for_repeated_calls(monkeypatch) -> None:
    fanout_type = getattr(batch_core_module, "SyncFanoutExecutor", None)
    assert fanout_type is not None, "batch fanout must expose a reusable executor owner"

    original = batch_core_module.ThreadPoolExecutor
    created: list[Any] = []

    def create_executor(*args: Any, **kwargs: Any) -> Any:
        executor = original(*args, **kwargs)
        created.append(executor)
        return executor

    monkeypatch.setattr(batch_core_module, "ThreadPoolExecutor", create_executor)
    fanout = fanout_type(max_concurrency=4, thread_name_prefix="test-fanout")

    assert fanout.run([1, 2], lambda value: value * 2, concurrent=True) == [2, 4]
    assert fanout.run([3, 4], lambda value: value * 2, concurrent=True) == [6, 8]
    assert len(created) == 1

    fanout.close()
    with pytest.raises(RuntimeError, match="closed"):
        fanout.run([1, 2], lambda value: value, concurrent=True)


def test_reusable_sync_fanout_retries_failed_executor_shutdown(monkeypatch) -> None:
    fanout_type = batch_core_module.SyncFanoutExecutor

    class FailOnceExecutor:
        def __init__(self, **_kwargs: Any) -> None:
            self.shutdown_calls = 0

        def submit(self, operation: Any, item: int) -> Future[int]:
            future: Future[int] = Future()
            future.set_result(operation(item))
            return future

        def shutdown(self, *, wait: bool) -> None:
            assert wait is True
            self.shutdown_calls += 1
            if self.shutdown_calls == 1:
                raise RuntimeError("transient executor shutdown failure")

    executor = FailOnceExecutor()
    monkeypatch.setattr(batch_core_module, "ThreadPoolExecutor", lambda **_kwargs: executor)
    fanout = fanout_type(max_concurrency=2)
    assert fanout.run([1, 2], lambda value: value, concurrent=True) == [1, 2]

    with pytest.raises(RuntimeError, match="transient executor shutdown failure"):
        fanout.close()
    fanout.close()

    assert executor.shutdown_calls == 2


def test_sync_fanout_attempts_all_items_by_default_after_an_error(monkeypatch) -> None:
    class IndexedFuture(Future[int]):
        def __init__(self, index: int) -> None:
            super().__init__()
            self.index = index

    class ImmediateExecutor:
        def __init__(self) -> None:
            self.submitted: list[int] = []

        def submit(self, operation: Any, item: int) -> Future[int]:
            self.submitted.append(item)
            future = IndexedFuture(item)
            try:
                future.set_result(operation(item))
            except BaseException as exc:
                future.set_exception(exc)
            return future

    def ordered_wait(pending: Any, **_kwargs: Any) -> tuple[set[Any], set[Any]]:
        futures = set(pending)
        completed = min(futures, key=lambda future: future.index)
        return {completed}, futures - {completed}

    monkeypatch.setattr(batch_core_module, "wait", ordered_wait)
    executor = ImmediateExecutor()

    def operation(item: int) -> int:
        if item == 0:
            raise RuntimeError("first failed")
        return item

    with pytest.raises(RuntimeError, match="first failed"):
        run_sync_fanout_on_executor(
            list(range(20)),
            operation,
            executor=executor,
            max_concurrency=4,
        )

    assert executor.submitted == list(range(20))


def test_sync_fanout_stop_on_error_does_not_start_more_work(monkeypatch) -> None:
    class IndexedFuture(Future[int]):
        def __init__(self, index: int) -> None:
            super().__init__()
            self.index = index

    class ImmediateExecutor:
        def __init__(self) -> None:
            self.submitted: list[int] = []

        def submit(self, operation: Any, item: int) -> Future[int]:
            self.submitted.append(item)
            future = IndexedFuture(item)
            try:
                future.set_result(operation(item))
            except BaseException as exc:
                future.set_exception(exc)
            return future

    def ordered_wait(pending: Any, **_kwargs: Any) -> tuple[set[Any], set[Any]]:
        futures = set(pending)
        completed = min(futures, key=lambda future: future.index)
        return {completed}, futures - {completed}

    monkeypatch.setattr(batch_core_module, "wait", ordered_wait)
    executor = ImmediateExecutor()

    def operation(item: int) -> int:
        if item == 0:
            raise RuntimeError("first failed")
        return item

    with pytest.raises(RuntimeError, match="first failed"):
        run_sync_fanout_on_executor(
            list(range(20)),
            operation,
            executor=executor,
            max_concurrency=4,
            stop_on_error=True,
        )

    assert executor.submitted == [0, 1, 2, 3]


def test_async_fanout_creates_only_a_bounded_worker_set(monkeypatch) -> None:
    original_gather = asyncio.gather
    gather_widths: list[int] = []

    async def tracking_gather(*awaitables: Any, **kwargs: Any) -> list[Any]:
        gather_widths.append(len(awaitables))
        return await original_gather(*awaitables, **kwargs)

    monkeypatch.setattr(batch_core_module.asyncio, "gather", tracking_gather)

    async def operation(item: int) -> int:
        await asyncio.sleep(0)
        return item * 2

    async def run() -> list[int]:
        return await run_async_fanout(
            list(range(100)),
            operation,
            concurrent=True,
            max_concurrency=4,
        )

    assert asyncio.run(run()) == [item * 2 for item in range(100)]
    assert gather_widths
    assert max(gather_widths) <= 4


def test_many_item_error_uses_mapping_message_without_value() -> None:
    item = {"status": "error", "message": "ERR stale lease for flow-1"}

    error = many_item_error(item)

    assert isinstance(error, StaleLeaseError)
    assert "stale lease for flow-1" in str(error)
    assert error.raw is item


def test_many_item_error_uses_bytes_mapping_reason_without_value() -> None:
    item = {b"status": b"busy", b"reason": b"queue full"}

    error = many_item_error(item)

    assert isinstance(error, OverloadedError)
    assert "queue full" in str(error)
    assert error.raw is item


@pytest.mark.parametrize(
    ("name", "args", "keys"),
    [
        ("INCR", ("counter",), ("counter",)),
        ("GETSET", ("string-key", b"value"), ("string-key",)),
        (
            "HEXPIRETIME",
            ("hash-key", "FIELDS", 1, "field"),
            ("hash-key",),
        ),
        ("RENAME", ("old", "new"), ("old", "new")),
        ("MSETNX", ("a", b"1", "b", b"2"), ("a", "b")),
        ("BITOP", ("AND", "dest", "a", "b"), ("dest", "a", "b")),
        ("BLPOP", ("a", "b", 1), ("a", "b")),
        ("OBJECT", ("ENCODING", "object-key"), ("object-key",)),
        ("XINFO", ("STREAM", "stream-key"), ("stream-key",)),
        ("XGROUP", ("CREATE", "stream-key", "group", "$"), ("stream-key",)),
        ("MEMORY", ("USAGE", "memory-key"), ("memory-key",)),
        (
            "FLOW.CREATE",
            ("flow-auto", "TYPE", "order"),
            (_flow_id_route_key("flow-auto"),),
        ),
        (
            "FLOW.CREATE",
            ("flow-explicit", "TYPE", "order", "PARTITION", "tenant:1"),
            (_flow_partition_route_key("tenant:1"),),
        ),
        (
            "FLOW.CREATE",
            (b"flow-explicit", "TYPE", "order", b"PARTITION", b"tenant:bytes"),
            (_flow_partition_route_key(b"tenant:bytes"),),
        ),
        (
            "FLOW.CREATE",
            ("PARTITION", "TYPE", "order"),
            (_flow_id_route_key("PARTITION"),),
        ),
        (
            "FLOW.CREATE",
            ("flow-attribute", "TYPE", "order", "ATTRIBUTE", "name", "PARTITION", "NOW", 1),
            (_flow_id_route_key("flow-attribute"),),
        ),
        (
            "FLOW.COMPLETE",
            ("flow-lease", "PARTITION", "FENCING", 1),
            (_flow_id_route_key("flow-lease"),),
        ),
        (
            "FLOW.CREATE_MANY",
            ("tenant:2", "TYPE", "order", "ITEMS", "flow-1", b"payload"),
            (_flow_partition_route_key("tenant:2"),),
        ),
        (
            "FLOW.CREATE_MANY",
            ("MIXED", "TYPE", "order", "ITEMS", "flow-1", "tenant:1", b"payload"),
            (),
        ),
        (
            "FLOW.CLAIM_DUE",
            ("order", "STATE", "queued", "PARTITION", "tenant:3"),
            (_flow_partition_route_key("tenant:3"),),
        ),
        (
            "FLOW.CLAIM_DUE",
            ("order", "PARTITIONS", 2, "tenant:3", "tenant:4"),
            (
                _flow_partition_route_key("tenant:3"),
                _flow_partition_route_key("tenant:4"),
            ),
        ),
        ("FLOW.CLAIM_DUE", ("order", "PARTITION", "ANY"), ()),
        ("FLOW.CLAIM_DUE", ("order", "PARTITION", "GLOBAL"), ("f:{f}:route",)),
        (
            "FLOW.GET",
            ("flow-payload", "PAYLOAD", "PARTITION", "tenant:payload"),
            (_flow_partition_route_key("tenant:payload"),),
        ),
        (
            "FLOW.GET",
            (b"flow-payload", "PAYLOAD", "PARTITION", b"tenant:bytes"),
            (_flow_partition_route_key(b"tenant:bytes"),),
        ),
        (
            b"FLOW.GET",
            (b"flow-payload", b"PAYLOAD", b"PARTITION", b"tenant:all-bytes"),
            (_flow_partition_route_key(b"tenant:all-bytes"),),
        ),
        (
            "XREADGROUP",
            ("GROUP", "STREAMS", "consumer", "STREAMS", "orders", ">"),
            ("orders",),
        ),
        (
            b"XREADGROUP",
            (b"GROUP", b"group", b"STREAMS", b"STREAMS", b"orders", b">"),
            (b"orders",),
        ),
        ("FLOW.APPROVAL.GET", ("approval-1",), (_flow_partition_route_key("approval-1"),)),
        ("FLOW.BUDGET.GET", ("tenant-scope",), (_flow_partition_route_key("tenant-scope"),)),
        (
            "FLOW.VALUE.PUT",
            (b"value", "OWNER_FLOW_ID", "flow-owner", "NAME", "result"),
            (_flow_id_route_key("flow-owner"),),
        ),
        (
            "FLOW.VALUE.PUT",
            (b"value", "OWNER_FLOW_ID", "flow-owner"),
            ("f:{f}:route",),
        ),
        (
            "FLOW.VALUE.PUT",
            (
                b"value",
                "PARTITION",
                "tenant:value",
                "OWNER_FLOW_ID",
                "flow-owner",
                "NAME",
                "result",
            ),
            (_flow_partition_route_key("tenant:value"),),
        ),
        ("FLOW.VALUE.PUT", (b"value", "NOW", 1), ("f:{f}:route",)),
        ("FLOW.POLICY.GET", ("order",), ("f:{f}:route",)),
        (
            "FLOW.QUERY",
            (
                "FQL1",
                "FROM runs WHERE partition_key = @partition RETURN COUNT",
                "partition",
                "tenant:parent",
            ),
            (),
        ),
        ("FLOW.SCHEDULE.GET", ("daily-report",), ()),
        ("PING", (), ()),
    ],
)
def test_command_route_registry_extracts_generic_command_keys(
    name: str,
    args: tuple[object, ...],
    keys: tuple[object, ...],
) -> None:
    assert command_route_keys(name, args) == keys
