from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from ferricstore import AsyncFlowClient, BackpressurePolicy, FlowClient, OverloadedError
from ferricstore.backpressure import BackpressureController


def _zero_retry_budget() -> BackpressurePolicy:
    return BackpressurePolicy(
        max_elapsed_ms=0,
        base_delay_ms=0,
        max_delay_ms=0,
        jitter=0,
        shared=False,
    )


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), -1.0, True])
@pytest.mark.parametrize("field", ["max_elapsed_ms", "base_delay_ms", "max_delay_ms", "jitter"])
def test_backpressure_policy_rejects_non_finite_negative_or_boolean_timing(
    field: str, invalid: float
) -> None:
    with pytest.raises(ValueError, match=rf"{field} must be non-negative and finite"):
        BackpressurePolicy(**{field: invalid})


@pytest.mark.parametrize("field", ["base_delay_ms", "max_delay_ms"])
def test_backpressure_policy_rejects_delay_above_platform_wait_limit(field: str) -> None:
    with pytest.raises(ValueError, match=rf"{field} exceeds platform wait limit"):
        BackpressurePolicy(**{field: (threading.TIMEOUT_MAX + 1.0) * 1000.0})


@pytest.mark.parametrize("invalid", [True, 1.5, float("nan"), float("inf"), "3", -1])
def test_backpressure_policy_rejects_invalid_max_retries(invalid: Any) -> None:
    with pytest.raises(ValueError, match=r"max_retries must be (?:a )?non-negative"):
        BackpressurePolicy(max_retries=invalid)


def test_untrusted_retry_after_is_capped_to_platform_wait_limit(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr("ferricstore.backpressure.time.sleep", sleeps.append)
    controller = BackpressureController(
        BackpressurePolicy(base_delay_ms=0, max_delay_ms=0, jitter=0, shared=False)
    )

    assert controller.record_overload(
        0,
        retry_after_ms=int((threading.TIMEOUT_MAX + 1.0) * 1000.0),
    )
    assert sleeps == [threading.TIMEOUT_MAX]


def test_zero_elapsed_budget_allows_sync_initial_producer_request() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls: list[tuple[Any, ...]] = []

        def execute_command(self, *args: Any) -> bytes:
            self.calls.append(args)
            return b"OK"

    executor = Executor()
    client = FlowClient(executor, backpressure=_zero_retry_budget())

    assert client.enqueue("flow-1", type="order", now_ms=1) == b"OK"
    assert len(executor.calls) == 1


def test_zero_elapsed_budget_disables_sync_overload_retry() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls = 0

        def execute_command(self, *_args: Any) -> bytes:
            self.calls += 1
            raise OverloadedError("busy")

    executor = Executor()
    client = FlowClient(executor, backpressure=_zero_retry_budget())

    with pytest.raises(OverloadedError, match="busy"):
        client.enqueue("flow-1", type="order", now_ms=1)
    assert executor.calls == 1


def test_zero_elapsed_budget_allows_async_initial_request_and_disables_retry() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls = 0
            self.overloaded = False

        async def execute_command(self, *_args: Any) -> bytes:
            self.calls += 1
            if self.overloaded:
                raise OverloadedError("busy")
            return b"OK"

    async def run() -> None:
        executor = Executor()
        client = AsyncFlowClient(executor, backpressure=_zero_retry_budget())

        assert await client.enqueue("flow-1", type="order", now_ms=1) == b"OK"
        assert executor.calls == 1

        executor.overloaded = True
        with pytest.raises(OverloadedError, match="busy"):
            await client.enqueue("flow-2", type="order", now_ms=1)
        assert executor.calls == 2

    asyncio.run(run())
