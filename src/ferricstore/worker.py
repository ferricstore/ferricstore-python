from __future__ import annotations

import time
import uuid
import queue
import threading
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Literal

from ferricstore.client import FlowClient
from ferricstore.types import ClaimedItem, FlowRecord
from ferricstore.workflow import Workflow


FlowJob = ClaimedItem | FlowRecord
FlowHandler = Callable[[FlowJob], Any]
FlowBatchHandler = Callable[[list[FlowJob]], Any]
ErrorMode = Literal["retry", "fail", "raise"]


@dataclass(frozen=True)
class QueueFlowWorkerResult:
    claimed: int = 0
    completed: int = 0
    retried: int = 0
    failed: int = 0
    claim_calls: int = 0


@dataclass
class _HandledBatch:
    jobs: list[FlowJob]
    first_result: Any = None
    mixed_results: list[tuple[FlowJob, Any]] | None = None
    failures: list[tuple[FlowJob, Exception]] | None = None


@dataclass(frozen=True)
class FlowReadySignal:
    type: str
    state: str
    partition_key: str
    count: int = 1
    priority: int | None = 0
    server_shard: int | None = None
    epoch: int = 0
    due_at_ms: int | None = None


class FlowReadyCoordinator:
    """In-process ready-signal coordinator for QueueFlowWorker.

    Signals are advisory. CLAIM_DUE remains the correctness boundary.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._credits: dict[tuple[str, str, int | None, str, int | None], int] = {}
        self._meta: dict[tuple[str, str, int | None, str, int | None], FlowReadySignal] = {}
        self._queued: set[tuple[str, str, int | None, str, int | None]] = set()
        self._ready = deque()
        self.notifications = 0
        self.notified_jobs = 0

    def notify(self, signal: FlowReadySignal) -> None:
        if signal.count <= 0:
            return

        key = (signal.type, signal.state, signal.priority, signal.partition_key, signal.due_at_ms)
        with self._condition:
            self.notifications += 1
            self.notified_jobs += signal.count
            self._credits[key] = self._credits.get(key, 0) + signal.count
            current = self._meta.get(key)
            if current is None or signal.epoch >= current.epoch:
                self._meta[key] = signal
            if key not in self._queued:
                self._queued.add(key)
                self._ready.append(key)
            self._condition.notify()

    def notify_many(self, signals: Sequence[FlowReadySignal]) -> None:
        for signal in signals:
            self.notify(signal)

    def total_credit(self) -> int:
        with self._condition:
            return sum(self._credits.values())

    def matching_credit(
        self,
        *,
        type: str,
        state: str | None,
        states: Sequence[str] | None,
        priority: int | None,
        partition_keys: Sequence[str] | None,
    ) -> int:
        allowed_states = set(states) if states is not None else ({state} if state is not None else None)
        allowed_partitions = set(partition_keys) if partition_keys is not None else None

        with self._condition:
            total = 0
            for key in self._ready:
                credit = self._credits.get(key, 0)
                meta = self._meta.get(key)
                if credit <= 0 or meta is None:
                    continue
                if not self._signal_due_ready(meta):
                    continue
                if self._ready_key_matches(
                    key,
                    meta,
                    type=type,
                    allowed_states=allowed_states,
                    priority=priority,
                    allowed_partitions=allowed_partitions,
                    selected_shard=False,
                ):
                    total += credit
            return total

    def next_ready(
        self,
        *,
        type: str,
        state: str | None,
        states: Sequence[str] | None,
        priority: int | None,
        partition_keys: Sequence[str] | None,
        timeout_s: float,
        max_partitions: int,
        max_credit: int,
    ) -> tuple[list[str], int]:
        deadline = time.monotonic() + max(timeout_s, 0.0)
        allowed_states = set(states) if states is not None else ({state} if state is not None else None)
        allowed_partitions = set(partition_keys) if partition_keys is not None else None

        with self._condition:
            while True:
                selected, credit = self._select_ready_locked(
                    type=type,
                    allowed_states=allowed_states,
                    priority=priority,
                    allowed_partitions=allowed_partitions,
                    max_partitions=max(max_partitions, 1),
                    max_credit=max(max_credit, 1),
                )
                if selected:
                    return selected, credit

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise queue.Empty
                self._condition.wait(
                    self._wait_until_next_due_locked(
                        remaining,
                        type=type,
                        allowed_states=allowed_states,
                        priority=priority,
                        allowed_partitions=allowed_partitions,
                    )
                )

    def _select_ready_locked(
        self,
        *,
        type: str,
        allowed_states: set[str] | None,
        priority: int | None,
        allowed_partitions: set[str] | None,
        max_partitions: int,
        max_credit: int,
    ) -> tuple[list[str], int]:
        selected: list[tuple[str, str, int | None, str, int | None]] = []
        selected_partitions: list[str] = []
        selected_credit = 0
        selected_shard: int | None | bool = False
        scan_count = len(self._ready)

        for _ in range(scan_count):
            key = self._ready.popleft()
            self._queued.discard(key)
            credit = self._credits.get(key, 0)
            meta = self._meta.get(key)
            if credit <= 0 or meta is None:
                self._credits.pop(key, None)
                self._meta.pop(key, None)
                continue

            if not self._ready_key_matches(
                key,
                meta,
                type=type,
                allowed_states=allowed_states,
                priority=priority,
                allowed_partitions=allowed_partitions,
                selected_shard=selected_shard,
            ):
                self._queued.add(key)
                self._ready.append(key)
                continue

            if not self._signal_due_ready(meta):
                self._queued.add(key)
                self._ready.append(key)
                continue

            take = min(credit, max_credit - selected_credit)
            selected.append(key)
            selected_partitions.append(meta.partition_key)
            selected_credit += take
            selected_shard = meta.server_shard

            if len(selected) >= max_partitions or selected_credit >= max_credit:
                break

        remaining_budget = max_credit
        for key in selected:
            credit = self._credits.get(key, 0)
            taken = min(credit, remaining_budget)
            remaining_budget -= taken
            remaining = credit - taken
            if remaining > 0:
                self._credits[key] = remaining
                if key not in self._queued:
                    self._queued.add(key)
                    self._ready.append(key)
            else:
                self._credits.pop(key, None)
                self._meta.pop(key, None)

        return selected_partitions, min(selected_credit, max_credit)

    def _wait_until_next_due_locked(
        self,
        remaining: float,
        *,
        type: str,
        allowed_states: set[str] | None,
        priority: int | None,
        allowed_partitions: set[str] | None,
    ) -> float:
        now_ms = int(time.time() * 1000)
        next_due_ms: int | None = None

        for key in self._ready:
            meta = self._meta.get(key)
            credit = self._credits.get(key, 0)
            if credit <= 0 or meta is None:
                continue
            if not self._ready_key_matches(
                key,
                meta,
                type=type,
                allowed_states=allowed_states,
                priority=priority,
                allowed_partitions=allowed_partitions,
                selected_shard=False,
            ):
                continue
            if meta.due_at_ms is None or meta.due_at_ms <= now_ms:
                return 0.0
            if next_due_ms is None or meta.due_at_ms < next_due_ms:
                next_due_ms = meta.due_at_ms

        if next_due_ms is None:
            return remaining

        return min(remaining, max((next_due_ms - now_ms) / 1000.0, 0.0))

    @staticmethod
    def _signal_due_ready(meta: FlowReadySignal) -> bool:
        return meta.due_at_ms is None or meta.due_at_ms <= int(time.time() * 1000)

    @staticmethod
    def _ready_key_matches(
        key: tuple[str, str, int | None, str, int | None],
        meta: FlowReadySignal,
        *,
        type: str,
        allowed_states: set[str] | None,
        priority: int | None,
        allowed_partitions: set[str] | None,
        selected_shard: int | None | bool,
    ) -> bool:
        key_type, key_state, key_priority, key_partition, _key_due_at_ms = key
        if key_type != type:
            return False
        if allowed_states is not None and key_state not in allowed_states:
            return False
        if priority is not None and key_priority != priority:
            return False
        if allowed_partitions is not None and key_partition not in allowed_partitions:
            return False
        if selected_shard is not False and meta.server_shard != selected_shard:
            return False
        return True


class QueueFlowWorker:
    """High-level queue worker for the optimized FerricFlow hot path."""

    def __init__(
        self,
        client: FlowClient | str,
        *,
        type: str,
        worker: str | None = None,
        state: str | None = None,
        states: Sequence[str] | None = None,
        concurrency: int = 1,
        batch_size: int = 100,
        lease_ms: int = 30_000,
        priority: int | None = 0,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        on_error: ErrorMode = "retry",
        complete_independent: bool = True,
        partition_key: str | None = None,
        partition_keys: Sequence[str] | None = None,
        partition_indices: Sequence[int] | None = None,
        claim_partition_batch_size: int = 1,
        claim_drain_batches: int = 1,
        complete_async_depth: int = 0,
        completion_clients: Sequence[FlowClient] | None = None,
        wake_source: Any | None = None,
        wake_worker_index: int | None = None,
        wake_same_group: Callable[[int, int], bool] | None = None,
        wake_producers_done: Callable[[], bool] | None = None,
        wake_coalesce_s: float = 0.0,
        wake_fallback_after: int = 3,
        empty_claim_cooldown_s: float | None = None,
        partial_claim_cooldown_s: float | None = None,
    ) -> None:
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")
        if states is not None and not states:
            raise ValueError("states must be non-empty")
        if partition_keys is not None and not partition_keys:
            raise ValueError("partition_keys must be non-empty")
        if partition_indices is not None and partition_keys is None:
            raise ValueError("partition_indices requires partition_keys")
        if partition_indices is not None and len(partition_indices) != len(partition_keys or []):
            raise ValueError("partition_indices must match partition_keys length")
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if claim_partition_batch_size <= 0:
            raise ValueError("claim_partition_batch_size must be positive")
        if claim_drain_batches <= 0:
            raise ValueError("claim_drain_batches must be positive")
        if complete_async_depth < 0:
            raise ValueError("complete_async_depth must be non-negative")
        if wake_source is not None and wake_worker_index is None:
            raise ValueError("wake_worker_index is required when wake_source is set")
        if wake_coalesce_s < 0:
            raise ValueError("wake_coalesce_s must be non-negative")
        if wake_fallback_after <= 0:
            raise ValueError("wake_fallback_after must be positive")
        if empty_claim_cooldown_s is not None and empty_claim_cooldown_s < 0:
            raise ValueError("empty_claim_cooldown_s must be non-negative")
        if partial_claim_cooldown_s is not None and partial_claim_cooldown_s < 0:
            raise ValueError("partial_claim_cooldown_s must be non-negative")
        if on_error not in {"retry", "fail", "raise"}:
            raise ValueError("on_error must be 'retry', 'fail', or 'raise'")

        self.client = FlowClient.from_url(client) if isinstance(client, str) else client
        self.type = type
        self.worker = worker or f"{type}:worker:{uuid.uuid4().hex}"
        self.state = state
        self.states = list(states) if states is not None else None
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.lease_ms = lease_ms
        self.priority = priority
        self.reclaim_expired = reclaim_expired
        self.reclaim_ratio = reclaim_ratio
        self.claim_values = list(claim_values) if claim_values is not None else None
        self.value_max_bytes = value_max_bytes
        self.partition_key = partition_key
        self.partition_keys = list(partition_keys) if partition_keys is not None else None
        self.partition_indices = list(partition_indices) if partition_indices is not None else None
        self._partition_key_by_index = (
            dict(zip(self.partition_indices, self.partition_keys))
            if self.partition_indices is not None and self.partition_keys is not None
            else None
        )
        self.claim_partition_batch_size = claim_partition_batch_size
        self.claim_drain_batches = claim_drain_batches
        self.wake_source = wake_source
        self.wake_worker_index = wake_worker_index
        self.wake_same_group = wake_same_group
        self.wake_producers_done = wake_producers_done
        self.wake_coalesce_s = wake_coalesce_s
        self.wake_fallback_after = wake_fallback_after
        self.idle_sleep_s = max(idle_sleep_s, 0.0)
        self.max_idle_sleep_s = (
            max(max_idle_sleep_s, self.idle_sleep_s)
            if max_idle_sleep_s is not None
            else self.idle_sleep_s
        )
        self.on_error = on_error
        self.complete_independent = complete_independent
        self._running = False
        self._thread: threading.Thread | None = None
        self._totals = QueueFlowWorkerResult()
        self._executor = (
            ThreadPoolExecutor(max_workers=concurrency) if concurrency > 1 else None
        )
        self._completion_executor = (
            ThreadPoolExecutor(max_workers=complete_async_depth)
            if complete_async_depth > 0
            else None
        )
        self._completion_clients = (
            list(completion_clients)
            if completion_clients is not None
            else [self.client]
        )
        if not self._completion_clients:
            raise ValueError("completion_clients must be non-empty")
        self._completion_client_index = 0
        self._complete_async_depth = complete_async_depth
        self._pending_completions: list[Future[QueueFlowWorkerResult]] = []
        self._partition_cursor = 0
        self._wake_idle_rounds = 0
        self._wake_fallback_round = 0
        self._claim_cooldown_until: dict[str, float] = {}
        self.empty_claim_cooldown_s = (
            0.0 if empty_claim_cooldown_s is None else empty_claim_cooldown_s
        )
        self.partial_claim_cooldown_s = (
            0.0 if partial_claim_cooldown_s is None else partial_claim_cooldown_s
        )

    def run(self, handler: FlowHandler) -> None:
        self.run_forever(handler)

    def run_forever(self, handler: FlowHandler) -> None:
        self._run_loop(handler, batch_handler=False)

    def run_batch_forever(self, handler: FlowBatchHandler) -> None:
        self._run_loop(handler, batch_handler=True)

    def start(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool = False,
        daemon: bool = True,
    ) -> "QueueFlowWorker":
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("worker already started")

        self._thread = threading.Thread(
            target=self._run_loop,
            args=(handler,),
            kwargs={"batch_handler": batch_handler},
            daemon=daemon,
        )
        self._thread.start()
        return self

    def join(self, timeout: float | None = None) -> QueueFlowWorkerResult:
        if self._thread is not None:
            self._thread.join(timeout)
        return self.stats

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> QueueFlowWorkerResult:
        return self._totals

    def _run_loop(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> None:
        self._running = True
        idle_sleep_s = self.idle_sleep_s
        try:
            while self._running:
                result = (
                    self.run_batch_once(handler)
                    if batch_handler
                    else self.run_once(handler)
                )
                self._totals = self._merge_results(self._totals, result)
                if result.claimed == 0:
                    time.sleep(idle_sleep_s)
                    idle_sleep_s = min(
                        self.max_idle_sleep_s,
                        max(idle_sleep_s * 2, self.idle_sleep_s),
                    )
                else:
                    idle_sleep_s = self.idle_sleep_s
        finally:
            self._running = False

    def stop(self) -> None:
        self._running = False

    def close(self) -> None:
        self.stop()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join()
        result = self.flush()
        self._totals = self._merge_results(self._totals, result)
        if self._executor is not None:
            self._executor.shutdown(wait=True)
        if self._completion_executor is not None:
            self._completion_executor.shutdown(wait=True)

    def flush(self) -> QueueFlowWorkerResult:
        return self._drain_pending_completions(block=True)

    def run_once(self, handler: FlowHandler) -> QueueFlowWorkerResult:
        return self._run_once(handler, batch_handler=False)

    def run_batch_once(self, handler: FlowBatchHandler) -> QueueFlowWorkerResult:
        return self._run_once(handler, batch_handler=True)

    def _run_once(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> QueueFlowWorkerResult:
        result = self._drain_pending_completions(block=False)
        if self.wake_source is not None:
            wake_plan = self._next_wake_claim()
            if wake_plan is not None:
                self._wake_idle_rounds = 0
                claim_partition_key, claim_partition_keys, claim_credit = wake_plan
                return self._drain_claim_plan(
                    handler,
                    result,
                    claim_partition_key,
                    claim_partition_keys,
                    claim_credit,
                    batch_handler=batch_handler,
                )

            self._wake_idle_rounds += 1
            if self._wake_idle_rounds < self.wake_fallback_after:
                return result
            self._wake_idle_rounds = 0
            claim_partition_key, claim_partition_keys = self._fallback_claim_partition()
            return self._drain_claim_plan(
                handler,
                result,
                claim_partition_key,
                claim_partition_keys,
                self.batch_size,
                batch_handler=batch_handler,
            )

        claim_partition_key, claim_partition_keys = self._next_claim_partition()
        max_credit = self.batch_size * self.claim_drain_batches
        return self._drain_claim_plan(
            handler,
            result,
            claim_partition_key,
            claim_partition_keys,
            max_credit,
            batch_handler=batch_handler,
        )

    def _drain_claim_plan(
        self,
        handler: FlowHandler | FlowBatchHandler,
        result: QueueFlowWorkerResult,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        claim_credit: int,
        *,
        batch_handler: bool,
    ) -> QueueFlowWorkerResult:
        remaining_credit = max(claim_credit, 1)
        drain_count = 0

        while remaining_credit > 0 and drain_count < self.claim_drain_batches:
            limit = min(self.batch_size, remaining_credit)
            jobs = self._claim_jobs(
                claim_partition_key=claim_partition_key,
                claim_partition_keys=claim_partition_keys,
                limit=limit,
            )
            result = self._merge_results(result, QueueFlowWorkerResult(claim_calls=1))
            if not jobs:
                self._cool_claim_keys(claim_partition_key, claim_partition_keys, self.empty_claim_cooldown_s)
                break

            handled = (
                self._run_batch_handler(jobs, handler)
                if batch_handler
                else self._run_handlers(jobs, handler)
            )
            result = self._merge_results(
                result,
                QueueFlowWorkerResult(claimed=len(jobs)),
            )

            if self._completion_executor is not None:
                while len(self._pending_completions) >= self._complete_async_depth:
                    result = self._merge_results(
                        result,
                        self._drain_pending_completions(block=True, limit=1),
                    )
                complete_client = self._next_completion_client()
                self._pending_completions.append(
                    self._completion_executor.submit(
                        self._finish_batch,
                        handled,
                        complete_client,
                    )
                )
            else:
                result = self._merge_results(
                    result,
                    self._finish_batch(handled, self.client),
                )

            remaining_credit -= len(jobs)
            drain_count += 1

            if len(jobs) < limit:
                self._cool_claim_keys(
                    claim_partition_key,
                    claim_partition_keys,
                    self.partial_claim_cooldown_s,
                )
                break

        return result

    def _claim_jobs(
        self,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
    ) -> list[FlowJob]:
        if self.claim_values:
            return self.client.claim_due(
                self.type,
                state=self.state,
                states=self.states,
                worker=self.worker,
                partition_key=claim_partition_key,
                partition_keys=claim_partition_keys,
                lease_ms=self.lease_ms,
                limit=limit,
                priority=self.priority,
                reclaim_expired=self.reclaim_expired,
                reclaim_ratio=self.reclaim_ratio,
                payload=False,
                values=self.claim_values,
                value_max_bytes=self.value_max_bytes,
            )

        return self.client.claim_jobs(
            self.type,
            state=self.state,
            states=self.states,
            worker=self.worker,
            partition_key=claim_partition_key,
            partition_keys=claim_partition_keys,
            lease_ms=self.lease_ms,
            limit=limit,
            priority=self.priority,
            reclaim_expired=self.reclaim_expired,
            reclaim_ratio=self.reclaim_ratio,
        )

    def _next_wake_claim(self) -> tuple[str | None, list[str] | None, int] | None:
        if self.wake_source is None or self.wake_worker_index is None:
            return None
        next_ready = getattr(self.wake_source, "next_ready", None)
        if callable(next_ready):
            try:
                wake_partition_keys = (
                    [self.partition_key] if self.partition_key is not None else self.partition_keys
                )
                partition_keys, partition_credit = next_ready(
                    type=self.type,
                    state=self.state,
                    states=self.states,
                    priority=self.priority,
                    partition_keys=wake_partition_keys,
                    timeout_s=self.idle_sleep_s,
                    max_partitions=self.claim_partition_batch_size,
                    max_credit=self.batch_size,
                )
            except queue.Empty:
                return None

            if not partition_keys or partition_credit <= 0:
                return None
            if (
                self.wake_coalesce_s > 0
                and partition_credit < self.batch_size
                and not self._wake_producers_are_done()
            ):
                time.sleep(min(self.wake_coalesce_s, 0.002))
                remaining_partitions = max(self.claim_partition_batch_size - len(partition_keys), 0)
                remaining_credit = max(self.batch_size - partition_credit, 0)
                if remaining_partitions > 0 and remaining_credit > 0:
                    try:
                        extra_keys, extra_credit = next_ready(
                            type=self.type,
                            state=self.state,
                            states=self.states,
                            priority=self.priority,
                            partition_keys=wake_partition_keys,
                            timeout_s=0,
                            max_partitions=remaining_partitions,
                            max_credit=remaining_credit,
                        )
                    except queue.Empty:
                        extra_keys = []
                        extra_credit = 0
                    partition_keys.extend(extra_keys)
                    partition_credit += extra_credit
            if len(partition_keys) == 1:
                return partition_keys[0], None, partition_credit
            return None, partition_keys, partition_credit

        if self._partition_key_by_index is None:
            return None

        try:
            partition_indices, partition_credit = self.wake_source.next_partitions(
                self.wake_worker_index,
                self.idle_sleep_s,
                self.claim_partition_batch_size,
                self.batch_size,
                same_group=self.wake_same_group,
            )
        except queue.Empty:
            return None

        if not partition_indices or partition_credit <= 0:
            return None

        if (
            self.wake_coalesce_s > 0
            and partition_credit < self.batch_size
            and not self._wake_producers_are_done()
        ):
            time.sleep(min(self.wake_coalesce_s, 0.002))
            partition_credit += self.wake_source.take_credit(
                self.wake_worker_index,
                partition_indices[0],
            )
            remaining_credit = max(
                self.batch_size - partition_credit,
                0,
            )
            if len(partition_indices) < self.claim_partition_batch_size and remaining_credit > 0:
                try:
                    extra_indices, extra_credit = self.wake_source.next_partitions(
                        self.wake_worker_index,
                        0,
                        self.claim_partition_batch_size - len(partition_indices),
                        remaining_credit,
                        same_group=self.wake_same_group,
                    )
                except queue.Empty:
                    extra_indices = []
                    extra_credit = 0
                partition_indices.extend(extra_indices)
                partition_credit += extra_credit

        partition_keys = [
            self._partition_key_by_index[index]
            for index in partition_indices
            if index in self._partition_key_by_index
        ]
        if not partition_keys:
            return None
        if len(partition_keys) == 1:
            return partition_keys[0], None, partition_credit
        return None, partition_keys, partition_credit

    def _wake_should_wait_for_signal(self) -> bool:
        producers_done = self.wake_producers_done
        if producers_done is not None and not producers_done():
            return True

        matching_credit = getattr(self.wake_source, "matching_credit", None)
        if callable(matching_credit):
            try:
                wake_partition_keys = (
                    [self.partition_key] if self.partition_key is not None else self.partition_keys
                )
                return (
                    matching_credit(
                        type=self.type,
                        state=self.state,
                        states=self.states,
                        priority=self.priority,
                        partition_keys=wake_partition_keys,
                    )
                    > 0
                )
            except Exception:
                return False

        total_credit = getattr(self.wake_source, "total_credit", None)
        if callable(total_credit):
            try:
                return total_credit() > 0
            except Exception:
                return False

        return False

    def _wake_producers_are_done(self) -> bool:
        producers_done = self.wake_producers_done
        if producers_done is None:
            return False
        return producers_done()

    def _run_handlers(
        self,
        jobs: list[FlowJob],
        handler: FlowHandler,
    ) -> _HandledBatch:
        success_jobs: list[FlowJob] = []
        failures: list[tuple[FlowJob, Exception]] = []
        first_result: Any = None
        first_result_set = False
        mixed_results: list[tuple[ClaimedItem, Any]] | None = None

        def record_success(job: FlowJob, result: Any) -> None:
            nonlocal first_result, first_result_set, mixed_results
            if not first_result_set:
                first_result = result
                first_result_set = True
                success_jobs.append(job)
                return

            if mixed_results is None and result != first_result:
                mixed_results = [(existing, first_result) for existing in success_jobs]

            success_jobs.append(job)
            if mixed_results is not None:
                mixed_results.append((job, result))

        if self._executor is None:
            for job in jobs:
                try:
                    record_success(job, handler(job))
                except Exception as exc:
                    failures.append((job, exc))
            return _HandledBatch(
                jobs=success_jobs,
                first_result=first_result,
                mixed_results=mixed_results,
                failures=failures,
            )

        futures = [(job, self._executor.submit(handler, job)) for job in jobs]
        for job, future in futures:
            try:
                record_success(job, future.result())
            except Exception as exc:
                failures.append((job, exc))
        return _HandledBatch(
            jobs=success_jobs,
            first_result=first_result,
            mixed_results=mixed_results,
            failures=failures,
        )

    def _run_batch_handler(
        self,
        jobs: list[FlowJob],
        handler: FlowBatchHandler,
    ) -> _HandledBatch:
        try:
            result = handler(jobs)
        except Exception as exc:
            return _HandledBatch(jobs=[], failures=[(job, exc) for job in jobs])

        return _HandledBatch(jobs=jobs, first_result=result)

    def _next_claim_partition(self) -> tuple[str | None, list[str] | None]:
        if self.partition_key is not None:
            return self.partition_key, None
        if not self.partition_keys:
            return None, None

        count = min(self.claim_partition_batch_size, len(self.partition_keys))
        now = time.monotonic()
        key_count = len(self.partition_keys)
        selected_start = self._partition_cursor
        for step in range(key_count):
            start = (self._partition_cursor + step * count) % key_count
            keys = [
                self.partition_keys[(start + offset) % key_count]
                for offset in range(count)
            ]
            if any(self._claim_cooldown_until.get(key, 0.0) <= now for key in keys):
                selected_start = start
                break
        else:
            keys = [
                self.partition_keys[(selected_start + offset) % key_count]
                for offset in range(count)
            ]

        self._partition_cursor = (selected_start + count) % key_count
        if len(keys) == 1:
            return keys[0], None
        return None, keys

    def _fallback_claim_partition(self) -> tuple[str | None, list[str] | None]:
        if self.partition_key is not None:
            return self.partition_key, None
        if self.partition_keys:
            return None, self.partition_keys
        return None, None

    def _cool_claim_keys(
        self,
        partition_key: str | None,
        partition_keys: list[str] | None,
        cooldown_s: float,
    ) -> None:
        if cooldown_s <= 0:
            return
        keys: list[str]
        if partition_key is not None:
            keys = [partition_key]
        elif partition_keys:
            keys = partition_keys
        else:
            return
        until = time.monotonic() + cooldown_s
        for key in keys:
            self._claim_cooldown_until[key] = until

    def _finish_batch(
        self,
        handled: _HandledBatch,
        client: FlowClient,
    ) -> QueueFlowWorkerResult:
        completed = self._complete_successes(handled, client)
        retried, failed = self._handle_failures(handled.failures or [], client)
        return QueueFlowWorkerResult(completed=completed, retried=retried, failed=failed)

    def _next_completion_client(self) -> FlowClient:
        client = self._completion_clients[
            self._completion_client_index % len(self._completion_clients)
        ]
        self._completion_client_index += 1
        return client

    def _drain_pending_completions(
        self,
        *,
        block: bool,
        limit: int | None = None,
    ) -> QueueFlowWorkerResult:
        if not self._pending_completions:
            return QueueFlowWorkerResult()

        drained = QueueFlowWorkerResult()
        if block:
            remaining = len(self._pending_completions) if limit is None else min(limit, len(self._pending_completions))
            for _ in range(remaining):
                future = self._pending_completions.pop(0)
                drained = self._merge_results(drained, future.result())
            return drained

        ready: list[Future[QueueFlowWorkerResult]] = []
        pending: list[Future[QueueFlowWorkerResult]] = []
        for future in self._pending_completions:
            if future.done():
                ready.append(future)
            else:
                pending.append(future)
        self._pending_completions = pending
        for future in ready:
            drained = self._merge_results(drained, future.result())
        return drained

    @staticmethod
    def _merge_results(left: QueueFlowWorkerResult, right: QueueFlowWorkerResult) -> QueueFlowWorkerResult:
        return QueueFlowWorkerResult(
            claimed=left.claimed + right.claimed,
            completed=left.completed + right.completed,
            retried=left.retried + right.retried,
            failed=left.failed + right.failed,
            claim_calls=left.claim_calls + right.claim_calls,
        )

    def _complete_successes(
        self,
        handled: _HandledBatch,
        client: FlowClient,
    ) -> int:
        if not handled.jobs:
            return 0

        if handled.mixed_results is None:
            client.complete_jobs(
                handled.jobs,
                result=handled.first_result,
                independent=self.complete_independent,
            )
            return len(handled.jobs)

        for job, result in handled.mixed_results:
            client.complete(
                job.id,
                lease_token=job.lease_token,
                fencing_token=job.fencing_token,
                partition_key=job.partition_key,
                result=result,
                return_record=False,
            )
        return len(handled.jobs)

    def _handle_failures(
        self,
        failures: list[tuple[FlowJob, Exception]],
        client: FlowClient,
    ) -> tuple[int, int]:
        if not failures:
            return 0, 0

        jobs = [job for job, _exc in failures]
        if self.on_error == "raise":
            raise failures[0][1]

        grouped: dict[str, list[FlowJob]] = {}
        for job, exc in failures:
            grouped.setdefault(str(exc), []).append(job)

        if self.on_error == "fail":
            for message, group_jobs in grouped.items():
                client.fail_many(
                    None,
                    group_jobs,
                    error=message,
                    independent=self.complete_independent,
                )
            return 0, len(jobs)

        for message, group_jobs in grouped.items():
            client.retry_many(
                None,
                group_jobs,
                error=message,
                independent=self.complete_independent,
            )
        self._notify_retried_jobs(jobs)
        return len(jobs), 0

    def _notify_retried_jobs(self, jobs: list[FlowJob]) -> None:
        if self.wake_source is None or not jobs:
            return

        signals: list[FlowReadySignal] = []
        for job in jobs:
            partition_key = (
                getattr(job, "partition_key", None)
                or self.partition_key
                or (self.partition_keys[0] if self.partition_keys else None)
            )
            if partition_key is None:
                continue
            for state in self._retry_wake_states(job):
                signals.append(
                    FlowReadySignal(
                        type=self.type,
                        state=state,
                        partition_key=partition_key,
                        priority=self.priority,
                    )
                )

        if not signals:
            return

        notify_many = getattr(self.wake_source, "notify_many", None)
        if callable(notify_many):
            notify_many(signals)
            return

        notify = getattr(self.wake_source, "notify", None)
        if callable(notify):
            for signal in signals:
                notify(signal)

    def _retry_wake_states(self, job: FlowJob) -> list[str]:
        run_state = getattr(job, "run_state", None)
        if run_state:
            return [run_state]

        job_state = getattr(job, "state", None)
        if job_state not in (None, "running"):
            return [job_state]

        if self.state is not None:
            return [self.state]
        if self.states:
            return list(dict.fromkeys(self.states))
        return ["queued"]


class Worker:
    """Simple polling worker for one workflow definition."""

    def __init__(
        self,
        workflow: Workflow,
        *,
        worker: str,
        states: list[str] | None = None,
        partition_key: str | None = None,
        limit: int = 10,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        partial_retry_delay_s: float = 0.001,
        partial_retries: int = 1,
        priority: int | None = None,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
    ) -> None:
        if limit <= 0:
            raise ValueError("limit must be positive")
        if states == []:
            raise ValueError("states must be non-empty")

        self.workflow = workflow
        self.worker = worker
        self.states = list(workflow._states.keys()) if states is None else states
        self.partition_key = partition_key
        self.limit = limit
        self.idle_sleep_s = idle_sleep_s
        self.max_idle_sleep_s = max_idle_sleep_s if max_idle_sleep_s is not None else idle_sleep_s
        self.partial_retry_delay_s = partial_retry_delay_s
        self.partial_retries = max(0, partial_retries)
        self.priority = priority
        self.reclaim_expired = reclaim_expired
        self.reclaim_ratio = reclaim_ratio
        self._running = False

    def run_forever(self) -> None:
        self._running = True
        idle_sleep_s = self.idle_sleep_s
        while self._running:
            processed = self.run_once()
            if processed == 0:
                time.sleep(idle_sleep_s)
                idle_sleep_s = min(
                    max(self.max_idle_sleep_s, self.idle_sleep_s),
                    max(idle_sleep_s * 2, self.idle_sleep_s),
                )
            else:
                idle_sleep_s = self.idle_sleep_s

    def stop(self) -> None:
        self._running = False

    def run_once(self) -> int:
        processed = 0
        for state in self.states:
            retries_left = self.partial_retries
            while True:
                results = self.workflow.run_once(
                    state,
                    worker=self.worker,
                    partition_key=self.partition_key,
                    limit=self.limit,
                    priority=self.priority,
                    reclaim_expired=self.reclaim_expired,
                    reclaim_ratio=self.reclaim_ratio,
                )
                processed += len(results)
                if len(results) >= self.limit:
                    continue
                if len(results) == 0 or retries_left <= 0:
                    break
                retries_left -= 1
                if self.partial_retry_delay_s > 0:
                    time.sleep(self.partial_retry_delay_s)
        return processed
