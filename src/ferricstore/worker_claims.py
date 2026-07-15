from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from ferricstore.batch_core import BatchValueMatcher, run_sync_fanout_on_executor
from ferricstore.client_core import FlowClient
from ferricstore.worker_completion import FlowJob, _HandledBatch
from ferricstore.worker_core import CloseDeadline, SyncWorkerRunGate
from ferricstore.worker_models import QueueFlowWorkerResult

FlowHandler = Callable[[FlowJob], Any]
FlowBatchHandler = Callable[[list[FlowJob]], Any]


@dataclass
class _PendingClaim:
    future: Future[list[FlowJob]]
    partition_key: str | None
    partition_keys: list[str] | None
    handler: FlowHandler | FlowBatchHandler
    batch_handler: bool


class SyncWorkerClaimMixin:
    """Claim prefetch, handler dispatch, and partition scheduling for a sync worker."""

    if TYPE_CHECKING:
        batch_size: int
        block_ms: int | None
        claim_client: FlowClient
        claim_drain_batches: int
        claim_partition_batch_size: int
        claim_prefetch: int
        claim_scan_block_ms: int | None
        claim_values: list[str] | None
        client: FlowClient
        concurrency: int
        lease_ms: int
        partition_key: str | None
        partition_keys: list[str] | None
        priority: int | None
        reclaim_expired: bool | None
        reclaim_ratio: int | None
        scan_before_blocking: bool
        state: str | None
        states: list[str] | None
        type: str
        value_max_bytes: int | None
        worker: str
        _claim_cooldown_until: dict[str, float]
        _complete_async_depth: int
        _completion_executor: ThreadPoolExecutor | None
        _executor: ThreadPoolExecutor | None
        _partition_cursor: int
        _pending_claims: list[_PendingClaim]
        _pending_completions: deque[Future[QueueFlowWorkerResult]]
        _run_gate: SyncWorkerRunGate
        _stop_event: threading.Event

        def _drain_pending_completions(
            self,
            *,
            block: bool,
            limit: int | None = None,
            deadline: CloseDeadline | None = None,
        ) -> QueueFlowWorkerResult: ...

        def _finish_batch(
            self,
            handled: _HandledBatch,
            client: FlowClient,
        ) -> QueueFlowWorkerResult: ...

        def _merge_results(
            self,
            left: QueueFlowWorkerResult,
            right: QueueFlowWorkerResult,
        ) -> QueueFlowWorkerResult: ...

        def _next_completion_client(self) -> FlowClient: ...

    def _available_claim_capacity(self) -> int:
        if self._complete_async_depth <= 0:
            return self.batch_size * max(self.claim_drain_batches, 1)

        completion_slots = self._complete_async_depth - len(self._pending_completions)
        if completion_slots <= 0:
            return 0

        return self.batch_size * max(1, min(self.claim_drain_batches, completion_slots))

    def _can_prefetch_claims(self) -> bool:
        if self.claim_prefetch <= 0 or self.block_ms is None:
            return False
        method_name = "claim_due_future" if self.claim_values else "claim_flows_future"
        return callable(getattr(self.claim_client, method_name, None))

    def _fill_pending_claims(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> None:
        if self._stop_event.is_set() or self._run_gate.closing:
            return
        while len(self._pending_claims) < self.claim_prefetch:
            partition_key, partition_keys = self._next_claim_partition()
            self._pending_claims.append(
                _PendingClaim(
                    future=self._claim_flows_future(
                        claim_partition_key=partition_key,
                        claim_partition_keys=partition_keys,
                        limit=self.batch_size,
                        block_ms=self.block_ms,
                    ),
                    partition_key=partition_key,
                    partition_keys=partition_keys,
                    handler=handler,
                    batch_handler=batch_handler,
                )
            )

    def _take_pending_claim(self, *, block: bool) -> _PendingClaim | None:
        if not self._pending_claims:
            return None

        for idx, pending in enumerate(self._pending_claims):
            if pending.future.done():
                return self._pending_claims.pop(idx)

        if not block:
            return None

        done, _pending = wait(
            [pending.future for pending in self._pending_claims],
            return_when=FIRST_COMPLETED,
        )
        if not done:
            return None
        done_future = next(iter(done))
        for idx, pending in enumerate(self._pending_claims):
            if pending.future is done_future:
                return self._pending_claims.pop(idx)
        return None

    def _claim_flows_future(
        self,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
        block_ms: int | None,
    ) -> Future[list[FlowJob]]:
        if self.claim_values:
            return cast(
                Future[list[FlowJob]],
                self.claim_client.claim_due_future(
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
                    block_ms=block_ms,
                    payload=False,
                    values=self.claim_values,
                    value_max_bytes=self.value_max_bytes,
                ),
            )

        return cast(
            Future[list[FlowJob]],
            self.claim_client.claim_flows_future(
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
                block_ms=block_ms,
            ),
        )

    def _process_claimed_jobs(
        self,
        jobs: list[FlowJob],
        handler: FlowHandler | FlowBatchHandler,
        result: QueueFlowWorkerResult,
        *,
        batch_handler: bool,
    ) -> QueueFlowWorkerResult:
        handled = (
            self._run_batch_handler(jobs, cast(FlowBatchHandler, handler))
            if batch_handler
            else self._run_handlers(jobs, cast(FlowHandler, handler))
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
            return result

        return self._merge_results(
            result,
            self._finish_batch(handled, self.client),
        )

    def _claim_flows(
        self,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
        block_ms: int | None,
    ) -> list[FlowJob]:
        if self.claim_values:
            return cast(
                list[FlowJob],
                self.claim_client.claim_due(
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
                    block_ms=block_ms,
                    payload=False,
                    values=self.claim_values,
                    value_max_bytes=self.value_max_bytes,
                ),
            )

        return cast(
            list[FlowJob],
            self.claim_client.claim_flows(
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
                block_ms=block_ms,
            ),
        )

    def _should_scan_owned_partitions_before_blocking(self) -> bool:
        return (
            self.scan_before_blocking
            and self.block_ms is not None
            and self.block_ms > 0
            and self.partition_key is None
            and bool(self.partition_keys)
        )

    def _should_block_on_owned_partitions(self) -> bool:
        return (
            not self.scan_before_blocking
            and self.block_ms is not None
            and self.block_ms > 0
            and self.partition_key is None
            and bool(self.partition_keys)
        )

    def _post_scan_block_ms(self) -> int | None:
        return self.block_ms if self.claim_scan_block_ms is None else self.claim_scan_block_ms

    def _owned_partition_scan_pages(self) -> int:
        if not self.partition_keys:
            return 1
        return max(
            1,
            (len(self.partition_keys) + self.claim_partition_batch_size - 1)
            // self.claim_partition_batch_size,
        )

    def _owned_partition_block_claim(self) -> tuple[str | None, list[str] | None]:
        if not self.partition_keys:
            return None, None
        if len(self.partition_keys) == 1:
            return self.partition_keys[0], None
        return None, list(self.partition_keys)

    def _run_handlers(
        self,
        jobs: list[FlowJob],
        handler: FlowHandler,
    ) -> _HandledBatch:
        success_jobs: list[FlowJob] = []
        failures: list[tuple[FlowJob, Exception]] = []
        first_result: Any = None
        first_result_set = False
        first_result_matcher: BatchValueMatcher | None = None
        mixed_results: list[tuple[FlowJob, Any]] | None = None

        def record_success(job: FlowJob, result: Any) -> None:
            nonlocal first_result, first_result_matcher, first_result_set, mixed_results
            if not first_result_set:
                first_result = result
                first_result_set = True
                first_result_matcher = BatchValueMatcher(result)
                success_jobs.append(job)
                return

            if (
                mixed_results is None
                and first_result_matcher is not None
                and not first_result_matcher.matches(result)
            ):
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

        def run_one(job: FlowJob) -> tuple[FlowJob, bool, Any]:
            try:
                return job, True, handler(job)
            except Exception as exc:
                return job, False, exc

        results = run_sync_fanout_on_executor(
            jobs,
            run_one,
            executor=self._executor,
            max_concurrency=self.concurrency,
        )
        for job, succeeded, value in results:
            if succeeded:
                record_success(job, value)
            else:
                failures.append((job, cast(Exception, value)))
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
            keys = [self.partition_keys[(start + offset) % key_count] for offset in range(count)]
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


__all__ = [
    "FlowBatchHandler",
    "FlowHandler",
    "SyncWorkerClaimMixin",
    "_PendingClaim",
]
