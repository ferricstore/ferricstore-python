from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.client import FlowClient
from ferricstore.types import (
    ClaimedFlow,
    ExceptionPolicy,
    FlowRecord,
    RetryPolicy,
    ValueConfig,
    WorkerConfig,
    normalize_exception_policy,
    resolve_worker_connection_counts,
)
from ferricstore.workflow import Workflow

FlowJob = ClaimedFlow | FlowRecord
FlowHandler = Callable[[FlowJob], Any]
FlowBatchHandler = Callable[[list[FlowJob]], Any]
ErrorMode = ExceptionPolicy | str
QUEUE_WORKER_CONFIG_KEYS = frozenset(
    {
        "concurrency",
        "command_connections",
        "claim_connections",
        "batch_size",
        "lease_ms",
        "priority",
        "reclaim_expired",
        "reclaim_ratio",
        "claim_values",
        "value_max_bytes",
        "block_ms",
        "claim_scan_block_ms",
        "idle_sleep_s",
        "max_idle_sleep_s",
        "exception_policy",
        "complete_independent",
        "claim_partition_batch_size",
        "claim_drain_batches",
        "claim_prefetch",
        "protocol_wake_hints",
        "scan_before_blocking",
        "complete_async_depth",
        "fuse_complete_claim",
        "empty_claim_cooldown_s",
        "partial_claim_cooldown_s",
    }
)

_PROTOCOL_URL_SCHEMES = {"ferric", "ferrics"}


def _is_protocol_url(value: str) -> bool:
    return urlparse(value).scheme.lower() in _PROTOCOL_URL_SCHEMES


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


@dataclass
class _PendingClaim:
    future: Future[list[FlowJob]]
    partition_key: str | None
    partition_keys: list[str] | None


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
        batch_size: int = 10,
        lease_ms: int = 30_000,
        priority: int | None = 0,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        claim_values: Sequence[str] | None = None,
        value_max_bytes: int | None = None,
        block_ms: int | None = None,
        claim_scan_block_ms: int | None = None,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        exception_policy: ErrorMode | None = None,
        on_error: ErrorMode | None = None,
        complete_independent: bool = True,
        partition_key: str | None = None,
        partition_keys: Sequence[str] | None = None,
        claim_partition_batch_size: int | None = None,
        claim_drain_batches: int = 1,
        claim_prefetch: int = 0,
        protocol_wake_hints: bool = False,
        scan_before_blocking: bool = False,
        complete_async_depth: int = 0,
        fuse_complete_claim: bool = False,
        completion_clients: Sequence[FlowClient] | None = None,
        claim_client: FlowClient | str | None = None,
        command_connections: int | None = None,
        claim_connections: int | None = None,
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
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if claim_partition_batch_size is not None and claim_partition_batch_size <= 0:
            raise ValueError("claim_partition_batch_size must be positive")
        if claim_drain_batches <= 0:
            raise ValueError("claim_drain_batches must be positive")
        if claim_prefetch < 0:
            raise ValueError("claim_prefetch must be non-negative")
        if complete_async_depth < 0:
            raise ValueError("complete_async_depth must be non-negative")
        if block_ms is not None and block_ms < 0:
            raise ValueError("block_ms must be non-negative")
        if claim_scan_block_ms is not None and claim_scan_block_ms < 0:
            raise ValueError("claim_scan_block_ms must be non-negative")
        if empty_claim_cooldown_s is not None and empty_claim_cooldown_s < 0:
            raise ValueError("empty_claim_cooldown_s must be non-negative")
        if partial_claim_cooldown_s is not None and partial_claim_cooldown_s < 0:
            raise ValueError("partial_claim_cooldown_s must be non-negative")
        if exception_policy is not None and on_error is not None:
            raise ValueError("exception_policy and on_error are mutually exclusive")
        resolved_on_error = normalize_exception_policy(
            exception_policy if exception_policy is not None else on_error,
            argument="exception_policy" if exception_policy is not None else "on_error",
        )

        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=1,
            concurrency=concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
        command_max_connections = (
            1 if isinstance(client, str) and command_connections is None else command_pool_size
        )
        if isinstance(client, str):
            self.client = FlowClient.from_url(client, max_connections=command_max_connections)
            self._owns_client = True
        else:
            self.client = client
            self._owns_client = False
        if claim_client is None:
            if isinstance(client, str):
                self.claim_client = self.client
                self._owns_claim_client = False
            else:
                self.claim_client = self.client
                self._owns_claim_client = False
        elif isinstance(claim_client, str):
            self.claim_client = FlowClient.from_url(claim_client, max_connections=claim_pool_size)
            self._owns_claim_client = True
        else:
            self.claim_client = claim_client
            self._owns_claim_client = False
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
        self.block_ms = block_ms
        self.claim_scan_block_ms = claim_scan_block_ms
        self.partition_key = partition_key
        self.partition_keys = list(partition_keys) if partition_keys is not None else None
        self.claim_partition_batch_size = (
            claim_partition_batch_size
            if claim_partition_batch_size is not None
            else len(self.partition_keys or []) or 1
        )
        self.claim_drain_batches = claim_drain_batches
        self.claim_prefetch = claim_prefetch
        self.protocol_wake_hints = bool(protocol_wake_hints)
        self.scan_before_blocking = scan_before_blocking
        self.idle_sleep_s = max(idle_sleep_s, 0.0)
        self.max_idle_sleep_s = (
            max(max_idle_sleep_s, self.idle_sleep_s)
            if max_idle_sleep_s is not None
            else self.idle_sleep_s
        )
        self.on_error = resolved_on_error
        self.complete_independent = complete_independent
        self._running = False
        self._thread: threading.Thread | None = None
        self._totals = QueueFlowWorkerResult()
        self._executor = ThreadPoolExecutor(max_workers=concurrency) if concurrency > 1 else None
        self._completion_executor = (
            ThreadPoolExecutor(max_workers=complete_async_depth)
            if complete_async_depth > 0
            else None
        )
        self._completion_clients = (
            list(completion_clients) if completion_clients is not None else [self.client]
        )
        if not self._completion_clients:
            raise ValueError("completion_clients must be non-empty")
        self._completion_client_index = 0
        self._complete_async_depth = complete_async_depth
        self.fuse_complete_claim = bool(fuse_complete_claim)
        self._pending_completions: list[Future[QueueFlowWorkerResult]] = []
        self._pending_claims: list[_PendingClaim] = []
        self._partition_cursor = 0
        self._claim_cooldown_until: dict[str, float] = {}
        default_claim_cooldown_s = min(self.idle_sleep_s, 0.001)
        self.empty_claim_cooldown_s = (
            default_claim_cooldown_s if empty_claim_cooldown_s is None else empty_claim_cooldown_s
        )
        self.partial_claim_cooldown_s = (
            default_claim_cooldown_s
            if partial_claim_cooldown_s is None
            else partial_claim_cooldown_s
        )
        self._protocol_wake_hints_enabled = False
        self._subscribe_protocol_wake_hints()

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
    ) -> QueueFlowWorker:
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
                    self.run_batch_once(cast(FlowBatchHandler, handler))
                    if batch_handler
                    else self.run_once(cast(FlowHandler, handler))
                )
                self._totals = self._merge_results(self._totals, result)
                if result.claimed == 0:
                    if not self._wait_for_protocol_wake_hint(idle_sleep_s):
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
        self._clear_pending_claims()
        result = self.flush()
        self._totals = self._merge_results(self._totals, result)
        if self._executor is not None:
            self._executor.shutdown(wait=True)
        if self._completion_executor is not None:
            self._completion_executor.shutdown(wait=True)
        if self._owns_claim_client and self.claim_client is not self.client:
            self.claim_client.close()
        if self._owns_client:
            self.client.close()

    def flush(self) -> QueueFlowWorkerResult:
        return self._drain_pending_completions(block=True)

    def _clear_pending_claims(self) -> None:
        for pending in self._pending_claims:
            pending.future.cancel()
        self._pending_claims.clear()

    def _subscribe_protocol_wake_hints(self) -> None:
        if not self.protocol_wake_hints:
            return
        subscribe = getattr(self.claim_client, "subscribe_flow_wake", None)
        wait_event = getattr(self.claim_client, "wait_event", None)
        if not callable(subscribe) or not callable(wait_event):
            return
        subscribe(
            self.type,
            state=self.state,
            states=self.states,
            partition_key=self.partition_key,
            partition_keys=self.partition_keys,
            priority=self.priority,
            limit=self.batch_size,
        )
        self._protocol_wake_hints_enabled = True

    def _wait_for_protocol_wake_hint(self, timeout_s: float) -> bool:
        if not self._protocol_wake_hints_enabled or timeout_s <= 0:
            return False
        wait_event = getattr(self.claim_client, "wait_event", None)
        if not callable(wait_event):
            return False
        return wait_event(timeout=timeout_s) is not None

    def run_once(self, handler: FlowHandler) -> QueueFlowWorkerResult:
        return self._run_once(handler, batch_handler=False)

    def run_batch_once(self, handler: FlowBatchHandler) -> QueueFlowWorkerResult:
        return self._run_once(handler, batch_handler=True)

    def run_batch_once_for_partition_keys(
        self,
        handler: FlowBatchHandler,
        partition_keys: Sequence[str],
        *,
        claim_credit: int | None = None,
        block_ms: int | None = None,
    ) -> QueueFlowWorkerResult:
        keys = list(partition_keys)
        if not keys:
            return self._drain_pending_completions(block=False)

        result = self._drain_pending_completions(block=False)
        partition_key = keys[0] if len(keys) == 1 else None
        partition_key_list = None if partition_key is not None else keys
        return self._drain_claim_plan(
            handler,
            result,
            partition_key,
            partition_key_list,
            (
                claim_credit
                if claim_credit is not None
                else self.batch_size * self.claim_drain_batches
            ),
            block_ms=block_ms,
            batch_handler=True,
        )

    def _run_once(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> QueueFlowWorkerResult:
        if self._can_prefetch_claims():
            return self._run_prefetched_once(handler, batch_handler=batch_handler)

        result = self._drain_pending_completions(block=False)

        if self._should_scan_owned_partitions_before_blocking():
            pages = self._owned_partition_scan_pages()

            for _ in range(pages):
                before_claimed = result.claimed
                claim_partition_key, claim_partition_keys = self._next_claim_partition()
                result = self._drain_claim_plan(
                    handler,
                    result,
                    claim_partition_key,
                    claim_partition_keys,
                    self.batch_size * self.claim_drain_batches,
                    block_ms=None,
                    batch_handler=batch_handler,
                )
                if result.claimed > before_claimed:
                    return result

            claim_partition_key, claim_partition_keys = self._owned_partition_block_claim()
            return self._drain_claim_plan(
                handler,
                result,
                claim_partition_key,
                claim_partition_keys,
                self.batch_size * self.claim_drain_batches,
                block_ms=self._post_scan_block_ms(),
                batch_handler=batch_handler,
            )

        if self._should_block_on_owned_partitions():
            claim_partition_key, claim_partition_keys = self._owned_partition_block_claim()
            return self._drain_claim_plan(
                handler,
                result,
                claim_partition_key,
                claim_partition_keys,
                self.batch_size * self.claim_drain_batches,
                block_ms=self.block_ms,
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
            block_ms=self.block_ms,
            batch_handler=batch_handler,
        )

    def _run_prefetched_once(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> QueueFlowWorkerResult:
        result = self._drain_pending_completions(block=False)
        self._fill_pending_claims()
        pending = self._take_pending_claim(block=False)
        if pending is None and self._has_progress(result):
            return result
        if pending is None and self._pending_completions:
            result = self._merge_results(
                result,
                self._drain_pending_completions(block=True, limit=1),
            )
            if self._has_progress(result):
                return result
        if pending is None:
            pending = self._take_pending_claim(block=True)
        if pending is None:
            return result

        jobs = pending.future.result()
        result = self._merge_results(result, QueueFlowWorkerResult(claim_calls=1))
        if not jobs:
            self._cool_claim_keys(
                pending.partition_key,
                pending.partition_keys,
                self.empty_claim_cooldown_s,
            )
            self._fill_pending_claims()
            return result

        result = self._process_claimed_jobs(
            jobs,
            handler,
            result,
            batch_handler=batch_handler,
        )
        if len(jobs) < self.batch_size:
            self._cool_claim_keys(
                pending.partition_key,
                pending.partition_keys,
                self.partial_claim_cooldown_s,
            )
        self._fill_pending_claims()
        return result

    @staticmethod
    def _has_progress(result: QueueFlowWorkerResult) -> bool:
        return result.claimed > 0 or result.completed > 0 or result.retried > 0 or result.failed > 0

    def _drain_claim_plan(
        self,
        handler: FlowHandler | FlowBatchHandler,
        result: QueueFlowWorkerResult,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        claim_credit: int,
        *,
        block_ms: int | None,
        batch_handler: bool,
    ) -> QueueFlowWorkerResult:
        capacity = self._available_claim_capacity()
        if capacity <= 0:
            return result

        remaining_credit = min(max(claim_credit, 1), capacity)
        drain_count = 0
        pending_complete: _HandledBatch | None = None
        pending_claim_future: Future[list[FlowJob]] | None = None

        while remaining_credit > 0 and drain_count < self.claim_drain_batches:
            limit = min(self.batch_size, remaining_credit)
            if pending_claim_future is not None:
                jobs = pending_claim_future.result()
                pending_claim_future = None
            elif pending_complete is None:
                jobs = self._claim_flows(
                    claim_partition_key=claim_partition_key,
                    claim_partition_keys=claim_partition_keys,
                    limit=limit,
                    block_ms=block_ms,
                )
            else:
                jobs = self._complete_and_claim_flows(
                    pending_complete,
                    claim_partition_key=claim_partition_key,
                    claim_partition_keys=claim_partition_keys,
                    limit=limit,
                    block_ms=block_ms,
                )
                result = self._merge_results(
                    result,
                    QueueFlowWorkerResult(completed=len(pending_complete.jobs)),
                )
                pending_complete = None
            result = self._merge_results(result, QueueFlowWorkerResult(claim_calls=1))
            if not jobs:
                self._cool_claim_keys(
                    claim_partition_key, claim_partition_keys, self.empty_claim_cooldown_s
                )
                break

            handled = (
                self._run_batch_handler(jobs, cast(FlowBatchHandler, handler))
                if batch_handler
                else self._run_handlers(jobs, cast(FlowHandler, handler))
            )
            result = self._merge_results(result, QueueFlowWorkerResult(claimed=len(jobs)))

            remaining_credit -= len(jobs)
            drain_count += 1

            if len(jobs) < limit:
                result = self._finish_handled_batch(handled, result)
                self._cool_claim_keys(
                    claim_partition_key,
                    claim_partition_keys,
                    self.partial_claim_cooldown_s,
                )
                break

            if self._should_fuse_complete_claim(handled, remaining_credit, drain_count):
                pending_complete = handled
            elif self._should_async_fuse_complete_claim(handled, remaining_credit, drain_count):
                submitted = self._submit_async_complete_and_claim(
                    handled,
                    claim_partition_key=claim_partition_key,
                    claim_partition_keys=claim_partition_keys,
                    limit=min(self.batch_size, remaining_credit),
                    block_ms=block_ms,
                )
                if submitted is None:
                    result = self._finish_handled_batch(handled, result)
                else:
                    pending_claim_future = submitted
            else:
                result = self._finish_handled_batch(handled, result)

        if pending_complete is not None:
            result = self._finish_handled_batch(pending_complete, result)

        return result

    def _should_fuse_complete_claim(
        self,
        handled: _HandledBatch,
        remaining_credit: int,
        drain_count: int,
    ) -> bool:
        return (
            remaining_credit > 0
            and self.fuse_complete_claim
            and drain_count < self.claim_drain_batches
            and self._completion_executor is None
            and not self.claim_values
            and bool(handled.jobs)
            and handled.mixed_results is None
            and not handled.failures
            and callable(getattr(self.client, "complete_flows_and_claim_flows", None))
        )

    def _should_async_fuse_complete_claim(
        self,
        handled: _HandledBatch,
        remaining_credit: int,
        drain_count: int,
    ) -> bool:
        return (
            remaining_credit > 0
            and self.fuse_complete_claim
            and drain_count < self.claim_drain_batches
            and self._completion_executor is not None
            and not self.claim_values
            and bool(handled.jobs)
            and handled.mixed_results is None
            and not handled.failures
            and callable(getattr(self.claim_client, "submit_complete_flows_and_claim_flows", None))
        )

    def _submit_async_complete_and_claim(
        self,
        handled: _HandledBatch,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
        block_ms: int | None,
    ) -> Future[list[FlowJob]] | None:
        submitted = self.claim_client.submit_complete_flows_and_claim_flows(
            cast(list[ClaimedFlow], handled.jobs),
            result=handled.first_result,
            independent=self.complete_independent,
            type=self.type,
            state=self.state,
            states=self.states,
            worker=self.worker,
            partition_key=claim_partition_key,
            partition_keys=claim_partition_keys,
            lease_ms=self.lease_ms,
            limit=limit,
            priority=self.priority,
            block_ms=block_ms,
            reclaim_expired=self.reclaim_expired,
            reclaim_ratio=self.reclaim_ratio,
        )
        if submitted is None:
            return None

        complete_future, claim_future = submitted
        completion_result: Future[QueueFlowWorkerResult] = Future()

        def complete_done(source: Future[int]) -> None:
            if completion_result.cancelled():
                return
            try:
                completion_result.set_result(QueueFlowWorkerResult(completed=source.result()))
            except BaseException as exc:
                if not completion_result.cancelled():
                    completion_result.set_exception(exc)

        complete_future.add_done_callback(complete_done)
        self._pending_completions.append(completion_result)
        return cast(Future[list[FlowJob]], claim_future)

    def _complete_and_claim_flows(
        self,
        handled: _HandledBatch,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
        block_ms: int | None,
    ) -> list[FlowJob]:
        return cast(
            list[FlowJob],
            self.client.complete_flows_and_claim_flows(
                cast(list[ClaimedFlow], handled.jobs),
                result=handled.first_result,
                independent=self.complete_independent,
                type=self.type,
                state=self.state,
                states=self.states,
                worker=self.worker,
                partition_key=claim_partition_key,
                partition_keys=claim_partition_keys,
                lease_ms=self.lease_ms,
                limit=limit,
                priority=self.priority,
                block_ms=block_ms,
                reclaim_expired=self.reclaim_expired,
                reclaim_ratio=self.reclaim_ratio,
            ),
        )

    def _finish_handled_batch(
        self,
        handled: _HandledBatch,
        result: QueueFlowWorkerResult,
    ) -> QueueFlowWorkerResult:
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

        return self._merge_results(result, self._finish_batch(handled, self.client))

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

    def _fill_pending_claims(self) -> None:
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
        mixed_results: list[tuple[FlowJob, Any]] | None = None

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
            remaining = (
                len(self._pending_completions)
                if limit is None
                else min(limit, len(self._pending_completions))
            )
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
    def _merge_results(
        left: QueueFlowWorkerResult, right: QueueFlowWorkerResult
    ) -> QueueFlowWorkerResult:
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
                cast(list[ClaimedFlow], handled.jobs),
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
                    cast(list[ClaimedFlow], group_jobs),
                    error=message,
                    independent=self.complete_independent,
                )
            return 0, len(jobs)

        for message, group_jobs in grouped.items():
            client.retry_many(
                None,
                cast(list[ClaimedFlow], group_jobs),
                error=message,
                independent=self.complete_independent,
            )
        return len(jobs), 0


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


class Queue:
    """High-level durable queue bound to one FerricFlow type/state."""

    def __init__(
        self,
        client: FlowClient,
        *,
        claim_client: FlowClient | None = None,
        type: str,
        state: str = "queued",
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> None:
        self.client = client
        self.claim_client = claim_client if claim_client is not None else client
        self.type = type
        self.state = state
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()

    def enqueue(self, id: str, *, payload: Any = None, **attrs: Any) -> FlowRecord | bytes:
        return self.client.enqueue(
            id,
            type=self.type,
            state=attrs.pop("state", self.state),
            payload=payload,
            **attrs,
        )

    def enqueue_many(self, items: list[Any], **attrs: Any) -> list[Any] | Any:
        return self.client.enqueue_many(
            items,
            type=self.type,
            state=attrs.pop("state", self.state),
            **attrs,
        )

    def worker(self, **kwargs: Any) -> QueueFlowWorker:
        worker_kwargs = (
            self.worker_config.to_kwargs(QUEUE_WORKER_CONFIG_KEYS)
            if self.worker_config is not None
            else {}
        )
        if self.value_config.value_max_bytes is not None and "value_max_bytes" not in worker_kwargs:
            worker_kwargs["value_max_bytes"] = self.value_config.value_max_bytes
        worker_kwargs.update(kwargs)
        if "state" not in worker_kwargs and "states" not in worker_kwargs:
            worker_kwargs["state"] = self.state
        return QueueFlowWorker(
            self.client,
            claim_client=self.claim_client,
            type=self.type,
            **worker_kwargs,
        )

    def install_policy(
        self,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
    ) -> Any:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        return self.client.install_policy(self.type, retry=resolved_retry_policy)


class QueueClient:
    """High-level client for durable queue workloads."""

    def __init__(
        self,
        client: FlowClient | str,
        *,
        claim_client: FlowClient | str | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _owns_clients: bool = False,
    ) -> None:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_max_connections = (
            1
            if isinstance(client, str)
            and (worker_config is None or worker_config.command_connections is None)
            else command_pool_size
        )
        self._url = client if isinstance(client, str) else None
        self._base_url_kwargs: dict[str, Any] = {}
        self._claim_client_explicit = claim_client is not None
        self._owned_extra_claim_flows: list[FlowClient] = []
        self._claim_pool_size = claim_pool_size
        self.flow = (
            FlowClient.from_url(client, max_connections=command_max_connections)
            if isinstance(client, str)
            else client
        )
        if claim_client is None:
            self.claim_flow = self.flow
        else:
            self.claim_flow = (
                FlowClient.from_url(claim_client, max_connections=claim_pool_size)
                if isinstance(claim_client, str)
                else claim_client
            )
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._owns_flow = _owns_clients or isinstance(client, str)
        self._owns_claim_flow = (_owns_clients and self.claim_flow is not self.flow) or isinstance(
            claim_client, str
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> QueueClient:
        command_pool_size, _claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_kwargs = dict(kwargs)
        if worker_config is None or worker_config.command_connections is None:
            command_kwargs.setdefault("max_connections", 1)
        else:
            command_kwargs.setdefault("max_connections", command_pool_size)
        instance = cls(
            FlowClient.from_url(url, **command_kwargs),
            retry_policy=retry_policy,
            worker_config=worker_config,
            value_config=value_config,
            _owns_clients=True,
        )
        instance._url = url
        instance._base_url_kwargs = dict(kwargs)
        instance._claim_client_explicit = False
        instance._claim_pool_size = 1
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> FlowClient:
        return self.claim_flow

    def queue(
        self,
        *,
        type: str,
        state: str = "queued",
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> Queue:
        resolved_worker_config = worker_config if worker_config is not None else self.worker_config
        return Queue(
            self.flow,
            claim_client=self._claim_flow_for_worker_config(resolved_worker_config),
            type=type,
            state=state,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            worker_config=resolved_worker_config,
            value_config=value_config if value_config is not None else self.value_config,
        )

    def install_policy(
        self,
        type: str,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        states: dict[str, RetryPolicy] | None = None,
    ) -> Any:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        return self.flow.install_policy(type, retry=resolved_retry_policy, states=states)

    def close(self) -> None:
        for claim_flow in self._owned_extra_claim_flows:
            claim_flow.close()
        self._owned_extra_claim_flows.clear()
        if self._owns_claim_flow and self.claim_flow is not self.flow:
            self.claim_flow.close()
        if self._owns_flow:
            self.flow.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)
