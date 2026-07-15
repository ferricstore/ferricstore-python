from __future__ import annotations

import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Protocol, cast

from ferricstore.client_core import FlowClient
from ferricstore.lifecycle_core import SyncCloseTaskRegistry
from ferricstore.worker_claims import FlowBatchHandler, FlowHandler, _PendingClaim
from ferricstore.worker_completion import FlowJob, _HandledBatch
from ferricstore.worker_core import (
    CloseDeadline,
    CloseTimeoutError,
    SyncWorkerRunGate,
    WorkerInvocationTracker,
    WorkerTerminalState,
)
from ferricstore.worker_models import QueueFlowWorkerResult


class QueueWorkerRunLifecycleHost(Protocol):
    """Run-state coordination required while stopping a worker."""

    _running: bool
    _stop_event: threading.Event
    _thread: threading.Thread | None
    _active_thread: threading.Thread | None
    _run_gate: SyncWorkerRunGate
    _invocations: WorkerInvocationTracker
    _terminal_state: WorkerTerminalState


class QueueWorkerResourceOwnerHost(Protocol):
    """Pending work and owned resources required during worker shutdown."""

    _pending_claims: list[_PendingClaim]
    _pending_claim_drain_required: bool
    _close_operations: SyncCloseTaskRegistry
    _pending_claim_drain_resource: object
    _totals: QueueFlowWorkerResult
    _executor: ThreadPoolExecutor | None
    _completion_executor: ThreadPoolExecutor | None
    _owns_wake_client: bool
    _wake_client: FlowClient
    _owns_claim_client: bool
    claim_client: FlowClient
    _owns_client: bool
    client: FlowClient

    def _finish_pending_claims_for_close(self) -> None: ...

    def _drain_pending_completions(
        self,
        *,
        block: bool,
        deadline: CloseDeadline | None = None,
    ) -> QueueFlowWorkerResult: ...

    def _merge_results(
        self,
        left: QueueFlowWorkerResult,
        right: QueueFlowWorkerResult,
    ) -> QueueFlowWorkerResult: ...


class QueueWorkerCloseHost(
    QueueWorkerRunLifecycleHost,
    QueueWorkerResourceOwnerHost,
    Protocol,
):
    """Complete structural contract for the close coordinator."""


class QueueWorkerClaimPlanHost(Protocol):
    """Claim/handle/complete operations required by one drain plan."""

    batch_size: int
    claim_drain_batches: int
    empty_claim_cooldown_s: float
    partial_claim_cooldown_s: float

    def _merge_results(
        self,
        left: QueueFlowWorkerResult,
        right: QueueFlowWorkerResult,
    ) -> QueueFlowWorkerResult: ...

    def _available_claim_capacity(self) -> int: ...

    def _claim_flows(
        self,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
        block_ms: int | None,
    ) -> list[FlowJob]: ...

    def _complete_and_claim_flows(
        self,
        handled: _HandledBatch,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
        block_ms: int | None,
    ) -> list[FlowJob]: ...

    def _run_batch_handler(
        self,
        jobs: list[FlowJob],
        handler: FlowBatchHandler,
    ) -> _HandledBatch: ...

    def _run_handlers(
        self,
        jobs: list[FlowJob],
        handler: FlowHandler,
    ) -> _HandledBatch: ...

    def _cool_claim_keys(
        self,
        partition_key: str | None,
        partition_keys: list[str] | None,
        cooldown_s: float,
    ) -> None: ...

    def _finish_handled_batch(
        self,
        handled: _HandledBatch,
        result: QueueFlowWorkerResult,
    ) -> QueueFlowWorkerResult: ...

    def _should_fuse_complete_claim(
        self,
        handled: _HandledBatch,
        remaining_credit: int,
        drain_count: int,
    ) -> bool: ...

    def _should_async_fuse_complete_claim(
        self,
        handled: _HandledBatch,
        remaining_credit: int,
        drain_count: int,
    ) -> bool: ...

    def _submit_async_complete_and_claim(
        self,
        handled: _HandledBatch,
        *,
        claim_partition_key: str | None,
        claim_partition_keys: list[str] | None,
        limit: int,
        block_ms: int | None,
    ) -> Future[list[FlowJob]] | None: ...


def close_queue_worker(self: QueueWorkerCloseHost, timeout: float | None = 5.0) -> None:
    deadline = CloseDeadline.start(timeout)
    timeout_message = "queue worker close timed out"

    def begin_close() -> list[threading.Thread]:
        self._running = False
        self._stop_event.set()
        return list(
            dict.fromkeys(
                thread for thread in (self._thread, self._active_thread) if thread is not None
            )
        )

    threads = self._run_gate.begin_close(begin_close)
    self._invocations.begin_close()
    for thread in threads:
        deadline.join_thread(thread, f"{timeout_message} waiting for the worker thread")
    self._invocations.wait_for_idle(deadline, timeout_message)
    error = self._terminal_state.error()
    if self._pending_claims:
        self._pending_claim_drain_required = True
    if self._pending_claim_drain_required:
        try:
            deadline.check(timeout_message)
            self._close_operations.run(
                self._pending_claim_drain_resource,
                self._finish_pending_claims_for_close,
                lambda future: deadline.future_result(future, timeout_message),
            )
        except CloseTimeoutError:
            raise
        except BaseException as exc:
            if error is None:
                error = exc
    try:
        result = self._drain_pending_completions(block=True, deadline=deadline)
        self._totals = self._merge_results(self._totals, result)
    except CloseTimeoutError:
        raise
    except BaseException as exc:
        if error is None:
            error = exc
    for attribute in ("_executor", "_completion_executor"):
        executor = cast(ThreadPoolExecutor | None, getattr(self, attribute))
        if executor is None:
            continue

        def shutdown_executor(current: ThreadPoolExecutor = executor) -> None:
            current.shutdown(wait=True)

        try:
            deadline.check(timeout_message)
            self._close_operations.run(
                executor,
                shutdown_executor,
                lambda future: deadline.future_result(future, timeout_message),
            )
        except BaseException as exc:
            if error is None:
                error = exc
        else:
            setattr(self, attribute, None)
    for ownership_attribute, should_close, client in (
        ("_owns_wake_client", self._owns_wake_client, self._wake_client),
        (
            "_owns_claim_client",
            self._owns_claim_client and self.claim_client is not self.client,
            self.claim_client,
        ),
        ("_owns_client", self._owns_client, self.client),
    ):
        if not should_close:
            continue
        try:
            deadline.check(timeout_message)
            self._close_operations.run(
                client,
                client.close,
                lambda future: deadline.future_result(future, timeout_message),
            )
        except BaseException as exc:
            if error is None:
                error = exc
        else:
            setattr(self, ownership_attribute, False)
    deadline.check(timeout_message)
    if error is not None:
        raise error


def drain_claim_plan(
    self: QueueWorkerClaimPlanHost,
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


__all__ = [
    "QueueWorkerClaimPlanHost",
    "QueueWorkerCloseHost",
    "QueueWorkerResourceOwnerHost",
    "QueueWorkerRunLifecycleHost",
    "close_queue_worker",
    "drain_claim_plan",
]
