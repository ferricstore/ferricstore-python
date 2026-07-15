from __future__ import annotations

import contextlib
import threading
import uuid
from collections import deque
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.client_core import FlowClient
from ferricstore.client_ownership import resolve_sync_client_pair
from ferricstore.config_validation import (
    validate_nonnegative_int,
    validate_positive_int,
    validate_string_sequence,
)
from ferricstore.legacy_worker import Worker as Worker
from ferricstore.lifecycle_core import (
    SyncCloseTaskRegistry,
    try_set_future_exception,
    try_set_future_result,
)
from ferricstore.types import (
    ClaimedFlow,
    ExceptionPolicy,
    resolve_worker_connection_counts,
)
from ferricstore.worker_claims import (
    FlowBatchHandler as FlowBatchHandler,
)
from ferricstore.worker_claims import (
    FlowHandler as FlowHandler,
)
from ferricstore.worker_claims import (
    SyncWorkerClaimMixin,
    _PendingClaim,
)
from ferricstore.worker_completion import (
    FlowJob as FlowJob,
)
from ferricstore.worker_completion import (
    SyncWorkerCompletionMixin,
    _HandledBatch,
)
from ferricstore.worker_core import (
    SyncWorkerRunGate,
    WorkerIdleScheduler,
    WorkerInvocationTracker,
    WorkerTerminalState,
    can_fuse_complete_claim,
)
from ferricstore.worker_execution import close_queue_worker, drain_claim_plan
from ferricstore.worker_models import QueueFlowWorkerResult as QueueFlowWorkerResult
from ferricstore.worker_runtime_config import QueueWorkerRuntimeConfig

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


def _close_resource_safely(resource: Any) -> None:
    with contextlib.suppress(BaseException):
        resource.close()


def _shutdown_executor_safely(executor: ThreadPoolExecutor) -> None:
    with contextlib.suppress(BaseException):
        executor.shutdown(wait=False, cancel_futures=True)


def _is_protocol_url(value: str) -> bool:
    return urlparse(value).scheme.lower() in _PROTOCOL_URL_SCHEMES


class QueueFlowWorker(SyncWorkerClaimMixin, SyncWorkerCompletionMixin):
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
        runtime_config = QueueWorkerRuntimeConfig.build(
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            concurrency=concurrency,
            batch_size=batch_size,
            workers=1,
            claim_partition_batch_size=claim_partition_batch_size,
            block_ms=block_ms,
            idle_sleep_s=idle_sleep_s,
            max_idle_sleep_s=max_idle_sleep_s,
            exception_policy=exception_policy,
            on_error=on_error,
            empty_claim_cooldown_s=empty_claim_cooldown_s,
            partial_claim_cooldown_s=partial_claim_cooldown_s,
            lease_ms=lease_ms,
            priority=priority,
            reclaim_expired=reclaim_expired,
            reclaim_ratio=reclaim_ratio,
            claim_values=claim_values,
            value_max_bytes=value_max_bytes,
            complete_independent=complete_independent,
            protocol_wake_hints=protocol_wake_hints,
            scan_before_blocking=scan_before_blocking,
            fuse_complete_claim=fuse_complete_claim,
        )
        claim_drain_batches = validate_positive_int(
            claim_drain_batches,
            name="claim_drain_batches",
        )
        claim_prefetch = validate_nonnegative_int(claim_prefetch, name="claim_prefetch")
        complete_async_depth = validate_nonnegative_int(
            complete_async_depth,
            name="complete_async_depth",
        )
        if claim_scan_block_ms is not None:
            claim_scan_block_ms = validate_nonnegative_int(
                claim_scan_block_ms,
                name="claim_scan_block_ms",
            )
        resolved_completion_clients = (
            list(completion_clients) if completion_clients is not None else None
        )
        if resolved_completion_clients is not None and not resolved_completion_clients:
            raise ValueError("completion_clients must be non-empty")
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            workers=1,
            concurrency=runtime_config.concurrency,
            command_connections=command_connections,
            claim_connections=claim_connections,
        )
        command_max_connections = (
            1 if isinstance(client, str) and command_connections is None else command_pool_size
        )
        with contextlib.ExitStack() as rollback:
            clients = resolve_sync_client_pair(
                client,
                claim_client,
                from_url=FlowClient.from_url,
                command_kwargs={"max_connections": command_max_connections},
                claim_kwargs={"max_connections": claim_pool_size},
                rollback=rollback,
                close=_close_resource_safely,
            )
            self.client = clients.command
            self.claim_client = clients.claim
            self._owns_client = clients.owns_command
            self._owns_claim_client = clients.owns_claim
            self.type = type
            self.worker = worker or f"{type}:worker:{uuid.uuid4().hex}"
            self.state = state
            self.states = runtime_config.states
            self.concurrency = runtime_config.concurrency
            self.batch_size = runtime_config.batch_size
            self.lease_ms = runtime_config.lease_ms
            self.priority = runtime_config.priority
            self.reclaim_expired = runtime_config.reclaim_expired
            self.reclaim_ratio = runtime_config.reclaim_ratio
            self.claim_values = runtime_config.claim_values
            self.value_max_bytes = runtime_config.value_max_bytes
            self.block_ms = runtime_config.block_ms
            self.claim_scan_block_ms = claim_scan_block_ms
            self.partition_key = partition_key
            self.partition_keys = runtime_config.partition_keys
            self.claim_partition_batch_size = (
                runtime_config.claim_partition_batch_size
                if runtime_config.claim_partition_batch_size is not None
                else len(self.partition_keys or []) or 1
            )
            self.claim_drain_batches = claim_drain_batches
            self.claim_prefetch = claim_prefetch
            self.protocol_wake_hints = runtime_config.protocol_wake_hints
            self.scan_before_blocking = runtime_config.scan_before_blocking
            self.idle_sleep_s = runtime_config.idle_sleep_s
            self.max_idle_sleep_s = runtime_config.max_idle_sleep_s
            self.on_error = runtime_config.on_error
            self.complete_independent = runtime_config.complete_independent
            self._initialize_runtime_state(
                rollback=rollback,
                concurrency=runtime_config.concurrency,
                complete_async_depth=complete_async_depth,
                completion_clients=resolved_completion_clients,
                fuse_complete_claim=runtime_config.fuse_complete_claim,
                runtime_config=runtime_config,
            )
            self._wake_client = self.claim_client
            self._owns_wake_client = False
            self._protocol_wake_hints_enabled = False
            self._subscribe_protocol_wake_hints()
            rollback.pop_all()

    def _initialize_runtime_state(
        self,
        *,
        rollback: contextlib.ExitStack,
        concurrency: int,
        complete_async_depth: int,
        completion_clients: list[FlowClient] | None,
        fuse_complete_claim: bool,
        runtime_config: QueueWorkerRuntimeConfig,
    ) -> None:
        self._running = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._active_thread: threading.Thread | None = None
        self._run_gate = SyncWorkerRunGate(closing_message="queue worker is closing")
        self._terminal_state = WorkerTerminalState()
        self._invocations = WorkerInvocationTracker()
        self._close_operations = SyncCloseTaskRegistry()
        self._pending_claim_drain_resource = object()
        self._pending_claim_drain_required = False
        self._totals = QueueFlowWorkerResult()
        self._executor = ThreadPoolExecutor(max_workers=concurrency) if concurrency > 1 else None
        if self._executor is not None:
            rollback.callback(_shutdown_executor_safely, self._executor)
        self._completion_executor = (
            ThreadPoolExecutor(max_workers=complete_async_depth)
            if complete_async_depth > 0
            else None
        )
        if self._completion_executor is not None:
            rollback.callback(_shutdown_executor_safely, self._completion_executor)
        self._completion_clients = completion_clients or [self.client]
        self._completion_client_index = 0
        self._complete_async_depth = complete_async_depth
        self.fuse_complete_claim = bool(fuse_complete_claim)
        self._pending_completions: deque[Future[QueueFlowWorkerResult]] = deque()
        self._pending_claims: list[_PendingClaim] = []
        self._partition_cursor = 0
        self._claim_cooldown_until: dict[str, float] = {}
        default_claim_cooldown_s = min(self.idle_sleep_s, 0.001)
        self.empty_claim_cooldown_s = (
            default_claim_cooldown_s
            if runtime_config.empty_claim_cooldown_s is None
            else runtime_config.empty_claim_cooldown_s
        )
        self.partial_claim_cooldown_s = (
            default_claim_cooldown_s
            if runtime_config.partial_claim_cooldown_s is None
            else runtime_config.partial_claim_cooldown_s
        )

    def run(self, handler: FlowHandler) -> None:
        self.run_forever(handler)

    def run_forever(self, handler: FlowHandler) -> None:
        self._begin_run(active_thread=threading.current_thread())
        self._run_loop(handler, batch_handler=False)

    def run_batch_forever(self, handler: FlowBatchHandler) -> None:
        self._begin_run(active_thread=threading.current_thread())
        self._run_loop(handler, batch_handler=True)

    def start(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool = False,
        daemon: bool = True,
    ) -> QueueFlowWorker:
        def start_thread() -> None:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("worker already started")
            self._prepare_run()
            thread = threading.Thread(
                target=self._thread_entry,
                args=(handler,),
                kwargs={"batch_handler": batch_handler},
                daemon=daemon,
            )
            self._thread = thread
            try:
                thread.start()
            except BaseException:
                self._thread = None
                self._running = False
                raise

        self._run_gate.run_while_open(start_thread)
        return self

    def join(self, timeout: float | None = None) -> QueueFlowWorkerResult:
        thread = self._run_gate.synchronized(lambda: self._thread)
        if thread is not None:
            thread.join(timeout)
            if not thread.is_alive():
                self._terminal_state.raise_if_failed()
        return self.stats

    def _prepare_run(self, *, active_thread: threading.Thread | None = None) -> None:
        running_thread = self._active_thread
        if self._running or (
            running_thread is not None
            and running_thread is not active_thread
            and running_thread.is_alive()
        ):
            raise RuntimeError("worker already running")
        self._terminal_state.reset()
        self._stop_event.clear()
        self._running = True
        if active_thread is not None:
            self._active_thread = active_thread

    def _begin_run(self, *, active_thread: threading.Thread | None = None) -> None:
        self._run_gate.run_while_open(lambda: self._prepare_run(active_thread=active_thread))

    def _thread_entry(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> None:
        try:
            self._run_loop(handler, batch_handler=batch_handler)
        except BaseException as exc:
            self._terminal_state.capture(exc)

    @property
    def is_running(self) -> bool:
        return self._run_gate.synchronized(lambda: self._running)

    @property
    def stats(self) -> QueueFlowWorkerResult:
        return self._totals

    def _run_loop(
        self,
        handler: FlowHandler | FlowBatchHandler,
        *,
        batch_handler: bool,
    ) -> None:
        active_thread = threading.current_thread()

        def enter_loop() -> None:
            running_thread = self._active_thread
            if (
                running_thread is not None
                and running_thread is not active_thread
                and running_thread.is_alive()
            ):
                raise RuntimeError("worker already running")
            self._active_thread = active_thread
            if not self._running and not self._stop_event.is_set():
                self._running = True

        self._run_gate.synchronized(enter_loop)
        idle = WorkerIdleScheduler(self.idle_sleep_s, self.max_idle_sleep_s)
        try:
            while self._running and not self._stop_event.is_set():
                result = (
                    self.run_batch_once(cast(FlowBatchHandler, handler))
                    if batch_handler
                    else self.run_once(cast(FlowHandler, handler))
                )
                self._totals = self._merge_results(self._totals, result)
                if result.claimed == 0:
                    delay = idle.after_batch(0)
                    if not self._wait_for_protocol_wake_hint(delay):
                        self._stop_event.wait(delay)
                else:
                    idle.after_batch(result.claimed)
        finally:
            try:
                drained = self._drain_pending_claims_for_shutdown()
                self._totals = self._merge_results(self._totals, drained)
            finally:

                def finish_loop() -> None:
                    self._running = False
                    if self._active_thread is active_thread:
                        self._active_thread = None

                self._run_gate.synchronized(finish_loop)

    def stop(self) -> None:
        def stop_running() -> None:
            self._running = False
            self._stop_event.set()

        self._run_gate.synchronized(stop_running)

    def close(self, timeout: float | None = 5.0) -> None:
        close_queue_worker(self, timeout)

    def flush(self) -> QueueFlowWorkerResult:
        return self._drain_pending_completions(block=True)

    def _finish_pending_claims_for_close(self) -> None:
        try:
            drained = self._drain_pending_claims_for_shutdown(drain_completions=False)
        except BaseException as exc:
            self._terminal_state.capture(exc)
            raise
        self._totals = self._merge_results(self._totals, drained)
        self._pending_claim_drain_required = False

    def _drain_pending_claims_for_shutdown(
        self,
        *,
        drain_completions: bool = True,
    ) -> QueueFlowWorkerResult:
        result = QueueFlowWorkerResult()
        while self._pending_claims:
            pending = self._take_pending_claim(block=True)
            if pending is None:
                break
            jobs = pending.future.result()
            result = self._merge_results(result, QueueFlowWorkerResult(claim_calls=1))
            if jobs:
                result = self._process_claimed_jobs(
                    jobs,
                    pending.handler,
                    result,
                    batch_handler=pending.batch_handler,
                )
        if not drain_completions:
            return result
        return self._merge_results(result, self._drain_pending_completions(block=True))

    def _subscribe_protocol_wake_hints(self) -> None:
        if not self.protocol_wake_hints:
            return
        wake_client = self._wake_client
        owns_wake_client = self._owns_wake_client
        if wake_client is self.claim_client and not owns_wake_client:
            acquire = getattr(self.claim_client, "_acquire_subscription_client", None)
            if callable(acquire):
                wake_client, owns_wake_client = acquire()
        subscribe = getattr(wake_client, "subscribe_flow_wake", None)
        wait_event = getattr(wake_client, "wait_event", None)
        if not callable(subscribe) or not callable(wait_event):
            if owns_wake_client:
                _close_resource_safely(wake_client)
            return
        try:
            subscribe(
                self.type,
                state=self.state,
                states=self.states,
                partition_key=self.partition_key,
                partition_keys=self.partition_keys,
                priority=self.priority,
                limit=self.batch_size,
            )
        except BaseException:
            if owns_wake_client:
                _close_resource_safely(wake_client)
            raise
        self._wake_client = wake_client
        self._owns_wake_client = owns_wake_client
        self._protocol_wake_hints_enabled = True

    def _wait_for_protocol_wake_hint(self, timeout_s: float) -> bool:
        if not self._protocol_wake_hints_enabled or timeout_s <= 0:
            return False
        wait_event = getattr(self._wake_client, "wait_event", None)
        if not callable(wait_event):
            return False
        return wait_event(timeout=timeout_s) is not None

    def run_once(self, handler: FlowHandler) -> QueueFlowWorkerResult:
        if not self._begin_invocation():
            return QueueFlowWorkerResult()
        try:
            return self._run_once(handler, batch_handler=False)
        finally:
            self._invocations.end()

    def run_batch_once(self, handler: FlowBatchHandler) -> QueueFlowWorkerResult:
        if not self._begin_invocation():
            return QueueFlowWorkerResult()
        try:
            return self._run_once(handler, batch_handler=True)
        finally:
            self._invocations.end()

    def _begin_invocation(self) -> bool:
        try:
            self._invocations.begin("queue worker is closing")
        except RuntimeError:
            if self._stop_event.is_set() and threading.current_thread() is self._active_thread:
                return False
            raise
        return True

    def run_batch_once_for_partition_keys(
        self,
        handler: FlowBatchHandler,
        partition_keys: Sequence[str],
        *,
        claim_credit: int | None = None,
        block_ms: int | None = None,
    ) -> QueueFlowWorkerResult:
        self._invocations.begin("queue worker is closing")
        try:
            keys = list(
                validate_string_sequence(
                    partition_keys,
                    name="partition_keys",
                )
            )
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
        finally:
            self._invocations.end()

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
        self._fill_pending_claims(handler, batch_handler=batch_handler)
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
            self._fill_pending_claims(handler, batch_handler=batch_handler)
            return result

        result = self._process_claimed_jobs(
            jobs,
            pending.handler,
            result,
            batch_handler=pending.batch_handler,
        )
        if len(jobs) < self.batch_size:
            self._cool_claim_keys(
                pending.partition_key,
                pending.partition_keys,
                self.partial_claim_cooldown_s,
            )
        self._fill_pending_claims(handler, batch_handler=batch_handler)
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
        return drain_claim_plan(
            self,
            handler,
            result,
            claim_partition_key,
            claim_partition_keys,
            claim_credit,
            block_ms=block_ms,
            batch_handler=batch_handler,
        )

    def _should_fuse_complete_claim(
        self,
        handled: _HandledBatch,
        remaining_credit: int,
        drain_count: int,
    ) -> bool:
        return (
            remaining_credit > 0
            and drain_count < self.claim_drain_batches
            and self._completion_executor is None
            and can_fuse_complete_claim(
                enabled=self.fuse_complete_claim,
                has_jobs=bool(handled.jobs),
                has_mixed_results=handled.mixed_results is not None,
                has_failures=bool(handled.failures),
                claims_values=bool(self.claim_values),
                supported=callable(getattr(self.client, "complete_flows_and_claim_flows", None)),
            )
        )

    def _should_async_fuse_complete_claim(
        self,
        handled: _HandledBatch,
        remaining_credit: int,
        drain_count: int,
    ) -> bool:
        return (
            remaining_credit > 0
            and drain_count < self.claim_drain_batches
            and self._completion_executor is not None
            and can_fuse_complete_claim(
                enabled=self.fuse_complete_claim,
                has_jobs=bool(handled.jobs),
                has_mixed_results=handled.mixed_results is not None,
                has_failures=bool(handled.failures),
                claims_values=bool(self.claim_values),
                supported=callable(
                    getattr(self.claim_client, "submit_complete_flows_and_claim_flows", None)
                ),
            )
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
                value = QueueFlowWorkerResult(completed=source.result())
            except BaseException as exc:
                try_set_future_exception(completion_result, exc)
            else:
                try_set_future_result(completion_result, value)

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


_QUEUE_API_EXPORTS = frozenset({"Queue", "QueueClient"})


def __getattr__(name: str) -> Any:
    if name not in _QUEUE_API_EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from ferricstore import queue_api

    return getattr(queue_api, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | _QUEUE_API_EXPORTS)
