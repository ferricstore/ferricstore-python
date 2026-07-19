from __future__ import annotations

import builtins
import threading
import uuid
from collections import deque
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor

from ferricstore.config_validation import (
    validate_bounded_nonnegative_int,
    validate_nonnegative_int,
    validate_optional_bool,
    validate_optional_flow_priority,
    validate_optional_nonnegative_int,
    validate_optional_positive_int,
    validate_partition_key_sequence,
    validate_positive_int,
    validate_string_sequence,
)
from ferricstore.lifecycle_core import (
    SyncCloseTaskRegistry,
)
from ferricstore.types import (
    ClaimedFlow,
)
from ferricstore.worker_core import (
    CloseDeadline,
    CloseTimeoutError,
    SyncWorkerRunGate,
    WorkerInvocationTracker,
    WorkerTerminalState,
    validate_worker_idle_timing,
)
from ferricstore.workflow_runtime import Workflow
from ferricstore.workflow_types import WorkflowWorkerResult


class WorkflowWorker:
    """High-level state-machine worker for Workflow subclasses."""

    def __init__(
        self,
        workflow: Workflow,
        *,
        worker: str | None = None,
        state: str | None = None,
        states: Sequence[str] | None = None,
        batch_size: int = 10,
        priority: int | None = 0,
        reclaim_expired: bool | None = None,
        reclaim_ratio: int | None = None,
        partition_key: str | bytes | None = None,
        partition_keys: Sequence[str | bytes] | None = None,
        claim_partition_batch_size: int | None = None,
        block_ms: int | None = None,
        idle_sleep_s: float = 0.1,
        max_idle_sleep_s: float | None = None,
        apply_async_depth: int = 0,
    ) -> None:
        if state is not None and states is not None:
            raise ValueError("state and states are mutually exclusive")
        if partition_key is not None and partition_keys is not None:
            raise ValueError("partition_key and partition_keys are mutually exclusive")
        resolved_states = (
            list(validate_string_sequence(states, name="states", allow_empty=False))
            if states is not None
            else None
        )
        resolved_partition_keys = (
            list(validate_partition_key_sequence(partition_keys, allow_empty=False))
            if partition_keys is not None
            else None
        )
        batch_size = validate_positive_int(batch_size, name="batch_size")
        claim_partition_batch_size = validate_optional_positive_int(
            claim_partition_batch_size,
            name="claim_partition_batch_size",
        )
        block_ms = validate_optional_nonnegative_int(block_ms, name="block_ms")
        priority = validate_optional_flow_priority(priority)
        reclaim_expired = validate_optional_bool(
            reclaim_expired,
            name="reclaim_expired",
        )
        if reclaim_ratio is not None:
            reclaim_ratio = validate_bounded_nonnegative_int(
                reclaim_ratio,
                name="reclaim_ratio",
                maximum=100,
            )
        apply_async_depth = validate_nonnegative_int(
            apply_async_depth,
            name="apply_async_depth",
        )
        idle_sleep_s, max_idle_sleep_s = validate_worker_idle_timing(
            idle_sleep_s,
            max_idle_sleep_s,
        )

        self.workflow = workflow
        self.worker = worker or f"{workflow.type}:workflow-worker:{uuid.uuid4().hex}"
        if state is not None:
            self.states = [state]
        elif resolved_states is not None:
            self.states = resolved_states
        else:
            self.states = list(workflow._states)
        if not self.states:
            raise ValueError("workflow has no states")
        unknown_states = [name for name in self.states if name not in workflow._states]
        if unknown_states:
            raise ValueError(f"unknown workflow states: {unknown_states!r}")

        self.batch_size = batch_size
        self.priority = priority
        self.reclaim_expired = reclaim_expired
        self.reclaim_ratio = reclaim_ratio
        self.partition_key = partition_key
        self.partition_keys = resolved_partition_keys
        self.claim_partition_batch_size = (
            claim_partition_batch_size
            if claim_partition_batch_size is not None
            else len(self.partition_keys or []) or 1
        )
        self.block_ms = block_ms
        self.idle_sleep_s = idle_sleep_s
        self.max_idle_sleep_s = max_idle_sleep_s
        self.apply_async_depth = apply_async_depth
        self._state_cursor = 0
        self._partition_cursor = 0
        self._running = False
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._active_thread: threading.Thread | None = None
        self._run_gate = SyncWorkerRunGate(closing_message="workflow worker is closing")
        self._terminal_state = WorkerTerminalState()
        self._invocations = WorkerInvocationTracker()
        self._close_operations = SyncCloseTaskRegistry()
        self._totals = WorkflowWorkerResult()
        self._apply_executor = (
            ThreadPoolExecutor(max_workers=apply_async_depth) if apply_async_depth > 0 else None
        )
        self._pending_applies: deque[Future[int]] = deque()

    def run(self) -> None:
        self.run_forever()

    def run_forever(self) -> None:
        self._begin_run(active_thread=threading.current_thread())
        self._run_loop()

    def start(self, *, daemon: bool = True) -> WorkflowWorker:
        def start_thread() -> None:
            if self._thread is not None and self._thread.is_alive():
                raise RuntimeError("workflow worker already started")
            self._prepare_run()
            thread = threading.Thread(target=self._thread_entry, daemon=daemon)
            self._thread = thread
            try:
                thread.start()
            except BaseException:
                self._thread = None
                self._running = False
                raise

        self._run_gate.run_while_open(start_thread)
        return self

    def join(self, timeout: float | None = None) -> WorkflowWorkerResult:
        threads = self._run_gate.synchronized(
            lambda: list(
                dict.fromkeys(
                    thread for thread in (self._thread, self._active_thread) if thread is not None
                )
            )
        )
        for thread in threads:
            thread.join(timeout)
        if all(not thread.is_alive() for thread in threads):
            self._terminal_state.raise_if_failed()
        return self.stats

    @property
    def is_running(self) -> bool:
        return self._run_gate.synchronized(lambda: self._running)

    @property
    def stats(self) -> WorkflowWorkerResult:
        return self._totals

    def _prepare_run(self, *, active_thread: threading.Thread | None = None) -> None:
        running_thread = self._active_thread
        if self._running or (
            running_thread is not None
            and running_thread is not active_thread
            and running_thread.is_alive()
        ):
            raise RuntimeError("workflow worker already running")
        self._terminal_state.reset()
        self._stop_event.clear()
        self._running = True
        if active_thread is not None:
            self._active_thread = active_thread

    def _begin_run(self, *, active_thread: threading.Thread | None = None) -> None:
        self._run_gate.run_while_open(lambda: self._prepare_run(active_thread=active_thread))

    def _thread_entry(self) -> None:
        try:
            self._run_loop()
        except BaseException as exc:
            self._terminal_state.capture(exc)

    def _run_loop(self) -> None:
        active_thread = threading.current_thread()

        def enter_loop() -> None:
            running_thread = self._active_thread
            if (
                running_thread is not None
                and running_thread is not active_thread
                and running_thread.is_alive()
            ):
                raise RuntimeError("workflow worker already running")
            self._active_thread = active_thread
            if not self._running and not self._stop_event.is_set():
                self._running = True

        self._run_gate.synchronized(enter_loop)
        idle_sleep_s = self.idle_sleep_s
        try:
            while self._running and not self._stop_event.is_set():
                result = self.run_once()
                self._totals = self._merge_results(self._totals, result)
                if result.claimed == 0:
                    self._stop_event.wait(idle_sleep_s)
                    idle_sleep_s = min(
                        self.max_idle_sleep_s,
                        max(idle_sleep_s * 2, self.idle_sleep_s),
                    )
                else:
                    idle_sleep_s = self.idle_sleep_s
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

    def close(self, timeout: float | None = 5.0) -> WorkflowWorkerResult:
        deadline = CloseDeadline.start(timeout)
        timeout_message = "workflow worker close timed out"

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
        result = WorkflowWorkerResult()
        try:
            result = self._drain_pending_applies(block=True, deadline=deadline)
            self._totals = self._merge_results(self._totals, result)
        except CloseTimeoutError:
            raise
        except BaseException as exc:
            if error is None:
                error = exc
        if self._apply_executor is not None:
            try:
                deadline.check(timeout_message)
                executor = self._apply_executor
                self._close_operations.run(
                    executor,
                    lambda: executor.shutdown(wait=True),
                    lambda future: deadline.future_result(future, timeout_message),
                )
            except BaseException as exc:
                if error is None:
                    error = exc
            else:
                self._apply_executor = None
        deadline.check(timeout_message)
        if error is not None:
            raise error
        return result

    def flush(self) -> WorkflowWorkerResult:
        return self._drain_pending_applies(block=True)

    def run_once(self) -> WorkflowWorkerResult:
        if not self._begin_invocation():
            return WorkflowWorkerResult()
        try:
            return self._run_once()
        finally:
            self._invocations.end()

    def _begin_invocation(self) -> bool:
        try:
            self._invocations.begin("workflow worker is closing")
        except RuntimeError:
            if self._stop_event.is_set() and threading.current_thread() is self._active_thread:
                return False
            raise
        return True

    def _run_once(self) -> WorkflowWorkerResult:
        result = self._drain_pending_applies(block=False)
        if self._should_claim_any_state():
            return self._merge_results(result, self._run_once_any_state())

        state_name, partition_key, partition_keys = self._next_claim_target()
        jobs = self.workflow.claim_due(
            state_name,
            worker=self.worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=self.batch_size,
            priority=self.priority,
            reclaim_expired=self.reclaim_expired,
            reclaim_ratio=self.reclaim_ratio,
            block_ms=self.block_ms,
        )
        result = self._merge_results(result, WorkflowWorkerResult(claim_calls=1))
        if not jobs:
            return self._merge_results(result, WorkflowWorkerResult(empty_claims=1))

        claimed = len(jobs)
        result = self._merge_results(result, WorkflowWorkerResult(claimed=claimed))
        if self._apply_executor is not None and not self.workflow._states[state_name].return_record:
            while len(self._pending_applies) >= self.apply_async_depth:
                result = self._merge_results(
                    result, self._drain_pending_applies(block=True, limit=1)
                )
            self._pending_applies.append(
                self._apply_executor.submit(
                    self.workflow.handle_claimed_batch_count,
                    state_name,
                    jobs,
                )
            )
            return result

        if self.workflow._states[state_name].return_record:
            applied = len(self.workflow.handle_claimed_batch(state_name, jobs))
        else:
            applied = self.workflow.handle_claimed_batch_count(state_name, jobs)
        return self._merge_results(result, WorkflowWorkerResult(applied=applied))

    def _run_once_any_state(self) -> WorkflowWorkerResult:
        partition_key, partition_keys = self._next_claim_partition()
        jobs = self.workflow.claim_client.claim_flows(
            self.workflow.type,
            worker=self.worker,
            partition_key=partition_key,
            partition_keys=partition_keys,
            limit=self.batch_size,
            priority=self.priority,
            reclaim_expired=self.reclaim_expired,
            reclaim_ratio=self.reclaim_ratio,
            block_ms=self.block_ms,
            include_state=True,
        )
        result = WorkflowWorkerResult(claim_calls=1)
        if not jobs:
            return self._merge_results(result, WorkflowWorkerResult(empty_claims=1))

        for job in jobs:
            object.__setattr__(job, "type", self.workflow.type)
            object.__setattr__(job, "state", "running")

        result = self._merge_results(result, WorkflowWorkerResult(claimed=len(jobs)))

        for state_name, state_jobs in self._group_jobs_by_run_state(jobs).items():
            if (
                self._apply_executor is not None
                and not self.workflow._states[state_name].return_record
            ):
                while len(self._pending_applies) >= self.apply_async_depth:
                    result = self._merge_results(
                        result, self._drain_pending_applies(block=True, limit=1)
                    )
                self._pending_applies.append(
                    self._apply_executor.submit(
                        self.workflow.handle_claimed_batch_count,
                        state_name,
                        state_jobs,
                    )
                )
            elif self.workflow._states[state_name].return_record:
                applied = len(self.workflow.handle_claimed_batch(state_name, state_jobs))
                result = self._merge_results(result, WorkflowWorkerResult(applied=applied))
            else:
                applied = self.workflow.handle_claimed_batch_count(state_name, state_jobs)
                result = self._merge_results(result, WorkflowWorkerResult(applied=applied))

        return result

    def _should_claim_any_state(self) -> bool:
        if not self.block_ms or len(self.states) <= 1:
            return False
        if set(self.states) != set(self.workflow._states):
            return False
        return all(
            not config.claim_record and not config.claim_payload and not config.claim_values
            for name, config in self.workflow._states.items()
            if name in self.states
        )

    def _group_jobs_by_run_state(
        self, jobs: builtins.list[ClaimedFlow]
    ) -> dict[str, builtins.list[ClaimedFlow]]:
        grouped: dict[str, builtins.list[ClaimedFlow]] = {}
        for job in jobs:
            state_name = job.run_state
            if state_name not in self.workflow._states:
                raise ValueError(f"no handler for workflow state: {state_name!r}")
            grouped.setdefault(state_name, []).append(job)
        return grouped

    def _next_state(self) -> str:
        state_name = self.states[self._state_cursor]
        self._state_cursor = (self._state_cursor + 1) % len(self.states)
        return state_name

    def _next_claim_target(
        self,
    ) -> tuple[str, str | bytes | None, builtins.list[str | bytes] | None]:
        if self.partition_key is not None:
            return self._next_state(), self.partition_key, None
        if not self.partition_keys:
            return self._next_state(), None, None

        state_name = self.states[self._state_cursor]
        count = min(
            self.claim_partition_batch_size,
            len(self.partition_keys) - self._partition_cursor,
        )
        keys = self.partition_keys[self._partition_cursor : self._partition_cursor + count]
        self._partition_cursor += count
        if self._partition_cursor >= len(self.partition_keys):
            self._partition_cursor = 0
            self._state_cursor = (self._state_cursor + 1) % len(self.states)
        if len(keys) == 1:
            return state_name, keys[0], None
        return state_name, None, keys

    def _next_claim_partition(
        self,
    ) -> tuple[str | bytes | None, builtins.list[str | bytes] | None]:
        if self.partition_key is not None:
            return self.partition_key, None
        if not self.partition_keys:
            return None, None

        count = min(self.claim_partition_batch_size, len(self.partition_keys))
        keys = [
            self.partition_keys[(self._partition_cursor + offset) % len(self.partition_keys)]
            for offset in range(count)
        ]
        self._partition_cursor = (self._partition_cursor + count) % len(self.partition_keys)
        if len(keys) == 1:
            return keys[0], None
        return None, keys

    def _drain_pending_applies(
        self,
        *,
        block: bool,
        limit: int | None = None,
        deadline: CloseDeadline | None = None,
    ) -> WorkflowWorkerResult:
        if not self._pending_applies:
            return WorkflowWorkerResult()

        applied_total = 0
        count = 0
        if block:
            while self._pending_applies and (limit is None or count < limit):
                future = self._pending_applies[0]
                try:
                    applied = (
                        deadline.future_result(future, "workflow worker close timed out")
                        if deadline is not None
                        else future.result()
                    )
                except CloseTimeoutError:
                    raise
                except BaseException:
                    self._pending_applies.popleft()
                    raise
                self._pending_applies.popleft()
                applied_total += applied
                count += 1
        else:
            retained: deque[Future[int]] = deque()
            while self._pending_applies:
                future = self._pending_applies.popleft()
                if future.done() and (limit is None or count < limit):
                    try:
                        applied_total += future.result()
                    except BaseException:
                        retained.extend(self._pending_applies)
                        self._pending_applies = retained
                        raise
                    count += 1
                else:
                    retained.append(future)
            self._pending_applies = retained
        return WorkflowWorkerResult(applied=applied_total)

    @staticmethod
    def _merge_results(
        left: WorkflowWorkerResult, right: WorkflowWorkerResult
    ) -> WorkflowWorkerResult:
        return WorkflowWorkerResult(
            claimed=left.claimed + right.claimed,
            applied=left.applied + right.applied,
            claim_calls=left.claim_calls + right.claim_calls,
            empty_claims=left.empty_claims + right.empty_claims,
        )
