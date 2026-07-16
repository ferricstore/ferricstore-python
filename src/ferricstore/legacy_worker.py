from __future__ import annotations

import threading

from ferricstore.config_validation import (
    validate_bounded_nonnegative_int,
    validate_nonnegative_int,
    validate_optional_bool,
    validate_optional_flow_priority,
    validate_positive_int,
    validate_string_sequence,
    validate_thread_wait_seconds,
)
from ferricstore.worker_core import validate_worker_idle_timing
from ferricstore.workflow_runtime import Workflow


class Worker:
    """Compatibility polling worker for one workflow definition."""

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
        limit = validate_positive_int(limit, name="limit")
        resolved_states = (
            list(validate_string_sequence(states, name="states", allow_empty=False))
            if states is not None
            else None
        )
        partial_retry_delay_s = validate_thread_wait_seconds(
            partial_retry_delay_s,
            name="partial_retry_delay_s",
        )
        partial_retries = validate_nonnegative_int(
            partial_retries,
            name="partial_retries",
        )
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
        idle_sleep_s, max_idle_sleep_s = validate_worker_idle_timing(
            idle_sleep_s,
            max_idle_sleep_s,
        )

        self.workflow = workflow
        self.worker = worker
        self.states = list(workflow._states.keys()) if resolved_states is None else resolved_states
        self.partition_key = partition_key
        self.limit = limit
        self.idle_sleep_s = idle_sleep_s
        self.max_idle_sleep_s = max_idle_sleep_s
        self.partial_retry_delay_s = partial_retry_delay_s
        self.partial_retries = partial_retries
        self.priority = priority
        self.reclaim_expired = reclaim_expired
        self.reclaim_ratio = reclaim_ratio
        self._running = False
        self._stop_event = threading.Event()

    def run_forever(self) -> None:
        self._stop_event.clear()
        self._running = True
        idle_sleep_s = self.idle_sleep_s
        while self._running:
            processed = self.run_once()
            if processed == 0:
                if self._stop_event.wait(idle_sleep_s):
                    break
                idle_sleep_s = min(
                    max(self.max_idle_sleep_s, self.idle_sleep_s),
                    max(idle_sleep_s * 2, self.idle_sleep_s),
                )
            else:
                idle_sleep_s = self.idle_sleep_s

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()

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
                    break
                if len(results) == 0 or retries_left <= 0:
                    break
                retries_left -= 1
                if self.partial_retry_delay_s > 0 and self._stop_event.wait(
                    self.partial_retry_delay_s
                ):
                    return processed
        return processed


__all__ = ["Worker"]
