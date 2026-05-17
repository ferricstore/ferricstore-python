from __future__ import annotations

import time

from ferricstore.workflow import Workflow


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
        self.workflow = workflow
        self.worker = worker
        self.states = states or list(workflow._states.keys())
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
