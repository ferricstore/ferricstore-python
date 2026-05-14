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
    ) -> None:
        self.workflow = workflow
        self.worker = worker
        self.states = states or list(workflow._states.keys())
        self.partition_key = partition_key
        self.limit = limit
        self.idle_sleep_s = idle_sleep_s
        self._running = False

    def run_forever(self) -> None:
        self._running = True
        while self._running:
            processed = self.run_once()
            if processed == 0:
                time.sleep(self.idle_sleep_s)

    def stop(self) -> None:
        self._running = False

    def run_once(self) -> int:
        processed = 0
        for state in self.states:
            results = self.workflow.run_once(
                state,
                worker=self.worker,
                partition_key=self.partition_key,
                limit=self.limit,
            )
            processed += len(results)
        return processed

