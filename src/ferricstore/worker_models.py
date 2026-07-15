from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class QueueFlowWorkerResult:
    claimed: int = 0
    completed: int = 0
    retried: int = 0
    failed: int = 0
    claim_calls: int = 0


QueueFlowWorkerResult.__module__ = "ferricstore.worker"


__all__ = ["QueueFlowWorkerResult"]
