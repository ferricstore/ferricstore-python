from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AsyncWorkflowWorkerResult:
    claimed: int = 0
    applied: int = 0
    claim_calls: int = 0
    empty_claims: int = 0


AsyncWorkflowWorkerResult.__module__ = "ferricstore.async_worker"


__all__ = ["AsyncWorkflowWorkerResult"]
