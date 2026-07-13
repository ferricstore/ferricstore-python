from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any


class MutationKind(Enum):
    COMPLETE = "complete"
    TRANSITION = "transition"
    RETRY = "retry"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class JobMutation:
    """One fenced job mutation, independent of sync/async transport details."""

    kind: MutationKind
    job: Any
    options: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MutationBatchPlan:
    """An ordered mutation plan whose response cardinality matches its jobs."""

    mutations: tuple[JobMutation, ...]

    @classmethod
    def build(cls, mutations: Iterable[JobMutation]) -> MutationBatchPlan:
        return cls(tuple(mutations))

    @classmethod
    def failures(
        cls,
        failures: Iterable[tuple[Any, BaseException]],
        *,
        kind: MutationKind,
    ) -> MutationBatchPlan:
        if kind not in {MutationKind.RETRY, MutationKind.FAIL}:
            raise ValueError("failure plans only support retry or fail mutations")
        return cls.build(JobMutation(kind, job, {"error": str(error)}) for job, error in failures)

    def __bool__(self) -> bool:
        return bool(self.mutations)

    def __len__(self) -> int:
        return len(self.mutations)
