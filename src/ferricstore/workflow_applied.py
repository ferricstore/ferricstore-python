from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ferricstore.types import ClaimedFlow, FlowRecord


@dataclass(frozen=True, slots=True)
class AppliedWorkflowStep:
    """Internal marker for a workflow mutation already committed by the handler."""

    job: ClaimedFlow | FlowRecord
    result: Any = None
    error: BaseException | None = None
    uncertain: bool = False
    continuation: Any = None
    has_continuation: bool = False
