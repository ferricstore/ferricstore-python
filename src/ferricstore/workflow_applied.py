from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from ferricstore.errors import FerricStoreError
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
    # Set by wait_for_children: the spawn parked the Flow, so nothing follows.
    parked: bool = False


def parked_step(marker: AppliedWorkflowStep, outcome: Any) -> AppliedWorkflowStep:
    """Finish a handler that parked its Flow with wait_for_children."""

    if outcome is None:
        return marker
    return replace(
        marker,
        error=FerricStoreError(
            "a handler that called wait_for_children must return nothing: "
            "the Flow is already waiting for its children"
        ),
    )
