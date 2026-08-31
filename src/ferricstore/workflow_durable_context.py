from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from ferricstore.durable_step import DurableStepOutcomeUnknownError
from ferricstore.types import ClaimedFlow, FlowRecord
from ferricstore.workflow_applied import AppliedWorkflowStep


class SyncDurableWorkflowContextMixin:
    client: Any
    job: FlowRecord | ClaimedFlow
    state_name: str
    _applied_step: AppliedWorkflowStep | None

    def advance(
        self,
        *,
        to_state: str,
        lease_ms: int = 30_000,
        now_ms: int | None = None,
    ) -> ClaimedFlow:
        """Advance and retain the refreshed claim for the handler's continuation."""

        self._raise_if_uncertain()
        try:
            refreshed = cast(
                ClaimedFlow,
                self.client.advance(
                    self.job,
                    to_state=to_state,
                    lease_ms=lease_ms,
                    now_ms=now_ms,
                ),
            )
        except DurableStepOutcomeUnknownError as exc:
            self._record_uncertain_step(exc.original)
            raise exc.original from exc
        self._record_applied_step(refreshed)
        return refreshed

    def step(
        self,
        *,
        name: str,
        run: Callable[[], Any],
        to_state: str,
        lease_ms: int = 30_000,
        now_ms: int | None = None,
    ) -> Any:
        """Run a durable step and retain its refreshed claim for the continuation."""

        self._raise_if_uncertain()
        previous = self.job
        try:
            refreshed, result = self.client.step(
                previous,
                name=name,
                run=run,
                to_state=to_state,
                lease_ms=lease_ms,
                now_ms=now_ms,
            )
        except DurableStepOutcomeUnknownError as exc:
            self._record_uncertain_step(exc.original)
            raise exc.original from exc
        self._record_step_result(previous, refreshed, result)
        return result

    def _record_step_result(
        self,
        previous: FlowRecord | ClaimedFlow,
        refreshed: ClaimedFlow,
        result: Any,
    ) -> None:
        self.job = refreshed
        self.state_name = refreshed.run_state or self.state_name
        if (
            refreshed.lease_token != previous.lease_token
            or refreshed.fencing_token > previous.fencing_token
        ):
            self._applied_step = AppliedWorkflowStep(refreshed, result)

    def _record_applied_step(self, job: ClaimedFlow, result: Any = None) -> None:
        self.job = job
        self.state_name = job.run_state or self.state_name
        self._applied_step = AppliedWorkflowStep(job, result)

    def _record_uncertain_step(self, error: Exception) -> None:
        self._applied_step = AppliedWorkflowStep(
            self.job,
            error=error,
            uncertain=True,
        )

    def _raise_if_uncertain(self) -> None:
        if self._applied_step is not None and self._applied_step.uncertain:
            error = self._applied_step.error
            if isinstance(error, BaseException):
                raise error


class AsyncDurableWorkflowContextMixin:
    client: Any
    job: FlowRecord | ClaimedFlow
    state_name: str
    _applied_step: AppliedWorkflowStep | None

    async def advance(
        self,
        *,
        to_state: str,
        lease_ms: int = 30_000,
        now_ms: int | None = None,
    ) -> ClaimedFlow:
        """Advance and retain the refreshed claim for the handler's continuation."""

        self._raise_if_uncertain()
        try:
            refreshed = cast(
                ClaimedFlow,
                await self.client.advance(
                    self.job,
                    to_state=to_state,
                    lease_ms=lease_ms,
                    now_ms=now_ms,
                ),
            )
        except DurableStepOutcomeUnknownError as exc:
            self._record_uncertain_step(exc.original)
            raise exc.original from exc
        self._record_applied_step(refreshed)
        return refreshed

    async def step(
        self,
        *,
        name: str,
        run: Callable[[], Any],
        to_state: str,
        lease_ms: int = 30_000,
        now_ms: int | None = None,
    ) -> Any:
        """Run a durable step and retain its refreshed claim for the continuation."""

        self._raise_if_uncertain()
        previous = self.job
        try:
            refreshed, result = await self.client.step(
                previous,
                name=name,
                run=run,
                to_state=to_state,
                lease_ms=lease_ms,
                now_ms=now_ms,
            )
        except DurableStepOutcomeUnknownError as exc:
            self._record_uncertain_step(exc.original)
            raise exc.original from exc
        self._record_step_result(previous, refreshed, result)
        return result

    def _record_step_result(
        self,
        previous: FlowRecord | ClaimedFlow,
        refreshed: ClaimedFlow,
        result: Any,
    ) -> None:
        self.job = refreshed
        self.state_name = refreshed.run_state or self.state_name
        if (
            refreshed.lease_token != previous.lease_token
            or refreshed.fencing_token > previous.fencing_token
        ):
            self._applied_step = AppliedWorkflowStep(refreshed, result)

    def _record_applied_step(self, job: ClaimedFlow, result: Any = None) -> None:
        self.job = job
        self.state_name = job.run_state or self.state_name
        self._applied_step = AppliedWorkflowStep(job, result)

    def _record_uncertain_step(self, error: Exception) -> None:
        self._applied_step = AppliedWorkflowStep(
            self.job,
            error=error,
            uncertain=True,
        )

    def _raise_if_uncertain(self) -> None:
        if self._applied_step is not None and self._applied_step.uncertain:
            error = self._applied_step.error
            if isinstance(error, BaseException):
                raise error
