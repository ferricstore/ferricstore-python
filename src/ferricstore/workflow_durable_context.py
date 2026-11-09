from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, cast

from ferricstore.durable_step import DurableStepOutcomeUnknownError
from ferricstore.types import ChildSpec, ClaimedFlow, FlowRecord
from ferricstore.workflow_applied import AppliedWorkflowStep


def wait_for_children_options(
    children: Sequence[ChildSpec],
    *,
    wait_state: str,
    success: str,
    failure: str,
    group_id: str,
    wait: str,
    on_child_failed: str | None,
    on_parent_closed: str | None,
    max_active_ms: int | float | str | None,
    values: dict[str, Any] | None,
    value_refs: dict[str, str] | None,
) -> dict[str, Any]:
    """Validate a wait for children and return the spawn options."""

    if len(children) == 0:
        raise ValueError("wait_for_children children must be non-empty")
    for name, state in (("wait_state", wait_state), ("success", success), ("failure", failure)):
        if not isinstance(state, str) or not state:
            raise ValueError(f"wait_for_children {name} must be a non-empty string")
    if wait not in ("all", "any"):
        raise ValueError("wait_for_children wait must be 'all' or 'any'")
    return {
        "group_id": group_id,
        "wait": wait,
        "wait_state": wait_state,
        "success": success,
        "failure": failure,
        "on_child_failed": on_child_failed,
        "on_parent_closed": on_parent_closed,
        "max_active_ms": max_active_ms,
        "values": values,
        "value_refs": value_refs,
    }


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

    def wait_for_children(
        self,
        children: Sequence[ChildSpec],
        *,
        wait_state: str,
        success: str,
        failure: str,
        group_id: str = "default",
        wait: str = "all",
        on_child_failed: str | None = None,
        on_parent_closed: str | None = None,
        max_active_ms: int | float | str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
    ) -> None:
        """Spawn a child group and park this Flow in ``wait_state`` in one fenced command.

        The handler must return nothing afterwards. FerricStore moves the Flow to
        ``success`` or ``failure`` when the group resolves; the handler for that
        state reads the children with ``ctx.flow.child_results(group_id)``.
        """

        options = wait_for_children_options(
            children,
            wait_state=wait_state,
            success=success,
            failure=failure,
            group_id=group_id,
            wait=wait,
            on_child_failed=on_child_failed,
            on_parent_closed=on_parent_closed,
            max_active_ms=max_active_ms,
            values=values,
            value_refs=value_refs,
        )
        self._raise_if_uncertain()
        self.client.spawn_children(
            self.job.id,
            list(children),
            partition_key=self.job.partition_key,
            lease_token=self.job.lease_token,
            fencing_token=self.job.fencing_token,
            **options,
        )
        self._applied_step = AppliedWorkflowStep(self.job, parked=True)

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

    async def wait_for_children(
        self,
        children: Sequence[ChildSpec],
        *,
        wait_state: str,
        success: str,
        failure: str,
        group_id: str = "default",
        wait: str = "all",
        on_child_failed: str | None = None,
        on_parent_closed: str | None = None,
        max_active_ms: int | float | str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
    ) -> None:
        """Spawn a child group and park this Flow in ``wait_state`` in one fenced command.

        The handler must return nothing afterwards. FerricStore moves the Flow to
        ``success`` or ``failure`` when the group resolves; the handler for that
        state reads the children with ``ctx.flow.child_results(group_id)``.
        """

        options = wait_for_children_options(
            children,
            wait_state=wait_state,
            success=success,
            failure=failure,
            group_id=group_id,
            wait=wait,
            on_child_failed=on_child_failed,
            on_parent_closed=on_parent_closed,
            max_active_ms=max_active_ms,
            values=values,
            value_refs=value_refs,
        )
        self._raise_if_uncertain()
        await self.client.spawn_children(
            self.job.id,
            list(children),
            partition_key=self.job.partition_key,
            lease_token=self.job.lease_token,
            fencing_token=self.job.fencing_token,
            **options,
        )
        self._applied_step = AppliedWorkflowStep(self.job, parked=True)

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
