from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from ferricstore.errors import FerricStoreError
from ferricstore.governance_validation import validate_workflow_budget_options
from ferricstore.lifecycle_core import await_cancellation_safe, raise_primary_with_cleanup
from ferricstore.types import BudgetResult

if TYPE_CHECKING:
    from ferricstore.async_workflow_context import AsyncWorkflowContext


class AsyncWorkflowBudget:
    """Async budget reservation helper for workflow handlers."""

    def __init__(
        self,
        ctx: AsyncWorkflowContext,
        *,
        scope: str,
        amount: int,
        limit: int | None = None,
        window_ms: int | None = None,
        usage_key: str = "amount",
        attribute_prefix: str = "governance_budget",
    ) -> None:
        validate_workflow_budget_options(
            scope,
            amount,
            limit=limit,
            window_ms=window_ms,
            usage_key=usage_key,
            attribute_prefix=attribute_prefix,
        )
        self.ctx = ctx
        self.scope = scope
        self.amount = amount
        self.limit = limit
        self.window_ms = window_ms
        self.usage_key = usage_key
        self.attribute_prefix = attribute_prefix
        self.reservation: BudgetResult | None = None
        self._closed = False
        self._entered = False
        self._result: BudgetResult | None = None
        self._settlement_task: asyncio.Task[BudgetResult] | None = None
        self._settlement_kind: str | None = None

    @property
    def reservation_id(self) -> str:
        if self.reservation is None or self.reservation.reservation_id is None:
            raise FerricStoreError("budget reservation has not been opened")
        return self.reservation.reservation_id

    @property
    def is_open(self) -> bool:
        return (
            not self._closed
            and self.reservation is not None
            and self.reservation.reservation_id is not None
        )

    async def __aenter__(self) -> AsyncWorkflowBudget:
        if self._entered:
            raise FerricStoreError("budget reservation helper cannot be entered more than once")
        self._entered = True
        self.reservation = await self.ctx.client.budget_reserve(
            self.scope,
            self.amount,
            limit=self.limit,
            window_ms=self.window_ms,
        )
        _ = self.reservation_id
        self.ctx._record_budget_result(self.attribute_prefix, self.reservation)
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            try:
                await self.commit(self.amount)
            except BaseException as primary:
                cleanup_error: BaseException | None = None
                if self.is_open:
                    try:
                        await self.release()
                    except BaseException as cleanup:
                        cleanup_error = cleanup
                raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        else:
            try:
                await self.release()
            except BaseException as cleanup:
                if exc is not None:
                    raise_primary_with_cleanup(exc, tb, cleanup)
                raise

    async def commit(
        self, actual_amount: int | None = None, *, usage: dict[str, Any] | None = None
    ) -> BudgetResult:
        if self._closed:
            if self._result is None:
                raise FerricStoreError("budget reservation is already closed")
            return self._result
        actual = self.amount if actual_amount is None else actual_amount

        async def commit_and_record() -> BudgetResult:
            result = await self.ctx.client.budget_commit(
                self.scope,
                self.reservation_id,
                actual,
                usage=usage if usage is not None else {self.usage_key: actual},
            )
            self._closed = True
            self._result = result
            self.ctx._record_budget_result(self.attribute_prefix, result)
            return result

        task = self._settlement_task
        if task is None:
            task = asyncio.create_task(commit_and_record())
            self._settlement_task = task
            self._settlement_kind = "commit"
        try:
            return cast(BudgetResult, await await_cancellation_safe(task))
        except asyncio.CancelledError:
            task_failed = task.done() and (task.cancelled() or task.exception() is not None)
            if task_failed and self._settlement_task is task:
                self._settlement_task = None
                self._settlement_kind = None
            raise
        except BaseException:
            if self._settlement_task is task:
                self._settlement_task = None
                self._settlement_kind = None
            raise

    async def release(self) -> BudgetResult:
        if self._closed:
            if self._result is None:
                raise FerricStoreError("budget reservation is already closed")
            return self._result

        async def release_and_record() -> BudgetResult:
            result = await self.ctx.client.budget_release(self.scope, self.reservation_id)
            self._closed = True
            self._result = result
            self.ctx._record_budget_result(self.attribute_prefix, result)
            return result

        while True:
            task = self._settlement_task
            kind = self._settlement_kind
            if task is None:
                task = asyncio.create_task(release_and_record())
                self._settlement_task = task
                self._settlement_kind = "release"
                kind = "release"
            try:
                return cast(BudgetResult, await await_cancellation_safe(task))
            except asyncio.CancelledError:
                if kind == "commit" and task.cancelled():
                    if self._settlement_task is task:
                        release_task = asyncio.create_task(release_and_record())
                        self._settlement_task = release_task
                        self._settlement_kind = "release"
                    continue
                if kind == "release" and task.cancelled() and self._settlement_task is task:
                    self._settlement_task = None
                    self._settlement_kind = None
                raise
            except BaseException:
                if kind != "commit":
                    if self._settlement_task is task:
                        self._settlement_task = None
                        self._settlement_kind = None
                    raise
                if self._settlement_task is task:
                    release_task = asyncio.create_task(release_and_record())
                    self._settlement_task = release_task
                    self._settlement_kind = "release"
                continue


__all__ = ["AsyncWorkflowBudget"]
