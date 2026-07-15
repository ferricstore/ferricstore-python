from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ferricstore.errors import FerricStoreError
from ferricstore.governance_validation import validate_workflow_budget_options
from ferricstore.lifecycle_core import raise_primary_with_cleanup
from ferricstore.types import BudgetResult

if TYPE_CHECKING:
    from ferricstore.workflow_models import WorkflowContext


class WorkflowBudget:
    """Synchronous budget reservation helper for workflow handlers."""

    __slots__ = (
        "_closed",
        "_entered",
        "_result",
        "amount",
        "attribute_prefix",
        "ctx",
        "limit",
        "reservation",
        "scope",
        "usage_key",
        "window_ms",
    )

    def __init__(
        self,
        ctx: WorkflowContext,
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

    def __enter__(self) -> WorkflowBudget:
        if self._entered:
            raise FerricStoreError("budget reservation helper cannot be entered more than once")
        self._entered = True
        self.reservation = self.ctx.client.budget_reserve(
            self.scope,
            self.amount,
            limit=self.limit,
            window_ms=self.window_ms,
        )
        _ = self.reservation_id
        self.ctx._record_budget_result(self.attribute_prefix, self.reservation)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._closed:
            return
        if exc_type is None:
            try:
                self.commit(self.amount)
            except BaseException as primary:
                cleanup_error: BaseException | None = None
                if self.is_open:
                    try:
                        self.release()
                    except BaseException as cleanup:
                        cleanup_error = cleanup
                raise_primary_with_cleanup(primary, primary.__traceback__, cleanup_error)
        else:
            try:
                self.release()
            except BaseException as cleanup:
                if exc is not None:
                    raise_primary_with_cleanup(exc, tb, cleanup)
                raise

    def commit(
        self,
        actual_amount: int | None = None,
        *,
        usage: dict[str, Any] | None = None,
    ) -> BudgetResult:
        if self._closed:
            if self._result is None:
                raise FerricStoreError("budget reservation is already closed")
            return self._result
        actual = self.amount if actual_amount is None else actual_amount
        result = self.ctx.client.budget_commit(
            self.scope,
            self.reservation_id,
            actual,
            usage=usage if usage is not None else {self.usage_key: actual},
        )
        self._closed = True
        self._result = result
        self.ctx._record_budget_result(self.attribute_prefix, result)
        return result

    def release(self) -> BudgetResult:
        if self._closed:
            if self._result is None:
                raise FerricStoreError("budget reservation is already closed")
            return self._result
        result = self.ctx.client.budget_release(self.scope, self.reservation_id)
        self._closed = True
        self._result = result
        self.ctx._record_budget_result(self.attribute_prefix, result)
        return result


__all__ = ["WorkflowBudget"]
