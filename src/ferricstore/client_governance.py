from __future__ import annotations

import builtins
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, cast

from ferricstore.client_helpers import (
    _append,
    _append_bool,
    _append_extra_options,
    _normalize_admin_response,
    _ok_response,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.types import (
    ApprovalResult,
    BudgetResult,
    CircuitBreakerStatus,
    EffectResult,
    FlowStatePolicyLike,
    GovernanceOverview,
    RetryPolicy,
    ScheduleResult,
)


class _ClientGovernanceMixin(_ClientMixinBase):
    if TYPE_CHECKING:
        _append_retry_policy: Callable[[builtins.list[Any], RetryPolicy], None]
        _append_state_policy: Callable[[builtins.list[Any], FlowStatePolicyLike], None]

    def schedule_create(
        self,
        id: str,
        *,
        target: dict[str, Any],
        kind: str | None = None,
        at_ms: int | None = None,
        delay_ms: int | None = None,
        start_at_ms: int | None = None,
        every_ms: int | None = None,
        cron: str | None = None,
        timezone: str | None = None,
        overlap_policy: str | None = None,
        overlap_retry_ms: int | None = None,
        max_fires: int | None = None,
        end_at_ms: int | None = None,
        overwrite: bool | None = None,
        now_ms: int | None = None,
        extra_options: dict[str, Any] | None = None,
    ) -> ScheduleResult:
        """Create or replace a durable Flow schedule."""

        args: builtins.list[Any] = ["FLOW.SCHEDULE.CREATE", id]
        _append(args, "KIND", kind)
        _append(args, "AT_MS", at_ms)
        _append(args, "DELAY_MS", delay_ms)
        _append(args, "START_AT_MS", start_at_ms)
        _append(args, "EVERY_MS", every_ms)
        _append(args, "CRON", cron)
        _append(args, "TIMEZONE", timezone)
        _append(args, "TARGET", target)
        _append(args, "OVERLAP_POLICY", overlap_policy)
        _append(args, "OVERLAP_RETRY_MS", overlap_retry_ms)
        _append(args, "MAX_FIRES", max_fires)
        _append(args, "END_AT_MS", end_at_ms)
        _append_bool(args, "OVERWRITE", overwrite)
        _append(args, "NOW", now_ms)
        _append_extra_options(args, extra_options)
        return ScheduleResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_get(self, id: str, *, now_ms: int | None = None) -> ScheduleResult | None:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.GET", id]
        _append(args, "NOW", now_ms)
        response = cast(
            dict[str, Any] | None, _normalize_admin_response(self.executor.execute_command(*args))
        )
        return ScheduleResult.from_resp(response) if response is not None else None

    def schedule_fire(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.FIRE", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_pause(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.PAUSE", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_resume(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.RESUME", id]
        _append(args, "NOW", now_ms)
        return ScheduleResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_delete(self, id: str, *, now_ms: int | None = None) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.DELETE", id]
        _append(args, "NOW", now_ms)
        response = _normalize_admin_response(self.executor.execute_command(*args))
        if _ok_response(response):
            return ScheduleResult(id=id, status="deleted", raw={"id": id, "status": "deleted"})
        return ScheduleResult.from_resp(cast(dict[str, Any], response))

    def schedule_fire_due(
        self,
        *,
        now_ms: int | None = None,
        worker: str | None = None,
        block_ms: int | None = None,
        limit: int | None = None,
    ) -> ScheduleResult:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.FIRE_DUE"]
        _append(args, "NOW", now_ms)
        _append(args, "WORKER", worker)
        _append(args, "BLOCK", block_ms)
        _append(args, "LIMIT", limit)
        return ScheduleResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def schedule_list(
        self,
        *,
        kind: str | None = None,
        state: str | None = None,
        timezone: str | None = None,
        target_type: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        count: int | None = None,
        rev: bool | None = None,
    ) -> builtins.list[ScheduleResult]:
        args: builtins.list[Any] = ["FLOW.SCHEDULE.LIST"]
        _append(args, "KIND", kind)
        _append(args, "STATE", state)
        _append(args, "TIMEZONE", timezone)
        _append(args, "TARGET_TYPE", target_type)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append(args, "COUNT", count)
        _append_bool(args, "REV", rev)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(self.executor.execute_command(*args)),
        )
        return [ScheduleResult.from_resp(item) for item in response]

    def install_policy(
        self,
        type: str,
        *,
        retry: RetryPolicy | None = None,
        states: dict[str, FlowStatePolicyLike] | None = None,
        indexed_state_meta: str | None = None,
    ) -> Any:
        args: builtins.list[Any] = ["FLOW.POLICY.SET", type]
        _append(args, "INDEXED_STATE_META", indexed_state_meta)
        if retry is not None:
            self._append_retry_policy(args, retry)
        for state, policy in (states or {}).items():
            args.extend(["STATE", state])
            self._append_state_policy(args, policy)
        return self.executor.execute_command(*args)

    def policy_get(self, type: str, *, state: str | None = None) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.POLICY.GET", type]
        _append(args, "STATE", state)
        return dict(self.executor.execute_command(*args) or {})

    def effect_reserve(
        self,
        id: str,
        effect_key: str,
        effect_type: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        operation_digest: str,
        idempotency_key: str | None = None,
        governance_scope: str | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        args: builtins.list[Any] = ["FLOW.EFFECT.RESERVE", id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "EFFECT_TYPE", effect_type)
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "OPERATION_DIGEST", operation_digest)
        _append(args, "IDEMPOTENCY_KEY", idempotency_key)
        _append(args, "GOVERNANCE_SCOPE", governance_scope)
        _append(args, "NOW", now_ms)
        return EffectResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def effect_confirm(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return self._effect_status(
            "FLOW.EFFECT.CONFIRM",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            external_id=external_id,
            latency_ms=latency_ms,
            now_ms=now_ms,
        )

    def effect_fail(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return self._effect_status(
            "FLOW.EFFECT.FAIL",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            error=error,
            reason=reason,
            latency_ms=latency_ms,
            now_ms=now_ms,
        )

    def effect_compensate(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return self._effect_status(
            "FLOW.EFFECT.COMPENSATE",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            external_id=external_id,
            reason=reason,
            now_ms=now_ms,
        )

    def effect_get(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
    ) -> EffectResult | None:
        args: builtins.list[Any] = ["FLOW.EFFECT.GET", id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "PARTITION", partition_key)
        response = cast(
            dict[str, Any] | None, _normalize_admin_response(self.executor.execute_command(*args))
        )
        return EffectResult.from_resp(response) if response is not None else None

    def _effect_status(
        self,
        command: str,
        id: str,
        effect_key: str,
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        args: builtins.list[Any] = [command, id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "EXTERNAL_ID", external_id)
        _append(args, "ERROR", error)
        _append(args, "REASON", reason)
        _append(args, "LATENCY_MS", latency_ms)
        _append(args, "NOW", now_ms)
        return EffectResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def governance_ledger(
        self,
        id: str,
        *,
        partition_key: str | None = None,
        limit: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.GOVERNANCE.LEDGER", id]
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append_bool(args, "REV", rev)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(self.executor.execute_command(*args)),
        )

    def approval_request(
        self,
        id: str,
        *,
        flow_id: str,
        scope: str,
        reason: str | None = None,
        requested_by: str | None = None,
        assignees: Sequence[str] | None = None,
        policy_hash: str | None = None,
        policy_version: str | int | None = None,
        timeout_ms: int | None = None,
        expires_at_ms: int | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        args: builtins.list[Any] = ["FLOW.APPROVAL.REQUEST", id]
        _append(args, "FLOW_ID", flow_id)
        _append(args, "SCOPE", scope)
        _append(args, "REASON", reason)
        _append(args, "REQUESTED_BY", requested_by)
        _append(args, "ASSIGNEES", list(assignees) if assignees is not None else None)
        _append(args, "POLICY_HASH", policy_hash)
        _append(args, "POLICY_VERSION", policy_version)
        _append(args, "TIMEOUT_MS", timeout_ms)
        _append(args, "EXPIRES_AT_MS", expires_at_ms)
        _append(args, "NOW", now_ms)
        return ApprovalResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def approval_approve(
        self,
        id: str,
        *,
        approver: str,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        return self._approval_status("FLOW.APPROVAL.APPROVE", id, approver, reason, now_ms)

    def approval_reject(
        self,
        id: str,
        *,
        approver: str,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        return self._approval_status("FLOW.APPROVAL.REJECT", id, approver, reason, now_ms)

    def _approval_status(
        self,
        command: str,
        id: str,
        approver: str,
        reason: str | None,
        now_ms: int | None,
    ) -> ApprovalResult:
        args: builtins.list[Any] = [command, id]
        _append(args, "APPROVER", approver)
        _append(args, "REASON", reason)
        _append(args, "NOW", now_ms)
        return ApprovalResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def approval_get(self, id: str) -> ApprovalResult | None:
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(self.executor.execute_command("FLOW.APPROVAL.GET", id)),
        )
        return ApprovalResult.from_resp(response) if response is not None else None

    def approval_list(
        self,
        *,
        status: str | None = None,
        scope: str | None = None,
        partition_key: str | None = None,
        flow_id: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[ApprovalResult]:
        args: builtins.list[Any] = ["FLOW.APPROVAL.LIST"]
        _append(args, "STATUS", status)
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "FLOW_ID", flow_id)
        _append(args, "LIMIT", limit)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(self.executor.execute_command(*args)),
        )
        return [ApprovalResult.from_resp(item) for item in response]

    def governance_overview(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        status: str | None = None,
        flow_id: str | None = None,
        limit: int | None = None,
    ) -> GovernanceOverview:
        args: builtins.list[Any] = ["FLOW.GOVERNANCE.OVERVIEW"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "STATUS", status)
        _append(args, "FLOW_ID", flow_id)
        _append(args, "LIMIT", limit)
        return GovernanceOverview.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def circuit_open(
        self,
        scope: str,
        *,
        open_ms: int | None = None,
        failure_threshold: int | None = None,
        now_ms: int | None = None,
    ) -> CircuitBreakerStatus:
        args: builtins.list[Any] = ["FLOW.CIRCUIT.OPEN", scope]
        _append(args, "OPEN_MS", open_ms)
        _append(args, "FAILURE_THRESHOLD", failure_threshold)
        _append(args, "NOW", now_ms)
        return CircuitBreakerStatus.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def circuit_close(self, scope: str, *, now_ms: int | None = None) -> CircuitBreakerStatus:
        args: builtins.list[Any] = ["FLOW.CIRCUIT.CLOSE", scope]
        _append(args, "NOW", now_ms)
        return CircuitBreakerStatus.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def circuit_get(self, scope: str) -> CircuitBreakerStatus | None:
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(self.executor.execute_command("FLOW.CIRCUIT.GET", scope)),
        )
        return CircuitBreakerStatus.from_resp(response) if response is not None else None

    def budget_reserve(
        self,
        scope: str,
        amount: int,
        *,
        limit: int | None = None,
        window_ms: int | None = None,
        reservation_id: str | None = None,
        now_ms: int | None = None,
    ) -> BudgetResult:
        args: builtins.list[Any] = ["FLOW.BUDGET.RESERVE", scope]
        _append(args, "AMOUNT", amount)
        _append(args, "LIMIT", limit)
        _append(args, "WINDOW_MS", window_ms)
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def budget_commit(
        self,
        scope: str,
        reservation_id: str,
        actual_amount: int,
        *,
        usage: Mapping[str, Any] | None = None,
        now_ms: int | None = None,
    ) -> BudgetResult:
        args: builtins.list[Any] = ["FLOW.BUDGET.COMMIT", scope]
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "ACTUAL_AMOUNT", actual_amount)
        _append(args, "USAGE", dict(usage) if usage is not None else None)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def budget_release(
        self,
        scope: str,
        reservation_id: str,
        *,
        now_ms: int | None = None,
    ) -> BudgetResult:
        args: builtins.list[Any] = ["FLOW.BUDGET.RELEASE", scope]
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))
        )

    def budget_get(self, scope: str) -> BudgetResult | None:
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(self.executor.execute_command("FLOW.BUDGET.GET", scope)),
        )
        return BudgetResult.from_resp(response) if response is not None else None

    def budget_list(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[BudgetResult]:
        args: builtins.list[Any] = ["FLOW.BUDGET.LIST"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(self.executor.execute_command(*args)),
        )
        return [BudgetResult.from_resp(item) for item in response]

    def limit_lease(
        self,
        scope: str,
        *,
        shard_id: int,
        amount: int,
        ttl_ms: int,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.LIMIT.LEASE", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        _append(args, "LIMIT", limit)
        _append(args, "TTL_MS", ttl_ms)
        _append(args, "NOW", now_ms)
        return cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))

    def limit_spend(
        self,
        scope: str,
        *,
        shard_id: int,
        amount: int,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.LIMIT.SPEND", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        _append(args, "NOW", now_ms)
        return cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))

    def limit_release(self, scope: str, *, shard_id: int, amount: int) -> dict[str, Any]:
        args: builtins.list[Any] = ["FLOW.LIMIT.RELEASE", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        return cast(dict[str, Any], _normalize_admin_response(self.executor.execute_command(*args)))

    def limit_get(self, scope: str, *, now_ms: int | None = None) -> dict[str, Any] | None:
        args: builtins.list[Any] = ["FLOW.LIMIT.GET", scope]
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any] | None,
            _normalize_admin_response(self.executor.execute_command(*args)),
        )

    def limit_list(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> builtins.list[dict[str, Any]]:
        args: builtins.list[Any] = ["FLOW.LIMIT.LIST"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(self.executor.execute_command(*args)),
        )

    def retention_cleanup(
        self,
        *,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[Any, Any]:
        args: builtins.list[Any] = ["FLOW.RETENTION_CLEANUP"]
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return dict(self.executor.execute_command(*args) or {})
