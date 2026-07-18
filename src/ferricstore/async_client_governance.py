from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_helpers import (
    _append,
    _append_bool,
    _append_named_counts,
    _append_named_values,
    _has_named_item_values,
    _merge_named_map,
    _normalize_admin_response,
    _now_ms,
)
from ferricstore.config_validation import (
    normalize_optional_max_active_ms,
    validate_string_sequence,
)
from ferricstore.governance_validation import (
    validate_approval_decision,
    validate_approval_list,
    validate_approval_request,
    validate_budget_list,
    validate_budget_reserve,
    validate_budget_settlement,
    validate_circuit_open,
    validate_circuit_operation,
    validate_ledger_options,
    validate_limit_get,
    validate_limit_lease,
    validate_limit_list,
    validate_limit_release,
    validate_limit_spend,
    validate_nonempty_string,
    validate_retention_cleanup,
)
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    ApprovalResult,
    BudgetResult,
    ChildSpec,
    CircuitBreakerStatus,
    FlowStatePolicyLike,
    GovernanceOverview,
)


class _AsyncClientGovernanceMixin(_AsyncClientMixinBase):
    async def spawn_children(
        self,
        parent_flow_id: str,
        children: builtins.list[ChildSpec],
        *,
        partition_key: str | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        group_id: str = "default",
        wait: str = "all",
        wait_state: str | None = None,
        success: str | None = None,
        failure: str | None = None,
        from_state: str | None = None,
        on_child_failed: str | None = None,
        on_parent_closed: str | None = None,
        values: dict[str, Any] | None = None,
        value_refs: dict[str, str] | None = None,
        max_active_ms: int | float | str | None = None,
        now_ms: int | None = None,
    ) -> Any:
        args: builtins.list[Any] = [
            "FLOW.SPAWN_CHILDREN",
            parent_flow_id,
            "GROUP",
            group_id,
            "WAIT",
            wait,
            "NOW",
            now_ms if now_ms is not None else _now_ms(),
        ]
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "WAIT_STATE", wait_state)
        _append(args, "SUCCESS", success)
        _append(args, "FAILURE", failure)
        _append(args, "FROM_STATE", from_state)
        _append(args, "ON_CHILD_FAILED", on_child_failed)
        _append(args, "ON_PARENT_CLOSED", on_parent_closed)
        _append(args, "MAX_ACTIVE_MS", normalize_optional_max_active_ms(max_active_ms))
        mixed = any(child.partition_key is not None for child in children)
        has_child_max_active = any(child.max_active_ms is not None for child in children)
        if _has_named_item_values(children) or has_child_max_active:
            args.extend(["ITEMS_EXT_V2" if has_child_max_active else "ITEMS_EXT", len(children)])
            for child in children:
                if mixed and child.partition_key is None:
                    raise ValueError("mixed spawn_children items require partition_key")
                child_values = _merge_named_map(values, child.values)
                child_refs = _merge_named_map(value_refs, child.value_refs)
                args.extend(
                    [
                        child.id,
                        child.partition_key if child.partition_key is not None else "-",
                        child.type,
                        self.codec.encode(child.payload),
                    ]
                )
                if has_child_max_active:
                    child_max_active = normalize_optional_max_active_ms(child.max_active_ms)
                    args.append(child_max_active if child_max_active is not None else "-")
                _append_named_counts(args, self.codec, child_values, child_refs)
        else:
            _append_named_values(args, self.codec, values=values, value_refs=value_refs)
            args.append("ITEMS")
            if mixed:
                args.append("MIXED")
            for child in children:
                if mixed:
                    if child.partition_key is None:
                        raise ValueError("mixed spawn_children items require partition_key")
                    args.extend(
                        [
                            child.id,
                            child.partition_key,
                            child.type,
                            self.codec.encode(child.payload),
                        ]
                    )
                else:
                    args.extend([child.id, child.type, self.codec.encode(child.payload)])
        return await self.executor.execute_command(*args)

    async def install_policy(
        self,
        type: str,
        *,
        retry: RetryPolicy | None = None,
        states: dict[str, FlowStatePolicyLike] | None = None,
        indexed_state_meta: str | None = None,
        max_active_ms: int | float | str | None = None,
    ) -> Any:
        validate_nonempty_string(type, name="type")
        if indexed_state_meta is not None:
            validate_nonempty_string(indexed_state_meta, name="indexed_state_meta")
        args: builtins.list[Any] = ["FLOW.POLICY.SET", type]
        _append(args, "MAX_ACTIVE_MS", normalize_optional_max_active_ms(max_active_ms))
        _append(args, "INDEXED_STATE_META", indexed_state_meta)
        if retry is not None:
            self._append_retry_policy(args, retry)
        for state, policy in (states or {}).items():
            validate_nonempty_string(state, name="state")
            args.extend(["STATE", state])
            self._append_state_policy(args, policy)
        return await self.executor.execute_command(*args)

    async def policy_get(self, type: str, *, state: str | None = None) -> dict[Any, Any]:
        validate_nonempty_string(type, name="type")
        args: builtins.list[Any] = ["FLOW.POLICY.GET", type]
        _append(args, "STATE", state)
        return dict(await self.executor.execute_command(*args) or {})

    async def governance_ledger(
        self,
        id: str,
        *,
        partition_key: str | None = None,
        limit: int | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        rev: bool | None = None,
    ) -> builtins.list[dict[str, Any]]:
        validate_ledger_options(id, limit=limit, from_ms=from_ms, to_ms=to_ms, rev=rev)
        args: builtins.list[Any] = ["FLOW.GOVERNANCE.LEDGER", id]
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        _append(args, "FROM_MS", from_ms)
        _append(args, "TO_MS", to_ms)
        _append_bool(args, "REV", rev)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def approval_request(
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
        validate_approval_request(
            id=id,
            flow_id=flow_id,
            scope=scope,
            policy_hash=policy_hash,
            policy_version=policy_version,
            timeout_ms=timeout_ms,
            expires_at_ms=expires_at_ms,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.APPROVAL.REQUEST", id]
        _append(args, "FLOW_ID", flow_id)
        _append(args, "SCOPE", scope)
        _append(args, "REASON", reason)
        _append(args, "REQUESTED_BY", requested_by)
        _append(
            args,
            "ASSIGNEES",
            list(validate_string_sequence(assignees, name="assignees"))
            if assignees is not None
            else None,
        )
        _append(args, "POLICY_HASH", policy_hash)
        _append(args, "POLICY_VERSION", policy_version)
        _append(args, "TIMEOUT_MS", timeout_ms)
        _append(args, "EXPIRES_AT_MS", expires_at_ms)
        _append(args, "NOW", now_ms)
        return ApprovalResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def approval_approve(
        self,
        id: str,
        *,
        approver: str,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        return await self._approval_status("FLOW.APPROVAL.APPROVE", id, approver, reason, now_ms)

    async def approval_reject(
        self,
        id: str,
        *,
        approver: str,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> ApprovalResult:
        return await self._approval_status("FLOW.APPROVAL.REJECT", id, approver, reason, now_ms)

    async def _approval_status(
        self,
        command: str,
        id: str,
        approver: str,
        reason: str | None,
        now_ms: int | None,
    ) -> ApprovalResult:
        validate_approval_decision(id=id, approver=approver, now_ms=now_ms)
        args: builtins.list[Any] = [command, id]
        _append(args, "APPROVER", approver)
        _append(args, "REASON", reason)
        _append(args, "NOW", now_ms)
        return ApprovalResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def approval_get(self, id: str) -> ApprovalResult | None:
        validate_nonempty_string(id, name="id")
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command("FLOW.APPROVAL.GET", id)),
        )
        return ApprovalResult.from_resp(response) if response is not None else None

    async def approval_list(
        self,
        *,
        status: str | None = None,
        scope: str | None = None,
        partition_key: str | None = None,
        flow_id: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[ApprovalResult]:
        validate_approval_list(
            status=status,
            scope=scope,
            partition_key=partition_key,
            flow_id=flow_id,
            limit=limit,
        )
        args: builtins.list[Any] = ["FLOW.APPROVAL.LIST"]
        _append(args, "STATUS", status)
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "FLOW_ID", flow_id)
        _append(args, "LIMIT", limit)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return [ApprovalResult.from_resp(item) for item in response]

    async def governance_overview(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        status: str | None = None,
        flow_id: str | None = None,
        limit: int | None = None,
    ) -> GovernanceOverview:
        validate_approval_list(
            status=status,
            scope=scope,
            partition_key=partition_key,
            flow_id=flow_id,
            limit=limit,
        )
        args: builtins.list[Any] = ["FLOW.GOVERNANCE.OVERVIEW"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "STATUS", status)
        _append(args, "FLOW_ID", flow_id)
        _append(args, "LIMIT", limit)
        return GovernanceOverview.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def circuit_open(
        self,
        scope: str,
        *,
        open_ms: int | None = None,
        failure_threshold: int | None = None,
        window_ms: int | None = None,
        min_calls: int | None = None,
        failure_rate_pct: int | None = None,
        latency_threshold_ms: int | None = None,
        error_classes: Sequence[str] | None = None,
        half_open_max_probes: int | None = None,
        half_open_success_threshold: int | None = None,
        now_ms: int | None = None,
    ) -> CircuitBreakerStatus:
        validated_error_classes = validate_circuit_open(
            scope,
            open_ms=open_ms,
            failure_threshold=failure_threshold,
            window_ms=window_ms,
            min_calls=min_calls,
            failure_rate_pct=failure_rate_pct,
            latency_threshold_ms=latency_threshold_ms,
            error_classes=error_classes,
            half_open_max_probes=half_open_max_probes,
            half_open_success_threshold=half_open_success_threshold,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.CIRCUIT.OPEN", scope]
        _append(args, "OPEN_MS", open_ms)
        _append(args, "FAILURE_THRESHOLD", failure_threshold)
        _append(args, "WINDOW_MS", window_ms)
        _append(args, "MIN_CALLS", min_calls)
        _append(args, "FAILURE_RATE_PCT", failure_rate_pct)
        _append(args, "LATENCY_THRESHOLD_MS", latency_threshold_ms)
        _append(
            args,
            "ERROR_CLASSES",
            list(validated_error_classes) if validated_error_classes is not None else None,
        )
        _append(args, "HALF_OPEN_MAX_PROBES", half_open_max_probes)
        _append(args, "HALF_OPEN_SUCCESS_THRESHOLD", half_open_success_threshold)
        _append(args, "NOW", now_ms)
        return CircuitBreakerStatus.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def circuit_close(self, scope: str, *, now_ms: int | None = None) -> CircuitBreakerStatus:
        validate_circuit_operation(scope, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.CIRCUIT.CLOSE", scope]
        _append(args, "NOW", now_ms)
        return CircuitBreakerStatus.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def circuit_get(self, scope: str) -> CircuitBreakerStatus | None:
        validate_circuit_operation(scope)
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(
                await self.executor.execute_command("FLOW.CIRCUIT.GET", scope)
            ),
        )
        return CircuitBreakerStatus.from_resp(response) if response is not None else None

    async def budget_reserve(
        self,
        scope: str,
        amount: int,
        *,
        limit: int | None = None,
        window_ms: int | None = None,
        reservation_id: str | None = None,
        now_ms: int | None = None,
    ) -> BudgetResult:
        validate_budget_reserve(
            scope,
            amount,
            limit=limit,
            window_ms=window_ms,
            reservation_id=reservation_id,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.BUDGET.RESERVE", scope]
        _append(args, "AMOUNT", amount)
        _append(args, "LIMIT", limit)
        _append(args, "WINDOW_MS", window_ms)
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def budget_commit(
        self,
        scope: str,
        reservation_id: str,
        actual_amount: int,
        *,
        usage: Mapping[str, Any] | None = None,
        now_ms: int | None = None,
    ) -> BudgetResult:
        validate_budget_settlement(
            scope,
            reservation_id,
            actual_amount=actual_amount,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.BUDGET.COMMIT", scope]
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "ACTUAL_AMOUNT", actual_amount)
        _append(args, "USAGE", dict(usage) if usage is not None else None)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def budget_release(
        self,
        scope: str,
        reservation_id: str,
        *,
        now_ms: int | None = None,
    ) -> BudgetResult:
        validate_budget_settlement(scope, reservation_id, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.BUDGET.RELEASE", scope]
        _append(args, "RESERVATION_ID", reservation_id)
        _append(args, "NOW", now_ms)
        return BudgetResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def budget_get(self, scope: str) -> BudgetResult | None:
        validate_nonempty_string(scope, name="scope")
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(
                await self.executor.execute_command("FLOW.BUDGET.GET", scope)
            ),
        )
        return BudgetResult.from_resp(response) if response is not None else None

    async def budget_list(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
    ) -> builtins.list[BudgetResult]:
        validate_budget_list(scope=scope, partition_key=partition_key, limit=limit)
        args: builtins.list[Any] = ["FLOW.BUDGET.LIST"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        response = cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return [BudgetResult.from_resp(item) for item in response]

    async def limit_lease(
        self,
        scope: str,
        *,
        shard_id: int,
        amount: int,
        ttl_ms: int,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        validate_limit_lease(
            scope,
            shard_id=shard_id,
            amount=amount,
            ttl_ms=ttl_ms,
            limit=limit,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.LIMIT.LEASE", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        _append(args, "LIMIT", limit)
        _append(args, "TTL_MS", ttl_ms)
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_spend(
        self,
        scope: str,
        *,
        shard_id: int,
        amount: int,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        validate_limit_spend(scope, shard_id=shard_id, amount=amount, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.LIMIT.SPEND", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "AMOUNT", amount)
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_release(
        self,
        scope: str,
        *,
        shard_id: int,
        reservation_ids: Sequence[str],
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        """Release credits by their exact server-provided reservation IDs."""

        validated_reservation_ids = validate_limit_release(
            scope,
            shard_id=shard_id,
            reservation_ids=reservation_ids,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.LIMIT.RELEASE", scope]
        _append(args, "SHARD_ID", shard_id)
        _append(args, "RESERVATION_IDS", list(validated_reservation_ids))
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_get(self, scope: str, *, now_ms: int | None = None) -> dict[str, Any] | None:
        validate_limit_get(scope, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.LIMIT.GET", scope]
        _append(args, "NOW", now_ms)
        return cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def limit_list(
        self,
        *,
        scope: str | None = None,
        partition_key: str | None = None,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> builtins.list[dict[str, Any]]:
        validate_limit_list(
            scope=scope,
            partition_key=partition_key,
            limit=limit,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.LIMIT.LIST"]
        _append(args, "SCOPE", scope)
        _append(args, "PARTITION", partition_key)
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return cast(
            builtins.list[dict[str, Any]],
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )

    async def retention_cleanup(
        self,
        *,
        limit: int | None = None,
        now_ms: int | None = None,
    ) -> dict[Any, Any]:
        validate_retention_cleanup(limit=limit, now_ms=now_ms)
        args: builtins.list[Any] = ["FLOW.RETENTION_CLEANUP"]
        _append(args, "LIMIT", limit)
        _append(args, "NOW", now_ms)
        return dict(await self.executor.execute_command(*args) or {})
