from __future__ import annotations

import ast
import asyncio
from pathlib import Path

import pytest

from ferricstore import AsyncFlowClient, FerricStoreError, FlowClient
from ferricstore.async_workflow_budget import AsyncWorkflowBudget
from ferricstore.types import BudgetResult, ScheduleResult
from ferricstore.workflow_budget import WorkflowBudget

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "ferricstore"


def _class_methods(module: str, class_name: str) -> set[str]:
    tree = ast.parse((PACKAGE / f"{module}.py").read_text())
    class_node = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name
    )
    return {
        node.name
        for node in class_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _line_count(module: str) -> int:
    return len((PACKAGE / f"{module}.py").read_text().splitlines())


def test_schedule_api_has_a_bounded_domain_mixin() -> None:
    schedule_methods = {
        "schedule_create",
        "schedule_get",
        "schedule_fire",
        "schedule_pause",
        "schedule_resume",
        "schedule_delete",
        "schedule_fire_due",
        "schedule_list",
    }
    effect_methods = {
        "effect_reserve",
        "effect_confirm",
        "effect_fail",
        "effect_compensate",
        "effect_get",
        "_effect_status",
    }

    assert _class_methods("client_schedules", "_ClientSchedulesMixin") == schedule_methods
    assert (
        _class_methods("async_client_schedules", "_AsyncClientSchedulesMixin") == schedule_methods
    )
    assert schedule_methods.isdisjoint(
        _class_methods("client_governance", "_ClientGovernanceMixin")
    )
    assert schedule_methods.isdisjoint(
        _class_methods("async_client_governance", "_AsyncClientGovernanceMixin")
    )
    assert _class_methods("client_effects", "_ClientEffectsMixin") == effect_methods
    assert _class_methods("async_client_effects", "_AsyncClientEffectsMixin") == effect_methods
    assert effect_methods.isdisjoint(_class_methods("client_governance", "_ClientGovernanceMixin"))
    assert effect_methods.isdisjoint(
        _class_methods("async_client_governance", "_AsyncClientGovernanceMixin")
    )
    assert _line_count("client_schedules") <= 300
    assert _line_count("async_client_schedules") <= 300
    assert _line_count("client_effects") <= 300
    assert _line_count("async_client_effects") <= 300
    assert _line_count("client_governance") <= 550
    assert _line_count("async_client_governance") <= 650


def test_async_topology_endpoint_lifecycle_has_a_bounded_mixin() -> None:
    endpoint_methods = {
        "_control_adapter",
        "_adapter_for_url",
        "_adapter_for_endpoint",
        "_leased_adapter_for_endpoint",
        "_release_adapter_lease",
        "_install_topology",
        "_retired_adapter_became_idle",
        "_new_endpoint_adapter",
        "_register_adapter",
        "_schedule_adapter_cleanup",
        "_schedule_retained_adapter_cleanup",
        "_safe_warm_endpoint",
        "_safe_warm_connection",
        "_validate_endpoint",
        "_refresh_candidate_urls",
    }

    assert (
        _class_methods("protocol_async_endpoints", "AsyncTopologyEndpointMixin") == endpoint_methods
    )
    assert endpoint_methods.isdisjoint(
        _class_methods("protocol_async_topology", "AsyncTopologyProtocolAdapterPool")
    )
    assert _line_count("protocol_async_endpoints") <= 300
    assert _line_count("protocol_async_topology") <= 750


def test_async_worker_completion_has_a_bounded_domain_mixin() -> None:
    completion_methods = {
        "_finish_batch",
        "_complete_successes",
        "_handle_failures",
    }

    assert (
        _class_methods("async_worker_completion", "AsyncWorkerCompletionMixin")
        == completion_methods
    )
    assert completion_methods.isdisjoint(
        _class_methods("async_queue_runtime", "AsyncQueueFlowWorker")
    )
    assert _line_count("async_worker_completion") <= 250
    assert _line_count("async_queue_runtime") <= 820


def test_async_schedule_delete_normalizes_ok_like_sync_client() -> None:
    class Executor:
        async def execute_command(self, *args: object) -> str:
            assert args == ("FLOW.SCHEDULE.DELETE", "daily", "NOW", 200)
            return "OK"

    async def exercise() -> ScheduleResult:
        return await AsyncFlowClient(Executor()).schedule_delete("daily", now_ms=200)  # type: ignore[arg-type]

    result = asyncio.run(exercise())

    assert result.id == "daily"
    assert result.status == "deleted"


def test_workflow_budget_helpers_cannot_overwrite_an_open_reservation() -> None:
    class SyncBudgetClient:
        def __init__(self) -> None:
            self.reserve_calls = 0

        def budget_reserve(self, *_args: object, **_kwargs: object) -> BudgetResult:
            self.reserve_calls += 1
            return BudgetResult(reservation_id=f"sync-{self.reserve_calls}")

        def budget_release(self, *_args: object, **_kwargs: object) -> BudgetResult:
            return BudgetResult(status="released")

    class AsyncBudgetClient:
        def __init__(self) -> None:
            self.reserve_calls = 0

        async def budget_reserve(self, *_args: object, **_kwargs: object) -> BudgetResult:
            self.reserve_calls += 1
            return BudgetResult(reservation_id=f"async-{self.reserve_calls}")

        async def budget_release(self, *_args: object, **_kwargs: object) -> BudgetResult:
            return BudgetResult(status="released")

    class Context:
        def __init__(self, client: object) -> None:
            self.client = client

        def _record_budget_result(self, *_args: object) -> None:
            pass

    sync_client = SyncBudgetClient()
    sync_budget = WorkflowBudget(Context(sync_client), scope="tenant", amount=1)  # type: ignore[arg-type]
    sync_budget.__enter__()
    with pytest.raises(FerricStoreError, match="entered more than once"):
        sync_budget.__enter__()
    sync_budget.release()
    assert sync_client.reserve_calls == 1

    async def exercise_async() -> None:
        async_client = AsyncBudgetClient()
        async_budget = AsyncWorkflowBudget(  # type: ignore[arg-type]
            Context(async_client), scope="tenant", amount=1
        )
        await async_budget.__aenter__()
        with pytest.raises(FerricStoreError, match="entered more than once"):
            await async_budget.__aenter__()
        await async_budget.release()
        assert async_client.reserve_calls == 1

    asyncio.run(exercise_async())


def test_unopened_workflow_budget_errors_have_sync_async_parity() -> None:
    class Context:
        client = object()

    sync_budget = WorkflowBudget(Context(), scope="tenant", amount=1)  # type: ignore[arg-type]
    with pytest.raises(FerricStoreError, match="has not been opened"):
        _ = sync_budget.reservation_id

    async_budget = AsyncWorkflowBudget(Context(), scope="tenant", amount=1)  # type: ignore[arg-type]
    with pytest.raises(FerricStoreError, match="has not been opened"):
        _ = async_budget.reservation_id


INVALID_GOVERNANCE_CALLS = (
    ("install_policy", ("",), {}, "type"),
    ("policy_get", ("",), {}, "type"),
    (
        "effect_reserve",
        ("", "email", "email.send"),
        {"lease_token": b"lease", "fencing_token": 1, "operation_digest": "digest"},
        "id",
    ),
    (
        "effect_reserve",
        ("flow", "email", "email.send"),
        {"lease_token": None, "fencing_token": 1, "operation_digest": "digest"},
        "lease_token",
    ),
    (
        "effect_reserve",
        ("flow", "email", "email.send"),
        {"lease_token": b"lease", "fencing_token": 1, "operation_digest": ""},
        "operation_digest",
    ),
    (
        "effect_confirm",
        ("flow", "email"),
        {},
        "lease_token",
    ),
    (
        "effect_confirm",
        ("flow", "email"),
        {"lease_token": b"lease", "fencing_token": 1, "latency_ms": -1},
        "latency_ms",
    ),
    (
        "effect_fail",
        ("flow", "email"),
        {"lease_token": b"lease", "fencing_token": None},
        "fencing_token",
    ),
    ("effect_get", ("", "email"), {}, "id"),
    ("governance_ledger", ("",), {}, "id"),
    ("governance_ledger", ("flow",), {"rev": 1}, "rev"),
    ("retention_cleanup", (), {"limit": 0}, "limit"),
    ("retention_cleanup", (), {"now_ms": -1}, "now_ms"),
)


class _SyncExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def execute_command(self, *args: object) -> dict[object, object]:
        self.calls.append(args)
        return {}


class _AsyncExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    async def execute_command(self, *args: object) -> dict[object, object]:
        self.calls.append(args)
        return {}


def test_schedule_fire_exposes_kv_logical_occurrence_time() -> None:
    sync_executor = _SyncExecutor()
    FlowClient(sync_executor).schedule_fire(  # type: ignore[arg-type]
        "daily", fire_at_ms=125, now_ms=200
    )
    assert sync_executor.calls == [("FLOW.SCHEDULE.FIRE", "daily", "FIRE_AT_MS", 125, "NOW", 200)]

    async def exercise_async() -> None:
        async_executor = _AsyncExecutor()
        await AsyncFlowClient(async_executor).schedule_fire(  # type: ignore[arg-type]
            "daily", fire_at_ms=125, now_ms=200
        )
        assert async_executor.calls == [
            ("FLOW.SCHEDULE.FIRE", "daily", "FIRE_AT_MS", 125, "NOW", 200)
        ]

        invalid_executor = _AsyncExecutor()
        with pytest.raises(ValueError, match="fire_at_ms"):
            await AsyncFlowClient(invalid_executor).schedule_fire(  # type: ignore[arg-type]
                "daily", fire_at_ms=-1
            )
        assert invalid_executor.calls == []

    asyncio.run(exercise_async())

    invalid_executor = _SyncExecutor()
    with pytest.raises(ValueError, match="fire_at_ms"):
        FlowClient(invalid_executor).schedule_fire(  # type: ignore[arg-type]
            "daily", fire_at_ms=-1
        )
    assert invalid_executor.calls == []


def test_schedule_fire_due_exposes_kv_lease_duration() -> None:
    sync_executor = _SyncExecutor()
    FlowClient(sync_executor).schedule_fire_due(  # type: ignore[arg-type]
        lease_ms=45_000, limit=2
    )
    assert sync_executor.calls == [("FLOW.SCHEDULE.FIRE_DUE", "LEASE_MS", 45_000, "LIMIT", 2)]

    async def exercise_async() -> None:
        async_executor = _AsyncExecutor()
        await AsyncFlowClient(async_executor).schedule_fire_due(  # type: ignore[arg-type]
            lease_ms=45_000, limit=2
        )
        assert async_executor.calls == [("FLOW.SCHEDULE.FIRE_DUE", "LEASE_MS", 45_000, "LIMIT", 2)]

        invalid_executor = _AsyncExecutor()
        with pytest.raises(ValueError, match="lease_ms"):
            await AsyncFlowClient(invalid_executor).schedule_fire_due(  # type: ignore[arg-type]
                lease_ms=0
            )
        assert invalid_executor.calls == []

    asyncio.run(exercise_async())


@pytest.mark.parametrize(("method", "args", "kwargs", "message"), INVALID_GOVERNANCE_CALLS)
def test_sync_governance_rejects_kv_invalid_values_before_io(
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    executor = _SyncExecutor()
    client = FlowClient(executor)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=message):
        getattr(client, method)(*args, **kwargs)

    assert executor.calls == []


@pytest.mark.parametrize(("method", "args", "kwargs", "message"), INVALID_GOVERNANCE_CALLS)
def test_async_governance_rejects_kv_invalid_values_before_io(
    method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    async def exercise() -> None:
        executor = _AsyncExecutor()
        client = AsyncFlowClient(executor)  # type: ignore[arg-type]

        with pytest.raises(ValueError, match=message):
            await getattr(client, method)(*args, **kwargs)

        assert executor.calls == []

    asyncio.run(exercise())
