from __future__ import annotations

import asyncio
import inspect
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import ferricstore.async_client_queries as async_client_queries
import ferricstore.client_queries as client_queries
from ferricstore import AsyncFlowClient, FlowClient
from ferricstore.async_client_queries import _AsyncClientQueriesMixin
from ferricstore.async_workflow_context import AsyncWorkflowFlowCommands
from ferricstore.client_queries import _ClientQueriesMixin
from ferricstore.workflow_models import WorkflowFlowCommands
from ferricstore.workflow_runtime import Workflow

_LINEAGE_OPTIONS = (
    "partition_key",
    "state",
    "count",
    "from_ms",
    "to_ms",
    "rev",
    "attributes",
    "terminal_only",
    "include_cold",
    "consistent_projection",
)
_LINEAGE_METHODS = {
    "by_parent": "parent_flow_id",
    "by_root": "root_flow_id",
    "by_correlation": "correlation_id",
}


@pytest.mark.parametrize(
    "owner",
    (FlowClient, AsyncFlowClient, Workflow, WorkflowFlowCommands, AsyncWorkflowFlowCommands),
)
@pytest.mark.parametrize("method,identifier", _LINEAGE_METHODS.items())
def test_public_lineage_helpers_have_explicit_typed_keyword_options(
    owner: type[Any], method: str, identifier: str
) -> None:
    parameters = list(inspect.signature(getattr(owner, method)).parameters.values())

    assert [parameter.name for parameter in parameters] == [
        "self",
        identifier,
        *_LINEAGE_OPTIONS,
    ]
    assert parameters[1].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(parameter.kind is inspect.Parameter.KEYWORD_ONLY for parameter in parameters[2:])
    if owner in {WorkflowFlowCommands, AsyncWorkflowFlowCommands}:
        assert parameters[2].default is None
    else:
        assert parameters[2].default is inspect.Parameter.empty
    assert all(parameter.default is None for parameter in parameters[3:])


def test_sync_and_async_client_lineage_signatures_have_exact_parity() -> None:
    for method in _LINEAGE_METHODS:
        sync = list(inspect.signature(getattr(FlowClient, method)).parameters.values())[1:]
        async_ = list(inspect.signature(getattr(AsyncFlowClient, method)).parameters.values())[1:]
        assert [
            (parameter.name, parameter.kind, parameter.default, parameter.annotation)
            for parameter in async_
        ] == [
            (parameter.name, parameter.kind, parameter.default, parameter.annotation)
            for parameter in sync
        ]


def test_sync_lineage_helpers_forward_every_supported_option(monkeypatch) -> None:
    calls: list[tuple[str, str, dict[str, Any]]] = []

    def build(selector: str, identifier: str, **options: Any):
        calls.append((selector, identifier, options))
        return "query", {"partition_key": options["partition_key"]}

    class Subject:
        def _execute_flow_record_query(self, query: str, params: dict[str, Any]):
            assert (query, params) == ("query", {"partition_key": b"tenant"})
            return ["record"]

    options = {
        "partition_key": b"tenant",
        "state": "queued",
        "count": 7,
        "from_ms": 10,
        "to_ms": 20,
        "rev": True,
        "attributes": {"tenant": "acme"},
        "terminal_only": False,
        "include_cold": True,
        "consistent_projection": True,
    }
    monkeypatch.setattr(client_queries, "build_flow_lineage_query", build)
    subject = Subject()

    for method, selector in (
        (_ClientQueriesMixin.by_parent, "parent_flow_id"),
        (_ClientQueriesMixin.by_root, "root_flow_id"),
        (_ClientQueriesMixin.by_correlation, "correlation_id"),
    ):
        assert method(subject, "lineage", **options) == ["record"]  # type: ignore[arg-type]
        assert calls[-1] == (selector, "lineage", options)

    with pytest.raises(TypeError, match="unexpected keyword argument"):
        _ClientQueriesMixin.by_parent(  # type: ignore[call-arg]
            subject, "lineage", partition_key="tenant", unknown_option=True
        )


def test_async_lineage_helpers_forward_every_supported_option(monkeypatch) -> None:
    async def run() -> None:
        calls: list[tuple[str, str, dict[str, Any]]] = []

        def build(selector: str, identifier: str, **options: Any):
            calls.append((selector, identifier, options))
            return "query", {"partition_key": options["partition_key"]}

        class Subject:
            async def _execute_flow_record_query(self, query: str, params: dict[str, Any]):
                assert (query, params) == ("query", {"partition_key": b"tenant"})
                return ["record"]

        options = {
            "partition_key": b"tenant",
            "state": "queued",
            "count": 7,
            "from_ms": 10,
            "to_ms": 20,
            "rev": True,
            "attributes": {"tenant": "acme"},
            "terminal_only": False,
            "include_cold": True,
            "consistent_projection": True,
        }
        monkeypatch.setattr(async_client_queries, "build_flow_lineage_query", build)
        subject = Subject()

        for method, selector in (
            (_AsyncClientQueriesMixin.by_parent, "parent_flow_id"),
            (_AsyncClientQueriesMixin.by_root, "root_flow_id"),
            (_AsyncClientQueriesMixin.by_correlation, "correlation_id"),
        ):
            assert await method(subject, "lineage", **options) == [  # type: ignore[arg-type]
                "record"
            ]
            assert calls[-1] == (selector, "lineage", options)

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            await _AsyncClientQueriesMixin.by_parent(  # type: ignore[call-arg]
                subject, "lineage", partition_key="tenant", unknown_option=True
            )

    asyncio.run(run())


def test_lineage_mypy_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--no-incremental",
            str(root / "tests" / "typing" / "lineage_api_contract.py"),
        ],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
