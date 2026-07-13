from __future__ import annotations

import ast
import inspect
from pathlib import Path

import ferricstore.client as client_facade
import ferricstore.protocol as protocol_facade

PACKAGE = Path(__file__).resolve().parents[1] / "src" / "ferricstore"


def _module_lines(name: str) -> int:
    return len((PACKAGE / f"{name}.py").read_text().splitlines())


def _top_level_imports(name: str) -> set[str]:
    tree = ast.parse((PACKAGE / f"{name}.py").read_text())
    imports: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.add(node.module)
        elif isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
    return imports


def _assert_acyclic_modules(modules: set[str]) -> None:
    graph = {
        module: {
            dependency.removeprefix("ferricstore.")
            for dependency in _top_level_imports(module)
            if dependency.removeprefix("ferricstore.") in modules
        }
        for module in modules
    }
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(module: str) -> None:
        if module in visiting:
            raise AssertionError(f"cyclic module dependency through {module}")
        if module in visited:
            return
        visiting.add(module)
        for dependency in graph[module]:
            visit(dependency)
        visiting.remove(module)
        visited.add(module)

    for module in modules:
        visit(module)


def test_large_sdk_facades_are_thin_and_implementations_are_bounded() -> None:
    client_components = {
        "client_helpers",
        "client_sessions",
        "client_state",
        "client_support",
        "client_core",
        "client_management",
        "client_producer",
        "client_values",
        "client_claims",
        "client_mutations",
        "client_queries",
        "client_governance",
        "client_autobatch",
    }
    protocol_components = {
        "protocol_constants",
        "protocol_common",
        "protocol_pipeline_codec",
        "protocol_pipelines",
        "protocol_flow_codec",
        "protocol_commands",
        "protocol_responses",
        "protocol_sync",
        "protocol_sync_pool",
        "protocol_sync_topology",
        "protocol_async",
        "protocol_async_pool",
        "protocol_async_topology",
    }

    available = {path.stem for path in PACKAGE.glob("*.py")}
    assert client_components | protocol_components <= available
    assert _module_lines("client") <= 250
    assert _module_lines("protocol") <= 250
    for component in client_components | protocol_components:
        assert _module_lines(component) <= 2_500, component


def test_protocol_dependency_direction_keeps_wire_code_transport_free() -> None:
    forbidden = {
        "ferricstore.protocol_sync",
        "ferricstore.protocol_sync_pool",
        "ferricstore.protocol_sync_topology",
        "ferricstore.protocol_async",
        "ferricstore.protocol_async_pool",
        "ferricstore.protocol_async_topology",
        "ferricstore.protocol",
    }
    for module in (
        "protocol_constants",
        "protocol_common",
        "protocol_pipeline_codec",
        "protocol_flow_codec",
        "protocol_commands",
        "protocol_responses",
    ):
        assert _top_level_imports(module).isdisjoint(forbidden), module


def test_client_components_do_not_depend_on_compatibility_facade() -> None:
    for module in (
        "client_helpers",
        "client_sessions",
        "client_state",
        "client_support",
        "client_core",
        "client_management",
        "client_producer",
        "client_values",
        "client_claims",
        "client_mutations",
        "client_queries",
        "client_governance",
        "client_autobatch",
    ):
        assert "ferricstore.client" not in _top_level_imports(module), module


def test_extracted_component_import_graphs_are_acyclic() -> None:
    _assert_acyclic_modules(
        {
            "client_helpers",
            "client_sessions",
            "client_state",
            "client_support",
            "client_core",
            "client_management",
            "client_producer",
            "client_values",
            "client_claims",
            "client_mutations",
            "client_queries",
            "client_governance",
            "client_autobatch",
        }
    )
    _assert_acyclic_modules(
        {
            "protocol_constants",
            "protocol_common",
            "protocol_pipeline_codec",
            "protocol_pipelines",
            "protocol_flow_codec",
            "protocol_commands",
            "protocol_responses",
            "protocol_sync",
            "protocol_sync_pool",
            "protocol_sync_topology",
            "protocol_async",
            "protocol_async_pool",
            "protocol_async_topology",
        }
    )


def test_facades_preserve_class_and_function_compatibility() -> None:
    from ferricstore.client_autobatch import AutobatchFlowClient, _BatchOp
    from ferricstore.client_core import FlowClient
    from ferricstore.client_helpers import _auto_partition_key_for_id
    from ferricstore.protocol_async import AsyncProtocolAdapter
    from ferricstore.protocol_async_pool import AsyncProtocolAdapterPool
    from ferricstore.protocol_async_topology import AsyncTopologyProtocolAdapterPool
    from ferricstore.protocol_commands import build_protocol_command
    from ferricstore.protocol_constants import ProtocolCommand, ProtocolResponse
    from ferricstore.protocol_pipelines import AsyncProtocolPipeline, ProtocolPipeline
    from ferricstore.protocol_sync import ProtocolAdapter
    from ferricstore.protocol_sync_pool import ProtocolAdapterPool
    from ferricstore.protocol_sync_topology import RoutingTopology, TopologyProtocolAdapterPool

    expected = {
        "FlowClient": FlowClient,
        "AutobatchFlowClient": AutobatchFlowClient,
        "_BatchOp": _BatchOp,
        "_auto_partition_key_for_id": _auto_partition_key_for_id,
    }
    for name, value in expected.items():
        assert getattr(client_facade, name) is value

    expected_protocol = {
        "ProtocolCommand": ProtocolCommand,
        "ProtocolResponse": ProtocolResponse,
        "ProtocolAdapter": ProtocolAdapter,
        "ProtocolPipeline": ProtocolPipeline,
        "ProtocolAdapterPool": ProtocolAdapterPool,
        "TopologyProtocolAdapterPool": TopologyProtocolAdapterPool,
        "RoutingTopology": RoutingTopology,
        "AsyncProtocolAdapter": AsyncProtocolAdapter,
        "AsyncProtocolPipeline": AsyncProtocolPipeline,
        "AsyncProtocolAdapterPool": AsyncProtocolAdapterPool,
        "AsyncTopologyProtocolAdapterPool": AsyncTopologyProtocolAdapterPool,
        "build_protocol_command": build_protocol_command,
    }
    for name, value in expected_protocol.items():
        assert getattr(protocol_facade, name) is value


def test_flow_client_characterization_signatures_survive_decomposition() -> None:
    signatures = {
        "from_url": "(url: 'str', *, codec: 'Codec | None' = None, "
        "backpressure: 'BackpressurePolicy | None' = None, **kwargs: 'Any') -> 'FlowClient'",
        "create": "(self, id: 'str', *, type: 'str', state: 'str' = 'queued', "
        "payload: 'Any' = None, partition_key: 'str | None' = None, "
        "parent_flow_id: 'str | None' = None, root_flow_id: 'str | None' = None, "
        "correlation_id: 'str | None' = None, run_at_ms: 'int | None' = None, "
        "now_ms: 'int | None' = None, priority: 'int | None' = None, "
        "idempotent: 'bool | None' = None, retention_ttl_ms: 'int | None' = None, "
        "attributes: 'dict[str, Any] | None' = None, "
        "state_meta: 'dict[str, Any] | None' = None, values: 'dict[str, Any] | None' = None, "
        "value_refs: 'dict[str, str] | None' = None, return_record: 'bool' = False) "
        "-> 'FlowRecord | bytes'",
        "claim_due": "(self, type: 'str', *, state: 'str | None' = None, "
        "states: 'builtins.list[str] | None' = None, worker: 'str', "
        "partition_key: 'str | None' = None, "
        "partition_keys: 'builtins.list[str] | None' = None, lease_ms: 'int' = 30000, "
        "limit: 'int' = 1, priority: 'int | None' = None, now_ms: 'int | None' = None, "
        "block_ms: 'int | None' = None, reclaim_expired: 'bool | None' = None, "
        "reclaim_ratio: 'int | None' = None, include_record: 'bool | None' = None, "
        "job_only: 'bool | None' = None, payload: 'bool | None' = None, "
        "payload_max_bytes: 'int | None' = None, "
        "values: 'builtins.list[str] | None' = None, value_max_bytes: 'int | None' = None, "
        "include_state: 'bool' = False, include_attributes: 'bool' = True) "
        "-> 'builtins.list[FlowRecord] | builtins.list[ClaimedFlow]'",
    }

    for method, signature in signatures.items():
        assert str(inspect.signature(getattr(client_facade.FlowClient, method))) == signature
