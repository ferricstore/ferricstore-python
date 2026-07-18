from __future__ import annotations

import ast
import inspect
import runpy
import subprocess
import sys
from pathlib import Path

import pytest

import ferricstore as package_facade
import ferricstore.async_client as async_client_facade
import ferricstore.async_worker as async_worker_facade
import ferricstore.client as client_facade
import ferricstore.protocol as protocol_facade
import ferricstore.workflow as workflow_facade

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


def _class_node(module: str, class_name: str) -> ast.ClassDef:
    tree = ast.parse((PACKAGE / f"{module}.py").read_text())
    return next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == class_name
    )


def _class_lines(module: str, class_name: str) -> int:
    node = _class_node(module, class_name)
    assert node.end_lineno is not None
    return node.end_lineno - node.lineno + 1


def _function_metrics(module: str, function_name: str) -> tuple[int, int]:
    tree = ast.parse((PACKAGE / f"{module}.py").read_text())
    function = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name
    )
    assert function.end_lineno is not None
    branches = sum(
        isinstance(
            node,
            (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.Match, ast.BoolOp),
        )
        for node in ast.walk(function)
    )
    return function.end_lineno - function.lineno + 1, branches


def _method_metrics(module: str, class_name: str, method_name: str) -> tuple[int, int]:
    method = next(
        node
        for node in _class_node(module, class_name).body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == method_name
    )
    assert method.end_lineno is not None
    branches = sum(
        isinstance(
            node,
            (ast.If, ast.For, ast.AsyncFor, ast.While, ast.Try, ast.Match, ast.BoolOp),
        )
        for node in ast.walk(method)
    )
    return method.end_lineno - method.lineno + 1, branches


def test_stateful_runtime_classes_have_bounded_responsibilities() -> None:
    limits = {
        ("protocol_sync", "ProtocolAdapter"): 800,
        ("protocol_sync_transport", "SyncProtocolTransportMixin"): 700,
        ("protocol_async", "AsyncProtocolAdapter"): 700,
        ("protocol_sync_topology", "TopologyProtocolAdapterPool"): 800,
        ("protocol_async_topology", "AsyncTopologyProtocolAdapterPool"): 850,
        ("worker", "QueueFlowWorker"): 800,
        ("async_queue_runtime", "AsyncQueueFlowWorker"): 750,
        ("workflow_runtime", "Workflow"): 800,
        ("async_workflow_runtime", "AsyncWorkflow"): 800,
        ("client_autobatch", "AutobatchFlowClient"): 850,
    }

    oversized = {
        f"{module}.{class_name}": (_class_lines(module, class_name), limit)
        for (module, class_name), limit in limits.items()
        if _class_lines(module, class_name) > limit
    }
    assert not oversized


def test_worker_configuration_is_a_dedicated_acyclic_boundary() -> None:
    """Keep runtime knobs out of the already broad wire-model module."""

    from ferricstore import ExceptionPolicy, ValueConfig, WorkerConfig
    from ferricstore.types import (
        ExceptionPolicy as CompatibilityExceptionPolicy,
    )
    from ferricstore.types import (
        ValueConfig as CompatibilityValueConfig,
    )
    from ferricstore.types import (
        WorkerConfig as CompatibilityWorkerConfig,
    )
    from ferricstore.worker_config import (
        ExceptionPolicy as CanonicalExceptionPolicy,
    )
    from ferricstore.worker_config import (
        ValueConfig as CanonicalValueConfig,
    )
    from ferricstore.worker_config import (
        WorkerConfig as CanonicalWorkerConfig,
    )

    assert _module_lines("types") <= 900
    assert _module_lines("worker_config") <= 300
    _assert_acyclic_modules({"config_validation", "types", "worker_config"})
    assert ExceptionPolicy is CompatibilityExceptionPolicy is CanonicalExceptionPolicy
    assert ValueConfig is CompatibilityValueConfig is CanonicalValueConfig
    assert WorkerConfig is CompatibilityWorkerConfig is CanonicalWorkerConfig


def test_result_model_parsing_is_a_dedicated_acyclic_boundary() -> None:
    """Keep shared parsing and scheduler models out of the broad type facade."""

    from ferricstore import ScheduleResult as PublicScheduleResult
    from ferricstore.schedule_types import ScheduleResult as CanonicalScheduleResult
    from ferricstore.types import ScheduleResult as CompatibilityScheduleResult

    assert _module_lines("model_core") <= 150
    assert _module_lines("schedule_types") <= 150
    _assert_acyclic_modules({"model_core", "schedule_types", "types"})
    assert PublicScheduleResult is CompatibilityScheduleResult is CanonicalScheduleResult


def test_autobatch_queue_state_is_a_dedicated_acyclic_boundary() -> None:
    """Keep queue accounting separate from the already broad batch API."""

    assert _module_lines("client_autobatch") <= 950
    assert _module_lines("client_autobatch_dispatch") <= 100
    assert _module_lines("client_autobatch_queue") <= 100
    _assert_acyclic_modules(
        {"client_autobatch", "client_autobatch_dispatch", "client_autobatch_queue"}
    )


def test_create_many_wire_building_has_one_shared_boundary() -> None:
    """Keep sync and async producer command construction from drifting apart."""

    assert (PACKAGE / "producer_commands.py").is_file()
    assert _module_lines("producer_commands") <= 150
    for module in ("client_producer", "async_client_producer"):
        assert "ferricstore.producer_commands" in _top_level_imports(module)
        tree = ast.parse((PACKAGE / f"{module}.py").read_text())
        create_many_literals = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and node.value == "FLOW.CREATE_MANY"
        ]
        assert create_many_literals == []
    _assert_acyclic_modules(
        {"client_helpers", "producer_commands", "client_producer", "async_client_producer"}
    )


def test_workflow_mutation_planning_has_one_shared_boundary() -> None:
    """Keep sync and async workflow outcome translation from drifting apart."""

    assert (PACKAGE / "workflow_mutations.py").is_file()
    assert _module_lines("workflow_mutations") <= 250
    assert _module_lines("workflow_producer") <= 200
    assert _module_lines("async_workflow_producer") <= 200
    assert _module_lines("workflow_runtime") <= 900
    assert _module_lines("async_workflow_runtime") <= 850
    for module in ("workflow_runtime", "async_workflow_runtime"):
        assert "ferricstore.workflow_mutations" in _top_level_imports(module)
    assert "ferricstore.workflow_producer" in _top_level_imports("workflow_runtime")
    assert "ferricstore.async_workflow_producer" in _top_level_imports("async_workflow_runtime")
    _assert_acyclic_modules(
        {
            "mutation_core",
            "workflow_mutations",
            "workflow_execution",
            "async_workflow_execution",
            "workflow_producer",
            "async_workflow_producer",
            "workflow_runtime",
            "async_workflow_runtime",
        }
    )


def test_raw_pipeline_encoding_is_a_dedicated_acyclic_boundary() -> None:
    """Keep raw command parsing separate from prepared-command encoding."""

    assert (PACKAGE / "protocol_pipeline_raw.py").is_file()
    assert _module_lines("protocol_pipeline_codec") <= 850
    assert _module_lines("protocol_pipeline_raw") <= 250
    _assert_acyclic_modules(
        {"protocol_pipeline_codec", "protocol_pipeline_mutations", "protocol_pipeline_raw"}
    )


def test_sync_topology_endpoint_lifecycle_is_a_dedicated_boundary() -> None:
    """Keep endpoint ownership symmetric with the async topology implementation."""

    assert (PACKAGE / "protocol_sync_endpoints.py").is_file()
    assert (PACKAGE / "protocol_sync_topology_mset.py").is_file()
    assert _module_lines("protocol_sync_topology") <= 850
    assert _module_lines("protocol_sync_endpoints") <= 250
    assert _module_lines("protocol_sync_topology_mset") <= 120
    _assert_acyclic_modules(
        {
            "protocol_sync_endpoints",
            "protocol_sync_routing",
            "protocol_sync_topology",
            "protocol_sync_topology_mset",
        }
    )


def test_command_and_async_queue_modules_have_cohesive_responsibilities() -> None:
    limits = {
        "commands": 900,
        "async_commands": 900,
        "command_helpers": 180,
        "protocol_commands": 250,
        "protocol_basic_commands": 500,
        "protocol_flow_commands": 600,
        "protocol_flow_payloads": 600,
        "protocol_command_options": 300,
        "protocol_mset": 120,
        "protocol_compact_commands": 1_100,
        "async_queue_runtime": 1_100,
        "async_worker_completion": 250,
        "async_queue_producer": 200,
        "async_queue_api": 850,
    }

    oversized = {
        module: (_module_lines(module), limit)
        for module, limit in limits.items()
        if _module_lines(module) > limit
    }
    assert not oversized
    _assert_acyclic_modules(set(limits))


def test_data_command_surface_has_one_authoritative_source() -> None:
    commands_tree = ast.parse((PACKAGE / "commands.py").read_text())
    command_classes = {node.name for node in commands_tree.body if isinstance(node, ast.ClassDef)}

    assert "DataCommandsMixin" in command_classes
    assert "AsyncDataCommandsMixin" not in command_classes
    assert _module_lines("commands") <= 900
    assert (PACKAGE / "async_commands.py").is_file()

    repository = PACKAGE.parents[1]
    subprocess.run(
        [sys.executable, "tools/generate_async_commands.py", "--check"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )


def test_async_command_generator_uses_syntax_aware_transformation() -> None:
    repository = PACKAGE.parents[1]
    generator = runpy.run_path(str(repository / "tools" / "generate_async_commands.py"))
    source = '''
class DataCommandsMixin:
    """Text containing self.command( must remain text."""

    def get(self, key: str) -> object:
        marker = "self.command( is not executable"
        return self.command("GET", key), marker

    def pairs(self, values: list[tuple[str, object]]) -> dict[str, object]:
        return {key: value for key, value in values}
'''

    generated = generator["_async_class_source"](source)
    tree = ast.parse(generated)
    generated_class = tree.body[0]

    assert isinstance(generated_class, ast.ClassDef)
    assert generated_class.name == "AsyncDataCommandsMixin"
    method = next(node for node in generated_class.body if isinstance(node, ast.AsyncFunctionDef))
    awaits = [node for node in ast.walk(method) if isinstance(node, ast.Await)]
    strings = [node.value for node in ast.walk(method) if isinstance(node, ast.Constant)]
    assert len(awaits) == 1
    assert "self.command( is not executable" in strings
    assert "for key, value in values" in generated


def test_worker_constructors_delegate_configuration_and_ownership() -> None:
    limits = {
        ("worker", "QueueFlowWorker"): (150, 28),
        ("async_queue_runtime", "AsyncQueueFlowWorker"): (130, 22),
    }
    oversized = {
        f"{module}.{class_name}.__init__": (
            _method_metrics(module, class_name, "__init__"),
            limit,
        )
        for (module, class_name), limit in limits.items()
        if any(
            actual > maximum
            for actual, maximum in zip(
                _method_metrics(module, class_name, "__init__"), limit, strict=True
            )
        )
    }

    assert not oversized


def test_protocol_state_uses_shared_validated_runtime_configuration() -> None:
    for module in ("protocol_sync_state", "protocol_async_state"):
        assert "ferricstore.protocol_config" in _top_level_imports(module)
        constructor = next(
            node
            for node in _class_node(
                module,
                "_SyncProtocolStateMixin"
                if module == "protocol_sync_state"
                else "_AsyncProtocolStateMixin",
            ).body
            if isinstance(node, ast.FunctionDef) and node.name == "__init__"
        )
        calls = {
            ast.unparse(node.func) for node in ast.walk(constructor) if isinstance(node, ast.Call)
        }
        assert "ProtocolRuntimeConfig.build" in calls


def test_async_client_core_does_not_shadow_generated_data_commands() -> None:
    sync_methods = {
        node.name
        for node in _class_node("commands", "DataCommandsMixin").body
        if isinstance(node, ast.FunctionDef)
    }
    core_methods = {
        node.name
        for node in _class_node("async_client_core", "_AsyncClientCoreMixin").body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert sync_methods & core_methods == {"command"}


def test_worker_execution_uses_small_typed_host_protocols() -> None:
    tree = ast.parse((PACKAGE / "worker_execution.py").read_text())
    protocols = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.ClassDef)
        and any(ast.unparse(base).endswith("Protocol") for base in node.bases)
    }

    assert "QueueWorkerRuntimeHost" not in protocols
    expected = {
        "QueueWorkerRunLifecycleHost": 10,
        "QueueWorkerResourceOwnerHost": 16,
        "QueueWorkerClaimPlanHost": 5,
    }
    assert expected.keys() <= protocols.keys()
    for name, field_limit in expected.items():
        fields = [node for node in protocols[name].body if isinstance(node, ast.AnnAssign)]
        assert len(fields) <= field_limit, name
        assert all(ast.unparse(field.annotation) != "Any" for field in fields), name


def test_sync_protocol_adapter_composes_batch_behavior_directly() -> None:
    from ferricstore.protocol_sync import ProtocolAdapter
    from ferricstore.protocol_sync_batch import SyncProtocolBatchMixin

    assert SyncProtocolBatchMixin in ProtocolAdapter.__mro__


def test_sync_protocol_adapter_aggregate_runtime_surface_is_bounded() -> None:
    from ferricstore.protocol_sync import ProtocolAdapter

    def runtime_class_lines(runtime_class: type[object]) -> int:
        method = next(
            (
                value
                for value in vars(runtime_class).values()
                if inspect.isfunction(value) and inspect.getsourcefile(value) is not None
            ),
            None,
        )
        if method is None:
            return 0
        source = Path(inspect.getsourcefile(method) or "")
        tree = ast.parse(source.read_text())
        node = next(
            item
            for item in tree.body
            if isinstance(item, ast.ClassDef) and item.name == runtime_class.__name__
        )
        assert node.end_lineno is not None
        return node.end_lineno - node.lineno + 1

    implementation_lines = sum(
        runtime_class_lines(base)
        for base in ProtocolAdapter.__mro__
        if base.__module__.startswith("ferricstore.")
    )
    # Pending-request bookkeeping is composed behind a registry rather than
    # expanding the transport adapter's inheritance surface.
    assert implementation_lines <= 2_250


def test_command_dispatch_hotspots_have_bounded_complexity() -> None:
    limits = {
        ("protocol_compact_commands", "_compact_pipeline_payload_from_raw"): (120, 30),
        ("protocol_basic_commands", "_build_basic_protocol_command"): (120, 30),
        ("protocol_flow_commands", "_build_native_flow_protocol_command"): (120, 30),
        ("protocol_pipeline_codec", "_compact_pipeline_payload"): (120, 30),
    }
    oversized = {
        f"{module}.{function}": (_function_metrics(module, function), limit)
        for (module, function), limit in limits.items()
        if any(
            actual > maximum
            for actual, maximum in zip(_function_metrics(module, function), limit, strict=True)
        )
    }
    assert not oversized


def test_response_and_async_shutdown_dispatchers_have_bounded_complexity() -> None:
    limits = {
        ("protocol_responses", "_try_fast_response_value_at"): (35, 6),
    }
    method_limits = {
        ("async_queue_api", "AsyncQueueFlow", "_close_in_phases"): (45, 10),
        ("async_workflow_runtime", "AsyncWorkflow", "_close_in_phases"): (45, 10),
    }
    oversized = {
        f"{module}.{function}": (_function_metrics(module, function), limit)
        for (module, function), limit in limits.items()
        if any(
            actual > maximum
            for actual, maximum in zip(_function_metrics(module, function), limit, strict=True)
        )
    }
    oversized.update(
        {
            f"{module}.{class_name}.{method}": (
                _method_metrics(module, class_name, method),
                limit,
            )
            for (module, class_name, method), limit in method_limits.items()
            if any(
                actual > maximum
                for actual, maximum in zip(
                    _method_metrics(module, class_name, method), limit, strict=True
                )
            )
        }
    )

    assert not oversized


def test_response_contracts_and_owned_async_cleanup_are_shared_components() -> None:
    assert (PACKAGE / "protocol_response_contracts.py").is_file()
    assert "ferricstore.protocol_response_contracts" in _top_level_imports("protocol_responses")

    for module in ("async_queue_api", "async_workflow_runtime"):
        tree = ast.parse((PACKAGE / f"{module}.py").read_text())
        calls = {ast.unparse(node.func) for node in ast.walk(tree) if isinstance(node, ast.Call)}
        assert "close_owned_resources_async" in calls, module


def test_numeric_configuration_validation_has_one_authoritative_module() -> None:
    forbidden_definitions = {
        "validated_optional_nonnegative_int",
        "validated_nonnegative_int",
        "validated_optional_positive_int",
        "validated_pending_limit",
    }
    for module in ("protocol_framing", "protocol_lifecycle", "protocol_codec"):
        tree = ast.parse((PACKAGE / f"{module}.py").read_text())
        definitions = {
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert definitions.isdisjoint(forbidden_definitions), module
        assert "ferricstore.config_validation" in _top_level_imports(module), module


def test_basic_protocol_dispatch_uses_one_command_registry() -> None:
    function = next(
        node
        for node in ast.parse((PACKAGE / "protocol_commands.py").read_text()).body
        if isinstance(node, ast.FunctionDef) and node.name == "build_protocol_command"
    )

    assert not any(isinstance(node, ast.Set) for node in ast.walk(function))


def test_sync_batch_composition_has_no_transport_host_proxy() -> None:
    tree = ast.parse((PACKAGE / "protocol_sync_batch.py").read_text())
    class_names = {node.name for node in tree.body if isinstance(node, ast.ClassDef)}

    assert class_names == {"SyncProtocolBatchHost", "SyncProtocolBatchMixin"}
    host = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "SyncProtocolBatchHost"
    )
    assert any(ast.unparse(base).endswith("Protocol") for base in host.bases)


def test_compatibility_facades_do_not_rewrite_canonical_class_metadata() -> None:
    canonical_classes = {
        "ferricstore.queue_api": ["Queue", "QueueClient"],
        "ferricstore.client_core": ["FlowClient"],
        "ferricstore.protocol_sync": ["ProtocolAdapter"],
        "ferricstore.protocol_async": ["AsyncProtocolAdapter"],
        "ferricstore.workflow_client": ["WorkflowClient"],
        "ferricstore.workflow_budget": ["WorkflowBudget"],
        "ferricstore.workflow_runtime": ["Workflow"],
        "ferricstore.async_workflow_budget": ["AsyncWorkflowBudget"],
        "ferricstore.async_workflow_client": ["AsyncWorkflowClient"],
        "ferricstore.async_workflow_runtime": ["AsyncWorkflow"],
    }

    for module_name, class_names in canonical_classes.items():
        module = __import__(module_name, fromlist=class_names)
        for class_name in class_names:
            assert getattr(module, class_name).__module__ == module_name


def test_extracted_api_classes_have_canonical_facade_routes() -> None:
    expected_root_routes = {
        "Queue": ("ferricstore.queue_api", "Queue"),
        "QueueClient": ("ferricstore.queue_api", "QueueClient"),
        "WorkflowClient": ("ferricstore.workflow_client", "WorkflowClient"),
        "WorkflowBudget": ("ferricstore.workflow_budget", "WorkflowBudget"),
        "AsyncWorkflowBudget": (
            "ferricstore.async_workflow_budget",
            "AsyncWorkflowBudget",
        ),
        "AsyncWorkflowClient": (
            "ferricstore.async_workflow_client",
            "AsyncWorkflowClient",
        ),
    }
    for name, route in expected_root_routes.items():
        assert package_facade._EXPORTS[name] == route

    assert workflow_facade._EXPORTS["WorkflowClient"] == expected_root_routes["WorkflowClient"]
    assert workflow_facade._EXPORTS["WorkflowBudget"] == expected_root_routes["WorkflowBudget"]
    assert (
        async_worker_facade._EXPORTS["AsyncWorkflowClient"]
        == expected_root_routes["AsyncWorkflowClient"]
    )
    reserved_partition_exports = {
        name
        for name in async_worker_facade._EXPORTS
        if "auto_partition" in name.lower() or name == "AUTO_PARTITION_PREFIX"
    }
    assert not reserved_partition_exports

    assert "ferricstore.queue_api" not in _top_level_imports("worker")
    assert "ferricstore.workflow_client" not in _top_level_imports("workflow_runtime")
    assert "ferricstore.async_workflow_client" not in _top_level_imports("async_workflow_runtime")


def test_protocol_compatibility_manifest_covers_intentional_component_exports() -> None:
    component_modules = (
        "protocol_responses",
        "protocol_pipeline_codec",
        "protocol_compact_commands",
        "protocol_flow_codec",
        "protocol_commands",
        "protocol_common",
        "protocol_sync_topology",
        "protocol_async_topology",
        "protocol_async_pool",
        "protocol_sync_pool",
        "protocol_async",
        "protocol_sync",
    )
    routes = protocol_facade._EXPORTS

    for component in component_modules:
        module = __import__(f"ferricstore.{component}", fromlist=["__all__"])
        assert set(module.__all__) <= routes.keys(), component


def test_protocol_compatibility_manifest_is_owned_by_a_dedicated_module() -> None:
    assert (PACKAGE / "protocol_compat.py").is_file()
    assert _module_lines("protocol") <= 100
    assert "ferricstore.protocol_compat" in _top_level_imports("protocol")


def test_extracted_mixins_have_explicit_host_contracts() -> None:
    mixins = {
        ("protocol_sync_prepared", "SyncPreparedCommandMixin"),
        ("protocol_sync_transport", "SyncProtocolTransportMixin"),
        ("protocol_sync_batch", "SyncProtocolBatchMixin"),
        ("protocol_async_batch", "AsyncProtocolBatchMixin"),
        ("protocol_async_state", "_AsyncProtocolStateMixin"),
        ("protocol_sync_routing", "SyncTopologyRoutingMixin"),
        ("protocol_sync_endpoints", "SyncTopologyEndpointMixin"),
        ("protocol_subscriptions", "SyncProtocolSubscriptionMixin"),
        ("protocol_subscriptions", "AsyncProtocolSubscriptionMixin"),
        ("worker_completion", "SyncWorkerCompletionMixin"),
        ("async_worker_completion", "AsyncWorkerCompletionMixin"),
        ("worker_claims", "SyncWorkerClaimMixin"),
    }

    for module, class_name in mixins:
        mixin = _class_node(module, class_name)
        methods = [
            node
            for node in mixin.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not node.name.startswith("_abc_")
        ]
        assert methods, f"{module}.{class_name}"
        for method in methods:
            self_arg = method.args.args[0]
            assert self_arg.annotation is None or ast.unparse(self_arg.annotation) != "Any", (
                f"{module}.{class_name}.{method.name}"
            )


def test_large_sdk_facades_are_thin_and_implementations_are_bounded() -> None:
    client_components = {
        "client_claim_options",
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
        "client_schedules",
        "client_effects",
        "client_governance",
        "client_autobatch",
        "client_autobatch_dispatch",
    }
    protocol_components = {
        "command_grammar",
        "protocol_constants",
        "protocol_basic_commands",
        "protocol_flow_commands",
        "protocol_flow_payloads",
        "protocol_command_options",
        "protocol_common",
        "protocol_compat",
        "protocol_config",
        "protocol_pipeline_codec",
        "protocol_pipeline_raw",
        "protocol_pipeline_mutations",
        "protocol_compact_commands",
        "protocol_pipelines",
        "protocol_flow_codec",
        "protocol_commands",
        "protocol_planning",
        "protocol_response_contracts",
        "protocol_response_collections",
        "protocol_response_primitives",
        "protocol_responses",
        "flow_options",
        "flow_routing",
        "topology_security",
        "protocol_sync",
        "protocol_sync_batch",
        "protocol_sync_prepared",
        "protocol_sync_pending",
        "protocol_sync_routing",
        "protocol_sync_state",
        "protocol_sync_transport",
        "protocol_subscriptions",
        "protocol_sync_pool",
        "protocol_sync_endpoints",
        "protocol_sync_topology",
        "protocol_async",
        "protocol_async_batch",
        "protocol_async_state",
        "protocol_async_pool",
        "protocol_async_endpoints",
        "protocol_async_topology",
    }
    async_client_components = {
        "async_client_sessions",
        "async_client_state",
        "async_client_core",
        "async_client_management",
        "async_client_producer",
        "async_client_claims",
        "async_client_mutations",
        "async_client_queries",
        "async_client_schedules",
        "async_client_effects",
        "async_client_governance",
        "async_client_support",
        "async_ownership",
    }
    worker_components = {
        "legacy_worker",
        "async_queue_api",
        "async_partitioning",
        "async_queue_runtime",
        "async_worker_completion",
        "async_wake",
        "async_workflow_context",
        "async_workflow_budget",
        "async_workflow_client",
        "async_workflow_runtime",
        "async_workflow_types",
        "workflow_models",
        "workflow_budget",
        "workflow_runtime",
        "workflow_client",
        "workflow_types",
        "workflow_worker",
        "workflow_core",
        "worker_claims",
        "worker_completion",
        "worker_execution",
        "worker_runtime_config",
        "client_ownership",
        "worker_models",
        "worker",
        "queue_api",
        "workflow_execution",
        "async_workflow_execution",
    }

    available = {path.stem for path in PACKAGE.glob("*.py")}
    components = (
        client_components | protocol_components | async_client_components | worker_components
    )
    assert components <= available
    for facade in ("client", "protocol", "async_client", "async_worker", "workflow"):
        assert _module_lines(facade) <= 250, facade
    for component in components:
        assert _module_lines(component) <= 2_500, component
    assert _module_lines("protocol_sync") <= 1_800


def test_handwritten_production_modules_stay_below_one_thousand_lines() -> None:
    """Keep decomposed implementation modules reviewable as the SDK grows."""
    oversized = {
        path.name: len(path.read_text().splitlines())
        for path in PACKAGE.glob("*.py")
        if len(path.read_text().splitlines()) > 1_000
    }

    assert not oversized


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
        "command_grammar",
        "protocol_constants",
        "protocol_common",
        "protocol_pipeline_codec",
        "protocol_pipeline_mutations",
        "protocol_flow_codec",
        "protocol_commands",
        "protocol_planning",
        "protocol_response_contracts",
        "protocol_response_collections",
        "protocol_response_primitives",
        "protocol_responses",
    ):
        assert _top_level_imports(module).isdisjoint(forbidden), module


def test_client_components_do_not_depend_on_compatibility_facade() -> None:
    for module in (
        "client_claim_options",
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
        "client_schedules",
        "client_effects",
        "client_governance",
        "client_autobatch",
    ):
        assert "ferricstore.client" not in _top_level_imports(module), module


def test_async_and_worker_components_do_not_depend_on_compatibility_facades() -> None:
    facades = {
        "ferricstore.async_client",
        "ferricstore.async_worker",
        "ferricstore.client",
        "ferricstore.workflow",
    }
    for module in (
        "async_client_sessions",
        "async_client_state",
        "async_client_core",
        "async_client_management",
        "async_client_producer",
        "async_client_claims",
        "async_client_mutations",
        "async_client_queries",
        "async_client_schedules",
        "async_client_effects",
        "async_client_governance",
        "async_client_support",
        "async_queue_runtime",
        "async_worker_completion",
        "async_partitioning",
        "async_wake",
        "async_workflow_context",
        "async_workflow_budget",
        "async_workflow_client",
        "async_workflow_runtime",
        "async_workflow_types",
        "workflow_models",
        "workflow_runtime",
        "workflow_client",
        "workflow_types",
        "workflow_worker",
        "workflow_core",
        "worker_claims",
        "worker_completion",
        "worker_execution",
        "worker",
        "queue_api",
        "workflow_execution",
        "async_workflow_execution",
    ):
        assert _top_level_imports(module).isdisjoint(facades), module


def test_async_runtime_does_not_import_the_sync_worker_stack() -> None:
    assert "ferricstore.worker" not in _top_level_imports("async_queue_runtime")
    assert "ferricstore.worker_models" in _top_level_imports("async_queue_runtime")


def test_extracted_modules_do_not_erase_public_types_with_any_aliases() -> None:
    aliases = {
        "workflow_models": "Workflow",
        "workflow_runtime": "WorkflowWorker",
        "async_workflow_context": "AsyncWorkflow",
        "async_client_sessions": "AsyncFlowClient",
    }
    for module, alias in aliases.items():
        tree = ast.parse((PACKAGE / f"{module}.py").read_text())
        erased = [
            node
            for node in tree.body
            if isinstance(node, (ast.Assign, ast.AnnAssign))
            and (
                any(isinstance(target, ast.Name) and target.id == alias for target in node.targets)
                if isinstance(node, ast.Assign)
                else isinstance(node.target, ast.Name) and node.target.id == alias
            )
            and isinstance(node.value, ast.Name)
            and node.value.id == "Any"
        ]
        assert not erased, f"{module}.{alias} is erased to Any"


def test_extracted_component_import_graphs_are_acyclic() -> None:
    _assert_acyclic_modules(
        {
            "client_claim_options",
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
            "client_schedules",
            "client_effects",
            "client_governance",
            "client_autobatch",
        }
    )
    _assert_acyclic_modules(
        {
            "async_client_sessions",
            "async_client_state",
            "async_client_core",
            "async_client_management",
            "async_client_producer",
            "async_client_claims",
            "async_client_mutations",
            "async_client_queries",
            "async_client_schedules",
            "async_client_effects",
            "async_client_governance",
            "async_client_support",
            "legacy_worker",
            "async_queue_runtime",
            "async_worker_completion",
            "async_partitioning",
            "async_wake",
            "async_workflow_context",
            "async_workflow_budget",
            "async_workflow_client",
            "async_workflow_runtime",
            "async_workflow_types",
            "workflow_models",
            "workflow_runtime",
            "workflow_client",
            "workflow_types",
            "workflow_worker",
            "workflow_core",
            "worker_claims",
            "worker_completion",
            "worker_execution",
            "worker_models",
            "worker",
            "queue_api",
            "workflow_execution",
            "async_workflow_execution",
        }
    )
    _assert_acyclic_modules(
        {
            "command_grammar",
            "protocol_constants",
            "protocol_common",
            "protocol_pipeline_codec",
            "protocol_pipeline_mutations",
            "protocol_pipelines",
            "protocol_flow_codec",
            "protocol_commands",
            "protocol_planning",
            "protocol_response_contracts",
            "protocol_response_collections",
            "protocol_response_primitives",
            "protocol_responses",
            "protocol_sync",
            "protocol_sync_batch",
            "protocol_sync_prepared",
            "protocol_sync_routing",
            "protocol_subscriptions",
            "protocol_sync_pool",
            "protocol_sync_topology",
            "protocol_async",
            "protocol_async_batch",
            "protocol_async_state",
            "protocol_async_pool",
            "protocol_async_endpoints",
            "protocol_async_topology",
            "flow_options",
            "flow_routing",
            "topology_security",
            "protocol_sync_state",
            "protocol_sync_transport",
        }
    )


def test_sync_root_import_does_not_load_asyncio() -> None:
    code = "import sys; from ferricstore import FlowClient; assert 'asyncio' not in sys.modules"
    subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )


def test_production_invariants_do_not_depend_on_assert_statements() -> None:
    asserted = {
        path.name: [
            node.lineno
            for node in ast.walk(ast.parse(path.read_text()))
            if isinstance(node, ast.Assert)
        ]
        for path in PACKAGE.glob("*.py")
    }

    assert not {name: lines for name, lines in asserted.items() if lines}


@pytest.mark.parametrize(
    ("import_statement", "forbidden_module"),
    [
        ("import ferricstore.client", "ferricstore.client_core"),
        ("import ferricstore.async_client", "ferricstore.async_client_core"),
        ("from ferricstore.workflow import Complete", "ferricstore.workflow_runtime"),
        (
            "from ferricstore.async_worker import AsyncWorkflowWorkerResult",
            "ferricstore.async_queue_runtime",
        ),
    ],
)
def test_compatibility_facades_resolve_only_requested_components(
    import_statement: str,
    forbidden_module: str,
) -> None:
    code = f"import sys; {import_statement}; assert {forbidden_module!r} not in sys.modules"
    subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
    )


def test_client_and_pending_registries_use_typed_host_contracts() -> None:
    expected_executors = {
        "client_state": ("_ClientMixinBase", "CommandExecutor"),
        "async_client_state": ("_AsyncClientMixinBase", "AsyncCommandExecutor"),
    }
    for module, (class_name, expected) in expected_executors.items():
        fields = {
            node.target.id: ast.unparse(node.annotation)
            for node in _class_node(module, class_name).body
            if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
        }
        assert fields["executor"] == expected

    tree = ast.parse((PACKAGE / "protocol_sync_pending.py").read_text())
    host = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "SyncPendingRequestHost"
    )
    assert any(ast.unparse(base).endswith("Protocol") for base in host.bases)
    registry = _class_node("protocol_sync_pending", "SyncPendingRequestRegistry")
    constructor = next(
        node
        for node in registry.body
        if isinstance(node, ast.FunctionDef) and node.name == "__init__"
    )
    assert ast.unparse(constructor.args.args[1].annotation) == "SyncPendingRequestHost"


def test_facades_preserve_class_and_function_compatibility() -> None:
    from ferricstore.client_autobatch import AutobatchFlowClient, _BatchOp
    from ferricstore.client_core import FlowClient
    from ferricstore.legacy_worker import Worker
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
    }
    for name, value in expected.items():
        assert getattr(client_facade, name) is value

    from ferricstore import worker as worker_module

    assert worker_module.Worker is Worker

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

    from ferricstore.async_client_core import AsyncFlowClient
    from ferricstore.async_client_sessions import (
        AsyncCommandPipeline,
        AsyncPubSubSession,
        AsyncTransactionSession,
    )
    from ferricstore.async_queue_runtime import (
        AsyncQueue,
        AsyncQueueClient,
        AsyncQueueFlow,
        AsyncQueueFlowWorker,
    )
    from ferricstore.async_workflow_client import AsyncWorkflowClient
    from ferricstore.async_workflow_context import AsyncWorkflowContext
    from ferricstore.async_workflow_runtime import AsyncWorkflow
    from ferricstore.workflow_client import WorkflowClient
    from ferricstore.workflow_runtime import FlowWorkflow, Workflow
    from ferricstore.workflow_types import Complete, Fail, Retry, Transition
    from ferricstore.workflow_worker import WorkflowWorker

    expected_async_client = {
        "AsyncFlowClient": AsyncFlowClient,
        "AsyncCommandPipeline": AsyncCommandPipeline,
        "AsyncPubSubSession": AsyncPubSubSession,
        "AsyncTransactionSession": AsyncTransactionSession,
    }
    for name, value in expected_async_client.items():
        assert getattr(async_client_facade, name) is value

    expected_async_worker = {
        "AsyncQueueFlowWorker": AsyncQueueFlowWorker,
        "AsyncQueueFlow": AsyncQueueFlow,
        "AsyncQueue": AsyncQueue,
        "AsyncQueueClient": AsyncQueueClient,
        "AsyncWorkflowContext": AsyncWorkflowContext,
        "AsyncWorkflow": AsyncWorkflow,
        "AsyncWorkflowClient": AsyncWorkflowClient,
    }
    for name, value in expected_async_worker.items():
        assert getattr(async_worker_facade, name) is value

    expected_workflow = {
        "Transition": Transition,
        "Complete": Complete,
        "Retry": Retry,
        "Fail": Fail,
        "Workflow": Workflow,
        "FlowWorkflow": FlowWorkflow,
        "WorkflowClient": WorkflowClient,
        "WorkflowWorker": WorkflowWorker,
    }
    for name, value in expected_workflow.items():
        assert getattr(workflow_facade, name) is value


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
        "max_active_ms: 'int | float | str | None' = None, "
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
