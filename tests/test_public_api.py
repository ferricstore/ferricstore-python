from __future__ import annotations

import pickle
import subprocess
import sys
from pathlib import Path

import ferricstore


def test_every_declared_public_export_resolves() -> None:
    missing = [name for name in ferricstore.__all__ if not hasattr(ferricstore, name)]

    assert missing == []


def test_serializable_public_values_keep_pre_refactor_pickle_modules() -> None:
    import ferricstore.workflow as workflow_module

    values = [
        workflow_module.StateConfig("queued"),
        workflow_module.WorkflowWorkerResult(),
        workflow_module.Transition("running"),
        workflow_module.Complete(),
        workflow_module.Retry(),
        workflow_module.Fail(),
        ferricstore.QueueFlowWorkerResult(),
        ferricstore.AsyncWorkflowWorkerResult(),
        ferricstore.ScheduleResult(),
    ]
    expected_modules = [
        "ferricstore.workflow",
        "ferricstore.workflow",
        "ferricstore.workflow",
        "ferricstore.workflow",
        "ferricstore.workflow",
        "ferricstore.workflow",
        "ferricstore.worker",
        "ferricstore.async_worker",
        "ferricstore.types",
    ]

    for value, expected_module in zip(values, expected_modules, strict=True):
        assert type(value).__module__ == expected_module
        payload = pickle.dumps(value)
        assert expected_module.encode() in payload
        assert pickle.loads(payload) == value


def test_package_root_defers_heavy_sdk_modules_until_attribute_access() -> None:
    repository = Path(__file__).resolve().parents[1]
    script = """
import sys
import ferricstore

heavy = {
    "ferricstore.async_client",
    "ferricstore.async_worker",
    "ferricstore.client",
    "ferricstore.protocol",
    "ferricstore.worker",
    "ferricstore.workflow",
}
loaded_eagerly = sorted(heavy.intersection(sys.modules))
assert loaded_eagerly == [], loaded_eagerly

raw_codec = ferricstore.RawCodec
assert raw_codec.__name__ == "RawCodec"
assert "ferricstore.codecs" in sys.modules
assert "ferricstore.protocol" not in sys.modules

flow_client = ferricstore.FlowClient
assert flow_client.__name__ == "FlowClient"
assert "ferricstore.client_core" in sys.modules
assert "ferricstore.client" not in sys.modules
assert ferricstore.QueueWorker is ferricstore.QueueFlowWorker
assert ferricstore.QueueWorkerResult is ferricstore.QueueFlowWorkerResult
"""

    subprocess.run(
        [sys.executable, "-c", script],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )


def test_lightweight_root_exports_do_not_cross_runtime_boundaries() -> None:
    repository = Path(__file__).resolve().parents[1]
    scripts = [
        """
import sys
from ferricstore import ProtocolCommand
loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert "ferricstore.protocol_constants" in loaded
assert not loaded.intersection(
    {"ferricstore.protocol", "ferricstore.protocol_sync", "ferricstore.protocol_async"}
)
assert len(loaded) <= 4, sorted(loaded)
""",
        """
import sys
from ferricstore import Complete
loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert "ferricstore.workflow_types" in loaded
assert not loaded.intersection(
    {
        "ferricstore.workflow",
        "ferricstore.workflow_models",
        "ferricstore.workflow_runtime",
        "ferricstore.client_core",
    }
)
assert len(loaded) <= 4, sorted(loaded)
""",
        """
import sys
from ferricstore import QueueFlowWorkerResult
loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert loaded == {"ferricstore", "ferricstore.worker_models"}, sorted(loaded)
""",
        """
import sys
from ferricstore import AsyncWorkflowWorkerResult
loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert "ferricstore.async_workflow_types" in loaded
assert not loaded.intersection(
    {
        "ferricstore.async_worker",
        "ferricstore.async_workflow_context",
        "ferricstore.async_client_core",
    }
)
assert len(loaded) <= 3, sorted(loaded)
""",
        """
import sys
from ferricstore import AsyncFlowClient
loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert "ferricstore.async_client_core" in loaded
assert not loaded.intersection(
    {
        "ferricstore.async_client",
        "ferricstore.client",
        "ferricstore.client_core",
        "ferricstore.client_autobatch",
    }
)
""",
        """
import sys
from ferricstore import ScheduleResult
loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert "ferricstore.schedule_types" in loaded
assert "ferricstore.types" not in loaded
assert len(loaded) <= 3, sorted(loaded)
""",
    ]

    for script in scripts:
        subprocess.run(
            [sys.executable, "-c", script],
            cwd=repository,
            check=True,
            capture_output=True,
            text=True,
        )


def test_protocol_compatibility_facade_is_lazy() -> None:
    repository = Path(__file__).resolve().parents[1]
    script = """
import sys
import ferricstore.protocol

loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert loaded == {
    "ferricstore",
    "ferricstore.protocol",
    "ferricstore.protocol_compat",
}, sorted(loaded)

from ferricstore.protocol import ProtocolCommand
loaded = {name for name in sys.modules if name.startswith("ferricstore")}
assert "ferricstore.protocol_constants" in loaded
assert "ferricstore.protocol_sync" not in loaded
assert "ferricstore.protocol_async" not in loaded
"""

    subprocess.run(
        [sys.executable, "-c", script],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )


def test_missing_protocol_compatibility_attribute_does_not_import_runtime_graph() -> None:
    repository = Path(__file__).resolve().parents[1]
    script = """
import sys
import ferricstore.protocol as protocol

before = {name for name in sys.modules if name.startswith("ferricstore")}
assert not hasattr(protocol, "_definitely_missing_protocol_attribute")
after = {name for name in sys.modules if name.startswith("ferricstore")}
assert after == before, sorted(after - before)
"""

    subprocess.run(
        [sys.executable, "-c", script],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    )
