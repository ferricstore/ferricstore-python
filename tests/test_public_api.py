from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import ferricstore


def test_every_declared_public_export_resolves() -> None:
    missing = [name for name in ferricstore.__all__ if not hasattr(ferricstore, name)]

    assert missing == []


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
assert "ferricstore.client" in sys.modules
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
