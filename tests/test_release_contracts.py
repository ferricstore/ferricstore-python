from __future__ import annotations

import ast
import json
import runpy
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10 CI
    import tomli as tomllib

from ferricstore import __version__

REPOSITORY = Path(__file__).resolve().parents[1]


def test_package_version_has_one_build_metadata_source() -> None:
    config = tomllib.loads((REPOSITORY / "pyproject.toml").read_text())

    assert "version" not in config["project"]
    assert "version" in config["project"]["dynamic"]
    assert config["tool"]["hatch"]["version"]["path"] == "src/ferricstore/__init__.py"


def test_release_version_checker_matches_tag_to_runtime_version() -> None:
    checker = runpy.run_path(str(REPOSITORY / "scripts" / "check_release_version.py"))

    assert checker["package_version"]() == __version__
    checker["check_release_tag"](f"v{__version__}")
    with pytest.raises(ValueError, match="does not match"):
        checker["check_release_tag"]("v999.0.0")


def test_publish_requires_tag_validation_and_live_integration() -> None:
    workflow = (REPOSITORY / ".github" / "workflows" / "publish.yml").read_text()

    assert "workflow_dispatch:" not in workflow
    assert "scripts/check_release_version.py" in workflow
    assert "integration:" in workflow
    assert 'FERRICSTORE_INTEGRATION: "1"' in workflow
    assert "needs: [build, integration]" in workflow


def test_native_integration_coverage_is_observation_based() -> None:
    source = (REPOSITORY / "tests" / "integration" / "test_ferricstore_integration.py").read_text()
    tree = ast.parse(source)

    assert "_NATIVE_PROTOCOL_INTEGRATION_EXERCISED" not in source
    assert "_NATIVE_PROTOCOL_INTEGRATION_OBSERVED" in source
    assert any(
        isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.Sub)
        and isinstance(node.left, ast.Name)
        and node.left.id == "catalog_names"
        and isinstance(node.right, ast.Name)
        and node.right.id == "_NATIVE_PROTOCOL_INTEGRATION_OBSERVED"
        for node in ast.walk(tree)
    )


def test_native_integration_observer_preserves_executor_capabilities() -> None:
    integration = runpy.run_path(
        str(REPOSITORY / "tests" / "integration" / "test_ferricstore_integration.py")
    )
    observed_executor = integration["_ObservedExecutor"]

    class BasicExecutor:
        def execute_command(self, *args: object) -> object:
            return args

    basic = observed_executor(BasicExecutor())
    assert getattr(basic, "acquire_session_for_keys", None) is None

    session = BasicExecutor()

    class SessionExecutor(BasicExecutor):
        def acquire_session_for_keys(self, *_args: object) -> BasicExecutor:
            return session

    capable = observed_executor(SessionExecutor())
    assert isinstance(capable.acquire_session_for_keys(("key",)), observed_executor)

    observed = integration["_NATIVE_PROTOCOL_INTEGRATION_OBSERVED"]
    observed.clear()
    integration["_observe_command"](("CLIENT.SETNAME", "sdk-test"))
    assert "CLIENT" in observed


def test_critical_coverage_checker_rejects_per_module_regressions(tmp_path: Path) -> None:
    checker = runpy.run_path(str(REPOSITORY / "scripts" / "check_critical_coverage.py"))
    report = tmp_path / "coverage.json"
    report.write_text(
        json.dumps(
            {
                "files": {
                    "src/ferricstore/protocol_framing.py": {"summary": {"percent_covered": 91.25}},
                    "src/ferricstore/protocol_lifecycle.py": {
                        "summary": {"percent_covered": 87.99}
                    },
                }
            }
        )
    )
    thresholds = {
        "src/ferricstore/protocol_framing.py": 90.0,
        "src/ferricstore/protocol_lifecycle.py": 88.0,
    }

    with pytest.raises(ValueError, match=r"protocol_lifecycle\.py.*87\.99.*88\.00"):
        checker["check_critical_coverage"](report, thresholds=thresholds)

    thresholds["src/ferricstore/protocol_lifecycle.py"] = 87.0
    assert checker["check_critical_coverage"](report, thresholds=thresholds) == {
        "src/ferricstore/protocol_framing.py": 91.25,
        "src/ferricstore/protocol_lifecycle.py": 87.99,
    }


def test_ci_and_publish_enforce_critical_module_coverage() -> None:
    config = tomllib.loads((REPOSITORY / "pyproject.toml").read_text())
    thresholds = config["tool"]["ferricstore"]["critical_coverage"]
    assert thresholds["src/ferricstore/protocol_framing.py"] >= 88
    assert thresholds["src/ferricstore/protocol_response_contracts.py"] >= 93

    for workflow_name in ("ci.yml", "publish.yml"):
        workflow = (REPOSITORY / ".github" / "workflows" / workflow_name).read_text()
        assert "--cov-report=json:coverage.json" in workflow
        assert "scripts/check_critical_coverage.py coverage.json" in workflow
