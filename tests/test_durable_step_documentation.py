from __future__ import annotations

import ast
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[1]


def test_public_docs_explain_the_durable_step_recovery_contract() -> None:
    documentation = "\n".join(
        (REPOSITORY / path).read_text()
        for path in ("README.md", "docs/quickstart.md", "docs/workflow.md")
    )

    for required in (
        "client.advance(",
        "client.step(",
        "ctx.step(",
        "stable provider idempotency key",
        "must remain stable across retries",
        "does not occupy a worker",
        "step_continue()",
    ):
        assert required in documentation


def test_public_workflow_examples_use_the_durable_step_api() -> None:
    for example_name in ("order_workflow.py", "state_machine_workflow.py"):
        source = (REPOSITORY / "examples" / example_name).read_text()
        tree = ast.parse(source)

        assert any(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "step"
            for node in ast.walk(tree)
        )
        assert "idempotency_key" in source
