# Contributing

Thanks for helping improve the FerricStore Python SDK.

This repository is currently in public-alpha shape. APIs may change while the
server protocol and SDK ergonomics stabilize.

## Development setup

```bash
git clone https://github.com/ferricstore/ferricstore-python.git
cd ferricstore-python

python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

## Run checks

```bash
pytest
ruff check .
mypy src/ferricstore
python -m build
```

If `python -m build` is unavailable:

```bash
pip install build
python -m build
```

## Pull request expectations

Before opening a PR:

- Add or update tests for SDK behavior changes.
- Update docs for public API changes.
- Keep examples runnable with local FerricStore.
- Do not include generated files such as `__pycache__`, `.pytest_cache`, `dist`, or build artifacts.
- Keep high-level docs focused on `QueueClient` / `WorkflowClient`; use `FlowClient` only for low-level command control.

## Compatibility policy

During `0.x`, minor versions may include API changes. We still try to keep the
common high-level client path stable:

- `QueueClient`
- `WorkflowClient`
- `AsyncQueueClient`
- `AsyncWorkflowClient`
- `RetryPolicy`
- `WorkerConfig`
- `ValueConfig`

If a change affects these APIs, document migration steps in the PR.

## Reporting bugs

Please include:

- SDK version.
- FerricStore server version or commit.
- Python version.
- OS and architecture.
- Minimal reproduction.
- Whether the issue is sync or async.
- Relevant Flow type/state/partition options.

