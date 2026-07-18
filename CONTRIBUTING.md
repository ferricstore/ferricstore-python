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
ruff check .
ruff format --check .
mypy src/ferricstore
python tools/generate_async_commands.py --check
pytest --cov=ferricstore --cov-report=term-missing
python -m bandit -q -c pyproject.toml -r src/ferricstore
pip-audit
python -m build
twine check dist/*
```

Run the Docker-backed integration test when touching protocol behavior:

```bash
docker compose up -d ferricstore
python scripts/wait_for_ferricstore.py
FERRICSTORE_INTEGRATION=1 pytest tests/integration
docker compose down -v
```

If `python -m build` is unavailable:

```bash
pip install -e ".[dev]"
```

## Pull request expectations

Before opening a PR:

- Add or update tests for SDK behavior changes.
- Update docs for public API changes.
- Keep examples runnable with local FerricStore.
- Do not include transient files such as `__pycache__`, `.pytest_cache`, `dist`, or build artifacts.
- Keep high-level docs focused on `QueueClient` / `WorkflowClient`; use `FlowClient` only for low-level command control.

## Data command generation

`src/ferricstore/commands.py` is the authoritative sync command surface. After
changing it, regenerate the checked-in async surface used for runtime typing:

```bash
python tools/generate_async_commands.py
```

Do not edit `src/ferricstore/async_commands.py` directly. Its freshness and
sync/async signature parity are enforced by the architecture tests.

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
