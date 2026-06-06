# Release Process

The SDK is currently `0.x` public alpha.

## Versioning

Until `1.0`, APIs may change between minor versions. Maintainers should still
prefer migration-friendly changes for the high-level client surface:

- `QueueClient`
- `WorkflowClient`
- `AsyncQueueClient`
- `AsyncWorkflowClient`
- `RetryPolicy`
- `WorkerConfig`
- `ValueConfig`

Patch releases should be bug fixes, documentation updates, or compatibility
fixes.

## Checklist

Before publishing:

```bash
ruff check .
ruff format --check .
mypy src/ferricstore
pytest --cov=ferricstore --cov-report=term-missing
bandit -q -r src/ferricstore
pip-audit
python -m build
twine check dist/*
```

Then:

1. Update `pyproject.toml` version.
2. Update README status if needed.
3. Update benchmark docs if results are cited.
4. Tag the release.
5. Publish package artifacts.

## Public-alpha language

Use this wording for `0.x` releases:

```text
FerricStore Python SDK is public alpha. APIs and server protocol may change
before 1.0. Feedback and production-shape testing are welcome.
```
