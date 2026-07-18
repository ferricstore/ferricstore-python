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
python -m bandit -q -c pyproject.toml -r src/ferricstore
pip-audit
python -m build
twine check dist/*
python scripts/check_release_version.py "v$(python -c 'import ferricstore; print(ferricstore.__version__)')"
```

Run the real FerricStore integration test:

```bash
docker compose up -d ferricstore
python scripts/wait_for_ferricstore.py
FERRICSTORE_INTEGRATION=1 pytest tests/integration
docker compose down -v
```

Then:

1. Update `src/ferricstore/__init__.py` `__version__` (the build reads this single source).
2. Update README status if needed.
3. Update benchmark docs if results are cited.
4. Tag the release with the exact version, for example `v0.5.1`.
5. Push the tag; the publish workflow validates it, reruns unit and live-server
   integration tests, builds the distribution, and publishes only after both jobs pass.

## Public-alpha language

Use this wording for `0.x` releases:

```text
FerricStore Python SDK is public alpha. APIs and server protocol may change
before 1.0. Feedback and production-shape testing are welcome.
```
