## Summary

Describe the change and why it is needed.

## Type

- [ ] Bug fix
- [ ] Feature
- [ ] Documentation
- [ ] Tests
- [ ] Refactor
- [ ] Benchmark

## Checklist

- [ ] Tests added or updated.
- [ ] Docs updated for public API changes.
- [ ] High-level APIs remain centered on `QueueClient` / `WorkflowClient`.
- [ ] No generated files or local data directories included.
- [ ] `ruff check .` and `ruff format --check .` pass locally, or reason noted below.
- [ ] `mypy src/ferricstore` passes locally, or reason noted below.
- [ ] `pytest --cov=ferricstore --cov-report=term-missing` passes locally, or reason noted below.

## Notes

Add migration notes, benchmark caveats, or follow-up work.
