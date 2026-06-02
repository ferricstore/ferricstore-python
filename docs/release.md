# Release checklist

FerricStore Python SDK releases are published from tagged commits using PyPI
Trusted Publishing.

## Before tagging

1. Update `version` in `pyproject.toml`.
2. Update `__version__` in `src/ferricstore/__init__.py`.
3. Move relevant `CHANGELOG.md` entries from `Unreleased` to the release version.
4. Run local validation:

```bash
python -m ruff check .
python -m mypy src/ferricstore
python -m pytest
python -m build
python -m twine check dist/*
```

## Publish

Create and push a version tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The `Publish` GitHub Actions workflow builds the distribution and publishes it
to PyPI from the `pypi` environment.

## PyPI Trusted Publishing setup

Create a trusted publisher for:

- Owner: `ferricstore`
- Repository: `ferricstore-python`
- Workflow: `publish.yml`
- Environment: `pypi`

No API token should be stored in GitHub secrets for normal releases.
