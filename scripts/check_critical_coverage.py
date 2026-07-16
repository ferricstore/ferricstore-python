from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 uses the dev dependency
    import tomli as tomllib

_REPOSITORY = Path(__file__).resolve().parents[1]
_CONFIG = _REPOSITORY / "pyproject.toml"


def coverage_thresholds(config_path: Path = _CONFIG) -> dict[str, float]:
    config = tomllib.loads(config_path.read_text())
    raw = config["tool"]["ferricstore"]["critical_coverage"]
    thresholds: dict[str, float] = {}
    for path, minimum in raw.items():
        if isinstance(minimum, bool) or not isinstance(minimum, (int, float)):
            raise ValueError(f"critical coverage threshold for {path} must be numeric")
        value = float(minimum)
        if not 0.0 <= value <= 100.0:
            raise ValueError(f"critical coverage threshold for {path} must be between 0 and 100")
        thresholds[path.replace("\\", "/")] = value
    if not thresholds:
        raise ValueError("at least one critical coverage threshold is required")
    return thresholds


def check_critical_coverage(
    report_path: Path,
    *,
    thresholds: Mapping[str, float] | None = None,
) -> dict[str, float]:
    report: Any = json.loads(report_path.read_text())
    files = report.get("files") if isinstance(report, dict) else None
    if not isinstance(files, dict):
        raise ValueError("coverage report must contain a files mapping")
    normalized_files = {str(path).replace("\\", "/"): value for path, value in files.items()}
    required = coverage_thresholds() if thresholds is None else dict(thresholds)
    observed: dict[str, float] = {}
    failures: list[str] = []
    for raw_path, minimum in required.items():
        path = raw_path.replace("\\", "/")
        details = normalized_files.get(path)
        summary = details.get("summary") if isinstance(details, dict) else None
        percent = summary.get("percent_covered") if isinstance(summary, dict) else None
        if isinstance(percent, bool) or not isinstance(percent, (int, float)):
            failures.append(f"{path}: missing from coverage report")
            continue
        value = float(percent)
        observed[path] = value
        if value < float(minimum):
            failures.append(f"{path}: {value:.2f}% < {float(minimum):.2f}%")
    if failures:
        raise ValueError("critical coverage below threshold: " + "; ".join(failures))
    return observed


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        raise SystemExit("usage: check_critical_coverage.py COVERAGE_JSON")
    try:
        observed = check_critical_coverage(Path(args[0]))
    except (OSError, ValueError) as exc:
        raise SystemExit(str(exc)) from exc
    sys.stdout.write(f"critical coverage verified for {len(observed)} modules\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
