from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

_REPOSITORY = Path(__file__).resolve().parents[1]
_VERSION_FILE = _REPOSITORY / "src" / "ferricstore" / "__init__.py"
_RELEASE_TAG = re.compile(r"v(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:[a-zA-Z0-9.-]+)?)\Z")


def package_version() -> str:
    tree = ast.parse(_VERSION_FILE.read_text(), filename=str(_VERSION_FILE))
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        if not any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in targets
        ):
            continue
        value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        break
    raise ValueError(f"{_VERSION_FILE} must define a literal __version__")


def check_release_tag(tag: str) -> None:
    match = _RELEASE_TAG.fullmatch(tag)
    if match is None:
        raise ValueError(f"release tag must use vMAJOR.MINOR.PATCH syntax: {tag!r}")
    version = package_version()
    if match.group("version") != version:
        raise ValueError(f"release tag {tag!r} does not match package version {version!r}")


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        raise SystemExit("usage: check_release_version.py vMAJOR.MINOR.PATCH")
    try:
        check_release_tag(args[0])
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    sys.stdout.write(f"release version verified: {package_version()}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
