from __future__ import annotations

from typing import Any

from ferricstore.client_helpers import _parse_kv_response


def metrics_text_response(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    if isinstance(value, str):
        return value
    raise TypeError(f"expected Prometheus metrics text, got {type(value).__name__}")


def parse_metrics_response(value: Any) -> dict[str, Any]:
    if isinstance(value, (dict, list, tuple)):
        return _parse_kv_response(value)

    text = metrics_text_response(value)
    result: dict[str, Any] = {}

    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        legacy_pair = _legacy_metrics_pair(line)
        if legacy_pair is not None:
            key, raw_value = legacy_pair
            result[key] = _coerce_metric_value(raw_value, line_number)
            continue

        identity_end = _prometheus_identity_end(line, line_number)
        identity = line[:identity_end]
        fields = line[identity_end:].split()
        if len(fields) not in {1, 2}:
            raise ValueError(f"malformed Prometheus sample line {line_number}")

        result[identity] = _coerce_metric_value(fields[0], line_number)
        if len(fields) == 2:
            try:
                int(fields[1])
            except ValueError as exc:
                raise ValueError(
                    f"Prometheus sample line {line_number} has an invalid timestamp"
                ) from exc

    return result


def _legacy_metrics_pair(line: str) -> tuple[str, str] | None:
    separator = line.find(":")
    if separator <= 0 or separator >= len(line) - 1:
        return None
    if line[separator + 1] not in {" ", "\t"}:
        return None

    key = line[:separator].strip()
    raw_value = line[separator + 1 :].strip()
    if not key or not raw_value or any(character in key for character in "{} \t\r"):
        return None
    return key, raw_value


def _prometheus_identity_end(line: str, line_number: int) -> int:
    in_labels = False
    in_quote = False
    escaped = False
    closed_labels = False

    for index, character in enumerate(line):
        if in_quote:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_quote = False
            continue

        if character == "{":
            if in_labels or closed_labels or index == 0:
                raise ValueError(f"invalid label braces on Prometheus sample line {line_number}")
            in_labels = True
        elif character == "}":
            if not in_labels:
                raise ValueError(f"unmatched label brace on Prometheus sample line {line_number}")
            in_labels = False
            closed_labels = True
        elif character == '"':
            if not in_labels:
                raise ValueError(f"quote outside labels on Prometheus sample line {line_number}")
            in_quote = True
        elif character in {" ", "\t", "\r"} and not in_labels:
            if index == 0:
                break
            return index
        elif closed_labels:
            raise ValueError(f"characters after label set on Prometheus sample line {line_number}")

    if in_labels or in_quote or escaped:
        raise ValueError(f"unterminated label set on Prometheus sample line {line_number}")
    raise ValueError(f"Prometheus sample line {line_number} is missing a value")


def _coerce_metric_value(value: str, line_number: int) -> int | float:
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError as exc:
            raise ValueError(f"Prometheus sample line {line_number} has an invalid value") from exc
