from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ferricstore.config_validation import MAX_POLICY_GENERATION
from ferricstore.errors import FerricStoreError
from ferricstore.model_core import (
    _MappingResult,
    _optional_str,
    _optional_str_or_int,
    _raw_map,
    _str,
    _str_key_map,
)
from ferricstore.types import FlowStateMode


@dataclass(frozen=True, slots=True)
class PolicySnapshot(_MappingResult):
    """Typed, mapping-compatible snapshot returned by Flow policy commands."""

    type: str
    generation: int
    state: str | None = None
    mode: FlowStateMode | None = None
    version: str | int | None = None
    max_active_ms: int | str | None = None
    retry: dict[str, Any] | None = None
    retention: dict[str, Any] | None = None
    indexed_attributes: tuple[str, ...] = ()
    indexed_state_meta: str | None = None
    governance: dict[str, Any] | None = None
    states: dict[str, dict[str, Any]] | None = None
    raw: dict[str, Any] | None = None

    @classmethod
    def from_resp(cls, value: Any) -> PolicySnapshot:
        if not isinstance(value, dict):
            raise FerricStoreError("FLOW.POLICY returned an invalid snapshot", raw=value)

        raw = _raw_map(value)
        generation = raw.get("generation")
        if type(generation) is not int or generation < 0 or generation > MAX_POLICY_GENERATION:
            raise FerricStoreError(
                "FLOW.POLICY returned an invalid generation",
                raw=value,
            )

        type_name = raw.get("type")
        if not isinstance(type_name, str) or not type_name:
            raise FerricStoreError("FLOW.POLICY returned an invalid type", raw=value)

        raw_mode = _optional_str(raw.get("mode"))
        try:
            mode = FlowStateMode(raw_mode.lower()) if raw_mode is not None else None
        except ValueError:
            raise FerricStoreError(
                "FLOW.POLICY returned an invalid state mode",
                raw=value,
            ) from None

        raw_indexed_attributes = raw.get("indexed_attributes")
        if raw_indexed_attributes is None:
            indexed_attributes: tuple[str, ...] = ()
        elif isinstance(raw_indexed_attributes, (list, tuple)):
            indexed_attributes = tuple(_str(item) for item in raw_indexed_attributes)
        else:
            raise FerricStoreError(
                "FLOW.POLICY returned invalid indexed_attributes",
                raw=value,
            )

        raw_states = raw.get("states")
        states: dict[str, dict[str, Any]] | None = None
        if raw_states is not None:
            if not isinstance(raw_states, dict):
                raise FerricStoreError("FLOW.POLICY returned invalid states", raw=value)
            states = {}
            for state_name, state_policy in raw_states.items():
                if not isinstance(state_policy, dict):
                    raise FerricStoreError("FLOW.POLICY returned invalid states", raw=value)
                states[_str(state_name)] = _str_key_map(state_policy)

        return cls(
            type=type_name,
            generation=generation,
            state=_optional_str(raw.get("state")),
            mode=mode,
            version=_optional_str_or_int(raw.get("version")),
            max_active_ms=_policy_max_active_ms(raw.get("max_active_ms"), value),
            retry=_optional_map(raw.get("retry"), value, "retry"),
            retention=_optional_map(raw.get("retention"), value, "retention"),
            indexed_attributes=indexed_attributes,
            indexed_state_meta=_optional_str(raw.get("indexed_state_meta")),
            governance=_optional_map(raw.get("governance"), value, "governance"),
            states=states,
            raw=raw,
        )


def _optional_map(value: Any, raw: Any, field: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise FerricStoreError(f"FLOW.POLICY returned invalid {field}", raw=raw)
    return _str_key_map(value)


def _policy_max_active_ms(value: Any, raw: Any) -> int | str | None:
    if value is None or type(value) is int or isinstance(value, str):
        return value
    raise FerricStoreError("FLOW.POLICY returned invalid max_active_ms", raw=raw)


__all__ = ["MAX_POLICY_GENERATION", "PolicySnapshot"]
