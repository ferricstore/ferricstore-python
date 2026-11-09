"""Typed reads of a parent Flow's child group."""

from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from ferricstore.errors import FerricStoreError
from ferricstore.model_core import _get, _str

_CHILD_STATUSES = frozenset({"running", "completed", "failed", "cancelled"})
_GROUP_WAITS = frozenset({"all", "any", "none"})
_GROUP_RESOLUTIONS = frozenset({"success", "failure"})


@dataclass(frozen=True, slots=True)
class ChildResult:
    """One child of a group: its status and, once terminal, its result or error."""

    id: str
    status: str
    partition_key: str | None = None
    result: Any = None
    error: Any = None


@dataclass(frozen=True, slots=True)
class ChildGroupSummary:
    total: int
    completed: int
    failed: int
    cancelled: int


@dataclass(frozen=True, slots=True)
class ChildGroupResults:
    """A parent's child group, with children ordered by id.

    ``resolved`` is ``"success"`` or ``"failure"`` once the group resolved and
    the parent moved to the matching state, and ``None`` before.
    """

    group_id: str
    wait: str
    resolved: str | None
    summary: ChildGroupSummary
    children: builtins.list[ChildResult] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ChildGroupRead:
    """A decoded child group whose result and error values still need one value read."""

    group: ChildGroupResults
    refs: builtins.list[str]
    targets: builtins.list[tuple[int, str]]

    def with_values(self, values: Sequence[Any]) -> ChildGroupResults:
        if len(values) != len(self.refs):
            raise FerricStoreError("FLOW.VALUE.MGET returned a different number of child values")
        children = list(self.group.children)
        for (index, name), value in zip(self.targets, values, strict=True):
            if value is not None:
                children[index] = replace(children[index], **{name: value})
        return replace(self.group, children=children)


def child_group_get_args(parent_id: str, partition_key: str | bytes | None) -> builtins.list[Any]:
    args: builtins.list[Any] = ["FLOW.GET", parent_id]
    if partition_key is not None:
        args.extend(["PARTITION", partition_key])
    return args


def require_child_group(
    flow_id: str, group_id: str, group: ChildGroupResults | None
) -> ChildGroupResults:
    """Return a handler's own child group, which must exist."""

    if group is None:
        raise FerricStoreError(f"Flow {flow_id} has no child group {group_id}")
    return group


def read_child_group(record: Any, group_id: str) -> ChildGroupRead | None:
    """Decode one group from a ``FLOW.GET`` response, or ``None`` when it is absent."""

    if record is None:
        return None
    if not isinstance(record, Mapping):
        raise FerricStoreError("FLOW.GET returned an unexpected response")
    groups = _get(dict(record), "child_groups")
    if groups is None:
        return None
    group = _get(_mapping(groups, "child groups"), group_id)
    if group is None:
        return None
    group = _mapping(group, "child group")

    partitions = _text_keys(_get(group, "child_partitions"), "child group partitions")
    results = _text_keys(_get(group, "results"), "child group results")
    children = sorted(
        (
            ChildResult(
                id=child_id,
                status=_one_of(status, _CHILD_STATUSES, "child status"),
                partition_key=None
                if partitions.get(child_id) is None
                else _str(partitions[child_id]),
            )
            for child_id, status in _text_keys(
                _get(group, "children"), "child group children"
            ).items()
        ),
        key=lambda child: child.id,
    )
    refs: builtins.list[str] = []
    targets: builtins.list[tuple[int, str]] = []
    for index, child in enumerate(children):
        terminal = results.get(child.id)
        if terminal is None:
            continue
        terminal = _mapping(terminal, "child result")
        for ref_field, name in (("result_ref", "result"), ("error_ref", "error")):
            ref = _get(terminal, ref_field)
            if ref is not None:
                refs.append(_str(ref))
                targets.append((index, name))

    resolved = _get(group, "resolved")
    summary = _mapping(_get(group, "summary") or {}, "child group summary")
    return ChildGroupRead(
        group=ChildGroupResults(
            group_id=group_id,
            wait=_one_of(_get(group, "wait"), _GROUP_WAITS, "child group wait"),
            resolved=None
            if resolved is None
            else _one_of(resolved, _GROUP_RESOLUTIONS, "child group resolution"),
            summary=ChildGroupSummary(
                total=_count(summary, "total"),
                completed=_count(summary, "completed"),
                failed=_count(summary, "failed"),
                cancelled=_count(summary, "cancelled"),
            ),
            children=children,
        ),
        refs=refs,
        targets=targets,
    )


def _mapping(value: Any, context: str) -> dict[Any, Any]:
    if not isinstance(value, Mapping):
        raise FerricStoreError(f"FLOW.GET returned an invalid {context}")
    return dict(value)


def _text_keys(value: Any, context: str) -> dict[str, Any]:
    if value is None:
        return {}
    return {_str(key): item for key, item in _mapping(value, context).items()}


def _one_of(value: Any, allowed: frozenset[str], context: str) -> str:
    text = _str(value)
    if text not in allowed:
        raise FerricStoreError(f"FLOW.GET returned an unknown {context}: {text!r}")
    return text


def _count(summary: dict[Any, Any], name: str) -> int:
    value = _get(summary, name, 0)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise FerricStoreError(f"FLOW.GET returned an invalid child group {name} count")
    return value


__all__ = [
    "ChildGroupRead",
    "ChildGroupResults",
    "ChildGroupSummary",
    "ChildResult",
    "child_group_get_args",
    "read_child_group",
    "require_child_group",
]
