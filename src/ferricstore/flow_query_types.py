from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from ferricstore.errors import FerricStoreError


@dataclass(frozen=True, slots=True)
class FlowQueryPage:
    has_more: bool
    cursor: str | None


@dataclass(frozen=True, slots=True)
class FlowQueryQuality:
    exactness: str
    freshness: str
    coverage: str
    pagination: str


@dataclass(frozen=True, slots=True)
class FlowQueryUsage:
    range_seeks: int
    range_pages: int
    scanned_entries: int
    scanned_bytes: int
    hydrated_records: int
    residual_checks: int
    duplicate_entries: int
    result_records: int
    response_bytes: int
    memory_high_water_bytes: int
    wall_time_us: int


@dataclass(frozen=True, slots=True)
class FlowQueryResult:
    version: str
    records: tuple[dict[Any, Any], ...] | None
    page: FlowQueryPage | None
    count: int | None
    quality: FlowQueryQuality
    usage: FlowQueryUsage
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryErrorPosition:
    byte: int
    line: int
    column: int


class FlowQueryError(FerricStoreError):
    """An actionable, value-redacted FQL diagnostic returned by the server."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        detail: str | None,
        hint: str | None,
        retryable: bool,
        safe_to_retry: bool,
        retry_after_ms: int,
        position: FlowQueryErrorPosition | None,
        context: dict[Any, Any] | None,
        raw: Any,
    ) -> None:
        super().__init__(
            message,
            raw=raw,
            retryable=retryable,
            safe_to_retry=safe_to_retry,
            retry_after_ms=retry_after_ms,
        )
        self.code = code
        self.detail = detail
        self.hint = hint
        self.position = position
        self.context = context


@dataclass(frozen=True, slots=True)
class FlowExplainResult:
    version: str
    query_fingerprint: str
    status: str
    plan: dict[Any, Any]
    estimate: dict[Any, Any]
    bounds: dict[Any, Any]
    stats: dict[Any, Any] | None
    quality: FlowQueryQuality | None
    pressure: dict[Any, Any] | None
    decision: dict[Any, Any] | None
    alternatives: tuple[dict[Any, Any], ...]
    capabilities: dict[Any, Any] | None
    actual: FlowQueryUsage | None
    diagnostic: FlowQueryError | None
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexRegistry:
    epoch: int
    catalog_version: int


@dataclass(frozen=True, slots=True)
class FlowQueryIndexServices(Mapping[str, Any]):
    registry: str
    lifecycle_worker: str
    statistics_store: str
    statistics_worker: str
    raw: dict[Any, Any]

    def __getitem__(self, key: str) -> Any:
        if key in {"registry", "lifecycle_worker", "statistics_store", "statistics_worker"}:
            return getattr(self, key)
        if key in self.raw:
            return self.raw[key]
        encoded = key.encode()
        if encoded in self.raw:
            return self.raw[encoded]
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        yield from self._keys()

    def __len__(self) -> int:
        return len(self._keys())

    def _keys(self) -> tuple[str, ...]:
        canonical = ["registry", "lifecycle_worker", "statistics_store", "statistics_worker"]
        seen = set(canonical)
        for raw_key in self.raw:
            if isinstance(raw_key, bytes):
                try:
                    key = raw_key.decode("utf-8")
                except UnicodeDecodeError:
                    continue
            elif isinstance(raw_key, str):
                key = raw_key
            else:
                continue
            if key not in seen:
                canonical.append(key)
                seen.add(key)
        return tuple(canonical)


@dataclass(frozen=True, slots=True)
class FlowQueryIndexField:
    name: str
    direction: str
    encoding: str
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexFormat:
    query_row: str
    key: str
    entry: str
    reverse: str
    counter: str | None
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexCoverage:
    complete_shards: int
    total_shards: int
    validation: str
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexBuild:
    scope: str
    phase_counts: dict[str, int]
    current_phases: tuple[str, ...]
    completed_shards: int
    total_shards: int
    scanned_records: int
    written_entries: int
    written_bytes: int
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexValidation:
    scope: str
    status: str
    phase_counts: dict[str, int]
    current_phases: tuple[str, ...]
    completed_shards: int
    total_shards: int
    checked_records: int
    checked_entries: int
    mismatches: int
    failure_reason: str | None
    validated_at_ms: int | None
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexRetirement:
    status: str
    phase_counts: dict[str, int] | None
    current_phases: tuple[str, ...] | None
    completed_shards: int | None
    total_shards: int | None
    deleted_entries: int | None
    deleted_bytes: int | None
    rewritten_reverse_rows: int | None
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexStatistics:
    status: str
    samples: int
    fresh_samples: int
    stale_samples: int
    future_samples: int
    oldest_collected_at_ms: int | None
    newest_collected_at_ms: int | None
    oldest_age_ms: int | None
    newest_age_ms: int | None
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndex:
    id: str
    version: int
    build_id: str
    source: str
    state: str
    queryable: bool
    fields: tuple[FlowQueryIndexField, ...]
    workloads: tuple[str, ...]
    count_prefixes: tuple[int, ...]
    covering_fields: tuple[str, ...]
    format: FlowQueryIndexFormat
    coverage: FlowQueryIndexCoverage
    build: FlowQueryIndexBuild
    validation: FlowQueryIndexValidation
    retirement: FlowQueryIndexRetirement
    statistics: FlowQueryIndexStatistics
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexStatus:
    contract_version: str
    observed_at_ms: int
    statistics_max_age_ms: int
    registry: FlowQueryIndexRegistry
    services: FlowQueryIndexServices
    indexes: tuple[FlowQueryIndex, ...]
    raw: dict[Any, Any]


__all__ = [
    "FlowExplainResult",
    "FlowQueryError",
    "FlowQueryErrorPosition",
    "FlowQueryIndex",
    "FlowQueryIndexBuild",
    "FlowQueryIndexCoverage",
    "FlowQueryIndexField",
    "FlowQueryIndexFormat",
    "FlowQueryIndexRegistry",
    "FlowQueryIndexRetirement",
    "FlowQueryIndexServices",
    "FlowQueryIndexStatistics",
    "FlowQueryIndexStatus",
    "FlowQueryIndexValidation",
    "FlowQueryPage",
    "FlowQueryQuality",
    "FlowQueryResult",
    "FlowQueryUsage",
]
