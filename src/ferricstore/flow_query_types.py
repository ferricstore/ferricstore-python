from __future__ import annotations

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
    actual: FlowQueryUsage | None
    diagnostic: FlowQueryError | None
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexRegistry:
    epoch: int
    catalog_version: int


@dataclass(frozen=True, slots=True)
class FlowQueryIndex:
    id: str
    version: int
    build_id: str
    state: str
    queryable: bool
    raw: dict[Any, Any]


@dataclass(frozen=True, slots=True)
class FlowQueryIndexStatus:
    contract_version: str
    observed_at_ms: int
    statistics_max_age_ms: int
    registry: FlowQueryIndexRegistry
    services: dict[Any, Any]
    indexes: tuple[FlowQueryIndex, ...]
    raw: dict[Any, Any]


__all__ = [
    "FlowExplainResult",
    "FlowQueryError",
    "FlowQueryErrorPosition",
    "FlowQueryIndex",
    "FlowQueryIndexRegistry",
    "FlowQueryIndexStatus",
    "FlowQueryPage",
    "FlowQueryQuality",
    "FlowQueryResult",
    "FlowQueryUsage",
]
