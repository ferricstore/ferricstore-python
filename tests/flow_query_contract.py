from __future__ import annotations

from typing import Any


def with_flow_query_contract(capabilities: dict[str, Any]) -> dict[str, Any]:
    capabilities["flow_query"] = {
        "request_contract": "ferric.flow.query.request/v1",
        "result_contract": "ferric.flow.query.result/v1",
        "explain_contract": "ferric.flow.explain/v1",
        "index_status_contract": "ferric.flow.query.indexes/v1",
        "language_versions": ["FQL1"],
        "capabilities": [
            "flow_query_v1",
            "flow_explain_v1",
            "flow_explain_analyze_v1",
            "flow_composite_index_v1",
            "flow_query_index_status_v1",
        ],
        "shapes": [
            "runs_by_run_id_record",
            "runs_by_partition_and_run_id_record",
            "runs_by_partition_predicates_ordered_records",
            "runs_by_partition_type_state_ordered_records",
            "runs_by_partition_type_terminals_ordered_records",
            "runs_by_partition_metadata_ordered_records",
            "runs_by_partition_type_running_lease_deadline_ordered_records",
            "runs_by_partition_parent_ordered_records",
            "runs_by_partition_root_ordered_records",
            "runs_by_partition_correlation_ordered_records",
            "runs_by_partition_predicates_count",
            "events_by_run_id_ordered_records",
        ],
    }
    schemas = capabilities.setdefault("schemas", {})
    schemas["FLOW.QUERY"] = {
        "required": ["version", "query"],
        "fields": ["version", "query", "params", "deadline_ms"],
    }
    return capabilities


__all__ = ["with_flow_query_contract"]
