from __future__ import annotations

# ruff: noqa: SIM905

# Static routes for historical private imports. This module deliberately owns
# data only, so importing the compatibility facade does not load transports.
_GROUPS = (
    (
        "ferricstore.protocol_responses",
        (
            "_batch_item_value _decode_flow_record_value_at _decode_protocol_response "
            "_extract_traced_value _flow_many_group_values _is_custom_compact_nil "
            "_normalize_trace_map _ok_scalar _pipeline_pair_list "
            "_preflight_compact_collection _read_compact_binary "
            "_read_compact_optional_binary _read_custom_binary_list "
            "_read_custom_binary_map _read_custom_claim_job _read_custom_flow_record "
            "_read_custom_flow_record_list _read_custom_flow_value_ref "
            "_read_tagged_binary _read_tagged_i64 _read_u32 _require_available "
            "_require_compact_collection_count _response_value _status_text "
            "_try_decode_binary_list _try_decode_claim_jobs_compact "
            "_try_decode_custom_binary_list_list _try_decode_custom_binary_map_list "
            "_try_decode_custom_claim_jobs _try_decode_custom_claim_jobs_width "
            "_try_decode_custom_flow_record _try_decode_custom_flow_record_list "
            "_try_decode_custom_integer_list _try_decode_custom_kv_get "
            "_try_decode_custom_kv_mget _try_decode_custom_kv_mget_fixed "
            "_try_decode_custom_ok_list _try_decode_custom_pipeline_response "
            "_try_fast_response_value _try_fast_response_value_at"
        ).split(),
    ),
    (
        "ferricstore.protocol_pipeline_codec",
        (
            "_CompactPayloadBudget _binary_wire_size _blocks_forever "
            "_compact_flow_get_pipeline_payload "
            "_compact_flow_get_pipeline_payload_from_raw "
            "_compact_flow_history_pipeline_payload_from_raw _compact_kv_keys_payload "
            "_compact_kv_set_keys_value_payload _compact_kv_set_pairs_payload "
            "_compact_mixed_pipeline_payload_from_raw "
            "_compact_pipeline_hmget_payload_from_raw "
            "_compact_pipeline_hset_payload_from_raw "
            "_compact_pipeline_keys_payload_from_raw _compact_pipeline_payload "
            "_compact_pipeline_range_payload_from_raw "
            "_compact_pipeline_set_payload_from_raw "
            "_compact_pipeline_two_binary_payload_from_raw "
            "_compact_pipeline_zadd_payload_from_raw "
            "_expected_command_collection_items _expected_payload_collection_items "
            "_pipeline_frame_supported _stateful_command_exec"
        ).split(),
    ),
    (
        "ferricstore.protocol_compact_commands",
        (
            "_compact_flow_complete_many_payloads_from_raw "
            "_compact_flow_create_many_payloads_from_raw "
            "_compact_flow_many_payloads_from_raw "
            "_compact_flow_signal_pipeline_payload_from_raw "
            "_compact_flow_start_and_claim_payloads_from_raw "
            "_compact_flow_step_continue_payloads_from_raw "
            "_compact_flow_value_put_payloads_from_raw "
            "_compact_pipeline_payload_from_raw _parse_compact_flow_start_and_claim_raw "
            "_parse_compact_flow_step_continue_raw _parse_compact_flow_value_put_raw"
        ).split(),
    ),
    (
        "ferricstore.protocol_flow_codec",
        (
            "_compact_binary _compact_bool_marker _compact_create_many_return_mode "
            "_compact_flow_cancel_many_payload _compact_flow_claim_due_payload "
            "_compact_flow_claimed_many_payload _compact_flow_complete_many_payload "
            "_compact_flow_create_many_payload "
            "_compact_flow_transition_many_payload _compact_flow_value_mget_payload "
            "_compact_flow_value_put_payload _compact_optional_binary "
            "_compact_i64 "
            "_compact_partition_request _compact_return_mode "
            "_compact_terminal_independent_marker _maybe_bytes "
            "_ok_on_success_return_mode _optional_bytes _raw_int"
        ).split(),
    ),
    (
        "ferricstore.protocol_commands",
        (
            "_build_basic_protocol_command _field_value_map _int_arg _kv_set_options "
            "_require_values _zadd_items _zrange_payload _command_exec_protocol_command "
            "_generic_option_map _normalize_request_context "
            "_normalize_request_context_scopes _option_map "
            "_compact_flow_complete_many_payloads_from_raw "
            "_compact_flow_create_many_payloads_from_raw "
            "_compact_flow_many_payloads_from_raw "
            "_compact_flow_signal_pipeline_payload_from_raw "
            "_compact_flow_start_and_claim_payloads_from_raw "
            "_compact_flow_step_continue_payloads_from_raw "
            "_compact_flow_value_put_payloads_from_raw "
            "_compact_pipeline_payload_from_raw _parse_compact_flow_start_and_claim_raw "
            "_parse_compact_flow_step_continue_raw _parse_compact_flow_value_put_raw "
            "_build_flow_protocol_command _collapse_states _find_item_token "
            "_flow_claimed_many_payload _flow_create_many_payload "
            "_flow_fenced_many_payload _flow_policy_option_map "
            "_flow_policy_set_option_map _flow_spawn_children_payload "
            "_normalize_flow_search_state_meta_payload _parse_claimed_items "
            "_parse_create_items _parse_create_items_ext _parse_fenced_items "
            "_parse_spawn_children _parse_spawn_children_ext _split_refs_and_options "
            "build_protocol_command encode_frame"
        ).split(),
    ),
    (
        "ferricstore.protocol_common",
        (
            "RoutingTopology _RequestBodyBuffer _async_adapter_outer_fanout_limit "
            "_close_adapter_async _close_adapter_sync _close_adapters_async "
            "_close_adapters_sync _coerce_bool _command_name _command_token "
            "_compact_payload_budget _connection_endpoint_key _encode_request_body "
            "_endpoint_adapter_is_idle _endpoint_from_url _error_message "
            "_flow_hash_tag _flow_wake_payload _hash_tag_or_key _int_or_none "
            "_is_retryable_route_error _is_safe_control_retry _lane_for_opcode "
            "_map_get _normalize_protocol_url_kwargs _normalized_host_set "
            "_notify_event_listeners _optional_int _optional_text "
            "_pending_request_capacity_error _pop_response_item_count "
            "_protocol_collection_limit _protocol_connection_count "
            "_protocol_lane_count _request_body_byte_limit _require_arg "
            "_response_identity_map _response_item_count_map _send_frame "
            "_set_wire_future_sources _sync_adapter_deadline _text _text_or_none "
            "_timeout_with_deadline _unique_adapters _url_from_endpoint _url_host "
            "_valid_port _validate_pending_response_identity _validated_route_lane"
        ).split(),
    ),
    ("ferricstore.protocol_sync_topology", "RoutingTopology TopologyProtocolAdapterPool".split()),
    ("ferricstore.protocol_async_topology", "AsyncTopologyProtocolAdapterPool".split()),
    (
        "ferricstore.protocol_async_pool",
        "AsyncProtocolAdapterPool _AsyncProtocolAdapterSession".split(),
    ),
    (
        "ferricstore.protocol_sync_pool",
        "ProtocolAdapterPool _ProtocolAdapterSession".split(),
    ),
    ("ferricstore.protocol_async", "AsyncProtocolAdapter AsyncProtocolPipeline".split()),
    ("ferricstore.protocol_sync", "ProtocolAdapter ProtocolPipeline".split()),
)

COMPAT_EXPORTS = {name: (module_name, name) for module_name, names in _GROUPS for name in names}

__all__ = ["COMPAT_EXPORTS"]
