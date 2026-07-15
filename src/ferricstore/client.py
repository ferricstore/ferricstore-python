from __future__ import annotations

# ruff: noqa: SIM905
from importlib import import_module
from typing import TYPE_CHECKING, Any

_EXPORTS: dict[str, tuple[str, str]] = {
    "AutobatchFlowClient": ("ferricstore.client_autobatch", "AutobatchFlowClient"),
    "_BatchOp": ("ferricstore.client_autobatch", "_BatchOp"),
    "FlowClient": ("ferricstore.client_core", "FlowClient"),
    "CommandPipeline": ("ferricstore.client_sessions", "CommandPipeline"),
    "PubSubSession": ("ferricstore.client_sessions", "PubSubSession"),
    "TransactionSession": ("ferricstore.client_sessions", "TransactionSession"),
}

_HELPER_EXPORTS = (
    "_append _append_attributes _append_bool _append_claimed_items_args "
    "_append_encoded _append_extra_options _append_named_counts _append_named_values "
    "_append_payload_read _append_read_options _append_search_state_meta "
    "_append_state_meta _append_value_return _auto_partition_key_for_id "
    "_batch_key_value _batch_named_key _coerce_diag_value "
    "_command_with_request_context _complete_command_args _complete_jobs_command_args "
    "_complete_many_args _expand_many_response _fail_command_args _flow_return "
    "_has_named_item_values _invocation_create_args _invocation_definition_put_args "
    "_job_mutation_command_args _json_arg _management_pair_args _management_rule_args "
    "_merge_named_map _normalize_admin_response _now_ms _ok_response _parse_kv_response "
    "_parse_text_sections _retry_command_args "
    "_run_steps_many_args _run_steps_many_items _shared_create_many_attributes "
    "_shared_create_many_state_meta _split_flow_state_policy _step_continue_args "
    "_text _transition_command_args"
).split()
for _name in _HELPER_EXPORTS:
    _EXPORTS[_name] = ("ferricstore.client_helpers", _name)
del _HELPER_EXPORTS, _name

for _name in (
    "_claim_due_command_args",
    "_claim_return_compat_args",
    "_claim_return_mode_unsupported",
    "_reclaim_command_args",
    "_resolve_include_record",
):
    _EXPORTS[_name] = ("ferricstore.client_claim_options", _name)
del _name

__all__ = [name for name in _EXPORTS if not name.startswith("_")]


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))


if TYPE_CHECKING:
    from ferricstore.client_autobatch import AutobatchFlowClient as AutobatchFlowClient
    from ferricstore.client_core import FlowClient as FlowClient
    from ferricstore.client_sessions import CommandPipeline as CommandPipeline
    from ferricstore.client_sessions import PubSubSession as PubSubSession
    from ferricstore.client_sessions import TransactionSession as TransactionSession
