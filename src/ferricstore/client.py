from __future__ import annotations

from ferricstore.client_autobatch import (
    AutobatchFlowClient as AutobatchFlowClient,
)
from ferricstore.client_autobatch import (
    _BatchOp as _BatchOp,
)
from ferricstore.client_core import FlowClient as FlowClient
from ferricstore.client_helpers import (
    _append as _append,
)
from ferricstore.client_helpers import (
    _append_attributes as _append_attributes,
)
from ferricstore.client_helpers import (
    _append_bool as _append_bool,
)
from ferricstore.client_helpers import (
    _append_claimed_items_args as _append_claimed_items_args,
)
from ferricstore.client_helpers import (
    _append_encoded as _append_encoded,
)
from ferricstore.client_helpers import (
    _append_extra_options as _append_extra_options,
)
from ferricstore.client_helpers import (
    _append_named_counts as _append_named_counts,
)
from ferricstore.client_helpers import (
    _append_named_values as _append_named_values,
)
from ferricstore.client_helpers import (
    _append_payload_read as _append_payload_read,
)
from ferricstore.client_helpers import (
    _append_read_options as _append_read_options,
)
from ferricstore.client_helpers import (
    _append_search_state_meta as _append_search_state_meta,
)
from ferricstore.client_helpers import (
    _append_state_meta as _append_state_meta,
)
from ferricstore.client_helpers import (
    _append_value_return as _append_value_return,
)
from ferricstore.client_helpers import (
    _auto_partition_key_for_id as _auto_partition_key_for_id,
)
from ferricstore.client_helpers import (
    _batch_key_value as _batch_key_value,
)
from ferricstore.client_helpers import (
    _batch_named_key as _batch_named_key,
)
from ferricstore.client_helpers import (
    _claim_due_command_args as _claim_due_command_args,
)
from ferricstore.client_helpers import (
    _claim_return_compat_args as _claim_return_compat_args,
)
from ferricstore.client_helpers import (
    _claim_return_mode_unsupported as _claim_return_mode_unsupported,
)
from ferricstore.client_helpers import (
    _coerce_diag_value as _coerce_diag_value,
)
from ferricstore.client_helpers import (
    _command_with_request_context as _command_with_request_context,
)
from ferricstore.client_helpers import (
    _complete_command_args as _complete_command_args,
)
from ferricstore.client_helpers import (
    _complete_jobs_command_args as _complete_jobs_command_args,
)
from ferricstore.client_helpers import (
    _complete_many_args as _complete_many_args,
)
from ferricstore.client_helpers import (
    _expand_many_response as _expand_many_response,
)
from ferricstore.client_helpers import (
    _fail_command_args as _fail_command_args,
)
from ferricstore.client_helpers import (
    _flow_return as _flow_return,
)
from ferricstore.client_helpers import (
    _has_named_item_values as _has_named_item_values,
)
from ferricstore.client_helpers import (
    _invocation_create_args as _invocation_create_args,
)
from ferricstore.client_helpers import (
    _invocation_definition_put_args as _invocation_definition_put_args,
)
from ferricstore.client_helpers import (
    _job_mutation_command_args as _job_mutation_command_args,
)
from ferricstore.client_helpers import (
    _json_arg as _json_arg,
)
from ferricstore.client_helpers import (
    _management_pair_args as _management_pair_args,
)
from ferricstore.client_helpers import (
    _management_rule_args as _management_rule_args,
)
from ferricstore.client_helpers import (
    _merge_named_map as _merge_named_map,
)
from ferricstore.client_helpers import (
    _normalize_admin_response as _normalize_admin_response,
)
from ferricstore.client_helpers import (
    _now_ms as _now_ms,
)
from ferricstore.client_helpers import (
    _ok_response as _ok_response,
)
from ferricstore.client_helpers import (
    _parse_kv_response as _parse_kv_response,
)
from ferricstore.client_helpers import (
    _parse_text_sections as _parse_text_sections,
)
from ferricstore.client_helpers import (
    _resolve_include_record as _resolve_include_record,
)
from ferricstore.client_helpers import (
    _retry_command_args as _retry_command_args,
)
from ferricstore.client_helpers import (
    _run_steps_many_args as _run_steps_many_args,
)
from ferricstore.client_helpers import (
    _run_steps_many_items as _run_steps_many_items,
)
from ferricstore.client_helpers import (
    _shared_create_many_attributes as _shared_create_many_attributes,
)
from ferricstore.client_helpers import (
    _shared_create_many_state_meta as _shared_create_many_state_meta,
)
from ferricstore.client_helpers import (
    _split_flow_state_policy as _split_flow_state_policy,
)
from ferricstore.client_helpers import (
    _step_continue_args as _step_continue_args,
)
from ferricstore.client_helpers import (
    _text as _text,
)
from ferricstore.client_helpers import (
    _transition_command_args as _transition_command_args,
)
from ferricstore.client_sessions import (
    CommandPipeline as CommandPipeline,
)
from ferricstore.client_sessions import (
    PubSubSession as PubSubSession,
)
from ferricstore.client_sessions import (
    TransactionSession as TransactionSession,
)

for _public_class in (
    FlowClient,
    AutobatchFlowClient,
    CommandPipeline,
    PubSubSession,
    TransactionSession,
):
    _public_class.__module__ = __name__
del _public_class
