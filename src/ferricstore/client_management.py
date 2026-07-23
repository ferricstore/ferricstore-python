from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ferricstore.client_helpers import (
    _append,
    _command_with_request_context,
    _invocation_create_args,
    _invocation_definition_put_args,
    _management_pair_args,
    _management_rule_args,
    _normalize_admin_response,
    _ok_response,
    _parse_kv_response,
    _validate_ownership_token,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.metrics_response import metrics_text_response, parse_metrics_response
from ferricstore.types import FetchOrComputeResult, KeyInfo, RateLimitResult


class _ClientManagementMixin(_ClientMixinBase):
    def cas(self, key: str, expected: Any, value: Any, *, ex: int | None = None) -> bool:
        args: builtins.list[Any] = [
            "CAS",
            key,
            self.codec.encode(expected),
            self.codec.encode(value),
        ]
        _append(args, "EX", ex)
        return bool(self.executor.execute_command(*args))

    def lock(self, key: str, owner: str, ttl_ms: int) -> bool:
        return _ok_response(self.executor.execute_command("LOCK", key, owner, ttl_ms))

    def unlock(self, key: str, owner: str) -> int:
        return int(self.executor.execute_command("UNLOCK", key, owner))

    def extend_lock(self, key: str, owner: str, ttl_ms: int) -> int:
        return int(self.executor.execute_command("EXTEND", key, owner, ttl_ms))

    def ratelimit_add(
        self,
        key: str,
        *,
        window_ms: int,
        max: int,
        count: int = 1,
    ) -> RateLimitResult:
        return RateLimitResult.from_resp(
            self.executor.execute_command("RATELIMIT.ADD", key, window_ms, max, count)
        )

    def key_info(self, key: str) -> KeyInfo:
        return KeyInfo.from_resp(self.executor.execute_command("FERRICSTORE.KEY_INFO", key))

    def fetch_or_compute(
        self,
        key: str,
        *,
        ttl_ms: int,
        hint: str | None = None,
    ) -> FetchOrComputeResult:
        args: builtins.list[Any] = ["FETCH_OR_COMPUTE", key, ttl_ms]
        if hint is not None:
            args.append(hint)
        response = self.executor.execute_command(*args)
        return FetchOrComputeResult.from_resp(response, decode=self.codec.decode)

    def fetch_or_compute_result(
        self,
        key: str,
        ownership_token: bytes,
        value: Any,
        *,
        ttl_ms: int,
    ) -> bool:
        _validate_ownership_token(ownership_token)
        response = self.executor.execute_command(
            "FETCH_OR_COMPUTE_RESULT",
            key,
            ownership_token,
            self.codec.encode(value),
            ttl_ms,
        )
        return _ok_response(response)

    def fetch_or_compute_error(self, key: str, ownership_token: bytes, message: str) -> bool:
        _validate_ownership_token(ownership_token)
        return _ok_response(
            self.executor.execute_command(
                "FETCH_OR_COMPUTE_ERROR",
                key,
                ownership_token,
                message,
            )
        )

    def cluster_health(self) -> Any:
        return _parse_kv_response(self.executor.execute_command("CLUSTER.HEALTH"))

    def cluster_stats(self) -> Any:
        return _parse_kv_response(self.executor.execute_command("CLUSTER.STATS"))

    def cluster_keyslot(self, key: str) -> int:
        return int(self.executor.execute_command("CLUSTER.KEYSLOT", key))

    def cluster_slots(self) -> Any:
        return self.executor.execute_command("CLUSTER.SLOTS")

    def cluster_status(self) -> Any:
        return _parse_kv_response(self.executor.execute_command("CLUSTER.STATUS"))

    def cluster_role(self) -> Any:
        return self.executor.execute_command("CLUSTER.ROLE")

    def cluster_join(self, node: str, *, replace: bool = False) -> bool:
        args: builtins.list[Any] = ["CLUSTER.JOIN", node]
        if replace:
            args.append("REPLACE")
        return _ok_response(self.executor.execute_command(*args))

    def cluster_leave(self) -> bool:
        return _ok_response(self.executor.execute_command("CLUSTER.LEAVE"))

    def cluster_failover(self, shard_index: int, target_node: str) -> bool:
        return _ok_response(
            self.executor.execute_command("CLUSTER.FAILOVER", shard_index, target_node)
        )

    def cluster_promote(self, node: str) -> bool:
        return _ok_response(self.executor.execute_command("CLUSTER.PROMOTE", node))

    def cluster_demote(self, node: str) -> bool:
        return _ok_response(self.executor.execute_command("CLUSTER.DEMOTE", node))

    def ferricstore_config(self, *args: Any) -> Any:
        return self.executor.execute_command("FERRICSTORE.CONFIG", *args)

    def ferricstore_hotness(self, *args: Any) -> Any:
        return _parse_kv_response(self.executor.execute_command("FERRICSTORE.HOTNESS", *args))

    def ferricstore_metrics(self, *args: Any) -> Any:
        return parse_metrics_response(self.executor.execute_command("FERRICSTORE.METRICS", *args))

    def ferricstore_metrics_text(self, *args: Any) -> str:
        return metrics_text_response(self.executor.execute_command("FERRICSTORE.METRICS", *args))

    def ferricstore_blobgc(self, *args: Any) -> Any:
        return self.executor.execute_command("FERRICSTORE.BLOBGC", *args)

    def capabilities(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(self.executor.execute_command("FERRICSTORE.CAPABILITIES")),
        )

    def acl_set_user(self, username: str, rules: Sequence[Any] | Any) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command("ACL", "SETUSER", username, *_management_rule_args(rules))
        )

    def acl_del_user(self, username: str) -> Any:
        return _normalize_admin_response(self.executor.execute_command("ACL", "DELUSER", username))

    def acl_get_user(self, username: str) -> Any:
        return _normalize_admin_response(self.executor.execute_command("ACL", "GETUSER", username))

    def acl_list_users(self) -> Any:
        return _normalize_admin_response(self.executor.execute_command("ACL", "LIST"))

    def acl_save(self) -> Any:
        return _normalize_admin_response(self.executor.execute_command("ACL", "SAVE"))

    def ensure_namespace(
        self,
        prefix: str,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command(
                "FERRICSTORE.NAMESPACE",
                "ENSURE",
                prefix,
                *_management_pair_args(attrs, kwargs),
            )
        )

    def get_namespace(self, prefix: str) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command("FERRICSTORE.NAMESPACE", "GET", prefix)
        )

    def list_namespaces(self) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command("FERRICSTORE.NAMESPACE", "LIST")
        )

    def delete_namespace(self, prefix: str) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command("FERRICSTORE.NAMESPACE", "DELETE", prefix)
        )

    def set_quota(
        self,
        namespace: str,
        quota_spec: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command(
                "FERRICSTORE.QUOTA",
                "SET",
                namespace,
                *_management_pair_args(quota_spec, kwargs),
            )
        )

    def get_quota(self, namespace: str) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command("FERRICSTORE.QUOTA", "GET", namespace)
        )

    def quota_usage(self, namespace: str) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command("FERRICSTORE.QUOTA", "USAGE", namespace)
        )

    def cluster_info(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                self.executor.execute_command("FERRICSTORE.TELEMETRY", "CLUSTER_INFO")
            ),
        )

    def namespace_usage(self, prefix: str) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                self.executor.execute_command("FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", prefix)
            ),
        )

    def flow_query(
        self,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return cast(
            builtins.list[Any],
            _normalize_admin_response(
                self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY",
                    "FLOW_QUERY",
                    *_management_pair_args(attrs, kwargs),
                )
            ),
        )

    def flow_history(
        self,
        id: str,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return cast(
            builtins.list[Any],
            _normalize_admin_response(
                self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY",
                    "FLOW_HISTORY",
                    id,
                    *_management_pair_args(attrs, kwargs),
                )
            ),
        )

    def invocation_definition_put(
        self,
        definition: Mapping[str, Any] | str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.PUT",
                    _invocation_definition_put_args(definition),
                    request_context,
                )
            )
        )

    def invocation_definition_get(
        self,
        name: str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.GET",
                    [name],
                    request_context,
                )
            )
        )

    def invocation_definition_list(
        self,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.LIST",
                    [],
                    request_context,
                )
            )
        )

    def invocation_create(
        self,
        name: str,
        attrs: Mapping[str, Any],
        *,
        context: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.CREATE",
                    _invocation_create_args(
                        name,
                        attrs,
                        context=context,
                        idempotency_key=idempotency_key,
                    ),
                    request_context,
                )
            )
        )

    def invocation_get(
        self,
        id: str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            self.executor.execute_command(
                *_command_with_request_context("INVOCATION.GET", [id], request_context)
            )
        )

    def invocation_partition_list(
        self,
        name: str,
        *,
        scope: str | None = None,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        args: builtins.list[Any] = [name]
        _append(args, "SCOPE", scope)
        return _normalize_admin_response(
            self.executor.execute_command(
                *_command_with_request_context("INVOCATION.PARTITION.LIST", args, request_context)
            )
        )
