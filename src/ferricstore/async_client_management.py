from __future__ import annotations

import builtins
from collections.abc import Mapping, Sequence
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_helpers import (
    _append,
    _command_with_request_context,
    _invocation_create_args,
    _invocation_definition_put_args,
    _management_pair_args,
    _management_rule_args,
    _normalize_admin_response,
)


class _AsyncClientManagementMixin(_AsyncClientMixinBase):
    async def capabilities(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                await self.executor.execute_command("FERRICSTORE.CAPABILITIES")
            ),
        )

    async def acl_set_user(self, username: str, rules: Sequence[Any] | Any) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                "ACL",
                "SETUSER",
                username,
                *_management_rule_args(rules),
            )
        )

    async def acl_del_user(self, username: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("ACL", "DELUSER", username)
        )

    async def acl_get_user(self, username: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("ACL", "GETUSER", username)
        )

    async def acl_list_users(self) -> Any:
        return _normalize_admin_response(await self.executor.execute_command("ACL", "LIST"))

    async def acl_save(self) -> Any:
        return _normalize_admin_response(await self.executor.execute_command("ACL", "SAVE"))

    async def ensure_namespace(
        self,
        prefix: str,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                "FERRICSTORE.NAMESPACE",
                "ENSURE",
                prefix,
                *_management_pair_args(attrs, kwargs),
            )
        )

    async def get_namespace(self, prefix: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.NAMESPACE", "GET", prefix)
        )

    async def list_namespaces(self) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.NAMESPACE", "LIST")
        )

    async def delete_namespace(self, prefix: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.NAMESPACE", "DELETE", prefix)
        )

    async def set_quota(
        self,
        namespace: str,
        quota_spec: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                "FERRICSTORE.QUOTA",
                "SET",
                namespace,
                *_management_pair_args(quota_spec, kwargs),
            )
        )

    async def get_quota(self, namespace: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.QUOTA", "GET", namespace)
        )

    async def quota_usage(self, namespace: str) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command("FERRICSTORE.QUOTA", "USAGE", namespace)
        )

    async def cluster_info(self) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                await self.executor.execute_command("FERRICSTORE.TELEMETRY", "CLUSTER_INFO")
            ),
        )

    async def namespace_usage(self, prefix: str) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            _normalize_admin_response(
                await self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY", "NAMESPACE_USAGE", prefix
                )
            ),
        )

    async def flow_query(
        self,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return cast(
            builtins.list[Any],
            _normalize_admin_response(
                await self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY",
                    "FLOW_QUERY",
                    *_management_pair_args(attrs, kwargs),
                )
            ),
        )

    async def flow_history(
        self,
        id: str,
        attrs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> builtins.list[Any]:
        return cast(
            builtins.list[Any],
            _normalize_admin_response(
                await self.executor.execute_command(
                    "FERRICSTORE.TELEMETRY",
                    "FLOW_HISTORY",
                    id,
                    *_management_pair_args(attrs, kwargs),
                )
            ),
        )

    async def invocation_definition_put(
        self,
        definition: Mapping[str, Any] | str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.PUT",
                    _invocation_definition_put_args(definition),
                    request_context,
                )
            )
        )

    async def invocation_definition_get(
        self,
        name: str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.GET",
                    [name],
                    request_context,
                )
            )
        )

    async def invocation_definition_list(
        self,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context(
                    "INVOCATION.DEFINITION.LIST",
                    [],
                    request_context,
                )
            )
        )

    async def invocation_create(
        self,
        name: str,
        attrs: Mapping[str, Any],
        *,
        context: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
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

    async def invocation_get(
        self,
        id: str,
        *,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context("INVOCATION.GET", [id], request_context)
            )
        )

    async def invocation_partition_list(
        self,
        name: str,
        *,
        scope: str | None = None,
        request_context: Mapping[str, Any] | None = None,
    ) -> Any:
        args: builtins.list[Any] = [name]
        _append(args, "SCOPE", scope)
        return _normalize_admin_response(
            await self.executor.execute_command(
                *_command_with_request_context("INVOCATION.PARTITION.LIST", args, request_context)
            )
        )
