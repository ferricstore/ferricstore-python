from __future__ import annotations

import sys
from collections.abc import Awaitable, Callable, Sequence
from typing import TYPE_CHECKING, Any

from ferricstore.async_client_core import AsyncFlowClient
from ferricstore.async_ownership import (
    resolve_async_client_pair,
    rollback_async_resources,
)
from ferricstore.async_queue_runtime import _client_from
from ferricstore.lifecycle_core import AsyncCloseCoordinator, close_resources_async
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    FlowStatePolicyLike,
    ValueConfig,
    WorkerConfig,
    resolve_worker_connection_counts,
)

if TYPE_CHECKING:
    from ferricstore.async_workflow_runtime import AsyncWorkflow


def _default_value_config() -> ValueConfig:
    """Honor the canonical runtime module's injectable compatibility seam."""
    runtime = sys.modules.get("ferricstore.async_workflow_runtime")
    factory = getattr(runtime, "ValueConfig", ValueConfig) if runtime is not None else ValueConfig
    return factory()


class AsyncWorkflowClient:
    """High-level async durable workflow client."""

    def __init__(
        self,
        client: AsyncFlowClient | str | Any,
        *,
        claim_client: AsyncFlowClient | str | Any | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _owns_clients: bool = False,
    ) -> None:
        resolved_value_config = (
            value_config if value_config is not None else _default_value_config()
        )
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        self._url = client if isinstance(client, str) else None
        self._base_url_kwargs: dict[str, Any] = {}
        self._claim_client_explicit = claim_client is not None
        self._owned_extra_claim_flows: list[AsyncFlowClient] = []
        self._claim_flows_by_size: dict[int, AsyncFlowClient] = {}
        self._claim_pool_size = claim_pool_size
        command_max_connections = (
            1
            if isinstance(client, str)
            and (worker_config is None or worker_config.command_connections is None)
            else command_pool_size
        )
        clients = resolve_async_client_pair(
            client,
            claim_client,
            from_url=AsyncFlowClient.from_url,
            normalize=_client_from,
            command_kwargs={"max_connections": command_max_connections},
            claim_kwargs={"max_connections": claim_pool_size},
        )
        self.flow = clients.command
        self.claim_flow = clients.claim
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = resolved_value_config
        self._owns_flow = _owns_clients or clients.owns_command
        self._owns_claim_flow = self.claim_flow is not self.flow and (
            _owns_clients or clients.owns_claim
        )
        self._close_coordinator = AsyncCloseCoordinator()
        if self.claim_flow is not self.flow:
            self._claim_flows_by_size[claim_pool_size] = self.claim_flow

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> AsyncWorkflowClient:
        resolved_value_config = (
            value_config if value_config is not None else _default_value_config()
        )
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_kwargs = dict(kwargs)
        if worker_config is None or worker_config.command_connections is None:
            command_kwargs.setdefault("max_connections", 1)
        else:
            command_kwargs.setdefault("max_connections", command_pool_size)
        claim_kwargs = dict(kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        clients = resolve_async_client_pair(
            url,
            None,
            from_url=AsyncFlowClient.from_url,
            normalize=_client_from,
            command_kwargs=command_kwargs,
            claim_kwargs=claim_kwargs,
        )
        try:
            instance = cls(
                clients.command,
                claim_client=clients.claim,
                retry_policy=retry_policy,
                worker_config=worker_config,
                value_config=resolved_value_config,
                _owns_clients=True,
            )
        except BaseException:
            rollback_async_resources(clients.owned_resources())
            raise
        instance._url = url
        instance._base_url_kwargs = dict(kwargs)
        instance._claim_client_explicit = False
        instance._claim_pool_size = claim_pool_size
        instance._claim_flows_by_size = {claim_pool_size: instance.claim_flow}
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> AsyncFlowClient:
        if self._close_coordinator.started:
            raise RuntimeError("workflow client is closed")
        if self._claim_client_explicit or self._url is None:
            return self.claim_flow
        _, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        existing = self._claim_flows_by_size.get(claim_pool_size)
        if existing is not None:
            return existing
        claim_kwargs = dict(self._base_url_kwargs)
        claim_kwargs["max_connections"] = claim_pool_size
        claim_flow = AsyncFlowClient.from_url(self._url, **claim_kwargs)
        self._claim_flows_by_size[claim_pool_size] = claim_flow
        self._owned_extra_claim_flows.append(claim_flow)
        return claim_flow

    def workflow(
        self,
        *,
        type: str,
        states: Sequence[str] | None = None,
        initial_state: str = "queued",
        partition_by: Sequence[str] = (),
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        **kwargs: Any,
    ) -> AsyncWorkflow:
        from ferricstore.async_queue_runtime import ASYNC_WORKFLOW_CONFIG_KEYS
        from ferricstore.async_workflow_runtime import AsyncWorkflow

        resolved_worker_config = worker_config if worker_config is not None else self.worker_config
        workflow_kwargs = (
            resolved_worker_config.to_kwargs(ASYNC_WORKFLOW_CONFIG_KEYS)
            if resolved_worker_config is not None
            else {}
        )
        workflow_kwargs.update(kwargs)
        return AsyncWorkflow(
            self.flow,
            claim_client=self._claim_flow_for_worker_config(resolved_worker_config),
            type=type,
            states=states,
            initial_state=initial_state,
            partition_by=partition_by,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            value_config=value_config if value_config is not None else self.value_config,
            _producer_url=self._url,
            _producer_url_kwargs=self._base_url_kwargs,
            **workflow_kwargs,
        )

    async def install_policy(
        self,
        type: str,
        *,
        retry_policy: RetryPolicy | None = None,
        retry: RetryPolicy | None = None,
        states: dict[str, FlowStatePolicyLike] | None = None,
        indexed_state_meta: str | None = None,
        max_active_ms: int | float | str | None = None,
    ) -> Any:
        if retry_policy is not None and retry is not None:
            raise ValueError("retry_policy and retry are mutually exclusive")
        resolved_retry_policy = (
            retry_policy
            if retry_policy is not None
            else retry
            if retry is not None
            else self.retry_policy
        )
        kwargs: dict[str, Any] = {"retry": resolved_retry_policy, "states": states}
        if indexed_state_meta is not None:
            kwargs["indexed_state_meta"] = indexed_state_meta
        if max_active_ms is not None:
            kwargs["max_active_ms"] = max_active_ms
        return await self.flow.install_policy(type, **kwargs)

    async def close(self) -> None:
        await self._close_coordinator.run(self._close_owned_clients)

    async def _close_owned_clients(self) -> None:
        extra_claim_flows = tuple(self._owned_extra_claim_flows)
        self._claim_flows_by_size.clear()
        resources: list[Callable[[], Awaitable[None]]] = []
        for extra_claim_flow in extra_claim_flows:

            async def close_extra_claim_flow(
                flow: AsyncFlowClient = extra_claim_flow,
            ) -> None:
                await flow.close()
                self._owned_extra_claim_flows[:] = [
                    candidate
                    for candidate in self._owned_extra_claim_flows
                    if candidate is not flow
                ]

            resources.append(close_extra_claim_flow)
        if self._owns_claim_flow and self.claim_flow is not self.flow:

            async def close_claim_flow() -> None:
                await self.claim_flow.close()
                self._owns_claim_flow = False

            resources.append(close_claim_flow)
        if self._owns_flow:

            async def close_flow() -> None:
                await self.flow.close()
                self._owns_flow = False

            resources.append(close_flow)
        await close_resources_async(resources)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)


__all__ = ["AsyncWorkflowClient"]
