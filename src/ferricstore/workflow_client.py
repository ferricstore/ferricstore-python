from __future__ import annotations

import builtins
import contextlib
import threading
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from ferricstore.client_core import FlowClient
from ferricstore.lifecycle_core import SyncCloseCoordinator, close_resources_sync
from ferricstore.retry_policy import RetryPolicy
from ferricstore.types import (
    FlowStatePolicyLike,
    ValueConfig,
    WorkerConfig,
    resolve_worker_connection_counts,
)
from ferricstore.workflow_models import _close_resource_safely

if TYPE_CHECKING:
    from ferricstore.workflow_runtime import FlowWorkflow


class WorkflowClient:
    """High-level client for durable state-machine workflows."""

    def __init__(
        self,
        client: FlowClient | str,
        *,
        claim_client: FlowClient | str | None = None,
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
        _owns_clients: bool = False,
    ) -> None:
        command_pool_size, claim_pool_size = resolve_worker_connection_counts(
            worker_config=worker_config,
            default_workers=1,
        )
        command_max_connections = (
            1
            if isinstance(client, str)
            and (worker_config is None or worker_config.command_connections is None)
            else command_pool_size
        )
        self._url = client if isinstance(client, str) else None
        self._base_url_kwargs: dict[str, Any] = {}
        self._claim_client_explicit = claim_client is not None
        self._owned_extra_claim_flows: builtins.list[FlowClient] = []
        self._claim_flows_by_size: dict[int, FlowClient] = {}
        self._claim_pool_lock = threading.Lock()
        self._claim_pool_size = claim_pool_size
        with contextlib.ExitStack() as rollback:
            self.flow = (
                FlowClient.from_url(client, max_connections=command_max_connections)
                if isinstance(client, str)
                else client
            )
            owns_flow = _owns_clients or isinstance(client, str)
            if owns_flow:
                rollback.callback(_close_resource_safely, self.flow)
            if claim_client is None:
                self.claim_flow = (
                    FlowClient.from_url(client, max_connections=claim_pool_size)
                    if isinstance(client, str)
                    else self.flow
                )
            else:
                self.claim_flow = (
                    FlowClient.from_url(claim_client, max_connections=claim_pool_size)
                    if isinstance(claim_client, str)
                    else claim_client
                )
            owns_claim_flow = self.claim_flow is not self.flow and (
                _owns_clients or isinstance(client, str) or isinstance(claim_client, str)
            )
            if owns_claim_flow:
                rollback.callback(_close_resource_safely, self.claim_flow)
            rollback.pop_all()
        self.retry_policy = retry_policy
        self.worker_config = worker_config
        self.value_config = value_config or ValueConfig()
        self._owns_flow = owns_flow
        self._owns_claim_flow = owns_claim_flow
        self._close_coordinator = SyncCloseCoordinator()
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
    ) -> WorkflowClient:
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
        with contextlib.ExitStack() as rollback:
            flow = FlowClient.from_url(url, **command_kwargs)
            rollback.callback(_close_resource_safely, flow)
            claim_flow = FlowClient.from_url(url, **claim_kwargs)
            rollback.callback(_close_resource_safely, claim_flow)
            instance = cls(
                flow,
                claim_client=claim_flow,
                retry_policy=retry_policy,
                worker_config=worker_config,
                value_config=value_config,
                _owns_clients=True,
            )
            rollback.pop_all()
        instance._url = url
        instance._base_url_kwargs = dict(kwargs)
        instance._claim_client_explicit = False
        instance._claim_pool_size = claim_pool_size
        instance._claim_flows_by_size = {claim_pool_size: instance.claim_flow}
        return instance

    def _claim_flow_for_worker_config(
        self,
        worker_config: WorkerConfig | None,
    ) -> FlowClient:
        def resolve_claim_flow() -> FlowClient:
            with self._claim_pool_lock:
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
                claim_flow = FlowClient.from_url(self._url, **claim_kwargs)
                self._claim_flows_by_size[claim_pool_size] = claim_flow
                self._owned_extra_claim_flows.append(claim_flow)
                return claim_flow

        return self._close_coordinator.run_while_open(
            resolve_claim_flow,
            closed_message="workflow client is closed",
        )

    def workflow(
        self,
        *,
        type: str,
        initial_state: str = "queued",
        partition_by: Sequence[str] = (),
        retry_policy: RetryPolicy | None = None,
        worker_config: WorkerConfig | None = None,
        value_config: ValueConfig | None = None,
    ) -> FlowWorkflow:
        from ferricstore.workflow_runtime import FlowWorkflow

        resolved_worker_config = worker_config if worker_config is not None else self.worker_config
        return FlowWorkflow(
            self.flow,
            claim_client=self._claim_flow_for_worker_config(resolved_worker_config),
            type=type,
            initial_state=initial_state,
            partition_by=partition_by,
            retry_policy=retry_policy if retry_policy is not None else self.retry_policy,
            worker_config=resolved_worker_config,
            value_config=value_config if value_config is not None else self.value_config,
        )

    def install_policy(
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
        return self.flow.install_policy(type, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.flow, name)

    def close(self) -> None:
        self._close_coordinator.run(self._close_owned_clients)

    def _close_owned_clients(self) -> None:
        extra_claim_flows = tuple(self._owned_extra_claim_flows)
        self._claim_flows_by_size.clear()
        resources: list[Callable[[], Any]] = []
        for extra_claim_flow in extra_claim_flows:

            def close_extra_claim_flow(flow: FlowClient = extra_claim_flow) -> None:
                flow.close()
                self._owned_extra_claim_flows[:] = [
                    candidate
                    for candidate in self._owned_extra_claim_flows
                    if candidate is not flow
                ]

            resources.append(close_extra_claim_flow)
        if self._owns_claim_flow and self.claim_flow is not self.flow:

            def close_claim_flow() -> None:
                self.claim_flow.close()
                self._owns_claim_flow = False

            resources.append(close_claim_flow)
        if self._owns_flow:

            def close_flow() -> None:
                self.flow.close()
                self._owns_flow = False

            resources.append(close_flow)
        close_resources_sync(resources)


__all__ = ["WorkflowClient"]
