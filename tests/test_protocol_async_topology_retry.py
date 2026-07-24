from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest

from ferricstore.backpressure import BackpressureController, BackpressurePolicy
from ferricstore.errors import FerricStoreError
from ferricstore.flow_query_retry import execute_flow_query_read_with_retry_async
from ferricstore.protocol_async_topology_retry import AsyncTopologyRetryMixin
from ferricstore.topology_lifecycle import EndpointAdapterLease

_COMMAND = ("FLOW.QUERY.INDEXES",)
_ROUTE = {"endpoint": {"host": "leader.local"}, "lane_id": 1}


class _ControlAdapter:
    def __init__(self, outcomes: list[Any] | None = None) -> None:
        self.outcomes = list(outcomes or [b"control-ok"])

    def execute_command(self, *_args: Any) -> Any:
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


class _AsyncRetryHost(AsyncTopologyRetryMixin):
    def __init__(
        self,
        *,
        routes: list[Mapping[str, Any] | None],
        protocol_outcomes: list[Any] | None = None,
        control_outcomes: list[Any] | None = None,
        refresh_error: BaseException | None = None,
    ) -> None:
        self.routes = list(routes)
        self.protocol_outcomes = list(protocol_outcomes or [])
        self.control = _ControlAdapter(control_outcomes)
        self.refresh_error = refresh_error
        self.refreshes = 0
        self.released: list[EndpointAdapterLease[tuple[str, int]]] = []

    async def _route_data(self, _args: tuple[Any, ...]):
        route = self.routes.pop(0)
        return None if route is None else (object(), dict(route))

    def _leased_adapter_for_endpoint(
        self, _endpoint: Mapping[str, Any]
    ) -> EndpointAdapterLease[tuple[str, int]]:
        return EndpointAdapterLease(("leader.local", 6388), object(), 1)

    async def _execute_protocol_command(self, _adapter: Any, _prepared: Any, _lane_id: int) -> Any:
        outcome = self.protocol_outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    def _release_adapter_lease(self, lease: EndpointAdapterLease[tuple[str, int]]) -> None:
        self.released.append(lease)

    def _control_adapter(self) -> _ControlAdapter:
        return self.control

    async def refresh_topology(self) -> Any:
        self.refreshes += 1
        if self.refresh_error is not None:
            raise self.refresh_error
        return object()


def test_routed_retry_honors_delay_and_releases_each_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    retryable = FerricStoreError(
        "busy",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=25,
    )
    host = _AsyncRetryHost(
        routes=[_ROUTE, _ROUTE],
        protocol_outcomes=[retryable, b"routed-ok"],
    )
    sleeps: list[float] = []

    async def sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", sleep)

    assert asyncio.run(host.execute_command(*_COMMAND)) == b"routed-ok"
    assert sleeps == [0.025]
    assert host.refreshes == 0
    assert len(host.released) == 2


def test_routed_retry_can_fall_back_to_a_synchronous_control_adapter() -> None:
    host = _AsyncRetryHost(
        routes=[_ROUTE, None],
        protocol_outcomes=[OSError("connection closed")],
        control_outcomes=[b"control-ok"],
    )

    assert asyncio.run(host.execute_command(*_COMMAND)) == b"control-ok"
    assert host.refreshes == 1
    assert len(host.released) == 1


def test_routed_retry_fails_when_topology_refresh_fails() -> None:
    reroute = FerricStoreError(
        "reroute",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )
    host = _AsyncRetryHost(
        routes=[_ROUTE],
        protocol_outcomes=[reroute],
        refresh_error=RuntimeError("refresh failed"),
    )

    with pytest.raises(FerricStoreError, match="reroute") as raised:
        asyncio.run(host.execute_command(*_COMMAND))

    assert raised.value is reroute
    assert host.refreshes == 1
    assert len(host.released) == 1


def test_routed_nonretryable_failure_is_not_replayed() -> None:
    failure = ValueError("bad route")
    host = _AsyncRetryHost(routes=[_ROUTE], protocol_outcomes=[failure])

    with pytest.raises(ValueError, match="bad route") as raised:
        asyncio.run(host.execute_command(*_COMMAND))

    assert raised.value is failure
    assert host.refreshes == 0
    assert len(host.released) == 1


def test_high_level_query_retry_owner_prevents_topology_double_replay() -> None:
    reroute = FerricStoreError(
        "reroute",
        retryable=True,
        safe_to_retry=True,
        retry_after_ms=0,
    )
    host = _AsyncRetryHost(routes=[_ROUTE], protocol_outcomes=[reroute])
    controller = BackpressureController(
        BackpressurePolicy(max_retries=0, max_elapsed_ms=None, shared=False)
    )

    async def run() -> None:
        async def operation() -> Any:
            return await host.execute_command(*_COMMAND)

        with pytest.raises(FerricStoreError, match="reroute") as raised:
            await execute_flow_query_read_with_retry_async(operation, controller)
        assert raised.value is reroute

    asyncio.run(run())
    assert host.refreshes == 1
    assert len(host.released) == 1


def test_control_nonretryable_failure_is_not_replayed() -> None:
    failure = ValueError("bad control response")
    host = _AsyncRetryHost(routes=[None], control_outcomes=[failure])

    with pytest.raises(ValueError, match="bad control response") as raised:
        asyncio.run(host.execute_command(*_COMMAND))

    assert raised.value is failure
    assert host.refreshes == 0
