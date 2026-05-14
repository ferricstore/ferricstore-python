from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from ferricstore.client import FlowClient
from ferricstore.errors import FerricStoreError
from ferricstore.types import ChildSpec, CreateItem, FlowRecord, RetryPolicy


@dataclass(frozen=True)
class StateConfig:
    name: str
    lease_ms: int = 30_000
    claim_payload: bool = True
    on_error: str = "retry"
    retry: RetryPolicy | None = None


@dataclass(frozen=True)
class Transition:
    to_state: str
    payload: Any = None
    run_at_ms: int | None = None


@dataclass(frozen=True)
class Complete:
    result: Any = None
    payload: Any = None


@dataclass(frozen=True)
class Retry:
    error: Any = None
    payload: Any = None
    run_at_ms: int | None = None


@dataclass(frozen=True)
class Fail:
    error: Any = None
    payload: Any = None


Outcome = Transition | Complete | Retry | Fail
Handler = Callable[[Any, FlowRecord], Outcome]


def transition(to_state: str, *, payload: Any = None, run_at_ms: int | None = None) -> Transition:
    return Transition(to_state=to_state, payload=payload, run_at_ms=run_at_ms)


def complete(*, result: Any = None, payload: Any = None) -> Complete:
    return Complete(result=result, payload=payload)


def retry(*, error: Any = None, payload: Any = None, run_at_ms: int | None = None) -> Retry:
    return Retry(error=error, payload=payload, run_at_ms=run_at_ms)


def fail(*, error: Any = None, payload: Any = None) -> Fail:
    return Fail(error=error, payload=payload)


def state(
    name: str,
    *,
    lease_ms: int = 30_000,
    claim_payload: bool = True,
    on_error: str = "retry",
    retry: RetryPolicy | None = None,
) -> Callable[[Handler], Handler]:
    def decorate(fn: Handler) -> Handler:
        setattr(
            fn,
            "__ferric_state__",
            StateConfig(
                name=name,
                lease_ms=lease_ms,
                claim_payload=claim_payload,
                on_error=on_error,
                retry=retry,
            ),
        )
        return fn

    return decorate


class Workflow:
    """Base class for explicit FerricFlow state workflows."""

    type: str
    initial_state = "queued"
    partition_by: tuple[str, ...] = ()

    def __init__(self, client: FlowClient) -> None:
        self.client = client
        self._states = self._discover_states()

    def create(self, id: str, *, payload: Any = None, **attrs: Any) -> FlowRecord:
        partition_key = attrs.pop("partition_key", None) or self.partition_key(attrs)
        for name in self.partition_by:
            attrs.pop(name, None)
        return self.client.create(
            id,
            type=self.type,
            state=self.initial_state,
            payload=payload,
            partition_key=partition_key,
            **attrs,
        )

    def create_many(
        self,
        partition_key: str | None,
        items: list[CreateItem],
        **attrs: Any,
    ) -> list[FlowRecord]:
        return self.client.create_many(
            partition_key,
            items,
            type=self.type,
            state=attrs.pop("state", self.initial_state),
            **attrs,
        )

    def partition_key(self, attrs: dict[str, Any]) -> str | None:
        if not self.partition_by:
            return None
        return ":".join(str(attrs[name]) for name in self.partition_by)

    def install_policy(self) -> Any:
        state_policies = {
            config.name: config.retry for config in self._states.values() if config.retry is not None
        }
        return self.client.install_policy(self.type, states=state_policies)

    def policy_get(self, *, state: str | None = None) -> dict[Any, Any]:
        return self.client.policy_get(self.type, state=state)

    def claim_due(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        limit: int = 1,
    ) -> list[FlowRecord]:
        config = self._states[state_name]
        return self.client.claim_due(
            self.type,
            state=state_name,
            worker=worker,
            partition_key=partition_key,
            lease_ms=config.lease_ms,
            limit=limit,
        )

    def reclaim(
        self,
        *,
        worker: str,
        partition_key: str | None = None,
        lease_ms: int = 30_000,
        limit: int = 1,
    ) -> list[FlowRecord]:
        return self.client.reclaim(
            self.type,
            worker=worker,
            partition_key=partition_key,
            lease_ms=lease_ms,
            limit=limit,
        )

    def run_once(
        self,
        state_name: str,
        *,
        worker: str,
        partition_key: str | None = None,
        limit: int = 1,
    ) -> list[FlowRecord]:
        jobs = self.claim_due(
            state_name,
            worker=worker,
            partition_key=partition_key,
            limit=limit,
        )
        return [self.handle(job) for job in jobs]

    def get(self, id: str, *, partition_key: str | None = None) -> FlowRecord | None:
        return self.client.get(id, partition_key=partition_key)

    def history(self, id: str, **kwargs: Any) -> list[Any]:
        return self.client.history(id, **kwargs)

    def list(self, **kwargs: Any) -> list[FlowRecord]:
        return self.client.list(self.type, **kwargs)

    def terminals(self, **kwargs: Any) -> list[FlowRecord]:
        return self.client.terminals(self.type, **kwargs)

    def failures(self, **kwargs: Any) -> list[FlowRecord]:
        return self.client.failures(self.type, **kwargs)

    def by_parent(self, parent_flow_id: str, **kwargs: Any) -> list[FlowRecord]:
        return self.client.by_parent(parent_flow_id, **kwargs)

    def by_root(self, root_flow_id: str, **kwargs: Any) -> list[FlowRecord]:
        return self.client.by_root(root_flow_id, **kwargs)

    def by_correlation(self, correlation_id: str, **kwargs: Any) -> list[FlowRecord]:
        return self.client.by_correlation(correlation_id, **kwargs)

    def info(self, **kwargs: Any) -> dict[Any, Any]:
        return self.client.info(self.type, **kwargs)

    def stuck(self, **kwargs: Any) -> list[FlowRecord]:
        return self.client.stuck(self.type, **kwargs)

    def cancel(self, id: str, **kwargs: Any) -> FlowRecord:
        return self.client.cancel(id, **kwargs)

    def rewind(self, id: str, **kwargs: Any) -> FlowRecord:
        return self.client.rewind(id, **kwargs)

    def spawn_children(
        self,
        parent: FlowRecord,
        children: list[ChildSpec],
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("partition_key", parent.partition_key)
        kwargs.setdefault("lease_token", parent.lease_token)
        kwargs.setdefault("fencing_token", parent.fencing_token)
        return self.client.spawn_children(parent.id, children, **kwargs)

    def child(self, id: str, *, payload: bytes = b"", partition_key: str | None = None) -> ChildSpec:
        return ChildSpec(id=id, type=self.type, payload=payload, partition_key=partition_key)

    def handle(self, job: FlowRecord) -> FlowRecord:
        handler = self._handler_for(job.state)
        try:
            outcome = handler(job)
        except Exception as exc:
            return self._handle_exception(job, exc)
        return self.apply(job, outcome)

    def apply(self, job: FlowRecord, outcome: Outcome) -> FlowRecord:
        common = {
            "lease_token": job.lease_token,
            "fencing_token": job.fencing_token,
            "partition_key": job.partition_key,
        }
        if isinstance(outcome, Transition):
            return self.client.transition(
                job.id,
                from_state=job.state,
                to_state=outcome.to_state,
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                **common,
            )
        if isinstance(outcome, Complete):
            return self.client.complete(
                job.id,
                result=outcome.result,
                payload=outcome.payload,
                **common,
            )
        if isinstance(outcome, Retry):
            return self.client.retry(
                job.id,
                error=outcome.error,
                payload=outcome.payload,
                run_at_ms=outcome.run_at_ms,
                **common,
            )
        if isinstance(outcome, Fail):
            return self.client.fail(job.id, error=outcome.error, payload=outcome.payload, **common)
        raise FerricStoreError(f"unknown workflow outcome: {outcome!r}")

    def _handle_exception(self, job: FlowRecord, exc: Exception) -> FlowRecord:
        config = self._states[job.state]
        if config.on_error == "fail":
            return self.apply(job, Fail(error=str(exc)))
        return self.apply(job, Retry(error=str(exc)))

    def _discover_states(self) -> dict[str, StateConfig]:
        states: dict[str, StateConfig] = {}
        for name in dir(self):
            member = getattr(self, name)
            target = getattr(member, "__func__", member)
            config = getattr(target, "__ferric_state__", None)
            if isinstance(config, StateConfig):
                states[config.name] = config
        return states

    def _handler_for(self, state_name: str) -> Callable[[FlowRecord], Outcome]:
        for name in dir(self):
            member = getattr(self, name)
            target = getattr(member, "__func__", member)
            config = getattr(target, "__ferric_state__", None)
            if isinstance(config, StateConfig) and config.name == state_name:
                return member
        raise FerricStoreError(f"no handler for state {state_name!r}")
