from __future__ import annotations

import builtins
from collections.abc import Callable, Sequence
from typing import Any, cast
from urllib.parse import urlparse

from ferricstore.adapters import CommandExecutor
from ferricstore.backpressure import (
    BackpressureController,
    BackpressurePolicy,
    backpressure_scope_for,
)
from ferricstore.batch_core import (
    SyncFanoutExecutor,
)
from ferricstore.client_autobatch import AutobatchFlowClient
from ferricstore.client_claims import _ClientClaimsMixin
from ferricstore.client_effects import _ClientEffectsMixin
from ferricstore.client_governance import _ClientGovernanceMixin
from ferricstore.client_management import _ClientManagementMixin
from ferricstore.client_markers import SyncFlowClientMarker
from ferricstore.client_mutations import _ClientMutationsMixin
from ferricstore.client_producer import _ClientProducerMixin
from ferricstore.client_queries import _ClientQueriesMixin
from ferricstore.client_schedules import _ClientSchedulesMixin
from ferricstore.client_sessions import (
    CommandPipeline,
    PubSubSession,
    TransactionSession,
    _ErrorMappingExecutor,
)
from ferricstore.client_state import _ClientMixinBase
from ferricstore.client_support import _ClientSupportMixin
from ferricstore.client_values import _ClientValuesMixin
from ferricstore.codecs import Codec, RawCodec
from ferricstore.command_core import normalize_command_name
from ferricstore.commands import DataCommandsMixin
from ferricstore.config_validation import validate_string_sequence
from ferricstore.errors import InvalidCommandError
from ferricstore.lifecycle_core import (
    close_resources_sync,
)


class _ClientCoreMixin(_ClientMixinBase):
    def __init__(
        self,
        executor: CommandExecutor,
        codec: Codec | None = None,
        *,
        backpressure: BackpressurePolicy | None = None,
    ) -> None:
        self.executor = _ErrorMappingExecutor(executor)
        self.codec = codec if codec is not None else RawCodec()
        self.backpressure = BackpressureController(
            backpressure,
            scope=backpressure_scope_for(executor),
        )
        self._transaction_mode = False
        self._fanout_executor = SyncFanoutExecutor(thread_name_prefix="ferricstore-client-fanout")

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        codec: Codec | None = None,
        backpressure: BackpressurePolicy | None = None,
        **kwargs: Any,
    ) -> FlowClient:
        if urlparse(url).scheme.lower() not in {"ferric", "ferrics"}:
            raise ValueError("FerricStore SDK URLs must use ferric:// or ferrics://")

        from ferricstore.protocol import ProtocolAdapterPool

        return cast(
            FlowClient,
            cls(
                ProtocolAdapterPool.from_url(url, **kwargs),
                codec=codec,
                backpressure=backpressure,
            ),
        )

    @classmethod
    def from_urls(
        cls,
        urls: Sequence[str],
        *,
        codec: Codec | None = None,
        backpressure: BackpressurePolicy | None = None,
        **kwargs: Any,
    ) -> FlowClient:
        resolved_urls = validate_string_sequence(urls, name="urls", allow_empty=False)
        for url in resolved_urls:
            if urlparse(url).scheme.lower() not in {"ferric", "ferrics"}:
                raise ValueError("FerricStore SDK URLs must use ferric:// or ferrics://")

        from ferricstore.protocol import ProtocolAdapterPool

        return cast(
            FlowClient,
            cls(
                ProtocolAdapterPool.from_urls(list(resolved_urls), **kwargs),
                codec=codec,
                backpressure=backpressure,
            ),
        )

    def autobatch(
        self,
        *,
        max_batch: int = 100,
        max_delay_ms: float = 1.0,
        max_pending: int = 10_000,
    ) -> AutobatchFlowClient:
        return AutobatchFlowClient(
            cast(FlowClient, self),
            max_batch=max_batch,
            max_delay_ms=max_delay_ms,
            max_pending=max_pending,
        )

    def command(self, *args: Any) -> Any:
        if not args:
            return self.executor.execute_command(*args)

        name = normalize_command_name(args[0])
        tx_control = name in {"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH"}
        raw_executor = getattr(self.executor, "_executor", self.executor)
        if tx_control and getattr(raw_executor, "requires_explicit_session", False):
            raise InvalidCommandError(
                "connection-affine transaction commands require client.transaction()"
            )
        starting_transaction = name == "MULTI" and not self._transaction_mode
        ending_transaction = name in {"EXEC", "DISCARD"} and self._transaction_mode

        if starting_transaction:
            self._pause_heartbeat()

        try:
            if self._transaction_mode and not tx_control:
                result = self.executor.execute_command("COMMAND_EXEC", args[0], *args[1:])
            else:
                result = self.executor.execute_command(*args)
        except BaseException:
            if starting_transaction:
                self._resume_heartbeat()
            raise
        finally:
            if name in {"EXEC", "DISCARD"}:
                self._transaction_mode = False
                if ending_transaction:
                    self._resume_heartbeat()

        if name == "MULTI":
            self._transaction_mode = True

        return result

    def _pause_heartbeat(self) -> None:
        pause = getattr(self.executor, "pause_heartbeat", None)
        if callable(pause):
            pause()

    def _resume_heartbeat(self) -> None:
        resume = getattr(self.executor, "resume_heartbeat", None)
        if callable(resume):
            resume()

    def refresh_topology(self) -> Any:
        refresh = getattr(self.executor, "refresh_topology", None)
        if not callable(refresh):
            raise RuntimeError("topology refresh requires a topology-aware protocol executor")
        return refresh()

    def route(self, key: str | bytes) -> Any:
        route = getattr(self.executor, "route", None)
        if not callable(route):
            raise RuntimeError("route lookup requires a topology-aware protocol executor")
        return route(key)

    def pipeline(self) -> CommandPipeline:
        return CommandPipeline(cast(FlowClient, self))

    def transaction(
        self,
        key: str | bytes | None = None,
        *,
        watch: Sequence[str] | None = None,
    ) -> TransactionSession:
        return TransactionSession(cast(FlowClient, self), key=key, watch=watch)

    def pubsub_session(self) -> PubSubSession:
        return PubSubSession(cast(FlowClient, self))

    def _acquire_session_client(
        self,
        keys: Sequence[str | bytes] | None = None,
    ) -> tuple[FlowClient, bool]:
        raw_executor = getattr(self.executor, "_executor", self.executor)
        session_keys = tuple(keys or ())
        acquire_session = getattr(raw_executor, "acquire_session_for_keys", None)
        if callable(acquire_session) and session_keys:
            session_executor = acquire_session(session_keys)
            session_client = FlowClient(session_executor, codec=self.codec)
            session_client.backpressure = self.backpressure
            return session_client, True
        acquire_session_for_key = (
            getattr(raw_executor, "acquire_session_for_key", None) if session_keys else None
        )
        if callable(acquire_session_for_key):
            session_executor = acquire_session_for_key(session_keys[0])
        else:
            acquire_session = getattr(raw_executor, "acquire_session", None)
            if not callable(acquire_session):
                return cast(FlowClient, self), False
            session_executor = acquire_session()
        session_client = FlowClient(session_executor, codec=self.codec)
        session_client.backpressure = self.backpressure
        return session_client, True

    def _acquire_subscription_client(self) -> tuple[FlowClient, bool]:
        """Acquire a connection owned for the lifetime of one event subscription."""
        raw_executor = getattr(self.executor, "_executor", self.executor)
        acquire_session = getattr(raw_executor, "acquire_dedicated_session", None)
        if not callable(acquire_session):
            return self._acquire_session_client()
        session_executor = acquire_session()
        session_client = FlowClient(session_executor, codec=self.codec)
        session_client.backpressure = self.backpressure
        return session_client, True

    def close(self) -> None:
        if self._transaction_mode:
            self._transaction_mode = False
            self._resume_heartbeat()
        close = getattr(self.executor, "close", None)
        resources: list[Callable[[], Any]] = [self._fanout_executor.close]
        if callable(close):
            resources.append(close)
        close_resources_sync(resources)

    def _invalidate_connection(self) -> None:
        raw_executor = getattr(self.executor, "_executor", self.executor)
        invalidate = getattr(raw_executor, "invalidate", None)
        if callable(invalidate):
            invalidate()

    def subscribe_flow_wake(
        self,
        type: str,
        *,
        state: str | None = None,
        states: builtins.list[str] | None = None,
        partition_key: str | None = None,
        partition_keys: builtins.list[str] | None = None,
        priority: int | None = 0,
        limit: int | None = None,
    ) -> Any:
        subscribe = getattr(self.executor, "subscribe_flow_wake", None)
        if not callable(subscribe):
            raise RuntimeError("FLOW_WAKE subscriptions require protocol executor event support")
        return subscribe(
            type,
            state=state,
            states=states,
            partition_key=partition_key,
            partition_keys=partition_keys,
            priority=priority,
            limit=limit,
        )

    def wait_event(self, timeout: float | None = None) -> Any | None:
        wait_event = getattr(self.executor, "wait_event", None)
        if not callable(wait_event):
            return None
        return wait_event(timeout=timeout)


class FlowClient(
    _ClientCoreMixin,
    _ClientManagementMixin,
    _ClientProducerMixin,
    _ClientValuesMixin,
    _ClientClaimsMixin,
    _ClientMutationsMixin,
    _ClientQueriesMixin,
    _ClientSchedulesMixin,
    _ClientEffectsMixin,
    _ClientGovernanceMixin,
    _ClientSupportMixin,
    DataCommandsMixin,
    SyncFlowClientMarker,
):
    """FerricFlow client over the FerricStore native protocol."""
