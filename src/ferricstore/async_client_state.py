from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ferricstore.adapters import AsyncCommandExecutor
from ferricstore.backpressure import BackpressureController
from ferricstore.codecs import Codec
from ferricstore.types import FlowRecord


class _AsyncClientMixinBase:
    executor: AsyncCommandExecutor
    codec: Codec
    backpressure: BackpressureController
    _transaction_mode: bool

    if TYPE_CHECKING:

        def _record(self, value: Any) -> FlowRecord: ...

        def _records(self, values: Any) -> list[FlowRecord]: ...

        async def _record_or_get(
            self,
            value: Any,
            id: str,
            partition_key: str | bytes | None = None,
        ) -> FlowRecord: ...

        async def _index_query(
            self,
            command: str,
            key: str,
            **kwargs: Any,
        ) -> list[FlowRecord]: ...

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)
