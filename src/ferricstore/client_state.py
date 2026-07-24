from __future__ import annotations

from ferricstore.adapters import FlowQueryCommandExecutor
from ferricstore.backpressure import (
    BackpressureController,
)
from ferricstore.batch_core import (
    SyncFanoutExecutor,
)
from ferricstore.codecs import Codec


class _ClientMixinBase:
    executor: FlowQueryCommandExecutor
    codec: Codec
    backpressure: BackpressureController
    _transaction_mode: bool
    _fanout_executor: SyncFanoutExecutor
