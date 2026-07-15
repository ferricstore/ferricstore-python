from __future__ import annotations

from ferricstore.adapters import CommandExecutor
from ferricstore.backpressure import (
    BackpressureController,
)
from ferricstore.batch_core import (
    SyncFanoutExecutor,
)
from ferricstore.codecs import Codec


class _ClientMixinBase:
    executor: CommandExecutor
    codec: Codec
    backpressure: BackpressureController
    _transaction_mode: bool
    _fanout_executor: SyncFanoutExecutor
