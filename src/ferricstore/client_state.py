from __future__ import annotations

from typing import Any

from ferricstore.backpressure import (
    BackpressureController,
)
from ferricstore.batch_core import (
    SyncFanoutExecutor,
)
from ferricstore.codecs import Codec


class _ClientMixinBase:
    executor: Any
    codec: Codec
    backpressure: BackpressureController
    _transaction_mode: bool
    _fanout_executor: SyncFanoutExecutor
