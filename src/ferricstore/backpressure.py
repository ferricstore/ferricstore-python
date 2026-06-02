from __future__ import annotations

import asyncio
import math
import random
import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BackpressurePolicy:
    """Client-side overload retry policy for safe producer writes.

    This policy is intentionally used only for server-declared overload
    responses. Timeouts and disconnects can have unknown commit outcome and are
    not retried by this helper.
    """

    enabled: bool = True
    max_retries: int | None = None
    base_delay_ms: float = 5.0
    max_delay_ms: float = 500.0
    jitter: float = 0.25
    shared: bool = True


class _BackpressureState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.blocked_until = 0.0
        self.consecutive_overloads = 0


class BackpressureController:
    _shared_state = _BackpressureState()

    def __init__(self, policy: BackpressurePolicy | None = None) -> None:
        self.policy = policy or BackpressurePolicy()
        self._state = self._shared_state if self.policy.shared else _BackpressureState()

    def before_request(self) -> None:
        delay = self._wait_delay()
        if delay > 0:
            time.sleep(delay)

    async def before_request_async(self) -> None:
        delay = self._wait_delay()
        if delay > 0:
            await asyncio.sleep(delay)

    def can_retry(self, attempt: int) -> bool:
        if not self.policy.enabled:
            return False
        if self.policy.max_retries is None:
            return True
        return attempt < max(self.policy.max_retries, 0)

    def record_overload(self, attempt: int, retry_after_ms: int | None = None) -> None:
        delay = self._record_overload_delay(attempt, retry_after_ms)
        if delay > 0:
            time.sleep(delay)

    async def record_overload_async(self, attempt: int, retry_after_ms: int | None = None) -> None:
        delay = self._record_overload_delay(attempt, retry_after_ms)
        if delay > 0:
            await asyncio.sleep(delay)

    def record_success(self) -> None:
        if not self.policy.enabled:
            return
        state = self._state
        with state.lock:
            if state.consecutive_overloads > 0:
                state.consecutive_overloads -= 1

    def _wait_delay(self) -> float:
        if not self.policy.enabled:
            return 0.0
        now = time.monotonic()
        state = self._state
        with state.lock:
            return max(state.blocked_until - now, 0.0)

    def _record_overload_delay(self, attempt: int, retry_after_ms: int | None = None) -> float:
        if not self.policy.enabled:
            return 0.0

        state = self._state
        with state.lock:
            state.consecutive_overloads += 1
            pressure_attempt = max(attempt, state.consecutive_overloads - 1)
            delay = max(
                self._delay_for_attempt(pressure_attempt),
                self._retry_after_delay(retry_after_ms),
            )
            state.blocked_until = max(state.blocked_until, time.monotonic() + delay)
            return delay

    def _retry_after_delay(self, retry_after_ms: int | None) -> float:
        if retry_after_ms is None:
            return 0.0
        return max(retry_after_ms, 0) / 1000.0

    def _delay_for_attempt(self, attempt: int) -> float:
        base = max(self.policy.base_delay_ms, 0.0) / 1000.0
        cap = max(self.policy.max_delay_ms, 0.0) / 1000.0
        if base <= 0 or cap <= 0:
            return 0.0
        attempt = max(attempt, 0)

        if base >= cap:
            delay = cap
        else:
            max_exp_attempt = max(math.ceil(math.log2(cap / base)), 0)
            delay = cap if attempt >= max_exp_attempt else base * (2**attempt)

        jitter = max(self.policy.jitter, 0.0)
        if jitter > 0:
            low = max(1.0 - jitter, 0.0)
            high = 1.0 + jitter
            delay *= random.uniform(low, high)
        return min(delay, cap)
