from __future__ import annotations

import math
import random
import threading
import time
import weakref
from collections.abc import Hashable
from dataclasses import dataclass
from importlib import import_module
from typing import Any, cast

from ferricstore.config_validation import (
    validate_bool,
    validate_finite_nonnegative,
    validate_optional_nonnegative_int,
    validate_thread_wait_milliseconds,
)


@dataclass(frozen=True, slots=True)
class BackpressurePolicy:
    """Client-side overload retry policy for safe producer writes.

    This policy is intentionally used only for server-declared overload
    responses. Timeouts and disconnects can have unknown commit outcome and are
    not retried by this helper.
    """

    enabled: bool = True
    max_retries: int | None = None
    max_elapsed_ms: float | None = 30_000.0
    base_delay_ms: float = 5.0
    max_delay_ms: float = 500.0
    jitter: float = 0.25
    shared: bool = True

    def __post_init__(self) -> None:
        validate_bool(self.enabled, name="enabled")
        validate_bool(self.shared, name="shared")
        validate_optional_nonnegative_int(self.max_retries, name="max_retries")
        if self.max_elapsed_ms is not None:
            validate_finite_nonnegative(self.max_elapsed_ms, name="max_elapsed_ms")
        validate_thread_wait_milliseconds(self.base_delay_ms, name="base_delay_ms")
        validate_thread_wait_milliseconds(self.max_delay_ms, name="max_delay_ms")
        validate_finite_nonnegative(self.jitter, name="jitter")


class _BackpressureState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.blocked_until = 0.0
        self.consecutive_overloads = 0


class BackpressureController:
    _shared_states: weakref.WeakValueDictionary[Hashable, _BackpressureState] = (
        weakref.WeakValueDictionary()
    )
    _shared_states_lock = threading.Lock()

    def __init__(
        self,
        policy: BackpressurePolicy | None = None,
        *,
        scope: Hashable | None = None,
    ) -> None:
        self.policy = policy or BackpressurePolicy()
        self.scope = scope
        self._state = self._shared_state_for(scope) if self.policy.shared else _BackpressureState()

    @classmethod
    def _shared_state_for(cls, scope: Hashable | None) -> _BackpressureState:
        key: Hashable = ("default",) if scope is None else scope
        with cls._shared_states_lock:
            state = cls._shared_states.get(key)
            if state is None:
                state = _BackpressureState()
                cls._shared_states[key] = state
            return state

    def before_request(self, *, elapsed_s: float | None = None) -> bool:
        started = time.monotonic()
        while True:
            delay = self._wait_delay()
            if delay <= 0:
                return True
            current_elapsed = self._elapsed_after_wait(elapsed_s, started)
            if not self._retry_wait_fits_budget(delay, current_elapsed):
                return False
            time.sleep(delay)

    async def before_request_async(self, *, elapsed_s: float | None = None) -> bool:
        import asyncio

        started = time.monotonic()
        while True:
            delay = self._wait_delay()
            if delay <= 0:
                return True
            current_elapsed = self._elapsed_after_wait(elapsed_s, started)
            if not self._retry_wait_fits_budget(delay, current_elapsed):
                return False
            await asyncio.sleep(delay)

    def can_retry(self, attempt: int, *, elapsed_s: float | None = None) -> bool:
        if not self.policy.enabled:
            return False
        max_elapsed_ms = self.policy.max_elapsed_ms
        if (
            elapsed_s is not None
            and max_elapsed_ms is not None
            and elapsed_s >= max(max_elapsed_ms, 0.0) / 1000.0
        ):
            return False
        if self.policy.max_retries is None:
            return True
        return attempt < max(self.policy.max_retries, 0)

    def record_overload(
        self,
        attempt: int,
        retry_after_ms: int | None = None,
        *,
        elapsed_s: float | None = None,
    ) -> bool:
        delay = self._reserve_overload_delay(attempt, retry_after_ms, elapsed_s=elapsed_s)
        if delay is None:
            return False
        if delay > 0:
            time.sleep(delay)
        return True

    async def record_overload_async(
        self,
        attempt: int,
        retry_after_ms: int | None = None,
        *,
        elapsed_s: float | None = None,
    ) -> bool:
        import asyncio

        delay = self._reserve_overload_delay(attempt, retry_after_ms, elapsed_s=elapsed_s)
        if delay is None:
            return False
        if delay > 0:
            await asyncio.sleep(delay)
        return True

    def record_retry(
        self,
        retry_after_ms: int | None,
        *,
        elapsed_s: float | None = None,
    ) -> bool:
        """Honor a non-overload server retry hint without changing pressure state."""
        delay = self._retry_after_delay(retry_after_ms)
        if not self._retry_wait_fits_budget(delay, elapsed_s):
            return False
        if delay > 0:
            time.sleep(delay)
        return True

    async def record_retry_async(
        self,
        retry_after_ms: int | None,
        *,
        elapsed_s: float | None = None,
    ) -> bool:
        import asyncio

        delay = self._retry_after_delay(retry_after_ms)
        if not self._retry_wait_fits_budget(delay, elapsed_s):
            return False
        if delay > 0:
            await asyncio.sleep(delay)
        return True

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

    def _retry_wait_fits_budget(self, delay_s: float, elapsed_s: float | None) -> bool:
        max_elapsed_ms = self.policy.max_elapsed_ms
        if elapsed_s is None or max_elapsed_ms is None:
            return True
        budget_s = max(max_elapsed_ms, 0.0) / 1000.0
        return max(elapsed_s, 0.0) + max(delay_s, 0.0) < budget_s

    @staticmethod
    def _elapsed_after_wait(elapsed_s: float | None, started: float) -> float | None:
        if elapsed_s is None:
            return None
        return max(elapsed_s, 0.0) + max(time.monotonic() - started, 0.0)

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

    def _reserve_overload_delay(
        self,
        attempt: int,
        retry_after_ms: int | None,
        *,
        elapsed_s: float | None,
    ) -> float | None:
        if not self.policy.enabled:
            return 0.0

        state = self._state
        with state.lock:
            now = time.monotonic()
            next_overloads = state.consecutive_overloads + 1
            pressure_attempt = max(attempt, next_overloads - 1)
            requested_delay = max(
                self._delay_for_attempt(pressure_attempt),
                self._retry_after_delay(retry_after_ms),
            )
            wait_delay = max(state.blocked_until - now, requested_delay, 0.0)
            if not self._retry_wait_fits_budget(wait_delay, elapsed_s):
                return None
            state.consecutive_overloads = next_overloads
            state.blocked_until = max(state.blocked_until, now + requested_delay)
            return wait_delay

    def _retry_after_delay(self, retry_after_ms: int | None) -> float:
        if retry_after_ms is None:
            return 0.0
        return min(max(retry_after_ms, 0) / 1000.0, threading.TIMEOUT_MAX)

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
            delay *= random.uniform(low, high)  # nosec B311
        return min(delay, cap)


def backpressure_scope_for(executor: Any) -> Hashable:
    """Return a stable overload domain without coupling this module to transports."""
    explicit = getattr(executor, "backpressure_scope", None)
    if callable(explicit):
        explicit = explicit()
    if explicit is not None:
        try:
            hash(explicit)
        except TypeError:
            return ("executor", id(executor))
        return cast(Hashable, explicit)
    return ("executor", id(executor))


def __getattr__(name: str) -> Any:
    if name == "asyncio":
        value = import_module("asyncio")
        globals()[name] = value
        return value
    raise AttributeError(name)
