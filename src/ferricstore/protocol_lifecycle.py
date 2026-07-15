from __future__ import annotations

import heapq
import threading
from collections.abc import Callable

from ferricstore.config_validation import validate_optional_positive_int

DEFAULT_MAX_INFLIGHT_REQUESTS = 4_096
DEFAULT_MAX_PENDING_REQUEST_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_BATCH_ITEMS = 10_000


class PendingRequestCapacityError(RuntimeError):
    """Raised before a request is written when its pending budget is exhausted."""


def check_batch_item_limit(count: int, limit: int | None) -> None:
    """Reject batches before allocating one protocol object per item."""
    if limit is not None and count > limit:
        raise PendingRequestCapacityError(f"protocol batch items exceed max_batch_items={limit}")


class PendingRequestBudget:
    """Track count and encoded-byte admission for requests awaiting responses.

    The owner supplies synchronization. Sync adapters call this while holding their
    pending-state lock; async adapters call it from their event-loop thread.
    """

    def __init__(
        self,
        *,
        max_requests: int | None,
        max_bytes: int | None,
    ) -> None:
        self.max_requests = validate_optional_positive_int(
            max_requests,
            name="max_inflight_requests",
        )
        self.max_bytes = validate_optional_positive_int(
            max_bytes,
            name="max_pending_request_bytes",
        )
        self._sizes: dict[int, int] = {}
        self._total_bytes = 0

    @property
    def count(self) -> int:
        return len(self._sizes)

    @property
    def total_bytes(self) -> int:
        return self._total_bytes

    def reserve(self, request_id: int) -> None:
        if request_id in self._sizes:
            raise RuntimeError(f"duplicate pending protocol request_id: {request_id}")
        if self.max_requests is not None and len(self._sizes) >= self.max_requests:
            raise PendingRequestCapacityError(
                f"protocol pending requests exceed max_inflight_requests={self.max_requests}"
            )
        self._sizes[request_id] = 0

    def set_size(self, request_id: int, size: int) -> None:
        if size < 0:
            raise ValueError("pending request size must be non-negative")
        previous = self._sizes.get(request_id)
        if previous is None:
            raise KeyError(request_id)
        total = self._total_bytes - previous + size
        if self.max_bytes is not None and total > self.max_bytes:
            raise PendingRequestCapacityError(
                f"protocol pending request bytes exceed max_pending_request_bytes={self.max_bytes}"
            )
        self._sizes[request_id] = size
        self._total_bytes = total

    def remaining_bytes(self, request_id: int) -> int | None:
        """Return this request's current admission headroom without mutating state."""
        previous = self._sizes.get(request_id)
        if previous is None:
            raise KeyError(request_id)
        if self.max_bytes is None:
            return None
        return self.max_bytes - (self._total_bytes - previous)

    def release(self, request_id: int) -> bool:
        size = self._sizes.pop(request_id, None)
        if size is None:
            return False
        self._total_bytes -= size
        return True

    def clear(self) -> None:
        self._sizes.clear()
        self._total_bytes = 0


class SyncDeadlineScheduler:
    """One lazy daemon thread for many monotonic request deadlines.

    Cancelled heap entries are compacted amortized, avoiding both a timer thread per
    request and retention proportional to completed throughput.
    """

    _COMPACT_MIN_STALE = 64

    def __init__(
        self,
        on_expire: Callable[[int], None],
        *,
        thread_name: str,
    ) -> None:
        self._on_expire = on_expire
        self._thread_name = thread_name
        self._condition = threading.Condition()
        self._heap: list[tuple[float, int, int]] = []
        self._active: dict[int, int] = {}
        self._sequence = 0
        self._thread: threading.Thread | None = None
        self._closed = False

    def schedule(self, request_id: int, deadline: float) -> None:
        with self._condition:
            if self._closed:
                raise RuntimeError("request deadline scheduler is closed")
            self._sequence += 1
            token = self._sequence
            previous_head = self._heap[0][0] if self._heap else None
            self._active[request_id] = token
            heapq.heappush(self._heap, (deadline, token, request_id))
            self._compact_locked()
            if self._thread is None:
                thread = threading.Thread(
                    target=self._run,
                    name=self._thread_name,
                    daemon=True,
                )
                self._thread = thread
                try:
                    thread.start()
                except BaseException:
                    if thread.ident is None:
                        self._thread = None
                        if self._active.get(request_id) == token:
                            self._active.pop(request_id, None)
                        self._heap = [entry for entry in self._heap if entry[1] != token]
                        heapq.heapify(self._heap)
                    raise
            if previous_head is None or deadline < previous_head:
                self._condition.notify()

    def cancel(self, request_id: int) -> None:
        with self._condition:
            self._active.pop(request_id, None)
            self._compact_locked()

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            self._active.clear()
            self._heap.clear()
            thread = self._thread
            self._condition.notify_all()
        if (
            thread is not None
            and thread is not threading.current_thread()
            and thread.ident is not None
        ):
            thread.join()

    def _compact_locked(self) -> None:
        active_count = len(self._active)
        stale_count = len(self._heap) - active_count
        if stale_count < self._COMPACT_MIN_STALE or len(self._heap) <= active_count * 2:
            return
        self._heap = [entry for entry in self._heap if self._active.get(entry[2]) == entry[1]]
        heapq.heapify(self._heap)

    def _run(self) -> None:
        import time

        while True:
            request_id: int
            with self._condition:
                while True:
                    if self._closed:
                        return
                    while self._heap:
                        deadline, token, candidate = self._heap[0]
                        if self._active.get(candidate) == token:
                            break
                        heapq.heappop(self._heap)
                    if not self._heap:
                        self._condition.wait()
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining > 0:
                        self._condition.wait(remaining)
                        continue
                    heapq.heappop(self._heap)
                    if self._active.get(candidate) != token:
                        continue
                    self._active.pop(candidate, None)
                    request_id = candidate
                    break
            self._on_expire(request_id)
