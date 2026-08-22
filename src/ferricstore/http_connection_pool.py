from __future__ import annotations

import select
import socket
import ssl
import threading
from collections import deque
from collections.abc import Callable
from contextlib import suppress
from http.client import HTTPConnection
from time import monotonic

_ConnectionKey = tuple[bool, str, str | None]


class _KeepAlivePool:
    """Thread-safe bounded HTTP/1 connection pool shared across redirect origins."""

    def __init__(self, max_connections: int) -> None:
        self._max_connections = max_connections
        self._idle: dict[_ConnectionKey, deque[HTTPConnection]] = {}
        self._total_connections = 0
        self._closed = False
        self._condition = threading.Condition()

    def acquire(
        self,
        key: _ConnectionKey,
        create: Callable[[], HTTPConnection],
        timeout: float | None,
    ) -> HTTPConnection:
        deadline = None if timeout is None else monotonic() + timeout

        while True:
            connection, newly_created = self._take_or_create(key, create, deadline)
            if newly_created or not _connection_is_stale(connection):
                return connection
            self.release(key, connection, reusable=False)

    def release(
        self,
        key: _ConnectionKey,
        connection: HTTPConnection,
        *,
        reusable: bool,
    ) -> None:
        close_connection = not reusable
        with self._condition:
            if reusable and not self._closed:
                self._idle.setdefault(key, deque()).append(connection)
            else:
                self._total_connections -= 1
                close_connection = True
            self._condition.notify()
        if close_connection:
            connection.close()

    def close(self) -> None:
        with self._condition:
            if self._closed:
                return
            self._closed = True
            idle = [connection for connections in self._idle.values() for connection in connections]
            self._idle.clear()
            self._total_connections -= len(idle)
            self._condition.notify_all()
        for connection in idle:
            connection.close()

    def _take_or_create(
        self,
        key: _ConnectionKey,
        create: Callable[[], HTTPConnection],
        deadline: float | None,
    ) -> tuple[HTTPConnection, bool]:
        with self._condition:
            while True:
                if self._closed:
                    raise OSError("FerricStore HTTP connection pool is closed")

                idle = self._idle.get(key)
                if idle:
                    connection = idle.pop()
                    if not idle:
                        del self._idle[key]
                    return connection, False

                if self._total_connections < self._max_connections:
                    self._total_connections += 1
                    try:
                        return create(), True
                    except BaseException:
                        self._total_connections -= 1
                        self._condition.notify()
                        raise

                evicted = self._pop_idle_connection()
                if evicted is not None:
                    self._total_connections -= 1
                    evicted.close()
                    continue

                remaining = None if deadline is None else deadline - monotonic()
                if remaining is not None and remaining <= 0:
                    raise TimeoutError("timed out waiting for a FerricStore HTTP connection")
                self._condition.wait(remaining)

    def _pop_idle_connection(self) -> HTTPConnection | None:
        for key, connections in self._idle.items():
            if connections:
                connection = connections.pop()
                if not connections:
                    del self._idle[key]
                return connection
        return None


def _connection_is_stale(connection: HTTPConnection) -> bool:
    sock = connection.sock
    if sock is None:
        return True
    try:
        readable, _writable, _exceptional = select.select([sock], [], [], 0)
    except (OSError, ValueError):
        return True
    if not readable:
        return False

    previous_timeout = sock.gettimeout()
    try:
        sock.settimeout(0.0)
        flags = 0 if isinstance(sock, ssl.SSLSocket) else socket.MSG_PEEK
        sock.recv(1, flags)
        return True
    except (BlockingIOError, ssl.SSLWantReadError):
        return False
    except OSError:
        return True
    finally:
        with suppress(OSError):
            sock.settimeout(previous_timeout)


__all__ = ["_ConnectionKey", "_KeepAlivePool"]
