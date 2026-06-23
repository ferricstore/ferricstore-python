from __future__ import annotations

import os
import socket
import time
from urllib.parse import urlparse


def tcp_is_ready(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def native_is_ready(url: str) -> bool:
    try:
        from ferricstore import FlowClient

        client = FlowClient.from_url(url, timeout=1.0)
        try:
            return client.command("PING") in (b"PONG", "PONG")
        finally:
            client.close()
    except Exception:
        return False


def readiness_url(host: str, port: int) -> str:
    explicit = os.environ.get("FERRICSTORE_URL")
    if explicit:
        return explicit
    return f"ferric://{host}:{port}"


def is_ready(host: str, port: int) -> bool:
    url = readiness_url(host, port)
    parsed = urlparse(url)
    if parsed.scheme in {"ferric", "ferrics"}:
        return native_is_ready(url)
    return tcp_is_ready(host, port)


def main() -> None:
    host = os.environ.get("FERRICSTORE_HOST", "127.0.0.1")
    port = int(
        os.environ.get("FERRICSTORE_PORT", os.environ.get("FERRICSTORE_NATIVE_PORT", "6388"))
    )
    deadline = time.monotonic() + int(os.environ.get("FERRICSTORE_WAIT_SECONDS", "180"))

    while time.monotonic() < deadline:
        if is_ready(host, port):
            return
        time.sleep(0.5)

    raise TimeoutError(f"Timed out waiting for FerricStore at {host}:{port}")


if __name__ == "__main__":
    main()
