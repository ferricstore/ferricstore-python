from __future__ import annotations

import os
import socket
import time


def is_ready(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5) as sock:
            sock.settimeout(0.5)
            sock.sendall(b"*1\r\n$4\r\nPING\r\n")
            return b"PONG" in sock.recv(1024).upper()
    except OSError:
        return False


def main() -> None:
    host = os.environ.get("FERRICSTORE_HOST", "127.0.0.1")
    port = int(os.environ.get("FERRICSTORE_PORT", "6379"))
    deadline = time.monotonic() + int(os.environ.get("FERRICSTORE_WAIT_SECONDS", "60"))

    while time.monotonic() < deadline:
        if is_ready(host, port):
            return
        time.sleep(0.5)

    raise TimeoutError(f"Timed out waiting for FerricStore at {host}:{port}")


if __name__ == "__main__":
    main()
