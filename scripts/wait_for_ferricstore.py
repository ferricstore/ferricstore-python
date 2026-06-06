from __future__ import annotations

import os
import socket
import time


def main() -> None:
    host = os.environ.get("FERRICSTORE_HOST", "127.0.0.1")
    port = int(os.environ.get("FERRICSTORE_PORT", "6379"))
    deadline = time.monotonic() + int(os.environ.get("FERRICSTORE_WAIT_SECONDS", "60"))

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.5)

    raise TimeoutError(f"Timed out waiting for FerricStore at {host}:{port}")


if __name__ == "__main__":
    main()
