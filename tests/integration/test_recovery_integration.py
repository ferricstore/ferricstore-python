from __future__ import annotations

import os
import subprocess
import time
import uuid
from pathlib import Path

import pytest

from ferricstore import FerricStoreError, FlowClient, RawCodec

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_RECOVERY_INTEGRATION") != "1",
    reason="set FERRICSTORE_RECOVERY_INTEGRATION=1 to run restart recovery tests",
)


def _compose(*args: str, timeout: int = 90) -> None:
    project = os.environ.get("FERRICSTORE_RECOVERY_PROJECT", "ferricstore-python-recovery")
    compose_file = Path(os.environ.get("FERRICSTORE_RECOVERY_COMPOSE_FILE", "docker-compose.yml"))
    subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(compose_file), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_existing_client_recovers_after_prolonged_server_outage() -> None:
    url = os.environ.get("FERRICSTORE_URL", "ferric://127.0.0.1:56410")
    service = os.environ.get("FERRICSTORE_RECOVERY_SERVICE", "ferricstore")
    outage_seconds = float(os.environ.get("FERRICSTORE_OUTAGE_SECONDS", "3"))
    key = f"py-sdk-recovery:{uuid.uuid4().hex}"
    client = FlowClient.from_url(url, codec=RawCodec(), timeout=0.4, max_connections=2)
    stopped = False
    try:
        assert client.set(key, b"before") in {b"OK", "OK", True}
        _compose("stop", "-t", "1", service)
        stopped = True

        failures = 0
        deadline = time.monotonic() + outage_seconds
        while time.monotonic() < deadline:
            started = time.monotonic()
            with pytest.raises((FerricStoreError, OSError, TimeoutError, ConnectionError)):
                client.command("GET", key)
            assert time.monotonic() - started < 2.0
            failures += 1
            time.sleep(0.05)
        assert failures >= 2

        _compose("start", service)
        stopped = False
        recovery_deadline = time.monotonic() + 60
        last_error: BaseException | None = None
        while time.monotonic() < recovery_deadline:
            try:
                if client.command("GET", key) == b"before":
                    break
            except (FerricStoreError, OSError, TimeoutError, ConnectionError) as exc:
                last_error = exc
            time.sleep(0.1)
        else:
            raise AssertionError(f"existing client did not recover: {last_error!r}")

        assert client.set(key, b"after") in {b"OK", "OK", True}
        assert client.command("GET", key) == b"after"
    finally:
        if stopped:
            _compose("start", service)
        try:
            client.delete(key)
        finally:
            client.close()
