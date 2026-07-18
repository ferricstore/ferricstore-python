from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

SERVER_VERSION = "0.8.0"


def _load_wait_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "wait_for_ferricstore.py"
    spec = importlib.util.spec_from_file_location("wait_for_ferricstore", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_native_readiness_uses_protocol_ping(monkeypatch):
    wait = _load_wait_module()
    calls = []

    def fake_native_ready(url: str) -> bool:
        calls.append(url)
        return True

    monkeypatch.setenv("FERRICSTORE_URL", "ferric://127.0.0.1:6388")
    monkeypatch.setattr(wait, "native_is_ready", fake_native_ready)

    assert wait.is_ready("127.0.0.1", 6388) is True
    assert calls == ["ferric://127.0.0.1:6388"]


def test_non_native_readiness_uses_tcp(monkeypatch):
    wait = _load_wait_module()
    calls = []

    def fake_tcp_ready(host: str, port: int) -> bool:
        calls.append((host, port))
        return True

    monkeypatch.setenv("FERRICSTORE_URL", "tcp://127.0.0.1:6388")
    monkeypatch.setattr(wait, "tcp_is_ready", fake_tcp_ready)

    assert wait.is_ready("127.0.0.1", 6388) is True
    assert calls == [("127.0.0.1", 6388)]


def test_compose_fixtures_target_current_ferricstore_server_version():
    root = Path(__file__).resolve().parents[1]
    for name in ("docker-compose.yml", "docker-compose.cluster.yml", ".env.example"):
        text = (root / name).read_text()
        assert f"ghcr.io/ferricstore/ferricstore:{SERVER_VERSION}" in text
        assert "ghcr.io/ferricstore/ferricstore:0.7.2" not in text
        assert "ferricstore/ferricstore:latest" not in text
