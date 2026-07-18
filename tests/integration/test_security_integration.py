from __future__ import annotations

import asyncio
import os
import ssl
import uuid

import pytest

from ferricstore import AsyncFlowClient, FerricStoreError, FlowClient, RawCodec

pytestmark = pytest.mark.skipif(
    os.environ.get("FERRICSTORE_SECURITY_INTEGRATION") != "1",
    reason="set FERRICSTORE_SECURITY_INTEGRATION=1 to run TLS/auth integration tests",
)


def _tls_url() -> str:
    return os.environ.get("FERRICSTORE_TLS_URL", "ferrics://127.0.0.1:56421")


def _plain_url() -> str:
    return os.environ.get("FERRICSTORE_PLAIN_URL", "ferric://127.0.0.1:56420")


def _context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=os.environ["FERRICSTORE_TLS_CA_FILE"])


def test_tls_verification_and_plaintext_rejection() -> None:
    trusted = FlowClient.from_url(_tls_url(), ssl_context=_context(), timeout=1.0)
    try:
        assert trusted.ping() in {b"PONG", "PONG"}
    finally:
        trusted.close()

    untrusted: FlowClient | None = None
    try:
        with pytest.raises((FerricStoreError, OSError)):
            untrusted = FlowClient.from_url(_tls_url(), timeout=0.5)
            untrusted.ping()
    finally:
        if untrusted is not None:
            untrusted.close()

    plaintext: FlowClient | None = None
    try:
        with pytest.raises((FerricStoreError, OSError, TimeoutError)):
            plaintext = FlowClient.from_url(_plain_url(), timeout=0.5)
            plaintext.ping()
    finally:
        if plaintext is not None:
            plaintext.close()


def test_tls_acl_authentication_and_authorization_for_sync_and_async_clients() -> None:
    suffix = uuid.uuid4().hex
    username = f"sdk-user-{suffix}"
    readonly = f"sdk-readonly-{suffix}"
    secret = f"secret-{suffix}"
    key = f"sdk-security:{suffix}"
    large_key = f"{key}:large"
    large_value = b"x" * (65 * 1024)
    admin = FlowClient.from_url(_tls_url(), codec=RawCodec(), ssl_context=_context(), timeout=2.0)
    try:
        admin.acl_set_user(username, ["on", f">{secret}", "~*", "+@all"])
        admin.acl_set_user(
            readonly,
            ["on", f">{secret}", "~*", "-@all", "+ping", "+get"],
        )
        admin.set(key, b"secured")

        authenticated = FlowClient.from_url(
            _tls_url(),
            username=username,
            password=secret,
            ssl_context=_context(),
            timeout=2.0,
        )
        try:
            assert authenticated.ping() in {b"PONG", "PONG"}
            assert authenticated.command("GET", key) == b"secured"
            assert authenticated.command("SET", large_key, large_value) in {b"OK", "OK"}
            assert authenticated.command("GET", large_key) == large_value
        finally:
            authenticated.close()

        denied = FlowClient.from_url(
            _tls_url(),
            username=readonly,
            password=secret,
            ssl_context=_context(),
            timeout=2.0,
        )
        try:
            assert denied.command("GET", key) == b"secured"
            with pytest.raises(FerricStoreError):
                denied.command("SET", key, b"denied")
        finally:
            denied.close()

        wrong: FlowClient | None = None
        try:
            with pytest.raises(FerricStoreError):
                wrong = FlowClient.from_url(
                    _tls_url(),
                    username=username,
                    password="wrong-secret",
                    ssl_context=_context(),
                    timeout=1.0,
                )
                wrong.ping()
        finally:
            if wrong is not None:
                wrong.close()

        async def exercise_async() -> None:
            client = AsyncFlowClient.from_url(
                _tls_url(),
                username=username,
                password=secret,
                ssl_context=_context(),
                timeout=2.0,
            )
            try:
                assert await client.ping() in {b"PONG", "PONG"}
                assert await client.command("GET", key) == b"secured"
                assert await client.command("SET", large_key, large_value) in {b"OK", "OK"}
                assert await client.command("GET", large_key) == large_value
            finally:
                await client.close()

        asyncio.run(exercise_async())
    finally:
        try:
            admin.delete(key, large_key)
            admin.acl_del_user(username)
            admin.acl_del_user(readonly)
        finally:
            admin.close()
