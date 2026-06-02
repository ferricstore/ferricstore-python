# Security

The SDK uses `redis-py` and `redis.asyncio` for network transport.

## Auth and ACLs

High-level clients pass connection options through to `redis-py`, including
username, password, TLS, and pool settings.

```python
from ferricstore import QueueClient

client = QueueClient.from_url(
    "redis://app_user:secret@ferricstore.service:6379/0",
)
```

```python
client = QueueClient.from_url(
    "redis://ferricstore.service:6379/0",
    username="app_user",
    password="secret",
)
```

TLS:

```python
client = QueueClient.from_url(
    "rediss://app_user:secret@ferricstore.service:6380/0",
)
```

The SDK sends credentials. FerricStore server must enforce ACLs and command
permissions.

## Operational guidance

- Use TLS or a trusted private network.
- Use least-privilege ACL users.
- Keep `decode_responses=False`.
- Do not log payloads, named values, lease tokens, fencing tokens, or credentials.
- Use deterministic flow ids for idempotent request retries.
- Cap value hydration with `ValueConfig(value_max_bytes=...)`.

## Sensitive data

Payloads and named values are opaque bytes to FerricStore. If values contain PII
or secrets, handle encryption, redaction, and retention policies at the
application/server deployment level.

