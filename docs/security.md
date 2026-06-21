# Security

The SDK uses FerricStore native protocol transport. Use `ferric://` for plaintext private development networks and `ferrics://` when TLS is required.

## Auth and ACLs

Credentials can be embedded in the URL or passed explicitly. FerricStore server must enforce ACLs and command permissions.

```python
from ferricstore import QueueClient

client = QueueClient.from_url(
    "ferrics://app_user:secret@ferricstore.service:6389",
)
```

```python
client = QueueClient.from_url(
    "ferrics://ferricstore.service:6389",
    username="app_user",
    password="secret",
)
```

## Operational guidance

- Use TLS or a trusted private network.
- Use least-privilege ACL users.
- Do not log payloads, named values, lease tokens, fencing tokens, or credentials.
- Use deterministic flow ids for idempotent request retries.
- Cap value hydration with `ValueConfig(value_max_bytes=...)`.

## Sensitive data

Payloads and named values are opaque bytes to FerricStore. If values contain PII or secrets, handle encryption, redaction, and retention policies at the application/server deployment level.
