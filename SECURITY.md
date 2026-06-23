# Security Policy

## Supported versions

The SDK is currently public alpha.

| Version | Support |
| --- | --- |
| `0.1.x` | Security fixes best-effort |

## Reporting a vulnerability

Please do not open a public issue for vulnerabilities.

Report privately to the maintainers with:

- SDK version.
- FerricStore server version or commit.
- Description of impact.
- Minimal reproduction if possible.
- Whether credentials, ACLs, TLS, payloads, or workflow isolation are involved.

## Authentication and transport security

The SDK uses the FerricStore protocol transport. High-level `from_url` methods
accept `ferric://` for plaintext development and `ferrics://` for TLS.

Examples:

```python
QueueClient.from_url("ferric://app_user:secret@ferricstore.service:6388")
QueueClient.from_url("ferrics://app_user:secret@ferricstore.service:6389")
QueueClient.from_url(url, username="app_user", password="secret")
```

Rules:

- Use `ferrics://` or a trusted private network for production traffic.
- Use ACL users with least privilege when the FerricStore server supports ACLs.
- Do not log payloads, named values, lease tokens, fencing tokens, or credentials.
- Treat `lease_token` and `fencing_token` as authority to mutate a claimed flow.

## Payload and value safety

FerricStore treats payloads and named values as opaque bytes. `JsonCodec` is
SDK-side serialization and does not use server-side JSON commands.

Use `ValueConfig(value_max_bytes=...)` and explicit `claim_values=[...]` to avoid
unexpected large value hydration.

