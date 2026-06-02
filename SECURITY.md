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

The SDK uses `redis-py` / `redis.asyncio` under the hood. High-level `from_url`
methods pass through Redis connection options.

Examples:

```python
QueueClient.from_url("redis://app_user:secret@ferricstore.service:6379/0")
QueueClient.from_url("rediss://app_user:secret@ferricstore.service:6380/0")
QueueClient.from_url(url, username="app_user", password="secret")
```

Rules:

- Use `rediss://` or a trusted private network for production traffic.
- Use ACL users with least privilege when the FerricStore server supports ACLs.
- Keep `decode_responses=False`; the SDK decodes Flow values itself.
- Do not log payloads, named values, lease tokens, fencing tokens, or credentials.
- Treat `lease_token` and `fencing_token` as authority to mutate a claimed flow.

## Payload and value safety

FerricStore treats payloads and named values as opaque bytes. `JsonCodec` is
SDK-side serialization and does not use Redis JSON commands.

Use `ValueConfig(value_max_bytes=...)` and explicit `claim_values=[...]` to avoid
unexpected large value hydration.

