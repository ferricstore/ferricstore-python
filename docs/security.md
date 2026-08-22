# Security

The SDK supports direct FerricStore native transport and FerricStore HTTP
endpoints. Use `ferric://` only on trusted plaintext networks, `ferrics://` for
native TLS, and `https://` for production HTTP connections.

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

For per-user HTTP authentication, pass `username` and `password` to an
`https://` URL. The SDK sends standard HTTP Basic authentication and refuses to
send these credentials to an initial plaintext `http://` URL. A `bearer_token`
is for an endpoint configured to accept that bearer; it does not encode
a FerricStore username and password.

Standard HTTP redirects remain enabled. Redirect and gateway policy is part of
the deployment architecture: ensure every redirect destination that may
receive an authorization header is trusted, and avoid redirects from HTTPS to
plaintext HTTP when credentials are present. The HTTP/1.1 and optional HTTP/2
backends deliberately preserve the authorization header across redirects; the
SDK does not impose a same-origin policy on the user's gateway architecture.

Arbitrary command bytes are Base64-encoded only to make the JSON envelope
binary-safe; Base64 is not encryption. HTTPS is still required to protect
command data and credentials in transit.

## Operational guidance

- Use TLS or a trusted private network.
- Use least-privilege ACL users.
- Do not log payloads, named values, lease tokens, fencing tokens, or credentials.
- Use deterministic flow ids for idempotent request retries.
- Cap value hydration with `ValueConfig(value_max_bytes=...)`.

## Sensitive data

Payloads and named values are opaque bytes to FerricStore. If values contain PII or secrets, handle encryption, redaction, and retention policies at the application/server deployment level.
