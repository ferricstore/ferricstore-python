from __future__ import annotations

from base64 import b64encode
from collections.abc import Mapping
from urllib.parse import urlparse


def validate_base_url(url: str) -> str:
    if not isinstance(url, str) or not url:
        raise ValueError("HTTP endpoint URL must be a non-empty string")
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ValueError("FerricStore HTTP endpoint URLs must use http:// or https://")
    if not parsed.hostname:
        raise ValueError("FerricStore HTTP endpoint URL must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("HTTP endpoint credentials must use explicit options, not URL user info")
    if parsed.query or parsed.fragment:
        raise ValueError("FerricStore HTTP endpoint URL cannot contain a query or fragment")
    return url.rstrip("/")


def require_https_for_basic_credentials(
    base_url: str,
    username: str | None,
    password: str | None,
) -> None:
    if (username is not None or password is not None) and urlparse(
        base_url
    ).scheme.lower() != "https":
        raise ValueError("HTTP Basic username/password credentials require an https:// URL")


def validate_path(path: str) -> str:
    if not path.startswith("/") or "\r" in path or "\n" in path:
        raise ValueError("HTTP endpoint request path must be an absolute safe path")
    return path


def validate_timeout(value: float | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ValueError("timeout must be positive or None")
    return float(value)


def validate_positive_int(value: int, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def validated_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for name, value in (headers or {}).items():
        if not isinstance(name, str) or not name or not isinstance(value, str):
            raise TypeError("HTTP headers must map non-empty strings to strings")
        if "\r" in name or "\n" in name or "\r" in value or "\n" in value:
            raise ValueError("HTTP headers cannot contain newlines")
        result[name] = value
    return result


def build_headers(
    headers: Mapping[str, str] | None,
    bearer_token: str | None,
    username: str | None,
    password: str | None,
) -> dict[str, str]:
    result = validated_headers(headers)
    has_authorization = any(name.lower() == "authorization" for name in result)

    if bearer_token is not None and (username is not None or password is not None):
        raise ValueError("bearer_token and username/password credentials are mutually exclusive")

    if bearer_token is not None:
        if not isinstance(bearer_token, str) or not bearer_token:
            raise ValueError("bearer_token must be a non-empty string")
        if "\r" in bearer_token or "\n" in bearer_token:
            raise ValueError("bearer_token cannot contain newlines")
        if has_authorization:
            raise ValueError("bearer_token and an Authorization header are mutually exclusive")
        result["Authorization"] = f"Bearer {bearer_token}"

    if username is not None and password is None:
        raise ValueError("username requires password for HTTP Basic authentication")

    if password is not None:
        resolved_username = username if username is not None else "default"
        if not isinstance(resolved_username, str) or not resolved_username:
            raise ValueError("username must be a non-empty string")
        if not isinstance(password, str):
            raise TypeError("password must be a string")
        if ":" in resolved_username:
            raise ValueError("username cannot contain ':' for HTTP Basic authentication")
        if any(value in resolved_username or value in password for value in ("\r", "\n")):
            raise ValueError("username and password cannot contain newlines")
        if has_authorization:
            raise ValueError("username/password and an Authorization header are mutually exclusive")

        encoded = b64encode(f"{resolved_username}:{password}".encode()).decode("ascii")
        result["Authorization"] = f"Basic {encoded}"
    return result


__all__ = [
    "build_headers",
    "require_https_for_basic_credentials",
    "validate_base_url",
    "validate_path",
    "validate_positive_int",
    "validate_timeout",
    "validated_headers",
]
