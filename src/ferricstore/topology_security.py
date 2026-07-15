from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlparse

from ferricstore.config_validation import validate_bool, validate_string_sequence
from ferricstore.errors import FerricStoreError
from ferricstore.protocol_common import (
    _connection_endpoint_key,
    _endpoint_from_url,
    _map_get,
    _text_or_none,
)
from ferricstore.protocol_constants import _TLS_SCHEMES

EndpointPolicy = str | tuple[str, Sequence[str]]
EndpointValidator = Callable[[Mapping[str, Any]], bool | None]


@dataclass(frozen=True, slots=True)
class TopologyRuntimeConfig:
    """Validated security and eager-connection options shared by topology pools."""

    warm_connections: bool
    trusted_hosts: tuple[str, ...]
    endpoint_validator: EndpointValidator | None
    tls: bool

    @classmethod
    def build(
        cls,
        *,
        warm_connections: object,
        trusted_hosts: object | None,
        endpoint_validator: object | None,
        tls: object,
    ) -> TopologyRuntimeConfig:
        if endpoint_validator is not None and not callable(endpoint_validator):
            raise ValueError("endpoint_validator must be callable")
        return cls(
            warm_connections=validate_bool(
                warm_connections,
                name="warm_connections",
            ),
            trusted_hosts=(
                validate_string_sequence(trusted_hosts, name="trusted_hosts")
                if trusted_hosts is not None
                else ()
            ),
            endpoint_validator=endpoint_validator,
            tls=validate_bool(tls, name="tls"),
        )


def validate_topology_configuration(
    urls: Sequence[str],
    endpoint_policy: EndpointPolicy,
) -> None:
    """Validate seed transport and trust policy before allocating adapters."""
    secure_schemes: set[bool] = set()
    for url in urls:
        _endpoint_from_url(url)
        secure_schemes.add(urlparse(url).scheme.lower() in _TLS_SCHEMES)
    if len(secure_schemes) > 1:
        raise ValueError("topology seeds cannot mix ferric:// and ferrics:// URLs")

    if isinstance(endpoint_policy, str):
        valid = endpoint_policy in {"any", "none", "seed_hosts"}
    else:
        valid = (
            isinstance(endpoint_policy, tuple)
            and len(endpoint_policy) == 2
            and endpoint_policy[0] == "allow_hosts"
            and isinstance(endpoint_policy[1], Sequence)
            and not isinstance(endpoint_policy[1], (str, bytes))
            and all(isinstance(host, str) for host in endpoint_policy[1])
        )
    if not valid:
        raise ValueError(f"invalid endpoint_policy {endpoint_policy!r}")


class TopologyEndpointTrust:
    """Immutable, pre-normalized trust policy shared by sync and async topology pools."""

    __slots__ = (
        "_allow_hosts",
        "_policy",
        "_seed_endpoint_keys",
        "_tls",
        "_trusted_hosts",
        "_validator",
    )

    def __init__(
        self,
        *,
        policy: EndpointPolicy,
        seed_endpoint_keys: set[tuple[str, int]],
        trusted_hosts: Sequence[str],
        tls: bool,
        validator: EndpointValidator | None,
    ) -> None:
        self._policy = policy
        self._seed_endpoint_keys = frozenset(seed_endpoint_keys)
        self._trusted_hosts = frozenset(_normalized_hosts(trusted_hosts))
        self._tls = tls
        self._validator = validator
        self._allow_hosts = (
            frozenset(_normalized_hosts(policy[1]))
            if isinstance(policy, tuple) and len(policy) == 2 and policy[0] == "allow_hosts"
            else frozenset()
        )

    @property
    def trusted_hosts(self) -> frozenset[str]:
        return self._trusted_hosts

    def validate(self, endpoint: Mapping[str, Any]) -> None:
        host = _text_or_none(_map_get(endpoint, "host", "native_host"))
        if host is None:
            raise FerricStoreError("invalid learned endpoint", raw=endpoint)

        policy = self._policy
        if policy == "any":
            allowed = True
        elif policy == "none":
            allowed = self._endpoint_key(endpoint) in self._seed_endpoint_keys
        elif policy == "seed_hosts":
            allowed = (
                self._endpoint_key(endpoint) in self._seed_endpoint_keys
                or host.lower() in self._trusted_hosts
            )
        elif isinstance(policy, tuple) and len(policy) == 2 and policy[0] == "allow_hosts":
            allowed = host.lower() in self._allow_hosts
        else:
            raise FerricStoreError(f"invalid endpoint_policy {policy!r}")

        if not allowed:
            raise FerricStoreError("unsafe learned endpoint", raw=endpoint)
        if self._validator is not None and self._validator(endpoint) is False:
            raise FerricStoreError("unsafe learned endpoint", raw=endpoint)

    def _endpoint_key(self, endpoint: Mapping[str, Any]) -> tuple[str, int]:
        return _connection_endpoint_key(endpoint, tls=self._tls)


def seed_urls_by_endpoint(urls: Sequence[str]) -> dict[tuple[str, int], str]:
    """Return the first configured URL for each physical seed endpoint."""
    result: dict[tuple[str, int], str] = {}
    for url in urls:
        parsed = urlparse(url)
        key = _connection_endpoint_key(
            _endpoint_from_url(url),
            tls=parsed.scheme.lower() in _TLS_SCHEMES,
        )
        result.setdefault(key, url)
    return result


def validate_seed_credential_consistency(
    urls: Sequence[str],
    adapter_kwargs: Mapping[str, Any],
) -> None:
    """Reject ambiguous credentials for duplicate physical seed endpoints."""
    credentials_by_endpoint: dict[tuple[str, int], tuple[Any, Any]] = {}
    for url in urls:
        parsed = urlparse(url)
        key = _connection_endpoint_key(
            _endpoint_from_url(url),
            tls=parsed.scheme.lower() in _TLS_SCHEMES,
        )
        credentials = (
            adapter_kwargs["username"]
            if "username" in adapter_kwargs
            else unquote(parsed.username)
            if parsed.username is not None
            else None,
            adapter_kwargs["password"]
            if "password" in adapter_kwargs
            else unquote(parsed.password)
            if parsed.password is not None
            else None,
        )
        previous = credentials_by_endpoint.setdefault(key, credentials)
        if previous != credentials:
            raise ValueError(
                f"conflicting credentials for topology seed endpoint {key[0]}:{key[1]}"
            )


def _normalized_hosts(hosts: Sequence[str]) -> set[str]:
    return {host.lower() for host in hosts if host}


__all__ = [
    "EndpointPolicy",
    "EndpointValidator",
    "TopologyEndpointTrust",
    "seed_urls_by_endpoint",
    "validate_seed_credential_consistency",
    "validate_topology_configuration",
]
