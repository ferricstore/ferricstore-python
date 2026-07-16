from __future__ import annotations

import ssl
from typing import Protocol


class _TLSContextHost(Protocol):
    tls: bool
    ssl_context: ssl.SSLContext | None
    _default_ssl_context: ssl.SSLContext | None


class ProtocolTLSContextMixin:
    """Lazily create one default TLS context per adapter lifetime."""

    tls: bool
    ssl_context: ssl.SSLContext | None
    _default_ssl_context: ssl.SSLContext | None

    def _tls_context(self: _TLSContextHost) -> ssl.SSLContext | None:
        if not self.tls:
            return None
        context = self.ssl_context
        if context is None:
            context = self._default_ssl_context
            if context is None:
                context = ssl.create_default_context()
                self._default_ssl_context = context
        return context


__all__ = ["ProtocolTLSContextMixin"]
