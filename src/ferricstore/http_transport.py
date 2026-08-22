from __future__ import annotations

import importlib
import json
import socket
import ssl
from collections.abc import Mapping
from contextvars import ContextVar
from http.client import HTTPConnection, HTTPException, HTTPResponse, HTTPSConnection
from time import monotonic
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import (
    HTTPHandler,
    HTTPSHandler,
    OpenerDirector,
    Request,
    build_opener,
    getproxies,
    proxy_bypass,
)

from ferricstore import http_validation
from ferricstore.errors import HttpError, OverloadedError
from ferricstore.http_compact_codec import MessagePackCodec
from ferricstore.http_connection_pool import _ConnectionKey, _KeepAlivePool

DEFAULT_MAX_HTTP_RESPONSE_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_HTTP_REQUEST_BYTES = 1024 * 1024


class _Http2BackendError(Exception):
    def __init__(self, reason: Any, *, timed_out: bool) -> None:
        super().__init__(str(reason))
        self.reason = reason
        self.timed_out = timed_out


class _Http2Backend:
    def __init__(
        self,
        *,
        max_connections: int,
        ssl_context: ssl.SSLContext | None,
    ) -> None:
        try:
            httpx = importlib.import_module("httpx")
        except ImportError as exc:
            raise ImportError(
                "HTTP/2 support requires the 'ferricstore[http2]' optional dependency"
            ) from exc

        self._httpx = httpx
        self._client = httpx.Client(
            http2=True,
            follow_redirects=False,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_connections,
            ),
            verify=ssl_context if ssl_context is not None else True,
        )

    def close(self) -> None:
        self._client.close()

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        data: bytes | None,
        deadline: _HttpDeadline,
        max_response_bytes: int,
    ) -> tuple[int, Any, bytes]:
        current_method = method
        current_url = url
        current_headers = dict(headers)
        current_data = data

        try:
            for _redirect in range(11):
                request = self._client.build_request(
                    current_method,
                    current_url,
                    headers=current_headers,
                    content=current_data,
                    timeout=deadline.remaining(),
                )
                response = self._client.send(request, stream=True)

                if response.status_code in {301, 302, 303, 307, 308}:
                    location = response.headers.get("Location")
                    if location is not None and _redirect < 10:
                        _read_http2_bounded(response, max_response_bytes)
                        current_url = urljoin(str(response.request.url), location)
                        current_method, current_data, current_headers = _redirect_request(
                            response.status_code,
                            current_method,
                            current_data,
                            current_headers,
                        )
                        response.close()
                        continue

                try:
                    raw = _read_http2_bounded(response, max_response_bytes)
                    return int(response.status_code), response.headers, raw
                finally:
                    response.close()

            raise _Http2BackendError("too many HTTP redirects", timed_out=False)
        except self._httpx.TimeoutException as exc:
            raise _Http2BackendError(exc, timed_out=True) from exc
        except self._httpx.HTTPError as exc:
            raise _Http2BackendError(exc, timed_out=False) from exc


def _redirect_request(
    status: int,
    method: str,
    data: bytes | None,
    headers: dict[str, str],
) -> tuple[str, bytes | None, dict[str, str]]:
    switch_to_get = (status == 303 and method != "HEAD") or (
        status in {301, 302} and method == "POST"
    )
    if not switch_to_get:
        return method, data, headers

    redirected_headers = {
        name: value
        for name, value in headers.items()
        if name.lower() not in {"content-length", "content-type", "transfer-encoding"}
    }
    return "GET", None, redirected_headers


def _read_http2_bounded(response: Any, limit: int) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length is not None:
        try:
            declared = int(content_length)
        except ValueError:
            declared = -1
        if declared > limit:
            raise HttpError(
                "FerricStore HTTP endpoint response exceeds max_response_bytes",
                error_code="response_too_large",
                retryable=False,
                safe_to_retry=False,
            )

    chunks: list[bytes] = []
    size = 0
    for chunk in response.iter_bytes():
        size += len(chunk)
        if size > limit:
            raise HttpError(
                "FerricStore HTTP endpoint response exceeds max_response_bytes",
                error_code="response_too_large",
                retryable=False,
                safe_to_retry=False,
            )
        chunks.append(chunk)
    return b"".join(chunks)


class _HttpDeadline:
    """One monotonic deadline shared by every stage of an HTTP operation."""

    def __init__(self, timeout: float | None) -> None:
        self._expires_at = None if timeout is None else monotonic() + timeout

    @property
    def expires_at(self) -> float | None:
        return self._expires_at

    def remaining(self) -> float | None:
        if self._expires_at is None:
            return None
        remaining = self._expires_at - monotonic()
        if remaining <= 0:
            raise TimeoutError("FerricStore HTTP request deadline exceeded")
        return remaining


_ACTIVE_HTTP_DEADLINE: ContextVar[_HttpDeadline | None] = ContextVar(
    "ferricstore_http_deadline",
    default=None,
)


class _PooledResponse:
    """urllib-compatible response that returns a fully consumed connection."""

    def __init__(
        self,
        response: HTTPResponse,
        *,
        url: str,
        pool: _KeepAlivePool,
        key: _ConnectionKey,
        connection: HTTPConnection,
        deadline: _HttpDeadline | None,
    ) -> None:
        self._response = response
        self._pool = pool
        self._key = key
        self._connection = connection
        self._deadline = deadline
        self._closed = False
        self.url = url
        self.status = response.status
        self.code = response.status
        self.reason = response.reason
        self.msg = response.reason
        self.headers = response.headers

    def read(self, amount: int | None = None) -> bytes:
        self._apply_deadline()
        if amount is None:
            return self._response.read()
        return self._response.read(amount)

    def read1(self, amount: int = -1) -> bytes:
        self._apply_deadline()
        chunk = self._response.read1(amount)
        if self._response.length == 0 and not self._response.isclosed():
            self._response.read()
        return chunk

    def info(self) -> Any:
        return self.headers

    def geturl(self) -> str:
        return self.url

    def getcode(self) -> int:
        return self.code

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        reusable = (
            self._response.isclosed()
            and not self._response.will_close
            and self._connection.sock is not None
        )
        self._response.close()
        self._pool.release(self._key, self._connection, reusable=reusable)

    def __enter__(self) -> _PooledResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def _apply_deadline(self) -> None:
        if self._deadline is None:
            return
        timeout = self._deadline.remaining()
        self._connection.timeout = timeout
        if self._connection.sock is not None:
            self._connection.sock.settimeout(timeout)


class _PooledHTTPHandler(HTTPHandler):
    def __init__(self, pool: _KeepAlivePool) -> None:
        super().__init__()
        self._pool = pool
        self._debuglevel = 0

    def http_open(self, request: Request) -> HTTPResponse:
        return cast(
            HTTPResponse,
            _open_pooled_request(
                request,
                secure=False,
                pool=self._pool,
                ssl_context=None,
                debuglevel=self._debuglevel,
            ),
        )


class _PooledHTTPSHandler(HTTPSHandler):
    def __init__(self, pool: _KeepAlivePool, context: ssl.SSLContext | None) -> None:
        super().__init__(context=context)
        self._pool = pool
        self._ssl_context = context
        self._debuglevel = 0

    def https_open(self, request: Request) -> HTTPResponse:
        return cast(
            HTTPResponse,
            _open_pooled_request(
                request,
                secure=True,
                pool=self._pool,
                ssl_context=self._ssl_context,
                debuglevel=self._debuglevel,
            ),
        )


def _open_pooled_request(
    request: Request,
    *,
    secure: bool,
    pool: _KeepAlivePool,
    ssl_context: ssl.SSLContext | None,
    debuglevel: int,
) -> _PooledResponse:
    host = request.host
    if not host:
        raise URLError("no host given")

    headers = dict(request.unredirected_hdrs)
    headers.update({name: value for name, value in request.headers.items() if name not in headers})
    headers = {name.title(): value for name, value in headers.items()}

    tunnel_host = cast(str | None, getattr(request, "_tunnel_host", None))
    tunnel_headers: dict[str, str] = {}
    if tunnel_host:
        proxy_authorization = "Proxy-Authorization"
        if proxy_authorization in headers:
            tunnel_headers[proxy_authorization] = headers.pop(proxy_authorization)

    key = (secure, host, tunnel_host)
    deadline = _ACTIVE_HTTP_DEADLINE.get()

    def remaining_timeout() -> float | None:
        return deadline.remaining() if deadline is not None else request.timeout

    def create_connection() -> HTTPConnection:
        timeout = remaining_timeout()
        if secure:
            connection: HTTPConnection = HTTPSConnection(
                host,
                timeout=timeout,
                context=ssl_context,
            )
        else:
            connection = HTTPConnection(host, timeout=timeout)
        connection.set_debuglevel(debuglevel)
        if tunnel_host:
            connection.set_tunnel(tunnel_host, headers=tunnel_headers)
        return connection

    connection = pool.acquire(key, create_connection, remaining_timeout())
    timeout = remaining_timeout()
    connection.timeout = timeout
    if connection.sock is not None:
        connection.sock.settimeout(timeout)

    try:
        connection.request(
            request.get_method(),
            request.selector,
            request.data,
            headers,
            encode_chunked=request.has_header("Transfer-encoding"),
        )
        timeout = remaining_timeout()
        connection.timeout = timeout
        if connection.sock is not None:
            connection.sock.settimeout(timeout)
        response = connection.getresponse()
    except (HTTPException, OSError) as exc:
        pool.release(key, connection, reusable=False)
        raise URLError(exc) from exc
    except BaseException:
        pool.release(key, connection, reusable=False)
        raise

    return _PooledResponse(
        response,
        url=request.get_full_url(),
        pool=pool,
        key=key,
        connection=connection,
        deadline=deadline,
    )


class JsonHttpTransport:
    """Bounded JSON transport for a FerricStore HTTP endpoint."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str | None = None,
        username: str | None = None,
        password: str | None = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = 30.0,
        max_request_bytes: int = DEFAULT_MAX_HTTP_REQUEST_BYTES,
        max_response_bytes: int = DEFAULT_MAX_HTTP_RESPONSE_BYTES,
        max_connections: int = 1,
        http2: bool = False,
        messagepack: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        _opener: OpenerDirector | None = None,
    ) -> None:
        self.base_url = http_validation.validate_base_url(base_url)
        http_validation.require_https_for_basic_credentials(self.base_url, username, password)
        self.timeout = http_validation.validate_timeout(timeout)
        self.max_request_bytes = http_validation.validate_positive_int(
            max_request_bytes,
            name="max_request_bytes",
        )
        self.max_response_bytes = http_validation.validate_positive_int(
            max_response_bytes,
            name="max_response_bytes",
        )
        self.max_connections = http_validation.validate_positive_int(
            max_connections,
            name="max_connections",
        )
        if not isinstance(http2, bool):
            raise TypeError("http2 must be a boolean")
        if not isinstance(messagepack, bool):
            raise TypeError("messagepack must be a boolean")
        self.http2_enabled = http2
        self._ssl_context = ssl_context
        self._messagepack_codec = MessagePackCodec() if messagepack else None
        self.headers = http_validation.build_headers(
            headers,
            bearer_token,
            username,
            password,
        )
        if http2 and _opener is not None:
            raise ValueError("http2 cannot be combined with a custom urllib opener")
        self._http2_backend: _Http2Backend | None
        self._pool: _KeepAlivePool | None
        self._opener: OpenerDirector | None
        self._force_opener = _opener is not None
        self._proxies = getproxies() if _opener is None else {}
        if http2:
            self._http2_backend = _Http2Backend(
                max_connections=self.max_connections,
                ssl_context=ssl_context,
            )
            self._pool = None
            self._opener = None
        elif _opener is None:
            self._http2_backend = None
            pool = _KeepAlivePool(self.max_connections)
            self._pool = pool
            self._opener = build_opener(
                _PooledHTTPHandler(pool),
                _PooledHTTPSHandler(pool, ssl_context),
            )
        else:
            self._http2_backend = None
            self._pool = None
            self._opener = _opener

    def close(self) -> None:
        if self._http2_backend is not None:
            self._http2_backend.close()
        if self._pool is not None:
            self._pool.close()

    def new_deadline(self) -> _HttpDeadline:
        return _HttpDeadline(self.timeout)

    def request_json(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        expected_statuses: tuple[int, ...] = (200,),
        _deadline: _HttpDeadline | None = None,
    ) -> tuple[int, dict[str, Any]]:
        deadline = _deadline or self.new_deadline()
        request_headers = dict(self.headers)
        request_headers.update(http_validation.validated_headers(headers))
        data: bytes | None = None
        if body is not None:
            try:
                data = json.dumps(
                    body,
                    allow_nan=False,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            except (TypeError, ValueError, UnicodeEncodeError) as exc:
                raise ValueError("HTTP request body must contain JSON-compatible values") from exc
            if len(data) > self.max_request_bytes:
                raise HttpError(
                    "FerricStore HTTP endpoint request exceeds max_request_bytes",
                    error_code="request_too_large",
                    retryable=False,
                    safe_to_retry=False,
                )
            request_headers.setdefault("Content-Type", "application/json")
        request_headers.setdefault("Accept", "application/json")
        status, response_headers, raw = self._request_raw(
            method,
            path,
            data=data,
            headers=request_headers,
            deadline=deadline,
        )
        if status not in expected_statuses:
            payload = _decode_error_object(raw, status_code=status)
            raise _response_error(
                method,
                status,
                payload,
                retry_after_ms=_retry_after_ms(response_headers),
            )
        payload = _decode_json_object(raw, status_code=status)
        return status, payload

    def request_messagepack(
        self,
        method: str,
        path: str,
        *,
        body: Mapping[str, Any],
        expected_statuses: tuple[int, ...] = (200,),
        _deadline: _HttpDeadline | None = None,
    ) -> tuple[int, dict[str, Any]]:
        codec = self._messagepack_codec
        if codec is None:
            raise RuntimeError("MessagePack transport was not enabled")
        deadline = _deadline or self.new_deadline()
        data = codec.pack(body)
        self._check_request_size(data)
        content_type = "application/vnd.ferricstore.commands+msgpack"
        request_headers = dict(self.headers)
        request_headers.update({"Accept": content_type, "Content-Type": content_type})
        status, response_headers, raw = self._request_raw(
            method,
            path,
            data=data,
            headers=request_headers,
            deadline=deadline,
        )
        if status not in expected_statuses:
            payload = codec.unpack_error(raw, status_code=status)
            raise _response_error(
                method,
                status,
                payload,
                retry_after_ms=_retry_after_ms(response_headers),
            )
        return status, codec.unpack_object(raw, status_code=status)

    def messagepack_size(self, value: Any) -> int:
        codec = self._messagepack_codec
        if codec is None:
            raise RuntimeError("MessagePack transport was not enabled")
        return len(codec.pack(value))

    def request_bytes(
        self,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> bytes:
        deadline = self.new_deadline()
        request_headers = dict(self.headers)
        request_headers.update(http_validation.validated_headers(headers))
        request_headers.setdefault("Accept", "application/octet-stream")
        status, response_headers, raw = self._request_raw(
            "GET",
            path,
            data=None,
            headers=request_headers,
            deadline=deadline,
        )
        if status >= 400:
            payload = _decode_error_object(raw, status_code=status)
            raise _response_error(
                "GET",
                status,
                payload,
                retry_after_ms=_retry_after_ms(response_headers),
            )
        return raw

    def _check_request_size(self, data: bytes) -> None:
        if len(data) > self.max_request_bytes:
            raise HttpError(
                "FerricStore HTTP endpoint request exceeds max_request_bytes",
                error_code="request_too_large",
                retryable=False,
                safe_to_retry=False,
            )

    def _request_raw(
        self,
        method: str,
        path: str,
        *,
        data: bytes | None,
        headers: Mapping[str, str],
        deadline: _HttpDeadline,
    ) -> tuple[int, Any, bytes]:
        url = self.base_url + http_validation.validate_path(path)
        if self._http2_backend is not None:
            try:
                return self._http2_backend.request(
                    method,
                    url,
                    headers=headers,
                    data=data,
                    deadline=deadline,
                    max_response_bytes=self.max_response_bytes,
                )
            except _Http2BackendError as exc:
                reason = TimeoutError(str(exc.reason)) if exc.timed_out else exc.reason
                raise _http_transport_error(method, reason) from exc

        if self._pool is not None and not self._use_opener_for(url):
            try:
                return self._request_http1_direct(
                    method,
                    url,
                    data=data,
                    headers=headers,
                    deadline=deadline,
                )
            except (URLError, OSError, TimeoutError) as exc:
                reason = getattr(exc, "reason", exc)
                raise _http_transport_error(method, reason) from exc

        return self._request_with_opener(
            method,
            url,
            data=data,
            headers=headers,
            deadline=deadline,
        )

    def _request_http1_direct(
        self,
        method: str,
        url: str,
        *,
        data: bytes | None,
        headers: Mapping[str, str],
        deadline: _HttpDeadline,
    ) -> tuple[int, Any, bytes]:
        current_method = method
        current_url = url
        current_headers = dict(headers)
        current_data = data

        for redirect_count in range(11):
            if self._use_opener_for(current_url):
                return self._request_with_opener(
                    current_method,
                    current_url,
                    data=current_data,
                    headers=current_headers,
                    deadline=deadline,
                )

            status, response_headers, raw = self._request_http1_once(
                current_method,
                current_url,
                data=current_data,
                headers=current_headers,
                deadline=deadline,
            )
            location = response_headers.get("Location")
            if status not in {301, 302, 303, 307, 308} or location is None:
                return status, response_headers, raw
            if redirect_count >= 10:
                raise URLError("too many HTTP redirects")

            current_url = urljoin(current_url, location)
            current_method, current_data, current_headers = _redirect_request(
                status,
                current_method,
                current_data,
                current_headers,
            )

        raise URLError("too many HTTP redirects")

    def _request_http1_once(
        self,
        method: str,
        url: str,
        *,
        data: bytes | None,
        headers: Mapping[str, str],
        deadline: _HttpDeadline,
    ) -> tuple[int, Any, bytes]:
        pool = self._pool
        if pool is None:
            raise RuntimeError("HTTP/1 connection pool is unavailable")
        parsed = urlsplit(url)
        secure = parsed.scheme == "https"
        if not secure and parsed.scheme != "http":
            raise URLError("redirect URL must use HTTP or HTTPS")
        host = parsed.hostname
        if host is None:
            raise URLError("redirect URL has no host")
        port = parsed.port
        authority = parsed.netloc
        key = (secure, authority, None)

        def create_connection() -> HTTPConnection:
            timeout = deadline.remaining()
            if secure:
                return HTTPSConnection(
                    host,
                    port=port,
                    timeout=timeout,
                    context=self._ssl_context,
                )
            return HTTPConnection(host, port=port, timeout=timeout)

        connection = pool.acquire(key, create_connection, deadline.remaining())
        timeout = deadline.remaining()
        connection.timeout = timeout
        if connection.sock is not None:
            connection.sock.settimeout(timeout)
        target = parsed.path or "/"
        if parsed.query:
            target += f"?{parsed.query}"

        try:
            connection.request(method, target, data, dict(headers))
            timeout = deadline.remaining()
            connection.timeout = timeout
            if connection.sock is not None:
                connection.sock.settimeout(timeout)
            response = connection.getresponse()
        except (HTTPException, OSError) as exc:
            pool.release(key, connection, reusable=False)
            raise URLError(exc) from exc
        except BaseException:
            pool.release(key, connection, reusable=False)
            raise

        with _PooledResponse(
            response,
            url=url,
            pool=pool,
            key=key,
            connection=connection,
            deadline=deadline,
        ) as pooled:
            return response.status, response.headers, _read_bounded(pooled, self.max_response_bytes)

    def _request_with_opener(
        self,
        method: str,
        url: str,
        *,
        data: bytes | None,
        headers: Mapping[str, str],
        deadline: _HttpDeadline,
    ) -> tuple[int, Any, bytes]:
        request = Request(url, data=data, headers=dict(headers), method=method)
        opener = self._opener
        if opener is None:
            raise RuntimeError("urllib transport is unavailable")
        deadline_token = _ACTIVE_HTTP_DEADLINE.set(deadline)
        try:
            with opener.open(request, timeout=deadline.remaining()) as response:
                return (
                    int(response.status),
                    response.headers,
                    _read_bounded(response, self.max_response_bytes),
                )
        except HTTPError as exc:
            try:
                try:
                    raw = _read_bounded(exc, self.max_response_bytes)
                except (OSError, TimeoutError) as read_exc:
                    raise _http_transport_error(method, read_exc) from read_exc
            finally:
                exc.close()
            return int(exc.code), exc.headers, raw
        except (URLError, OSError, TimeoutError) as exc:
            reason = getattr(exc, "reason", exc)
            raise _http_transport_error(method, reason) from exc
        finally:
            _ACTIVE_HTTP_DEADLINE.reset(deadline_token)

    def _use_opener_for(self, url: str) -> bool:
        if self._force_opener:
            return True
        parsed = urlsplit(url)
        proxy = self._proxies.get(parsed.scheme) or self._proxies.get("all")
        return bool(proxy and parsed.hostname and not proxy_bypass(parsed.hostname))


def _read_bounded(response: Any, limit: int) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length is not None:
        try:
            declared = int(content_length)
        except ValueError:
            declared = -1
        if declared > limit:
            raise HttpError(
                "FerricStore HTTP endpoint response exceeds max_response_bytes",
                error_code="response_too_large",
                retryable=False,
                safe_to_retry=False,
            )
    chunks: list[bytes] = []
    size = 0
    read = getattr(response, "read1", response.read)
    while size <= limit:
        chunk = cast(bytes, read(min(64 * 1024, limit + 1 - size)))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        size += len(chunk)
    raise HttpError(
        "FerricStore HTTP endpoint response exceeds max_response_bytes",
        error_code="response_too_large",
        retryable=False,
        safe_to_retry=False,
    )


def _http_transport_error(method: str, reason: Any) -> HttpError:
    timed_out = isinstance(reason, (TimeoutError, socket.timeout))
    detail = "deadline exceeded" if timed_out else str(reason)
    return HttpError(
        f"FerricStore HTTP endpoint request failed: {detail}",
        error_code="transport_timeout" if timed_out else "transport_error",
        raw=reason,
        retryable=True,
        safe_to_retry=method == "GET",
    )


def _decode_json_object(raw: bytes, *, status_code: int) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HttpError(
            "FerricStore HTTP endpoint returned invalid JSON",
            status_code=status_code,
            error_code="invalid_response",
            raw=raw,
            retryable=False,
            safe_to_retry=False,
        ) from exc
    if not isinstance(value, dict):
        raise HttpError(
            "FerricStore HTTP endpoint returned a non-object JSON response",
            status_code=status_code,
            error_code="invalid_response",
            raw=value,
            retryable=False,
            safe_to_retry=False,
        )
    return value


def _decode_error_object(raw: bytes, *, status_code: int) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        value = None
    if isinstance(value, dict):
        return value
    return {
        "error": {
            "code": "http_error",
            "message": f"FerricStore HTTP endpoint returned status {status_code}",
        },
        "raw_body": raw.decode(errors="replace"),
    }


def _response_error(
    method: str,
    status_code: int,
    payload: dict[str, Any],
    *,
    retry_after_ms: int | None,
) -> HttpError | OverloadedError:
    error = payload.get("error")
    details = error if isinstance(error, dict) else {}
    code_value = details.get("code")
    code = code_value if isinstance(code_value, str) else "http_error"
    message_value = details.get("message")
    message = message_value if isinstance(message_value, str) else code.replace("_", " ")
    body_retry_after = payload.get("retry_after_ms")
    if retry_after_ms is None and isinstance(body_retry_after, int) and body_retry_after >= 0:
        retry_after_ms = body_retry_after
    raw = {"status_code": status_code, "body": payload}
    if status_code in {429, 503} or code in {"overload", "overloaded"}:
        return OverloadedError(
            message,
            raw=raw,
            retry_after_ms=retry_after_ms,
            reason=code,
            retryable=True,
            safe_to_retry=True,
        )
    return HttpError(
        message,
        status_code=status_code,
        error_code=code,
        raw=raw,
        retryable=status_code >= 500,
        safe_to_retry=method == "GET",
        retry_after_ms=retry_after_ms,
    )


def _retry_after_ms(headers: Any) -> int | None:
    value = headers.get("Retry-After") if headers is not None else None
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if seconds < 0:
        return None
    return int(seconds * 1000)


__all__ = [
    "DEFAULT_MAX_HTTP_REQUEST_BYTES",
    "DEFAULT_MAX_HTTP_RESPONSE_BYTES",
    "JsonHttpTransport",
]
