from __future__ import annotations

import builtins
from typing import Any

from ferricstore.codecs import Codec
from ferricstore.config_validation import validate_bool, validate_optional_positive_int


def _append(args: builtins.list[Any], name: str, value: Any) -> None:
    if value is not None:
        args.extend([name, value])


def _append_flag(args: builtins.list[Any], name: str, enabled: bool) -> None:
    enabled = validate_bool(enabled, name=name)
    if enabled:
        args.append(name)


def _set_args(
    codec: Codec,
    key: str,
    value: Any,
    *,
    ex: int | None = None,
    px: int | None = None,
    exat: int | None = None,
    pxat: int | None = None,
    nx: bool = False,
    xx: bool = False,
    get: bool = False,
    keepttl: bool = False,
    encode: bool = True,
) -> builtins.list[Any]:
    ex = validate_optional_positive_int(ex, name="EX")
    px = validate_optional_positive_int(px, name="PX")
    exat = validate_optional_positive_int(exat, name="EXAT")
    pxat = validate_optional_positive_int(pxat, name="PXAT")
    nx = validate_bool(nx, name="NX")
    xx = validate_bool(xx, name="XX")
    get = validate_bool(get, name="GET")
    keepttl = validate_bool(keepttl, name="KEEPTTL")
    encode = validate_bool(encode, name="encode")
    expiry_count = sum(value is not None for value in (ex, px, exat, pxat))
    if expiry_count > 1 or (keepttl and expiry_count):
        raise ValueError("SET expiry options and keepttl are mutually exclusive")
    if nx and xx:
        raise ValueError("SET nx and xx are mutually exclusive")
    args: builtins.list[Any] = ["SET", key, codec.encode(value) if encode else value]
    _append(args, "EX", ex)
    _append(args, "PX", px)
    _append(args, "EXAT", exat)
    _append(args, "PXAT", pxat)
    _append_flag(args, "NX", nx)
    _append_flag(args, "XX", xx)
    _append_flag(args, "GET", get)
    _append_flag(args, "KEEPTTL", keepttl)
    return args


def _getex_args(
    key: str,
    *,
    ex: int | None = None,
    px: int | None = None,
    exat: int | None = None,
    pxat: int | None = None,
    persist: bool = False,
) -> builtins.list[Any]:
    args: builtins.list[Any] = ["GETEX", key]
    _append(args, "EX", ex)
    _append(args, "PX", px)
    _append(args, "EXAT", exat)
    _append(args, "PXAT", pxat)
    _append_flag(args, "PERSIST", persist)
    return args


def _expire_args(
    command: str,
    key: str,
    value: int,
    *,
    nx: bool = False,
    xx: bool = False,
    gt: bool = False,
    lt: bool = False,
) -> builtins.list[Any]:
    args: builtins.list[Any] = [command, key, value]
    _append_flag(args, "NX", nx)
    _append_flag(args, "XX", xx)
    _append_flag(args, "GT", gt)
    _append_flag(args, "LT", lt)
    return args


def _xread_args(
    command: str,
    streams: dict[str, str],
    *,
    count: int | None = None,
    block_ms: int | None = None,
    group: str | None = None,
    consumer: str | None = None,
) -> builtins.list[Any]:
    args: builtins.list[Any] = [command]
    if group is not None or consumer is not None:
        if group is None or consumer is None:
            raise ValueError("XREADGROUP requires both group and consumer")
        args.extend(["GROUP", group, consumer])
    _append(args, "COUNT", count)
    _append(args, "BLOCK", block_ms)
    args.append("STREAMS")
    args.extend(streams.keys())
    args.extend(streams.values())
    return args


__all__ = ["_append", "_append_flag", "_expire_args", "_getex_args", "_set_args", "_xread_args"]
