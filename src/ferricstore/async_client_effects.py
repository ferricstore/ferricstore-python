from __future__ import annotations

import builtins
from typing import Any, cast

from ferricstore.async_client_state import _AsyncClientMixinBase
from ferricstore.client_helpers import _append, _normalize_admin_response
from ferricstore.governance_validation import (
    validate_effect_get,
    validate_effect_reserve,
    validate_effect_status,
)
from ferricstore.types import EffectResult


class _AsyncClientEffectsMixin(_AsyncClientMixinBase):
    async def effect_reserve(
        self,
        id: str,
        effect_key: str,
        effect_type: str,
        *,
        partition_key: str | bytes | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        operation_digest: str,
        idempotency_key: str | None = None,
        governance_scope: str | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        validate_effect_reserve(
            id,
            effect_key,
            effect_type,
            lease_token=lease_token,
            fencing_token=fencing_token,
            operation_digest=operation_digest,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = ["FLOW.EFFECT.RESERVE", id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "EFFECT_TYPE", effect_type)
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "OPERATION_DIGEST", operation_digest)
        _append(args, "IDEMPOTENCY_KEY", idempotency_key)
        _append(args, "GOVERNANCE_SCOPE", governance_scope)
        _append(args, "NOW", now_ms)
        return EffectResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )

    async def effect_confirm(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | bytes | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return await self._effect_status(
            "FLOW.EFFECT.CONFIRM",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            external_id=external_id,
            latency_ms=latency_ms,
            now_ms=now_ms,
        )

    async def effect_fail(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | bytes | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return await self._effect_status(
            "FLOW.EFFECT.FAIL",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            error=error,
            reason=reason,
            latency_ms=latency_ms,
            now_ms=now_ms,
        )

    async def effect_compensate(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | bytes | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        reason: str | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        return await self._effect_status(
            "FLOW.EFFECT.COMPENSATE",
            id,
            effect_key,
            partition_key=partition_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            external_id=external_id,
            reason=reason,
            now_ms=now_ms,
        )

    async def effect_get(
        self,
        id: str,
        effect_key: str,
        *,
        partition_key: str | bytes | None = None,
    ) -> EffectResult | None:
        validate_effect_get(id, effect_key)
        args: builtins.list[Any] = ["FLOW.EFFECT.GET", id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "PARTITION", partition_key)
        response = cast(
            dict[str, Any] | None,
            _normalize_admin_response(await self.executor.execute_command(*args)),
        )
        return EffectResult.from_resp(response) if response is not None else None

    async def _effect_status(
        self,
        command: str,
        id: str,
        effect_key: str,
        *,
        partition_key: str | bytes | None = None,
        lease_token: bytes | None = None,
        fencing_token: int | None = None,
        external_id: str | None = None,
        error: str | None = None,
        reason: str | None = None,
        latency_ms: int | None = None,
        now_ms: int | None = None,
    ) -> EffectResult:
        validate_effect_status(
            id,
            effect_key,
            lease_token=lease_token,
            fencing_token=fencing_token,
            latency_ms=latency_ms,
            now_ms=now_ms,
        )
        args: builtins.list[Any] = [command, id]
        _append(args, "EFFECT_KEY", effect_key)
        _append(args, "PARTITION", partition_key)
        _append(args, "LEASE_TOKEN", lease_token)
        _append(args, "FENCING", fencing_token)
        _append(args, "EXTERNAL_ID", external_id)
        _append(args, "ERROR", error)
        _append(args, "REASON", reason)
        _append(args, "LATENCY_MS", latency_ms)
        _append(args, "NOW", now_ms)
        return EffectResult.from_resp(
            cast(
                dict[str, Any],
                _normalize_admin_response(await self.executor.execute_command(*args)),
            )
        )
