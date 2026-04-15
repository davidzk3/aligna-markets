from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _hash_snapshot(snapshot: Dict[str, Any]) -> str:
    safe_snapshot = _json_safe(snapshot)
    payload = json.dumps(safe_snapshot, sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def persist_market_snapshot_audit(
    snapshot: Dict[str, Any],
    source: str = "snapshot_endpoint",
) -> Dict[str, Any]:
    snapshot = snapshot or {}
    safe_snapshot = _json_safe(snapshot)

    market_id = safe_snapshot.get("market_id")
    if not market_id:
        raise ValueError("snapshot missing market_id")

    ai_context = safe_snapshot.get("ai_context") or {}
    snapshot_meta = safe_snapshot.get("snapshot_meta") or {}

    structural_day = snapshot_meta.get("structural_day")
    social_day = snapshot_meta.get("social_day")
    alignment_day = snapshot_meta.get("alignment_day")

    structural_score = safe_snapshot.get("structural_score")
    social_score = safe_snapshot.get("social_score")
    structural_state = safe_snapshot.get("structural_state")
    social_state = safe_snapshot.get("social_state")
    alignment_state = safe_snapshot.get("alignment_state")
    integrity_band = safe_snapshot.get("integrity_band")
    contextual_summary = safe_snapshot.get("contextual_summary")
    is_mixed_horizon = bool(
        safe_snapshot.get("is_mixed_horizon")
        or snapshot_meta.get("is_mixed_horizon")
    )

    drivers = ai_context.get("drivers") or []
    caution_flags = ai_context.get("caution_flags") or []

    snapshot_hash = _hash_snapshot(safe_snapshot)

    q = """
    INSERT INTO public.market_snapshot_audit (
        market_id,
        structural_day,
        social_day,
        alignment_day,
        structural_score,
        social_score,
        structural_state,
        social_state,
        alignment_state,
        integrity_band,
        contextual_summary,
        is_mixed_horizon,
        snapshot_hash,
        source,
        snapshot_json,
        ai_context_json,
        drivers_json,
        caution_flags_json
    )
    VALUES (
        %(market_id)s,
        %(structural_day)s,
        %(social_day)s,
        %(alignment_day)s,
        %(structural_score)s,
        %(social_score)s,
        %(structural_state)s,
        %(social_state)s,
        %(alignment_state)s,
        %(integrity_band)s,
        %(contextual_summary)s,
        %(is_mixed_horizon)s,
        %(snapshot_hash)s,
        %(source)s,
        %(snapshot_json)s,
        %(ai_context_json)s,
        %(drivers_json)s,
        %(caution_flags_json)s
    )
    ON CONFLICT (market_id, snapshot_hash, source)
    WHERE snapshot_hash IS NOT NULL
    DO UPDATE SET
        captured_at = NOW(),
        snapshot_json = EXCLUDED.snapshot_json,
        ai_context_json = EXCLUDED.ai_context_json,
        drivers_json = EXCLUDED.drivers_json,
        caution_flags_json = EXCLUDED.caution_flags_json
    RETURNING id, market_id, snapshot_hash, captured_at;
    """

    params = {
        "market_id": market_id,
        "structural_day": structural_day,
        "social_day": social_day,
        "alignment_day": alignment_day,
        "structural_score": structural_score,
        "social_score": social_score,
        "structural_state": structural_state,
        "social_state": social_state,
        "alignment_state": alignment_state,
        "integrity_band": integrity_band,
        "contextual_summary": contextual_summary,
        "is_mixed_horizon": is_mixed_horizon,
        "snapshot_hash": snapshot_hash,
        "source": source,
        "snapshot_json": Json(safe_snapshot),
        "ai_context_json": Json(ai_context),
        "drivers_json": Json(drivers),
        "caution_flags_json": Json(caution_flags),
    }

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            row = cur.fetchone()
            conn.commit()

    return {
        "status": "ok",
        "audit_id": row[0],
        "market_id": row[1],
        "snapshot_hash": row[2],
        "captured_at": row[3].isoformat() if row and row[3] else None,
        "source": source,
    }