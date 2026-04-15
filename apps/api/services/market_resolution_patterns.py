from __future__ import annotations

from typing import Dict, Any, List, Optional

import psycopg

from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_resolution_patterns_v1"


def _safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def fetch_resolution_learning_rows(day: Optional[str] = None) -> List[Dict[str, Any]]:
    q = """
    SELECT
        market_id,
        resolution_day,
        link_method,
        link_confidence,
        structural_state_pre,
        social_state_pre,
        alignment_state_pre,
        resolution_state,
        resolution_clarity_score,
        oracle_risk_score,
        dispute_propensity_score,
        disputed,
        settled,
        request_to_settlement_hours,
        dispute_to_settlement_hours,
        resolution_complexity
    FROM public.market_resolved_learning_daily
    WHERE (%s::date IS NULL OR resolution_day = %s::date)
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (day, day))
            rows = cur.fetchall()

    out = []
    for r in rows:
        out.append(
            {
                "market_id": r[0],
                "resolution_day": r[1],
                "link_method": r[2],
                "link_confidence": _safe_float(r[3]),
                "structural_state_pre": r[4],
                "social_state_pre": r[5],
                "alignment_state_pre": r[6],
                "resolution_state": r[7],
                "resolution_clarity_score": _safe_float(r[8]),
                "oracle_risk_score": _safe_float(r[9]),
                "dispute_propensity_score": _safe_float(r[10]),
                "disputed": bool(r[11]),
                "settled": bool(r[12]),
                "request_to_settlement_hours": _safe_float(r[13]),
                "dispute_to_settlement_hours": _safe_float(r[14]),
                "resolution_complexity": r[15],
            }
        )
    return out


def _group_key(row: Dict[str, Any], pattern_type: str) -> Optional[str]:
    if pattern_type == "resolution_state":
        return row.get("resolution_state")
    if pattern_type == "resolution_complexity":
        return row.get("resolution_complexity")
    if pattern_type == "link_method":
        return row.get("link_method")
    if pattern_type == "alignment_state_pre":
        return row.get("alignment_state_pre") or "unknown"
    return None


def _avg(values: List[Optional[float]]) -> Optional[float]:
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return None
    return round(sum(vals) / len(vals), 6)


def build_pattern_rows(rows: List[Dict[str, Any]], day_value, pattern_type: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for row in rows:
        key = _group_key(row, pattern_type)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)

    out: List[Dict[str, Any]] = []

    for key, items in grouped.items():
        markets_count = len(items)
        disputed_count = sum(1 for x in items if x.get("disputed"))
        settled_count = sum(1 for x in items if x.get("settled"))

        row = {
            "day": day_value,
            "pattern_type": pattern_type,
            "pattern_value": key,
            "markets_count": markets_count,
            "disputed_count": disputed_count,
            "settled_count": settled_count,
            "dispute_rate": round(disputed_count / markets_count, 6) if markets_count > 0 else None,
            "avg_resolution_clarity_score": _avg([x.get("resolution_clarity_score") for x in items]),
            "avg_oracle_risk_score": _avg([x.get("oracle_risk_score") for x in items]),
            "avg_dispute_propensity_score": _avg([x.get("dispute_propensity_score") for x in items]),
            "avg_request_to_settlement_hours": _avg([x.get("request_to_settlement_hours") for x in items]),
            "avg_dispute_to_settlement_hours": _avg([x.get("dispute_to_settlement_hours") for x in items]),
            "engine_version": ENGINE_VERSION,
        }
        out.append(row)

    return out


def persist_pattern_rows(pattern_rows: List[Dict[str, Any]]) -> int:
    if not pattern_rows:
        return 0

    q = """
    INSERT INTO public.market_resolution_patterns_daily (
        day,
        pattern_type,
        pattern_value,
        markets_count,
        disputed_count,
        settled_count,
        dispute_rate,
        avg_resolution_clarity_score,
        avg_oracle_risk_score,
        avg_dispute_propensity_score,
        avg_request_to_settlement_hours,
        avg_dispute_to_settlement_hours,
        engine_version,
        updated_at
    )
    VALUES (
        %(day)s,
        %(pattern_type)s,
        %(pattern_value)s,
        %(markets_count)s,
        %(disputed_count)s,
        %(settled_count)s,
        %(dispute_rate)s,
        %(avg_resolution_clarity_score)s,
        %(avg_oracle_risk_score)s,
        %(avg_dispute_propensity_score)s,
        %(avg_request_to_settlement_hours)s,
        %(avg_dispute_to_settlement_hours)s,
        %(engine_version)s,
        NOW()
    )
    ON CONFLICT (day, pattern_type, pattern_value)
    DO UPDATE SET
        markets_count = EXCLUDED.markets_count,
        disputed_count = EXCLUDED.disputed_count,
        settled_count = EXCLUDED.settled_count,
        dispute_rate = EXCLUDED.dispute_rate,
        avg_resolution_clarity_score = EXCLUDED.avg_resolution_clarity_score,
        avg_oracle_risk_score = EXCLUDED.avg_oracle_risk_score,
        avg_dispute_propensity_score = EXCLUDED.avg_dispute_propensity_score,
        avg_request_to_settlement_hours = EXCLUDED.avg_request_to_settlement_hours,
        avg_dispute_to_settlement_hours = EXCLUDED.avg_dispute_to_settlement_hours,
        engine_version = EXCLUDED.engine_version,
        updated_at = NOW()
    """

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            for row in pattern_rows:
                cur.execute(q, row)
        conn.commit()

    return len(pattern_rows)


def compute_market_resolution_patterns(day: Optional[str] = None) -> Dict[str, Any]:
    rows = fetch_resolution_learning_rows(day=day)
    if not rows:
        return {
            "status": "ok",
            "day": day,
            "rows_written": 0,
            "engine_version": ENGINE_VERSION,
        }

    target_day = rows[0]["resolution_day"] if day is None else day

    all_pattern_rows: List[Dict[str, Any]] = []
    for pattern_type in [
        "resolution_state",
        "resolution_complexity",
        "link_method",
        "alignment_state_pre",
    ]:
        all_pattern_rows.extend(
            build_pattern_rows(rows, target_day, pattern_type)
        )

    written = persist_pattern_rows(all_pattern_rows)

    return {
        "status": "ok",
        "day": str(target_day),
        "rows_written": written,
        "patterns_built": [
            "resolution_state",
            "resolution_complexity",
            "link_method",
            "alignment_state_pre",
        ],
        "engine_version": ENGINE_VERSION,
    }