from __future__ import annotations

from typing import Optional, Dict, Any, List

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_resolved_learning_v1"


def _safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def fetch_selected_link(market_id: str) -> Optional[Dict[str, Any]]:
    q = """
    SELECT
        market_id,
        uma_request_id,
        link_method,
        confidence
    FROM public.market_oracle_links
    WHERE market_id = %s
    LIMIT 1
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "market_id": row[0],
                "linked_uma_request_id": row[1],
                "link_method": row[2],
                "link_confidence": _safe_float(row[3]),
            }


def fetch_resolution_row(market_id: str) -> Optional[Dict[str, Any]]:
    q = """
    SELECT
        market_id,
        day,
        resolution_state,
        resolution_clarity_score,
        oracle_risk_score,
        dispute_propensity_score,
        disputed,
        settled,
        request_to_settlement_hours,
        dispute_to_settlement_hours,
        resolution_complexity,
        flags
    FROM public.market_uma_resolution_intelligence_daily
    WHERE market_id = %s
    ORDER BY day DESC
    LIMIT 1
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "market_id": row[0],
                "resolution_day": row[1],
                "resolution_state": row[2],
                "resolution_clarity_score": _safe_float(row[3]),
                "oracle_risk_score": _safe_float(row[4]),
                "dispute_propensity_score": _safe_float(row[5]),
                "disputed": bool(row[6]),
                "settled": bool(row[7]),
                "request_to_settlement_hours": _safe_float(row[8]),
                "dispute_to_settlement_hours": _safe_float(row[9]),
                "resolution_complexity": row[10],
                "resolution_flags": row[11] or [],
            }


def fetch_latest_alignment_pre(market_id: str) -> Dict[str, Any]:
    q = """
    SELECT
        structural_state,
        social_state,
        alignment_state,
        alignment_score
    FROM public.market_alignment_daily
    WHERE market_id = %s
    ORDER BY day DESC, updated_at DESC
    LIMIT 1
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()
            if not row:
                return {}
            return {
                "structural_state_pre": row[0],
                "social_state_pre": row[1],
                "alignment_state_pre": row[2],
                "alignment_score_pre": _safe_float(row[3]),
            }


def fetch_latest_social_pre(market_id: str) -> Dict[str, Any]:
    q = """
    SELECT
        demand_state,
        demand_score
    FROM public.market_social_intelligence_daily
    WHERE market_id = %s
    ORDER BY day DESC, updated_at DESC
    LIMIT 1
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (market_id,))
            row = cur.fetchone()
            if not row:
                return {}
            return {
                "social_state_pre": row[0],
                "social_score_pre": _safe_float(row[1]),
            }


def fetch_latest_market_pre(market_id: str) -> Dict[str, Any]:
    candidate_queries: List[str] = [
        """
        SELECT
            market_quality_score,
            unique_traders,
            trades,
            volume,
            concentration_hhi
        FROM public.market_integrity_score_daily
        WHERE market_id = %s
        ORDER BY day DESC, updated_at DESC
        LIMIT 1
        """,
        """
        SELECT
            health_score,
            unique_traders,
            trades,
            volume,
            concentration_hhi
        FROM marts.market_day
        WHERE market_id = %s
        ORDER BY day DESC
        LIMIT 1
        """,
    ]

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            for q in candidate_queries:
                try:
                    cur.execute(q, (market_id,))
                    row = cur.fetchone()
                    if row:
                        return {
                            "structural_score_pre": _safe_float(row[0]),
                            "unique_traders_pre": _safe_float(row[1]),
                            "trades_pre": _safe_float(row[2]),
                            "volume_pre": _safe_float(row[3]),
                            "concentration_hhi_pre": _safe_float(row[4]),
                        }
                except Exception:
                    continue

    return {}


def build_learning_summary(row: Dict[str, Any]) -> str:
    resolution_state = row.get("resolution_state")
    disputed = row.get("disputed")
    alignment_state_pre = row.get("alignment_state_pre")
    structural_state_pre = row.get("structural_state_pre")
    social_state_pre = row.get("social_state_pre")

    if disputed:
        return (
            f"Resolved market entered dispute. Pre-resolution state was "
            f"structural={structural_state_pre or 'unknown'}, "
            f"social={social_state_pre or 'unknown'}, "
            f"alignment={alignment_state_pre or 'unknown'}."
        )

    return (
        f"Resolved market settled with resolution_state={resolution_state or 'unknown'}. "
        f"Pre-resolution alignment was {alignment_state_pre or 'unknown'}."
    )


def persist_learning_row(row: Dict[str, Any]) -> Dict[str, Any]:
    q = """
    INSERT INTO public.market_resolved_learning_daily (
        market_id,
        resolution_day,
        linked_uma_request_id,
        link_method,
        link_confidence,
        structural_state_pre,
        structural_score_pre,
        social_state_pre,
        social_score_pre,
        alignment_state_pre,
        alignment_score_pre,
        unique_traders_pre,
        trades_pre,
        volume_pre,
        concentration_hhi_pre,
        resolution_state,
        resolution_clarity_score,
        oracle_risk_score,
        dispute_propensity_score,
        disputed,
        settled,
        request_to_settlement_hours,
        dispute_to_settlement_hours,
        resolution_complexity,
        resolution_flags,
        learning_summary,
        engine_version,
        updated_at
    )
    VALUES (
        %(market_id)s,
        %(resolution_day)s,
        %(linked_uma_request_id)s,
        %(link_method)s,
        %(link_confidence)s,
        %(structural_state_pre)s,
        %(structural_score_pre)s,
        %(social_state_pre)s,
        %(social_score_pre)s,
        %(alignment_state_pre)s,
        %(alignment_score_pre)s,
        %(unique_traders_pre)s,
        %(trades_pre)s,
        %(volume_pre)s,
        %(concentration_hhi_pre)s,
        %(resolution_state)s,
        %(resolution_clarity_score)s,
        %(oracle_risk_score)s,
        %(dispute_propensity_score)s,
        %(disputed)s,
        %(settled)s,
        %(request_to_settlement_hours)s,
        %(dispute_to_settlement_hours)s,
        %(resolution_complexity)s,
        %(resolution_flags)s,
        %(learning_summary)s,
        %(engine_version)s,
        NOW()
    )
    ON CONFLICT (market_id, resolution_day)
    DO UPDATE SET
        linked_uma_request_id = EXCLUDED.linked_uma_request_id,
        link_method = EXCLUDED.link_method,
        link_confidence = EXCLUDED.link_confidence,
        structural_state_pre = EXCLUDED.structural_state_pre,
        structural_score_pre = EXCLUDED.structural_score_pre,
        social_state_pre = EXCLUDED.social_state_pre,
        social_score_pre = EXCLUDED.social_score_pre,
        alignment_state_pre = EXCLUDED.alignment_state_pre,
        alignment_score_pre = EXCLUDED.alignment_score_pre,
        unique_traders_pre = EXCLUDED.unique_traders_pre,
        trades_pre = EXCLUDED.trades_pre,
        volume_pre = EXCLUDED.volume_pre,
        concentration_hhi_pre = EXCLUDED.concentration_hhi_pre,
        resolution_state = EXCLUDED.resolution_state,
        resolution_clarity_score = EXCLUDED.resolution_clarity_score,
        oracle_risk_score = EXCLUDED.oracle_risk_score,
        dispute_propensity_score = EXCLUDED.dispute_propensity_score,
        disputed = EXCLUDED.disputed,
        settled = EXCLUDED.settled,
        request_to_settlement_hours = EXCLUDED.request_to_settlement_hours,
        dispute_to_settlement_hours = EXCLUDED.dispute_to_settlement_hours,
        resolution_complexity = EXCLUDED.resolution_complexity,
        resolution_flags = EXCLUDED.resolution_flags,
        learning_summary = EXCLUDED.learning_summary,
        engine_version = EXCLUDED.engine_version,
        updated_at = NOW()
    RETURNING market_id, resolution_day
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                q,
                {
                    **row,
                    "resolution_flags": Json(row.get("resolution_flags") or []),
                },
            )
            out = cur.fetchone()
        conn.commit()

    return {
        "market_id": out[0],
        "resolution_day": str(out[1]),
    }


def compute_market_resolved_learning(market_id: str) -> Dict[str, Any]:
    resolution = fetch_resolution_row(market_id)
    if not resolution:
        return {
            "status": "not_found",
            "market_id": market_id,
            "reason": "no_resolution_row",
        }

    link = fetch_selected_link(market_id) or {}
    market_pre = fetch_latest_market_pre(market_id)
    social_pre = fetch_latest_social_pre(market_id)
    alignment_pre = fetch_latest_alignment_pre(market_id)

    row = {
        "market_id": market_id,
        "resolution_day": resolution.get("resolution_day"),
        "linked_uma_request_id": link.get("linked_uma_request_id"),
        "link_method": link.get("link_method"),
        "link_confidence": link.get("link_confidence"),
        "structural_state_pre": alignment_pre.get("structural_state_pre"),
        "structural_score_pre": market_pre.get("structural_score_pre"),
        "social_state_pre": social_pre.get("social_state_pre") or alignment_pre.get("social_state_pre"),
        "social_score_pre": social_pre.get("social_score_pre"),
        "alignment_state_pre": alignment_pre.get("alignment_state_pre"),
        "alignment_score_pre": alignment_pre.get("alignment_score_pre"),
        "unique_traders_pre": market_pre.get("unique_traders_pre"),
        "trades_pre": market_pre.get("trades_pre"),
        "volume_pre": market_pre.get("volume_pre"),
        "concentration_hhi_pre": market_pre.get("concentration_hhi_pre"),
        "resolution_state": resolution.get("resolution_state"),
        "resolution_clarity_score": resolution.get("resolution_clarity_score"),
        "oracle_risk_score": resolution.get("oracle_risk_score"),
        "dispute_propensity_score": resolution.get("dispute_propensity_score"),
        "disputed": resolution.get("disputed"),
        "settled": resolution.get("settled"),
        "request_to_settlement_hours": resolution.get("request_to_settlement_hours"),
        "dispute_to_settlement_hours": resolution.get("dispute_to_settlement_hours"),
        "resolution_complexity": resolution.get("resolution_complexity"),
        "resolution_flags": resolution.get("resolution_flags") or [],
        "engine_version": ENGINE_VERSION,
    }

    row["learning_summary"] = build_learning_summary(row)

    persisted = persist_learning_row(row)

    return {
        "status": "ok",
        "market_id": market_id,
        "resolution_day": persisted["resolution_day"],
        "linked_uma_request_id": row.get("linked_uma_request_id"),
        "link_method": row.get("link_method"),
        "link_confidence": row.get("link_confidence"),
        "engine_version": ENGINE_VERSION,
        "learning_summary": row.get("learning_summary"),
    }