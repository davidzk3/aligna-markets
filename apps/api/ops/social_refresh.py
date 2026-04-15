from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

import psycopg

from apps.api.db import get_db_dsn
from apps.api.ingest.social.social_runner import ingest_social_for_market
from apps.api.services.social_feature_aggregation import compute_market_social_features_daily
from apps.api.services.social_scoring import score_market_social_features_daily
from apps.api.services.market_social_intelligence import compute_market_social_intelligence_daily
from apps.api.services.market_alignment_intelligence import compute_market_alignment_daily


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _fetch_latest_social_alignment_state(market_id: str) -> Dict[str, Any]:
    dsn = get_db_dsn()

    q_social = """
    SELECT
        market_id,
        day::text AS day,
        demand_state,
        recommendation,
        demand_score::float AS demand_score,
        attention_score::float AS attention_score,
        demand_strength_score::float AS demand_strength_score,
        demand_breadth_score::float AS demand_breadth_score,
        demand_quality_score::float AS demand_quality_score,
        narrative_coherence_score::float AS narrative_coherence_score,
        social_fragility_score::float AS social_fragility_score,
        summary,
        COALESCE(flags, ARRAY[]::text[]) AS flags,
        engine_version
    FROM public.market_social_intelligence_daily
    WHERE market_id = %s
    ORDER BY day DESC
    LIMIT 1;
    """

    q_alignment = """
    SELECT
        market_id,
        day::text AS day,
        structural_state,
        social_state,
        alignment_state,
        alignment_score::float AS alignment_score,
        attention_vs_structure_gap::float AS attention_vs_structure_gap,
        bottleneck_type,
        summary,
        COALESCE(flags, ARRAY[]::text[]) AS flags,
        engine_version
    FROM public.market_alignment_daily
    WHERE market_id = %s
    ORDER BY day DESC
    LIMIT 1;
    """

    out: Dict[str, Any] = {
        "social": {},
        "alignment": {},
    }

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(q_social, (market_id,))
            row = cur.fetchone()
            if row:
                cols = [c.name for c in cur.description]
                out["social"] = dict(zip(cols, row))

            cur.execute(q_alignment, (market_id,))
            row = cur.fetchone()
            if row:
                cols = [c.name for c in cur.description]
                out["alignment"] = dict(zip(cols, row))

    return out


def _diff_flags(before_flags: list[str], after_flags: list[str]) -> Dict[str, list[str]]:
    before_set = set(before_flags or [])
    after_set = set(after_flags or [])
    return {
        "added": sorted(list(after_set - before_set)),
        "removed": sorted(list(before_set - after_set)),
    }


def _build_score_diffs(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    before_social = before.get("social") or {}
    after_social = after.get("social") or {}
    before_alignment = before.get("alignment") or {}
    after_alignment = after.get("alignment") or {}

    social_before_score = _safe_float(before_social.get("demand_score"))
    social_after_score = _safe_float(after_social.get("demand_score"))

    alignment_before_score = _safe_float(before_alignment.get("alignment_score"))
    alignment_after_score = _safe_float(after_alignment.get("alignment_score"))

    social_score_delta = (
        None
        if social_before_score is None or social_after_score is None
        else round(social_after_score - social_before_score, 4)
    )

    alignment_score_delta = (
        None
        if alignment_before_score is None or alignment_after_score is None
        else round(alignment_after_score - alignment_before_score, 4)
    )

    driver_metrics = [
        "attention_score",
        "demand_strength_score",
        "demand_breadth_score",
        "demand_quality_score",
        "narrative_coherence_score",
        "social_fragility_score",
    ]

    drivers = []
    for metric in driver_metrics:
        b = _safe_float(before_social.get(metric))
        a = _safe_float(after_social.get(metric))
        if b is None or a is None:
            continue

        delta = round(a - b, 4)
        if abs(delta) > 0.0001:
            drivers.append(
                {
                    "metric": metric,
                    "before": b,
                    "after": a,
                    "delta": delta,
                }
            )

    drivers.sort(key=lambda x: abs(x["delta"]), reverse=True)

    summary_parts = []

    if social_score_delta is not None:
        if social_score_delta > 0:
            summary_parts.append(f"demand score improved by {social_score_delta:.2f}")
        elif social_score_delta < 0:
            summary_parts.append(f"demand score declined by {abs(social_score_delta):.2f}")

    if alignment_score_delta is not None:
        if alignment_score_delta > 0:
            summary_parts.append(f"alignment score improved by {alignment_score_delta:.2f}")
        elif alignment_score_delta < 0:
            summary_parts.append(f"alignment score declined by {abs(alignment_score_delta):.2f}")

    if not summary_parts:
        summary = "No material score change detected."
    else:
        summary = "; ".join(summary_parts) + "."

    return {
        "before": {
            "social_day": before_social.get("day"),
            "demand_state": before_social.get("demand_state") or before_social.get("recommendation"),
            "demand_score": social_before_score,
            "alignment_day": before_alignment.get("day"),
            "alignment_state": before_alignment.get("alignment_state"),
            "alignment_score": alignment_before_score,
        },
        "after": {
            "social_day": after_social.get("day"),
            "demand_state": after_social.get("demand_state") or after_social.get("recommendation"),
            "demand_score": social_after_score,
            "alignment_day": after_alignment.get("day"),
            "alignment_state": after_alignment.get("alignment_state"),
            "alignment_score": alignment_after_score,
        },
        "deltas": {
            "demand_score_delta": social_score_delta,
            "alignment_score_delta": alignment_score_delta,
            "demand_state_changed": (
                (before_social.get("demand_state") or before_social.get("recommendation"))
                != (after_social.get("demand_state") or after_social.get("recommendation"))
            ),
            "alignment_state_changed": before_alignment.get("alignment_state") != after_alignment.get("alignment_state"),
            "driver_deltas": drivers[:6],
            "social_flags": _diff_flags(
                before_social.get("flags") or [],
                after_social.get("flags") or [],
            ),
            "alignment_flags": _diff_flags(
                before_alignment.get("flags") or [],
                after_alignment.get("flags") or [],
            ),
        },
        "summary": summary,
    }


def refresh_social_for_market(market_id: str):
    target_day = datetime.now(timezone.utc).date()

    before_state = _fetch_latest_social_alignment_state(market_id)

    ingest_result = ingest_social_for_market(market_id) or {}
    features_result = compute_market_social_features_daily(
        day=target_day,
        market_id=market_id,
    ) or {}

    scoring_result = score_market_social_features_daily(
        day=target_day,
        market_id=market_id,
    ) or {}

    social_intelligence_result = compute_market_social_intelligence_daily(
        day=target_day,
        market_id=market_id,
        limit_markets=1,
    ) or {}

    alignment_result = compute_market_alignment_daily(
        market_id=market_id,
        limit_markets=1,
        horizon_mode="mixed_latest",
    ) or {}

    after_state = _fetch_latest_social_alignment_state(market_id)
    score_diffs = _build_score_diffs(before_state, after_state)

    # normalize nested day fields so refresh payload is easier to trust/read
    ingest_result["day"] = str(target_day)
    features_result["day"] = str(target_day)
    scoring_result["day"] = str(target_day)
    social_intelligence_result["day"] = str(target_day)
    alignment_result["day"] = str(target_day)

    return {
        "status": "ok",
        "market_id": market_id,
        "target_day": str(target_day),
        "ingest_result": ingest_result,
        "features_result": features_result,
        "scoring_result": scoring_result,
        "social_intelligence_result": social_intelligence_result,
        "alignment_result": alignment_result,
        "score_diffs": score_diffs,
    }