from typing import Any, Dict, Optional
import psycopg

from apps.api.db import get_db_dsn


def _to_text_day(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return str(value)
    except Exception:
        return None


def persist_market_intervention_intelligence(
    *,
    market_id: str,
    snapshot: Dict[str, Any],
    intervention: Dict[str, Any],
    engine_version: str = "market_intervention_intelligence_v1",
) -> Dict[str, Any]:
    market_block = snapshot.get("market") or {}
    alignment = snapshot.get("alignment_intelligence") or {}
    social = snapshot.get("social_intelligence") or {}

    day = (
        _to_text_day(alignment.get("structural_day"))
        or _to_text_day(market_block.get("day"))
        or _to_text_day(social.get("day"))
    )

    if not day:
        return {"persisted": False, "reason": "missing_day"}

    q = """
    INSERT INTO public.market_intervention_intelligence_daily (
      market_id,
      day,
      intervention_needed,
      recommended_intervention,
      recommended_action,
      action_priority,
      action_reason,
      incentive_dependency,
      activity_quality,
      organic_participation_ratio,
      distortion_risk,
      expected_failure_mode,
      intervention_effectiveness_estimate,
      confidence,
      inputs,
      engine_version
    )
    VALUES (
      %(market_id)s,
      %(day)s::date,
      %(intervention_needed)s,
      %(recommended_intervention)s,
      %(recommended_action)s,
      %(action_priority)s,
      %(action_reason)s,
      %(incentive_dependency)s,
      %(activity_quality)s,
      %(organic_participation_ratio)s,
      %(distortion_risk)s,
      %(expected_failure_mode)s,
      %(intervention_effectiveness_estimate)s,
      %(confidence)s,
      %(inputs)s::jsonb,
      %(engine_version)s
    )
    ON CONFLICT (market_id, day, engine_version)
    DO UPDATE SET
      intervention_needed = EXCLUDED.intervention_needed,
      recommended_intervention = EXCLUDED.recommended_intervention,
      recommended_action = EXCLUDED.recommended_action,
      action_priority = EXCLUDED.action_priority,
      action_reason = EXCLUDED.action_reason,
      incentive_dependency = EXCLUDED.incentive_dependency,
      activity_quality = EXCLUDED.activity_quality,
      organic_participation_ratio = EXCLUDED.organic_participation_ratio,
      distortion_risk = EXCLUDED.distortion_risk,
      expected_failure_mode = EXCLUDED.expected_failure_mode,
      intervention_effectiveness_estimate = EXCLUDED.intervention_effectiveness_estimate,
      confidence = EXCLUDED.confidence,
      inputs = EXCLUDED.inputs,
      updated_at = NOW()
    RETURNING id, market_id, day::text AS day, engine_version, updated_at::text AS updated_at;
    """

    payload = {
        "market_id": market_id,
        "day": day,
        "intervention_needed": intervention.get("intervention_needed"),
        "recommended_intervention": intervention.get("recommended_intervention"),
        "recommended_action": intervention.get("recommended_action"),
        "action_priority": intervention.get("action_priority"),
        "action_reason": intervention.get("action_reason"),
        "incentive_dependency": intervention.get("incentive_dependency"),
        "activity_quality": intervention.get("activity_quality"),
        "organic_participation_ratio": intervention.get("organic_participation_ratio"),
        "distortion_risk": intervention.get("distortion_risk"),
        "expected_failure_mode": intervention.get("expected_failure_mode"),
        "intervention_effectiveness_estimate": intervention.get("intervention_effectiveness_estimate"),
        "confidence": intervention.get("confidence"),
        "inputs": psycopg.types.json.Json(intervention.get("inputs") or {}),
        "engine_version": engine_version,
    }

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, payload)
            row = cur.fetchone()
            cols = [d.name for d in cur.description]
            return {"persisted": True, **dict(zip(cols, row))}