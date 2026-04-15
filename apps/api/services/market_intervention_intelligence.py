from typing import Any, Dict


def _safe_float(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except Exception:
        return 0.0


def _pick_first(*values):
    for v in values:
        if v is not None:
            return v
    return None


def compute_market_intervention_intelligence(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    market_block = snapshot.get("market") or {}
    social_intelligence = snapshot.get("social_intelligence") or {}
    alignment_intelligence = snapshot.get("alignment_intelligence") or {}
    traders_block = (snapshot.get("traders") or {}).get("same_day") or {}

    summary_rows = traders_block.get("summary") or []
    cohorts_rows = traders_block.get("cohorts_summary") or []

    total_trades = 0.0
    unique_traders = 0.0
    avg_spread = _safe_float(
        _pick_first(
            market_block.get("spread_median"),
            market_block.get("bid_ask_spread"),
        )
    )
    concentration_hhi = _safe_float(
        _pick_first(
            market_block.get("concentration_hhi"),
            market_block.get("trader_concentration_hhi"),
        )
    )

    if isinstance(summary_rows, list) and summary_rows:
        total_trades = sum(_safe_float(r.get("trades")) for r in summary_rows)
        unique_traders = float(len(summary_rows))

    if total_trades <= 0:
        total_trades = _safe_float(
            _pick_first(
                market_block.get("trades"),
                market_block.get("trade_count"),
                market_block.get("total_trades"),
            )
        )

    if unique_traders <= 0:
        unique_traders = _safe_float(
            _pick_first(
                market_block.get("unique_traders"),
                market_block.get("participant_count"),
            )
        )

    structural_state = (
        alignment_intelligence.get("structural_state")
        or market_block.get("structural_state")
        or "weak"
    )

    social_state = (
        alignment_intelligence.get("social_state")
        or social_intelligence.get("demand_state")
        or social_intelligence.get("social_state")
        or "absent"
    )

    alignment_state = (
        alignment_intelligence.get("alignment_state")
        or snapshot.get("alignment_state")
        or "weak"
    )

    organic_participation_ratio = 0.0
    if total_trades > 0:
        base_ratio = unique_traders / max(total_trades, 1.0)
    else:
        base_ratio = 0.0

    organic_participation_ratio = base_ratio

    # Penalize very small markets so a 1 trader = 1 trade situation does not look perfectly organic
    if total_trades < 25:
        organic_participation_ratio *= 0.70
    elif total_trades < 50:
        organic_participation_ratio *= 0.85

    if unique_traders < 15:
        organic_participation_ratio *= 0.85

    if concentration_hhi > 0.40:
        organic_participation_ratio -= 0.10
    elif concentration_hhi > 0.25:
        organic_participation_ratio -= 0.05

    if avg_spread > 0.05:
        organic_participation_ratio -= 0.05

    organic_participation_ratio = round(max(0.0, min(1.0, organic_participation_ratio)), 4)

    if structural_state == "weak" and social_state in {"strong", "established"}:
        incentive_dependency = "high"
    elif structural_state == "moderate" and social_state in {"strong", "established"}:
        incentive_dependency = "moderate"
    elif structural_state == "strong":
        incentive_dependency = "low"
    else:
        incentive_dependency = "none"

    if organic_participation_ratio > 0.60 and concentration_hhi < 0.20:
        activity_quality = "organic"
    elif organic_participation_ratio > 0.40:
        activity_quality = "supported"
    else:
        activity_quality = "distorted"

    distortion_risk = "low"
    if concentration_hhi > 0.40 or organic_participation_ratio < 0.30:
        distortion_risk = "high"
    elif avg_spread > 0.05 or concentration_hhi > 0.25:
        distortion_risk = "moderate"

    if structural_state == "weak":
        recommended_intervention = "liquidity_support"
    elif alignment_state == "conviction_mismatch" and social_state in {"strong", "established"}:
        recommended_intervention = "liquidity_support"
    elif social_state == "absent":
        recommended_intervention = "demand_generation"
    elif activity_quality == "distorted":
        recommended_intervention = "market_maker_support"
    elif alignment_state == "structure_led":
        recommended_intervention = "demand_generation"
    else:
        recommended_intervention = "none"

    if activity_quality == "distorted" and concentration_hhi > 0.40:
        expected_failure_mode = "mercenary_capital"
    elif alignment_state == "conviction_mismatch" and social_state in {"strong", "established"}:
        expected_failure_mode = "one_sided_liquidity"
    elif avg_spread > 0.05:
        expected_failure_mode = "one_sided_liquidity"
    elif organic_participation_ratio < 0.20:
        expected_failure_mode = "fake_volume"
    elif incentive_dependency in {"high", "moderate"} and recommended_intervention != "none":
        expected_failure_mode = "post_incentive_drop"
    else:
        expected_failure_mode = "none"

    intervention_needed = recommended_intervention != "none"

    confidence = 0.70
    if total_trades <= 0 or unique_traders <= 0:
        confidence = 0.55
    elif total_trades >= 25 and unique_traders >= 10:
        confidence = 0.78

    intervention_effectiveness_estimate = 0.60
    if recommended_intervention == "liquidity_support" and structural_state == "weak":
        intervention_effectiveness_estimate = 0.68
    elif recommended_intervention == "demand_generation" and social_state == "absent":
        intervention_effectiveness_estimate = 0.52
    elif recommended_intervention == "market_maker_support":
        intervention_effectiveness_estimate = 0.64

    if not intervention_needed:
        recommended_action = "monitor"
        action_priority = "low"
        action_reason = "No immediate intervention is required based on current structure, demand, and participation signals."
    elif activity_quality == "distorted":
        recommended_action = "tighten_or_redesign"
        action_priority = "high"
        action_reason = "Activity quality is distorted, so direct support alone may amplify weak or artificial participation."
    elif recommended_intervention == "liquidity_support":
        recommended_action = "add_targeted_liquidity"
        action_priority = "high" if structural_state == "weak" else "medium"
        action_reason = "Demand is present but market structure is not strong enough to convert attention into credible participation."
    elif recommended_intervention == "market_maker_support":
        recommended_action = "add_market_maker_support"
        action_priority = "medium"
        action_reason = "Participation appears narrow or unstable, so structured maker support is safer than broad user incentives."
    elif recommended_intervention == "demand_generation":
        recommended_action = "do_not_subsidize_yet"
        action_priority = "medium"
        action_reason = "Structure is ahead of demand, so liquidity support is premature until external demand strengthens."
    else:
        recommended_action = "monitor"
        action_priority = "low"
        action_reason = "Current signals do not justify a stronger intervention recommendation."

    return {
        "intervention_needed": intervention_needed,
        "recommended_intervention": recommended_intervention,
        "recommended_action": recommended_action,
        "action_priority": action_priority,
        "action_reason": action_reason,
        "incentive_dependency": incentive_dependency,
        "activity_quality": activity_quality,
        "organic_participation_ratio": round(organic_participation_ratio, 4),
        "distortion_risk": distortion_risk,
        "expected_failure_mode": expected_failure_mode,
        "intervention_effectiveness_estimate": round(intervention_effectiveness_estimate, 4),
        "confidence": round(confidence, 4),
        "inputs": {
            "structural_state": structural_state,
            "social_state": social_state,
            "alignment_state": alignment_state,
            "total_trades": total_trades,
            "unique_traders": unique_traders,
            "spread_median": avg_spread,
            "concentration_hhi": concentration_hhi,
            "cohort_rows_count": len(cohorts_rows),
        },
    }