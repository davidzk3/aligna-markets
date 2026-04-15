from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn


ENGINE_VERSION = "market_design_intelligence_v2"


EXCLUSION_PATTERNS = [
    "will not count",
    "will not qualify",
    "for the purposes of this market",
    "otherwise",
    "except",
    "excluding",
    "does not count",
    "shall not count",
    "do not count",
]

TIME_AMBIGUITY_PATTERNS = [
    "listed date",
    "specified week",
    "specified timeframe",
    "within 48 hours",
    "within 24 hours",
    "within 2 hours",
    "by the end of the third calendar date",
    "timeframe",
    "cannot be confirmed",
    "can be confirmed",
]

TIMEZONE_PATTERNS = [
    "gmt",
    "utc",
    "eet",
    "cet",
    "est",
    "pst",
    "israel time",
    "eastern european time",
]

SOURCE_PATTERNS = [
    "consensus of credible reporting",
    "primary resolution source",
    "official statements",
    "official information",
    "major international media",
    "national broadcasters",
    "government authorities",
    "military statements",
    "video evidence",
    "multilateral bodies",
]

BULLETIN_PATTERNS = [
    "bulletin board",
    "updates made by the question creator",
    "should be considered",
]

GEOGRAPHY_PATTERNS = [
    "territory",
    "municipality",
    "within",
    "exclusive of",
    "west bank",
    "gaza strip",
    "counts as",
    "ground territory",
]

NEUTRAL_OUTCOME_PATTERNS = [
    "50-50",
    "resolve to 50-50",
    "resolves to 50-50",
    "will resolve to 50-50",
]

CANCELLATION_PATTERNS = [
    "canceled",
    "not played at all",
    "delayed beyond",
    "forfeit",
    "disqualification",
    "walkover",
]

REMAKE_PATTERNS = [
    "remade",
    "remake",
]

SERIES_DEPENDENCY_PATTERNS = [
    "series result has already been determined",
    "before game 2 is needed",
    "clinches the series",
]

PROXY_EXCLUSION_PATTERNS = [
    "proxy forces",
    "hezbollah",
    "houthis",
]

TERRITORIAL_CARVEOUT_PATTERNS = [
    "west bank",
    "gaza strip",
    "municipality",
    "territorial territory",
    "ground territory",
]

CONFIRMATION_WINDOW_PATTERNS = [
    "within 48 hours",
    "within 24 hours",
    "within 2 hours",
    "by the end of the third calendar date",
]

OFFICIAL_SOURCE_PATTERNS = [
    "official information",
    "official statements",
    "official government",
    "official military",
]

CONSENSUS_FALLBACK_PATTERNS = [
    "consensus of credible reporting",
]

VIDEO_EVIDENCE_PATTERNS = [
    "video evidence",
]

CLOSED_WORLD_PATTERNS = [
    "game 2",
    "match",
    "series",
    "dragon",
    "elemental dragon",
    "elder dragon",
    "remade game",
]


def _normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    value = value.lower().strip()
    value = re.sub(r"\s+", " ", value)
    return value


def _count_occurrences(text: str, patterns: List[str]) -> int:
    t = _normalize_text(text)
    return sum(t.count(p.lower()) for p in patterns)


def _contains_any(text: str, patterns: List[str]) -> bool:
    t = _normalize_text(text)
    return any(p.lower() in t for p in patterns)


def _word_count(*parts: Optional[str]) -> int:
    text = " ".join([p for p in parts if p])
    return len(re.findall(r"\b\w+\b", text))


def _sha(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def fetch_latest_pattern_prior(pattern_type: str, pattern_value: str) -> Optional[Dict[str, Any]]:
    q = """
    SELECT
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
        avg_dispute_to_settlement_hours
    FROM public.market_resolution_patterns_daily
    WHERE pattern_type = %s
      AND pattern_value = %s
    ORDER BY day DESC
    LIMIT 1
    """
    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, (pattern_type, pattern_value))
            row = cur.fetchone()
            if not row:
                return None

            return {
                "day": str(row[0]),
                "pattern_type": row[1],
                "pattern_value": row[2],
                "markets_count": row[3],
                "disputed_count": row[4],
                "settled_count": row[5],
                "dispute_rate": float(row[6]) if row[6] is not None else None,
                "avg_resolution_clarity_score": float(row[7]) if row[7] is not None else None,
                "avg_oracle_risk_score": float(row[8]) if row[8] is not None else None,
                "avg_dispute_propensity_score": float(row[9]) if row[9] is not None else None,
                "avg_request_to_settlement_hours": float(row[10]) if row[10] is not None else None,
                "avg_dispute_to_settlement_hours": float(row[11]) if row[11] is not None else None,
            }


def _infer_category(title: str, description: str, explicit_category: Optional[str]) -> str:
    if explicit_category:
        return explicit_category.lower()

    text = _normalize_text(f"{title} {description}")

    if any(x in text for x in ["dragon", "match", "series", "esports", "game 2"]):
        return "esports"
    if any(x in text for x in ["strike", "military", "missile", "iran", "israel", "ukrainian", "russian"]):
        return "geopolitics"
    return "general"


def extract_design_features(payload: Dict[str, Any]) -> Dict[str, Any]:
    title = payload.get("title") or ""
    description = payload.get("description") or ""
    text = f"{title}\n{description}"

    category = _infer_category(title, description, payload.get("category"))

    word_count = _word_count(title, description)
    exclusion_count = _count_occurrences(text, EXCLUSION_PATTERNS)
    source_reference_count = _count_occurrences(text, SOURCE_PATTERNS)
    geography_constraint_count = _count_occurrences(text, GEOGRAPHY_PATTERNS)
    neutral_outcome_branch_count = _count_occurrences(text, NEUTRAL_OUTCOME_PATTERNS)

    has_time_ambiguity = _contains_any(text, TIME_AMBIGUITY_PATTERNS)
    has_wording_ambiguity = exclusion_count >= 2 or _contains_any(text, EXCLUSION_PATTERNS)
    has_source_fragility = _contains_any(text, SOURCE_PATTERNS)
    has_bulletin_board_dependency = _contains_any(text, BULLETIN_PATTERNS)
    has_timezone_complexity = _count_occurrences(text, TIMEZONE_PATTERNS) >= 2
    has_official_source_priority = _contains_any(text, OFFICIAL_SOURCE_PATTERNS)
    has_consensus_fallback = _contains_any(text, CONSENSUS_FALLBACK_PATTERNS)
    has_video_evidence_fallback = _contains_any(text, VIDEO_EVIDENCE_PATTERNS)
    has_remake_logic = _contains_any(text, REMAKE_PATTERNS)
    has_cancellation_logic = _contains_any(text, CANCELLATION_PATTERNS)
    has_series_dependency_logic = _contains_any(text, SERIES_DEPENDENCY_PATTERNS)
    has_proxy_actor_exclusion = _contains_any(text, PROXY_EXCLUSION_PATTERNS)
    has_territorial_carveout = _contains_any(text, TERRITORIAL_CARVEOUT_PATTERNS)
    has_confirmation_window = _contains_any(text, CONFIRMATION_WINDOW_PATTERNS)

    event_container_specificity_score = 0.0
    if category in {"esports", "sports"}:
        event_container_specificity_score += 0.30
    if _contains_any(text, CLOSED_WORLD_PATTERNS):
        event_container_specificity_score += 0.35
    if has_official_source_priority:
        event_container_specificity_score += 0.20
    if has_remake_logic or has_cancellation_logic:
        event_container_specificity_score += 0.15
    event_container_specificity_score = round(min(1.0, event_container_specificity_score), 4)

    return {
        "category": category,
        "word_count": word_count,
        "exclusion_count": exclusion_count,
        "source_reference_count": source_reference_count,
        "geography_constraint_count": geography_constraint_count,
        "neutral_outcome_branch_count": neutral_outcome_branch_count,
        "has_time_ambiguity": has_time_ambiguity,
        "has_wording_ambiguity": has_wording_ambiguity,
        "has_source_fragility": has_source_fragility,
        "has_bulletin_board_dependency": has_bulletin_board_dependency,
        "has_timezone_complexity": has_timezone_complexity,
        "has_official_source_priority": has_official_source_priority,
        "has_consensus_fallback": has_consensus_fallback,
        "has_video_evidence_fallback": has_video_evidence_fallback,
        "has_remake_logic": has_remake_logic,
        "has_cancellation_logic": has_cancellation_logic,
        "has_series_dependency_logic": has_series_dependency_logic,
        "has_proxy_actor_exclusion": has_proxy_actor_exclusion,
        "has_territorial_carveout": has_territorial_carveout,
        "has_confirmation_window": has_confirmation_window,
        "event_container_specificity_score": event_container_specificity_score,
    }


def score_design(features: Dict[str, Any]) -> Dict[str, Any]:
    category = features["category"]

    flags: List[str] = []
    suggestions: List[str] = []

    edge_case_branching_score = 0.0
    if features["has_cancellation_logic"]:
        edge_case_branching_score += 0.18
    if features["has_remake_logic"]:
        edge_case_branching_score += 0.12
    if features["has_series_dependency_logic"]:
        edge_case_branching_score += 0.12
    edge_case_branching_score += min(0.20, features["neutral_outcome_branch_count"] * 0.05)
    edge_case_branching_score = round(min(1.0, edge_case_branching_score), 4)

    interpretive_ambiguity_score = 0.0
    if features["has_wording_ambiguity"]:
        interpretive_ambiguity_score += 0.35
    if features["has_proxy_actor_exclusion"]:
        interpretive_ambiguity_score += 0.12
    if features["has_territorial_carveout"]:
        interpretive_ambiguity_score += 0.18
    interpretive_ambiguity_score += min(0.15, features["exclusion_count"] * 0.03)
    interpretive_ambiguity_score = round(min(1.0, interpretive_ambiguity_score), 4)

    source_fragility_score = 0.0
    if features["has_source_fragility"]:
        source_fragility_score += 0.20
    if features["has_consensus_fallback"]:
        source_fragility_score += 0.18
    if not features["has_official_source_priority"]:
        source_fragility_score += 0.12
    if features["has_video_evidence_fallback"]:
        source_fragility_score += 0.06
    if features["has_bulletin_board_dependency"]:
        source_fragility_score += 0.18
    source_fragility_score = round(min(1.0, source_fragility_score), 4)

    time_confirmation_fragility_score = 0.0
    if features["has_time_ambiguity"]:
        time_confirmation_fragility_score += 0.22
    if features["has_timezone_complexity"]:
        time_confirmation_fragility_score += 0.12
    if features["has_confirmation_window"]:
        time_confirmation_fragility_score += 0.18
    time_confirmation_fragility_score = round(min(1.0, time_confirmation_fragility_score), 4)

    closed_world_resolvability_score = features["event_container_specificity_score"]
    if features["has_official_source_priority"]:
        closed_world_resolvability_score += 0.15
    if features["has_cancellation_logic"] or features["has_remake_logic"]:
        closed_world_resolvability_score += 0.10
    if category == "geopolitics":
        closed_world_resolvability_score -= 0.10
    closed_world_resolvability_score = round(max(0.0, min(1.0, closed_world_resolvability_score)), 4)

    clarity = 0.82
    clarity -= interpretive_ambiguity_score * 0.35
    clarity -= source_fragility_score * 0.20
    clarity -= time_confirmation_fragility_score * 0.20
    clarity -= edge_case_branching_score * 0.08
    clarity += closed_world_resolvability_score * 0.18

    if features["word_count"] >= 180:
        clarity -= 0.05
        flags.append("LONG_RULE_TEXT")
    if features["word_count"] >= 300:
        clarity -= 0.05
        flags.append("VERY_LONG_RULE_TEXT")

    risk = 0.10
    risk += interpretive_ambiguity_score * 0.38
    risk += source_fragility_score * 0.24
    risk += time_confirmation_fragility_score * 0.22
    risk += edge_case_branching_score * 0.08
    risk -= closed_world_resolvability_score * 0.12

    dispute = 0.12
    dispute += interpretive_ambiguity_score * 0.32
    dispute += source_fragility_score * 0.20
    dispute += time_confirmation_fragility_score * 0.16
    dispute += edge_case_branching_score * 0.06
    dispute -= closed_world_resolvability_score * 0.08

    if category == "geopolitics":
        risk += 0.08
        dispute += 0.08
        interpretive_ambiguity_score = round(min(1.0, interpretive_ambiguity_score + 0.08), 4)
        source_fragility_score = round(min(1.0, source_fragility_score + 0.05), 4)
        time_confirmation_fragility_score = round(min(1.0, time_confirmation_fragility_score + 0.05), 4)
    elif category == "esports":
        risk -= 0.04
        dispute -= 0.04

    if features["has_wording_ambiguity"]:
        flags.append("WORDING_AMBIGUITY")
        suggestions.append("Tighten qualifying language so the event can be evaluated without interpretive drift.")

    if features["has_time_ambiguity"]:
        flags.append("TIME_AMBIGUITY")
        suggestions.append("Use one explicit event-time standard and one explicit verification window.")

    if features["has_source_fragility"]:
        flags.append("SOURCE_FRAGILITY")
        suggestions.append("Prefer a stronger primary source hierarchy and reduce reliance on consensus-sensitive reporting.")

    if features["has_bulletin_board_dependency"]:
        flags.append("BULLETIN_BOARD_DEPENDENCY")
        suggestions.append("Avoid relying on bulletin-board updates for core market meaning.")

    if features["has_territorial_carveout"]:
        flags.append("TERRITORIAL_CARVEOUT_COMPLEXITY")
        suggestions.append("Simplify territorial carveouts or define them in a more operationally precise way.")

    if features["has_proxy_actor_exclusion"]:
        flags.append("ACTOR_ATTRIBUTION_COMPLEXITY")
        suggestions.append("Clarify attribution standards so proxy-force exclusions do not create interpretive drift.")

    if features["neutral_outcome_branch_count"] >= 2:
        flags.append("MULTIPLE_NEUTRAL_OUTCOME_BRANCHES")
        suggestions.append("Reduce redundant neutral-outcome branches and consolidate them into one explicit fallback rule.")

    if features["has_official_source_priority"]:
        flags.append("OFFICIAL_SOURCE_PRIORITY")

    clarity = round(max(0.0, min(1.0, clarity)), 4)
    risk = round(max(0.0, min(1.0, risk)), 4)
    dispute = round(max(0.0, min(1.0, dispute)), 4)

    operational_points = 0
    operational_points += 1 if edge_case_branching_score >= 0.25 else 0
    operational_points += 1 if features["word_count"] >= 250 else 0
    operational_points += 1 if features["neutral_outcome_branch_count"] >= 2 else 0
    operational_points += 1 if features["has_remake_logic"] else 0
    operational_points += 1 if features["has_cancellation_logic"] else 0
    operational_points += 1 if features["has_series_dependency_logic"] else 0

    if operational_points >= 4:
        operational_complexity = "high"
    elif operational_points >= 2:
        operational_complexity = "moderate"
    else:
        operational_complexity = "low"

    interpretive_points = 0
    interpretive_points += 1 if interpretive_ambiguity_score >= 0.30 else 0
    interpretive_points += 1 if source_fragility_score >= 0.30 else 0
    interpretive_points += 1 if time_confirmation_fragility_score >= 0.25 else 0
    interpretive_points += 1 if features["has_proxy_actor_exclusion"] else 0
    interpretive_points += 1 if features["has_territorial_carveout"] else 0
    interpretive_points += 1 if features["has_confirmation_window"] else 0

    # Closed-world markets with strong event specificity should not be over-penalized
    # as highly interpretive just because they contain many explicit edge-case branches.
    if (
        features["category"] in {"esports", "sports"}
        and closed_world_resolvability_score >= 0.75
        and features["event_container_specificity_score"] >= 0.75
        and not features["has_proxy_actor_exclusion"]
        and not features["has_territorial_carveout"]
    ):
        interpretive_points = max(0, interpretive_points - 2)

    if interpretive_points >= 4:
        interpretive_complexity = "high"
    elif interpretive_points >= 2:
        interpretive_complexity = "moderate"
    else:
        interpretive_complexity = "low"

    if features["has_official_source_priority"] and features["has_consensus_fallback"]:
        evidence_dependence_tier = "official_primary_consensus_fallback"
    elif features["has_official_source_priority"]:
        evidence_dependence_tier = "official_primary"
    elif features["has_consensus_fallback"]:
        evidence_dependence_tier = "consensus_primary"
    else:
        evidence_dependence_tier = "unclear_source_hierarchy"

    design_complexity = (
        "high_complexity"
        if operational_complexity == "high" or interpretive_complexity == "high"
        else "moderate_complexity"
        if operational_complexity == "moderate" or interpretive_complexity == "moderate"
        else "low_complexity"
    )

    if risk >= 0.72 or clarity <= 0.38:
        expected_resolution_state = "fragile"
        launch_readiness = "high_resolution_risk"
    elif risk >= 0.45 or dispute >= 0.35:
        expected_resolution_state = "moderate"
        launch_readiness = "revise_before_launch"
    else:
        expected_resolution_state = "cleanly_resolvable"
        launch_readiness = "launch_ready"

    return {
        "edge_case_branching_score": edge_case_branching_score,
        "interpretive_ambiguity_score": interpretive_ambiguity_score,
        "source_fragility_score": source_fragility_score,
        "time_confirmation_fragility_score": time_confirmation_fragility_score,
        "closed_world_resolvability_score": closed_world_resolvability_score,
        "operational_complexity": operational_complexity,
        "interpretive_complexity": interpretive_complexity,
        "evidence_dependence_tier": evidence_dependence_tier,
        "design_clarity_score": clarity,
        "expected_resolution_risk_score": risk,
        "expected_dispute_propensity_score": dispute,
        "design_complexity": design_complexity,
        "expected_resolution_state": expected_resolution_state,
        "launch_readiness": launch_readiness,
        "flags": sorted(list(set(flags))),
        "suggestions": list(dict.fromkeys(suggestions)),
    }


def apply_historical_overlay(
    base_scores: Dict[str, Any],
    complexity_prior: Optional[Dict[str, Any]],
    category_prior: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    clarity = base_scores["design_clarity_score"]
    risk = base_scores["expected_resolution_risk_score"]
    dispute = base_scores["expected_dispute_propensity_score"]

    priors = {}

    if complexity_prior:
        priors["resolution_complexity"] = complexity_prior
        if complexity_prior.get("avg_resolution_clarity_score") is not None:
            clarity = round((clarity * 0.75) + (complexity_prior["avg_resolution_clarity_score"] * 0.25), 4)
        if complexity_prior.get("avg_oracle_risk_score") is not None:
            risk = round((risk * 0.75) + (complexity_prior["avg_oracle_risk_score"] * 0.25), 4)
        if complexity_prior.get("avg_dispute_propensity_score") is not None:
            dispute = round((dispute * 0.75) + (complexity_prior["avg_dispute_propensity_score"] * 0.25), 4)

    if category_prior:
        priors["category"] = category_prior
        if category_prior.get("avg_oracle_risk_score") is not None:
            risk = round((risk * 0.85) + (category_prior["avg_oracle_risk_score"] * 0.15), 4)
        if category_prior.get("avg_dispute_propensity_score") is not None:
            dispute = round((dispute * 0.85) + (category_prior["avg_dispute_propensity_score"] * 0.15), 4)

    out = {
        **base_scores,
        "design_clarity_score": clarity,
        "expected_resolution_risk_score": risk,
        "expected_dispute_propensity_score": dispute,
        "historical_priors": priors,
    }

    if risk >= 0.72 or clarity <= 0.38:
        out["expected_resolution_state"] = "fragile"
        out["launch_readiness"] = "high_resolution_risk"
    elif risk >= 0.45 or dispute >= 0.35:
        out["expected_resolution_state"] = "moderate"
        out["launch_readiness"] = "revise_before_launch"
    else:
        out["expected_resolution_state"] = "cleanly_resolvable"
        out["launch_readiness"] = "launch_ready"

    return out


def classify_score_band(score: float, kind: str) -> str:
    score = float(score or 0.0)

    if kind == "resolution_risk":
        if score < 0.30:
            return "low"
        if score < 0.55:
            return "manageable"
        if score < 0.75:
            return "high"
        return "critical"

    if kind == "dispute_propensity":
        if score < 0.25:
            return "low"
        if score < 0.45:
            return "moderate"
        if score < 0.65:
            return "high"
        return "critical"

    return "unknown"


def build_operator_decision(
    scores: Dict[str, Any],
    features: Dict[str, Any],
) -> Dict[str, str]:
    risk_score = float(scores.get("expected_resolution_risk_score") or 0.0)
    dispute_score = float(scores.get("expected_dispute_propensity_score") or 0.0)
    clarity_score = float(scores.get("design_clarity_score") or 0.0)

    resolution_risk_band = classify_score_band(risk_score, "resolution_risk")
    dispute_propensity_band = classify_score_band(dispute_score, "dispute_propensity")

    if resolution_risk_band == "critical" or clarity_score <= 0.38:
        operator_decision_band = "do_not_launch"
        decision_rationale = (
            "Resolution risk is too high for launch in the current form. "
            "The draft should be rewritten before it is considered for deployment."
        )
    elif resolution_risk_band == "high" or dispute_propensity_band in {"high", "critical"}:
        operator_decision_band = "revise_before_launch"
        decision_rationale = (
            "The market may be launchable later, but the current draft carries too much resolution or dispute risk. "
            "Rewrite the highest-risk clauses first."
        )
    elif resolution_risk_band == "manageable":
        operator_decision_band = "launch_with_review"
        decision_rationale = (
            "The market is broadly workable, but should receive operator review before launch."
        )
    else:
        operator_decision_band = "launch"
        decision_rationale = (
            "The market appears sufficiently clear and operationally manageable for launch."
        )

    # Closed-world markets with strong structure should be allowed through more easily
    if (
        features.get("category") in {"esports", "sports"}
        and scores.get("closed_world_resolvability_score", 0.0) >= 0.75
        and operator_decision_band == "launch_with_review"
    ):
        operator_decision_band = "launch"
        decision_rationale = (
            "The market is operationally dense but bounded and appears sufficiently well-specified for launch."
        )

    return {
        "resolution_risk_band": resolution_risk_band,
        "dispute_propensity_band": dispute_propensity_band,
        "operator_decision_band": operator_decision_band,
        "decision_rationale": decision_rationale,
    }

def build_summary(payload: Dict[str, Any], features: Dict[str, Any], scores: Dict[str, Any]) -> str:
    title = payload.get("title") or "Proposed market"
    readiness = scores.get("launch_readiness")
    complexity = scores.get("design_complexity")
    risk = scores.get("expected_resolution_risk_score")
    clarity = scores.get("design_clarity_score")
    category = features.get("category")

    if readiness == "high_resolution_risk":
        return (
            f"{title} appears high risk for launch. "
            f"This {category} market shows {complexity}, expected resolution risk {risk}, and clarity {clarity}."
        )

    if readiness == "revise_before_launch":
        return (
            f"{title} is potentially launchable but should be revised first. "
            f"This {category} market shows {complexity}, expected resolution risk {risk}, and clarity {clarity}."
        )

    return (
        f"{title} appears relatively cleanly resolvable for launch. "
        f"This {category} market shows {complexity}, expected resolution risk {risk}, and clarity {clarity}."
    )


def persist_design_evaluation(payload: Dict[str, Any], features: Dict[str, Any], scores: Dict[str, Any], input_hash: str) -> Dict[str, Any]:
    q = """
    INSERT INTO public.market_design_intelligence_evaluations (
        input_hash,
        title,
        description,
        category,
        market_type,
        protocol,
        oracle_family,
        word_count,
        exclusion_count,
        source_reference_count,
        geography_constraint_count,
        neutral_outcome_branch_count,
        has_time_ambiguity,
        has_wording_ambiguity,
        has_source_fragility,
        has_bulletin_board_dependency,
        has_timezone_complexity,
        has_official_source_priority,
        has_consensus_fallback,
        has_video_evidence_fallback,
        has_remake_logic,
        has_cancellation_logic,
        has_series_dependency_logic,
        has_proxy_actor_exclusion,
        has_territorial_carveout,
        has_confirmation_window,
        event_container_specificity_score,
        edge_case_branching_score,
        interpretive_ambiguity_score,
        source_fragility_score,
        time_confirmation_fragility_score,
        closed_world_resolvability_score,
        design_clarity_score,
        expected_resolution_risk_score,
        expected_dispute_propensity_score,
        operational_complexity,
        interpretive_complexity,
        evidence_dependence_tier,
        design_complexity,
        expected_resolution_state,
        launch_readiness,
        resolution_risk_band,
        dispute_propensity_band,
        operator_decision_band,
        decision_rationale,
        flags,
        suggestions,
        historical_priors_json,
        extracted_features_json,
        summary,
        engine_version,
        updated_at
    )
    VALUES (
        %(input_hash)s,
        %(title)s,
        %(description)s,
        %(category)s,
        %(market_type)s,
        %(protocol)s,
        %(oracle_family)s,
        %(word_count)s,
        %(exclusion_count)s,
        %(source_reference_count)s,
        %(geography_constraint_count)s,
        %(neutral_outcome_branch_count)s,
        %(has_time_ambiguity)s,
        %(has_wording_ambiguity)s,
        %(has_source_fragility)s,
        %(has_bulletin_board_dependency)s,
        %(has_timezone_complexity)s,
        %(has_official_source_priority)s,
        %(has_consensus_fallback)s,
        %(has_video_evidence_fallback)s,
        %(has_remake_logic)s,
        %(has_cancellation_logic)s,
        %(has_series_dependency_logic)s,
        %(has_proxy_actor_exclusion)s,
        %(has_territorial_carveout)s,
        %(has_confirmation_window)s,
        %(event_container_specificity_score)s,
        %(edge_case_branching_score)s,
        %(interpretive_ambiguity_score)s,
        %(source_fragility_score)s,
        %(time_confirmation_fragility_score)s,
        %(closed_world_resolvability_score)s,
        %(design_clarity_score)s,
        %(expected_resolution_risk_score)s,
        %(expected_dispute_propensity_score)s,
        %(operational_complexity)s,
        %(interpretive_complexity)s,
        %(evidence_dependence_tier)s,
        %(design_complexity)s,
        %(expected_resolution_state)s,
        %(launch_readiness)s,
        %(resolution_risk_band)s,
        %(dispute_propensity_band)s,
        %(operator_decision_band)s,
        %(decision_rationale)s,
        %(flags)s,
        %(suggestions)s,
        %(historical_priors_json)s,
        %(extracted_features_json)s,
        %(summary)s,
        %(engine_version)s,
        NOW()
    )
    ON CONFLICT (input_hash)
    DO UPDATE SET
        title = EXCLUDED.title,
        description = EXCLUDED.description,
        category = EXCLUDED.category,
        market_type = EXCLUDED.market_type,
        protocol = EXCLUDED.protocol,
        oracle_family = EXCLUDED.oracle_family,
        word_count = EXCLUDED.word_count,
        exclusion_count = EXCLUDED.exclusion_count,
        source_reference_count = EXCLUDED.source_reference_count,
        geography_constraint_count = EXCLUDED.geography_constraint_count,
        neutral_outcome_branch_count = EXCLUDED.neutral_outcome_branch_count,
        has_time_ambiguity = EXCLUDED.has_time_ambiguity,
        has_wording_ambiguity = EXCLUDED.has_wording_ambiguity,
        has_source_fragility = EXCLUDED.has_source_fragility,
        has_bulletin_board_dependency = EXCLUDED.has_bulletin_board_dependency,
        has_timezone_complexity = EXCLUDED.has_timezone_complexity,
        has_official_source_priority = EXCLUDED.has_official_source_priority,
        has_consensus_fallback = EXCLUDED.has_consensus_fallback,
        has_video_evidence_fallback = EXCLUDED.has_video_evidence_fallback,
        has_remake_logic = EXCLUDED.has_remake_logic,
        has_cancellation_logic = EXCLUDED.has_cancellation_logic,
        has_series_dependency_logic = EXCLUDED.has_series_dependency_logic,
        has_proxy_actor_exclusion = EXCLUDED.has_proxy_actor_exclusion,
        has_territorial_carveout = EXCLUDED.has_territorial_carveout,
        has_confirmation_window = EXCLUDED.has_confirmation_window,
        event_container_specificity_score = EXCLUDED.event_container_specificity_score,
        edge_case_branching_score = EXCLUDED.edge_case_branching_score,
        interpretive_ambiguity_score = EXCLUDED.interpretive_ambiguity_score,
        source_fragility_score = EXCLUDED.source_fragility_score,
        time_confirmation_fragility_score = EXCLUDED.time_confirmation_fragility_score,
        closed_world_resolvability_score = EXCLUDED.closed_world_resolvability_score,
        design_clarity_score = EXCLUDED.design_clarity_score,
        expected_resolution_risk_score = EXCLUDED.expected_resolution_risk_score,
        expected_dispute_propensity_score = EXCLUDED.expected_dispute_propensity_score,
        operational_complexity = EXCLUDED.operational_complexity,
        interpretive_complexity = EXCLUDED.interpretive_complexity,
        evidence_dependence_tier = EXCLUDED.evidence_dependence_tier,
        design_complexity = EXCLUDED.design_complexity,
        expected_resolution_state = EXCLUDED.expected_resolution_state,
        launch_readiness = EXCLUDED.launch_readiness,
        resolution_risk_band = EXCLUDED.resolution_risk_band,
        dispute_propensity_band = EXCLUDED.dispute_propensity_band,
        operator_decision_band = EXCLUDED.operator_decision_band,
        decision_rationale = EXCLUDED.decision_rationale,
        flags = EXCLUDED.flags,
        suggestions = EXCLUDED.suggestions,
        historical_priors_json = EXCLUDED.historical_priors_json,
        extracted_features_json = EXCLUDED.extracted_features_json,
        summary = EXCLUDED.summary,
        engine_version = EXCLUDED.engine_version,
        updated_at = NOW()
    RETURNING id
    """

    params = {
        "input_hash": input_hash,
        "title": payload.get("title"),
        "description": payload.get("description"),
        "category": features.get("category"),
        "market_type": payload.get("market_type"),
        "protocol": payload.get("protocol") or "polymarket",
        "oracle_family": payload.get("oracle_family") or "uma_oo",
        "word_count": features["word_count"],
        "exclusion_count": features["exclusion_count"],
        "source_reference_count": features["source_reference_count"],
        "geography_constraint_count": features["geography_constraint_count"],
        "neutral_outcome_branch_count": features["neutral_outcome_branch_count"],
        "has_time_ambiguity": features["has_time_ambiguity"],
        "has_wording_ambiguity": features["has_wording_ambiguity"],
        "has_source_fragility": features["has_source_fragility"],
        "has_bulletin_board_dependency": features["has_bulletin_board_dependency"],
        "has_timezone_complexity": features["has_timezone_complexity"],
        "has_official_source_priority": features["has_official_source_priority"],
        "has_consensus_fallback": features["has_consensus_fallback"],
        "has_video_evidence_fallback": features["has_video_evidence_fallback"],
        "has_remake_logic": features["has_remake_logic"],
        "has_cancellation_logic": features["has_cancellation_logic"],
        "has_series_dependency_logic": features["has_series_dependency_logic"],
        "has_proxy_actor_exclusion": features["has_proxy_actor_exclusion"],
        "has_territorial_carveout": features["has_territorial_carveout"],
        "has_confirmation_window": features["has_confirmation_window"],
        "event_container_specificity_score": features["event_container_specificity_score"],
        "edge_case_branching_score": scores["edge_case_branching_score"],
        "interpretive_ambiguity_score": scores["interpretive_ambiguity_score"],
        "source_fragility_score": scores["source_fragility_score"],
        "time_confirmation_fragility_score": scores["time_confirmation_fragility_score"],
        "closed_world_resolvability_score": scores["closed_world_resolvability_score"],
        "design_clarity_score": scores["design_clarity_score"],
        "expected_resolution_risk_score": scores["expected_resolution_risk_score"],
        "expected_dispute_propensity_score": scores["expected_dispute_propensity_score"],
        "operational_complexity": scores["operational_complexity"],
        "interpretive_complexity": scores["interpretive_complexity"],
        "evidence_dependence_tier": scores["evidence_dependence_tier"],
        "design_complexity": scores["design_complexity"],
        "expected_resolution_state": scores["expected_resolution_state"],
        "launch_readiness": scores["launch_readiness"],
        "resolution_risk_band": scores["resolution_risk_band"],
        "dispute_propensity_band": scores["dispute_propensity_band"],
        "operator_decision_band": scores["operator_decision_band"],
        "decision_rationale": scores["decision_rationale"],
        "flags": Json(scores.get("flags") or []),
        "suggestions": Json(scores.get("suggestions") or []),
        "historical_priors_json": Json(scores.get("historical_priors") or {}),
        "extracted_features_json": Json(features),
        "summary": scores.get("summary"),
        "engine_version": ENGINE_VERSION,
    }

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            row = cur.fetchone()
        conn.commit()

    return {"id": row[0]}


def evaluate_market_design(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_payload = {
        "title": payload.get("title"),
        "description": payload.get("description"),
        "category": payload.get("category"),
        "market_type": payload.get("market_type"),
        "protocol": payload.get("protocol") or "polymarket",
        "oracle_family": payload.get("oracle_family") or "uma_oo",
    }

    input_hash = _sha(normalized_payload)

    features = extract_design_features(normalized_payload)
    base_scores = score_design(features)

    complexity_prior = fetch_latest_pattern_prior(
        "resolution_complexity",
        base_scores["design_complexity"],
    )
    category_prior = fetch_latest_pattern_prior(
        "alignment_state_pre",
        features["category"] if features["category"] else "unknown",
    )

    final_scores = apply_historical_overlay(
        base_scores,
        complexity_prior=complexity_prior,
        category_prior=category_prior,
    )

    calibrated = build_operator_decision(final_scores, features)
    final_scores.update(calibrated)

    final_scores["summary"] = build_summary(normalized_payload, features, final_scores)

    persisted = persist_design_evaluation(
        normalized_payload,
        features,
        final_scores,
        input_hash,
    )

    return {
        "id": persisted["id"],
        "input_hash": input_hash,
        "title": normalized_payload["title"],
        "category": features["category"],
        "market_type": normalized_payload["market_type"],
        "protocol": normalized_payload["protocol"],
        "oracle_family": normalized_payload["oracle_family"],
        "event_container_specificity_score": features["event_container_specificity_score"],
        "edge_case_branching_score": final_scores["edge_case_branching_score"],
        "interpretive_ambiguity_score": final_scores["interpretive_ambiguity_score"],
        "source_fragility_score": final_scores["source_fragility_score"],
        "time_confirmation_fragility_score": final_scores["time_confirmation_fragility_score"],
        "closed_world_resolvability_score": final_scores["closed_world_resolvability_score"],
        "operational_complexity": final_scores["operational_complexity"],
        "interpretive_complexity": final_scores["interpretive_complexity"],
        "evidence_dependence_tier": final_scores["evidence_dependence_tier"],
        "design_clarity_score": final_scores["design_clarity_score"],
        "expected_resolution_risk_score": final_scores["expected_resolution_risk_score"],
        "expected_dispute_propensity_score": final_scores["expected_dispute_propensity_score"],
        "design_complexity": final_scores["design_complexity"],
        "expected_resolution_state": final_scores["expected_resolution_state"],
        "launch_readiness": final_scores["launch_readiness"],
        "resolution_risk_band": final_scores["resolution_risk_band"],
        "dispute_propensity_band": final_scores["dispute_propensity_band"],
        "operator_decision_band": final_scores["operator_decision_band"],
        "decision_rationale": final_scores["decision_rationale"],
        "flags": final_scores.get("flags") or [],
        "suggestions": final_scores.get("suggestions") or [],
        "historical_priors": final_scores.get("historical_priors") or {},
        "extracted_features": features,
        "summary": final_scores["summary"],
        "engine_version": ENGINE_VERSION,
    }