from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Dict, List, Optional

import psycopg
from psycopg.types.json import Json

from apps.api.db import get_db_dsn
from apps.api.services.market_design_intelligence import evaluate_market_design


ENGINE_VERSION = "market_design_rewrite_intelligence_v1"


def _sha(payload: Dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()


def _split_sentences(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[\.\?\!])\s+", text.strip())
    return [p.strip() for p in parts if p.strip()]


def _fix_mojibake(text: str) -> str:
    if not text:
        return text

    fixed = text

    # First try the real encoding repair path
    if "â" in fixed or "Ã" in fixed:
        try:
            fixed = fixed.encode("cp1252").decode("utf-8")
        except Exception:
            pass

    # Then apply direct fallback replacements
    replacements = {
        "â€™": "’",
        "â€˜": "‘",
        "â€œ": "“",
        "â€\x9d": "”",
        "â€“": "–",
        "â€”": "—",
        "â€¦": "…",
        "Ã©": "é",
        "Ã¨": "è",
        "Ã ": "à",
        "â": "’",
    }

    for bad, good in replacements.items():
        fixed = fixed.replace(bad, good)

    return fixed.strip()

def _normalize_input_text(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    return _fix_mojibake(text)


def _contains_any(text: str, patterns: List[str]) -> bool:
    t = (_fix_mojibake(text or "")).lower()
    return any(p.lower() in t for p in patterns)

def _rewrite_time_clause(text: str) -> str:
    t = text.strip()

    t = t.replace(
        "on the listed date in Israel Time (GMT+2)",
        "on the listed date using Israel Time (GMT+2) as the sole time standard",
    )

    t = t.replace(
        "within 48 hours",
        "within the explicit 48-hour confirmation window",
    )

    t = t.replace(
        "within 24 hours",
        "within the explicit 24-hour confirmation window",
    )

    return t


def _rewrite_source_clause(text: str) -> str:
    return (
        "The primary resolution source will be official government or military statements first. "
        "Only if those are unavailable within the explicit confirmation window may a consensus of credible reporting be used."
    )


def _rewrite_attribution_clause(text: str) -> str:
    lowered = text.lower()

    if "proxy" in lowered:
        return (
            "Only directly attributable actions by the named state actor count. "
            "Proxy-force actions do not count."
        )

    return (
        "Only actions directly attributable to the named state actor, using the stated attribution standard, count toward resolution."
    )


def _rewrite_scope_clause(text: str) -> str:
    lowered = text.lower()

    if "west bank" in lowered or "gaza strip" in lowered:
        return (
            "The market should use one explicit territorial scope and avoid layered geographic carveouts."
        )

    if "territory" in lowered or "municipality" in lowered:
        return (
            "The qualifying event should be defined against one narrowly specified territorial scope."
        )

    return (
        "The market should use one narrowly defined geographic scope."
    )


def _rewrite_neutral_branch_clause(text: str) -> str:
    return (
        "If the event does not validly occur under the defined conditions, this market will resolve to 50-50."
    )


def _rewrite_edge_case_clause(text: str) -> str:
    return (
        "Operational edge cases should be consolidated into one explicit fallback clause."
    )

TIME_PATTERNS = [
    "within 48 hours",
    "within 24 hours",
    "within 2 hours",
    "listed date",
    "specified week",
    "specified timeframe",
    "cannot be confirmed",
    "can be confirmed",
    "israel time",
    "eet",
    "utc",
    "gmt",
]

SOURCE_PATTERNS = [
    "consensus of credible reporting",
    "official information",
    "official statements",
    "major international media",
    "video evidence",
    "multilateral bodies",
]

CARVEOUT_PATTERNS = [
    "west bank",
    "gaza strip",
    "municipality",
    "territory",
    "ground territory",
]

ATTRIBUTION_PATTERNS = [
    "proxy forces",
    "explicitly claimed",
    "confirmed to have originated",
    "hezbollah",
    "houthis",
]

NEUTRAL_BRANCH_PATTERNS = [
    "50-50",
    "resolve to 50-50",
    "resolves to 50-50",
    "will resolve to 50-50",
]

EDGE_CASE_PATTERNS = [
    "canceled",
    "forfeit",
    "disqualification",
    "walkover",
    "remade",
    "surrender",
    "series result has already been determined",
]


def identify_problem_clauses(title: str, description: str, design_eval: Dict[str, Any]) -> List[Dict[str, Any]]:
    clauses = _split_sentences(description)
    problems: List[Dict[str, Any]] = []

    for idx, clause in enumerate(clauses, start=1):
        clause_flags = []

        if _contains_any(clause, TIME_PATTERNS):
            clause_flags.append("time_confirmation_fragility")
        if _contains_any(clause, SOURCE_PATTERNS):
            clause_flags.append("source_hierarchy_complexity")
        if _contains_any(clause, CARVEOUT_PATTERNS):
            clause_flags.append("territorial_carveout_complexity")
        if _contains_any(clause, ATTRIBUTION_PATTERNS):
            clause_flags.append("actor_attribution_complexity")
        if _contains_any(clause, NEUTRAL_BRANCH_PATTERNS):
            clause_flags.append("neutral_outcome_branch")
        if _contains_any(clause, EDGE_CASE_PATTERNS):
            clause_flags.append("edge_case_branch")

        if clause_flags:
            problems.append(
                {
                    "clause_index": idx,
                    "text": clause,
                    "flags": clause_flags,
                }
            )

    return problems


def build_rewrite_actions(design_eval: Dict[str, Any], problem_clauses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []

    flags = set(design_eval.get("flags") or [])

    if "WORDING_AMBIGUITY" in flags:
        actions.append(
            {
                "action": "tighten_qualifying_language",
                "reason": "Reduce interpretive drift in the event definition.",
            }
        )

    if "TIME_AMBIGUITY" in flags:
        actions.append(
            {
                "action": "standardize_time_reference",
                "reason": "Use one explicit event-time and one explicit confirmation window.",
            }
        )

    if "SOURCE_FRAGILITY" in flags:
        actions.append(
            {
                "action": "tighten_source_hierarchy",
                "reason": "Prefer a clear official-primary hierarchy before consensus fallback.",
            }
        )

    if "TERRITORIAL_CARVEOUT_COMPLEXITY" in flags:
        actions.append(
            {
                "action": "simplify_geographic_scope",
                "reason": "Reduce ambiguous territorial carveouts.",
            }
        )

    if "ACTOR_ATTRIBUTION_COMPLEXITY" in flags:
        actions.append(
            {
                "action": "clarify_attribution_standard",
                "reason": "Make actor attribution testable without interpretive drift.",
            }
        )

    if "MULTIPLE_NEUTRAL_OUTCOME_BRANCHES" in flags:
        actions.append(
            {
                "action": "consolidate_neutral_outcome_branches",
                "reason": "Collapse redundant 50-50 logic into one explicit fallback rule.",
            }
        )

    if not actions and problem_clauses:
        actions.append(
            {
                "action": "targeted_clause_cleanup",
                "reason": "The market is broadly workable, but several clauses should be simplified.",
            }
        )

    return actions


def build_structured_rewrite_actions(
    design_eval: Dict[str, Any],
    problem_clauses: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    structured: List[Dict[str, Any]] = []

    for clause in problem_clauses:
        clause_index = clause.get("clause_index")
        text = clause.get("text") or ""
        flags = clause.get("flags") or []

        for flag in flags:
            if flag == "time_confirmation_fragility":
                after_text = _rewrite_time_clause(text)
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "standardize_time_reference",
                        "before_text": text,
                        "after_text": after_text,
                        "reason": "This clause introduces timing uncertainty or confirmation-window ambiguity.",
                    }
                )

            elif flag == "source_hierarchy_complexity":
                after_text = _rewrite_source_clause(text)
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "tighten_source_hierarchy",
                        "before_text": text,
                        "after_text": after_text,
                        "reason": "This clause relies too heavily on layered or consensus-sensitive evidence.",
                    }
                )

            elif flag == "territorial_carveout_complexity":
                after_text = _rewrite_scope_clause(text)
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "simplify_geographic_scope",
                        "before_text": text,
                        "after_text": after_text,
                        "reason": "This clause carries geographic meaning that may create interpretive drift. Narrowing the scope is safer than piling on carveouts.",
                    }
                )

            elif flag == "actor_attribution_complexity":
                after_text = _rewrite_attribution_clause(text)
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "clarify_attribution_standard",
                        "before_text": text,
                        "after_text": after_text,
                        "reason": "This clause depends on attribution that may be difficult to verify cleanly at settlement time.",
                    }
                )

            elif flag == "neutral_outcome_branch":
                after_text = _rewrite_neutral_branch_clause(text)
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "consolidate_neutral_outcome_branches",
                        "before_text": text,
                        "after_text": after_text,
                        "reason": "This neutral-outcome branch should be merged into one explicit fallback rule.",
                    }
                )

            elif flag == "edge_case_branch":
                after_text = _rewrite_edge_case_clause(text)
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "retain_but_consolidate_edge_case_logic",
                        "before_text": text,
                        "after_text": after_text,
                        "reason": "This edge-case clause may be useful, but it should be consolidated with adjacent operational fallback logic.",
                    }
                )

    # remove identical rewrites
    filtered: List[Dict[str, Any]] = []
    seen = set()

    for row in structured:
        before_text = (row.get("before_text") or "").strip()
        after_text = (row.get("after_text") or "").strip()

        if not after_text:
            continue
        if before_text == after_text:
            continue

        key = (
            row["target_clause_index"],
            row["target_problem_type"],
            row["action_type"],
            after_text,
        )
        if key not in seen:
            seen.add(key)
            filtered.append(row)

    return filtered

    structured: List[Dict[str, Any]] = []

    for clause in problem_clauses:
        clause_index = clause.get("clause_index")
        text = clause.get("text") or ""
        flags = clause.get("flags") or []

        for flag in flags:
            if flag == "time_confirmation_fragility":
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "standardize_time_reference",
                        "before_text": text,
                        "after_text": text.replace(
                            "listed date",
                            "listed date using one explicit stated timezone"
                        ),
                        "reason": "This clause introduces timing uncertainty or confirmation-window ambiguity.",
                    }
                )

            elif flag == "source_hierarchy_complexity":
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "tighten_source_hierarchy",
                        "before_text": text,
                        "after_text": (
                            "The primary resolution source will be official government or military statements first. "
                            "Only if those are unavailable within the stated confirmation window may a consensus of credible reporting be used."
                        ),
                        "reason": "This clause relies too heavily on layered or consensus-sensitive evidence.",
                    }
                )

            elif flag == "territorial_carveout_complexity":
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "simplify_geographic_scope",
                        "before_text": text,
                        "after_text": text,
                        "reason": "This clause carries geographic meaning that may create interpretive drift. Narrowing the scope is safer than piling on carveouts.",
                    }
                )

            elif flag == "actor_attribution_complexity":
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "clarify_attribution_standard",
                        "before_text": text,
                        "after_text": text,
                        "reason": "This clause depends on attribution that may be difficult to verify cleanly at settlement time.",
                    }
                )

            elif flag == "neutral_outcome_branch":
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "consolidate_neutral_outcome_branches",
                        "before_text": text,
                        "after_text": "If the event does not validly occur under the defined match conditions, this market will resolve to 50-50.",
                        "reason": "This neutral-outcome branch should be merged into one explicit fallback rule.",
                    }
                )

            elif flag == "edge_case_branch":
                structured.append(
                    {
                        "target_clause_index": clause_index,
                        "target_problem_type": flag,
                        "action_type": "retain_but_consolidate_edge_case_logic",
                        "before_text": text,
                        "after_text": text,
                        "reason": "This edge-case clause may be useful, but it should be consolidated with adjacent operational fallback logic.",
                    }
                )

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in structured:
        key = (
            row["target_clause_index"],
            row["target_problem_type"],
            row["action_type"],
        )
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    return deduped


def validate_structured_rewrite_actions(
    structured_actions: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    valid: List[Dict[str, Any]] = []

    for action in structured_actions:
        before_text = (action.get("before_text") or "").strip()
        after_text = (action.get("after_text") or "").strip()

        if not after_text:
            continue
        if before_text == after_text:
            continue

        valid.append(action)

    return valid


def prioritize_rewrite_actions(
    structured_actions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if not structured_actions:
        return {
            "top_actions": [],
            "suppressed_actions": [],
            "primary_rewrite_strategy": "none",
        }

    priority_map = {
        "time_confirmation_fragility": 1,
        "source_hierarchy_complexity": 2,
        "actor_attribution_complexity": 3,
        "territorial_carveout_complexity": 4,
        "neutral_outcome_branch": 5,
        "edge_case_branch": 6,
    }

    def score(action: Dict[str, Any]) -> int:
        return priority_map.get(action.get("target_problem_type"), 999)

    # sort by priority
    sorted_actions = sorted(structured_actions, key=score)

    # dedupe by action_type (keep highest priority instance)
    seen_types = set()
    deduped: List[Dict[str, Any]] = []

    for a in sorted_actions:
        t = a.get("action_type")
        if t not in seen_types:
            seen_types.add(t)
            deduped.append(a)

    # cap top actions
    top_actions = deduped[:5]
    suppressed_actions = deduped[5:]

    # determine primary strategy
    strategy_map = {
        "standardize_time_reference": "clarify_time",
        "tighten_source_hierarchy": "tighten_sources",
        "clarify_attribution_standard": "clarify_attribution",
        "simplify_geographic_scope": "narrow_scope",
        "consolidate_neutral_outcome_branches": "simplify_resolution_logic",
    }

    primary = "general_cleanup"
    if top_actions:
        primary = strategy_map.get(top_actions[0]["action_type"], "general_cleanup")

    return {
        "top_actions": top_actions,
        "suppressed_actions": suppressed_actions,
        "primary_rewrite_strategy": primary,
    }

def build_compact_revised_description(
    structured_actions: List[Dict[str, Any]],
    original_description: str,
) -> str:
    if not structured_actions:
        return original_description

    sentences = _split_sentences(original_description)

    replacements = {
        a["target_clause_index"]: a
        for a in structured_actions[:5]
        if a.get("after_text")
    }

    rebuilt = []
    for idx, sentence in enumerate(sentences, start=1):
        if idx in replacements:
            rebuilt.append(replacements[idx]["after_text"])
        else:
            rebuilt.append(sentence)

    return " ".join(rebuilt).strip()


def build_rewrite_notes_compact(
    top_actions: List[Dict[str, Any]],
    split_recommendation: Dict[str, Any],
) -> List[str]:
    notes: List[str] = []

    action_map = {
        "standardize_time_reference": "Use one explicit event-time standard and one explicit verification window.",
        "tighten_source_hierarchy": "Use official-primary resolution sources before any consensus fallback.",
        "clarify_attribution_standard": "Tighten attribution standards so the triggering actor can be verified cleanly.",
        "simplify_geographic_scope": "Reduce geographic carveouts and narrow the territorial definition.",
        "consolidate_neutral_outcome_branches": "Collapse redundant 50-50 logic into one explicit fallback rule.",
        "retain_but_consolidate_edge_case_logic": "Keep necessary edge-case logic, but consolidate it into fewer clauses.",
    }

    seen = set()
    for action in top_actions:
        action_type = action.get("action_type")
        note = action_map.get(action_type)
        if note and note not in seen:
            seen.add(note)
            notes.append(note)

    if split_recommendation.get("should_split"):
        notes.append("This market may be better expressed as a narrower market before launch.")

    return notes

def build_launch_decision_note(
    design_eval: Dict[str, Any],
    split_recommendation: Dict[str, Any],
) -> str:
    readiness = design_eval.get("launch_readiness")
    interpretive_complexity = design_eval.get("interpretive_complexity")
    operational_complexity = design_eval.get("operational_complexity")

    if readiness == "high_resolution_risk":
        if split_recommendation.get("should_split"):
            return (
                "Do not launch this draft as-is. Rewrite and narrow scope first."
            )
        return (
            "Do not launch this draft as-is. Rewrite the highest-risk clauses first."
        )

    if readiness == "revise_before_launch":
        return (
            "Launch only after tightening the identified clauses."
        )

    if operational_complexity == "high" and interpretive_complexity in {"low", "moderate"}:
        return (
            "Launchable, but operationally dense. Keep the rule set tight and well-structured."
        )

    return (
        "Launchable with minor wording cleanup."
    )

def validate_cross_layer_consistency(result: Dict[str, Any]) -> Dict[str, Any]:
    issues: List[Dict[str, Any]] = []

    design = result.get("design_intelligence") or {}
    rewrite_readiness = result.get("rewrite_readiness")
    rewrite_priority = result.get("rewrite_priority")
    primary_strategy = result.get("primary_rewrite_strategy")
    top_actions = result.get("top_rewrite_actions") or []
    split_recommendation = result.get("split_recommendation") or {}
    revised_description_compact = (result.get("revised_description_compact") or "").lower()

    operator_decision_band = design.get("operator_decision_band")
    resolution_risk_band = design.get("resolution_risk_band")
    launch_readiness = design.get("launch_readiness")

    top_action_types = {a.get("action_type") for a in top_actions}

    if operator_decision_band == "do_not_launch":
        if rewrite_priority != "high":
            issues.append(
                {
                    "rule": "do_not_launch_requires_high_priority",
                    "message": "Operator decision is do_not_launch, but rewrite_priority is not high.",
                    "severity": "high",
                }
            )
        if rewrite_readiness != "rewrite_required":
            issues.append(
                {
                    "rule": "do_not_launch_requires_rewrite_required",
                    "message": "Operator decision is do_not_launch, but rewrite_readiness is not rewrite_required.",
                    "severity": "high",
                }
            )

    if resolution_risk_band in {"high", "critical"} and launch_readiness == "launch_ready":
        issues.append(
            {
                "rule": "high_risk_cannot_be_launch_ready",
                "message": "Resolution risk is high or critical, but launch_readiness is launch_ready.",
                "severity": "high",
            }
        )

    if primary_strategy == "clarify_time" and "standardize_time_reference" not in top_action_types:
        issues.append(
            {
                "rule": "clarify_time_requires_time_action",
                "message": "Primary rewrite strategy is clarify_time, but no time action is present in top actions.",
                "severity": "medium",
            }
        )

    if primary_strategy == "tighten_sources" and "tighten_source_hierarchy" not in top_action_types:
        issues.append(
            {
                "rule": "tighten_sources_requires_source_action",
                "message": "Primary rewrite strategy is tighten_sources, but no source action is present in top actions.",
                "severity": "medium",
            }
        )

    if split_recommendation.get("should_split"):
        if "split" not in revised_description_compact and "narrow" not in revised_description_compact:
            issues.append(
                {
                    "rule": "split_recommendation_should_surface_in_compact_rewrite",
                    "message": "Split recommendation exists, but compact rewrite does not mention narrowing or splitting.",
                    "severity": "medium",
                }
            )

    if "tighten_source_hierarchy" in top_action_types:
        if "official government or military statements first" not in revised_description_compact:
            issues.append(
                {
                    "rule": "source_fix_should_surface_in_compact_rewrite",
                    "message": "Top actions include source hierarchy tightening, but compact rewrite does not reflect official-primary source wording.",
                    "severity": "medium",
                }
            )

    if "clarify_attribution_standard" in top_action_types:
        if "directly attributable" not in revised_description_compact:
            issues.append(
                {
                    "rule": "attribution_fix_should_surface_in_compact_rewrite",
                    "message": "Top actions include attribution clarification, but compact rewrite does not surface it clearly.",
                    "severity": "medium",
                }
            )

    issue_count = len(issues)
    consistency_score = max(0.0, round(1.0 - (issue_count * 0.2), 4))

    if issue_count == 0:
        consistency_status = "pass"
    elif any(i["severity"] == "high" for i in issues):
        consistency_status = "fail"
    else:
        consistency_status = "warning"

    return {
        "consistency_status": consistency_status,
        "consistency_score": consistency_score,
        "consistency_issues": issues,
    }

def build_audit_trace(result: Dict[str, Any]) -> Dict[str, Any]:
    design = result.get("design_intelligence") or {}
    top_actions = result.get("top_rewrite_actions") or []
    split_recommendation = result.get("split_recommendation") or {}
    consistency_status = result.get("consistency_status")
    consistency_score = result.get("consistency_score")

    rule_hits: List[Dict[str, Any]] = []
    threshold_hits: List[Dict[str, Any]] = []
    decision_trace: List[Dict[str, Any]] = []

    risk_score = float(design.get("expected_resolution_risk_score") or 0.0)
    dispute_score = float(design.get("expected_dispute_propensity_score") or 0.0)
    clarity_score = float(design.get("design_clarity_score") or 0.0)

    resolution_risk_band = design.get("resolution_risk_band")
    dispute_propensity_band = design.get("dispute_propensity_band")
    operator_decision_band = design.get("operator_decision_band")

    # Threshold hits
    threshold_hits.append(
        {
            "metric": "expected_resolution_risk_score",
            "value": risk_score,
            "derived_band": resolution_risk_band,
        }
    )
    threshold_hits.append(
        {
            "metric": "expected_dispute_propensity_score",
            "value": dispute_score,
            "derived_band": dispute_propensity_band,
        }
    )
    threshold_hits.append(
        {
            "metric": "design_clarity_score",
            "value": clarity_score,
        }
    )

    # Rule hits from design → decision
    if resolution_risk_band in {"high", "critical"}:
        rule_hits.append(
            {
                "rule": "high_resolution_risk_band_detected",
                "effect": "elevated_launch_constraint",
            }
        )

    if dispute_propensity_band in {"high", "critical"}:
        rule_hits.append(
            {
                "rule": "high_dispute_propensity_band_detected",
                "effect": "rewrite_pressure_increased",
            }
        )

    if operator_decision_band == "do_not_launch":
        rule_hits.append(
            {
                "rule": "operator_decision_do_not_launch",
                "effect": "rewrite_required_high_priority",
            }
        )

    # Rewrite strategy hit
    if top_actions:
        primary_action = top_actions[0]
        rule_hits.append(
            {
                "rule": "primary_rewrite_strategy_selected_from_top_action",
                "action_type": primary_action.get("action_type"),
                "target_problem_type": primary_action.get("target_problem_type"),
            }
        )

    # Split recommendation hit
    if split_recommendation.get("should_split"):
        rule_hits.append(
            {
                "rule": "split_recommendation_triggered",
                "effect": split_recommendation.get("suggested_split_type"),
            }
        )

    # Decision trace
    decision_trace.append(
        {
            "step": "design_calibration",
            "resolution_risk_band": resolution_risk_band,
            "dispute_propensity_band": dispute_propensity_band,
            "operator_decision_band": operator_decision_band,
        }
    )

    decision_trace.append(
        {
            "step": "rewrite_prioritization",
            "primary_rewrite_strategy": result.get("primary_rewrite_strategy"),
            "top_action_types": [a.get("action_type") for a in top_actions],
        }
    )

    decision_trace.append(
        {
            "step": "split_decision",
            "should_split": split_recommendation.get("should_split"),
            "suggested_split_type": split_recommendation.get("suggested_split_type"),
            "rationale": split_recommendation.get("rationale") or [],
        }
    )

    decision_trace.append(
        {
            "step": "consistency_validation",
            "consistency_status": consistency_status,
            "consistency_score": consistency_score,
            "issues_count": len(result.get("consistency_issues") or []),
        }
    )

    engine_versions_snapshot = {
        "rewrite_engine": ENGINE_VERSION,
        "design_engine": design.get("engine_version"),
    }

    return {
        "rule_hits": rule_hits,
        "threshold_hits": threshold_hits,
        "decision_trace": decision_trace,
        "engine_versions_snapshot": engine_versions_snapshot,
    }


def build_compact_market_draft(
    payload: Dict[str, Any],
    design_eval: Dict[str, Any],
    top_actions: List[Dict[str, Any]],
    split_recommendation: Dict[str, Any],
) -> str:
    title = payload.get("title") or ""
    description = payload.get("description") or ""

    category = (payload.get("category") or "").lower()
    text = f"{title} {description}".lower()

    needs_time = any(a.get("action_type") == "standardize_time_reference" for a in top_actions)
    needs_sources = any(a.get("action_type") == "tighten_source_hierarchy" for a in top_actions)
    needs_attribution = any(a.get("action_type") == "clarify_attribution_standard" for a in top_actions)
    needs_scope = any(a.get("action_type") == "simplify_geographic_scope" for a in top_actions)

    if category == "geopolitics" or "strike" in text:
        lines = []

        lines.append(
            'This market resolves to "Yes" if the defined qualifying strike occurs within the stated jurisdiction and timeframe. Otherwise, it resolves to "No".'
        )

        if needs_time:
            lines.append(
                "The event time must be evaluated using one explicit stated timezone only."
            )

        if needs_scope:
            lines.append(
                "The market should use one narrowly defined territorial scope rather than multiple geographic carveouts."
            )

        if needs_attribution:
            lines.append(
                "Only directly attributable actions by the named state actor count; proxy-force actions do not count."
            )

        if needs_sources:
            lines.append(
                "The primary resolution source should be official government or military statements first. Only if those are unavailable within the explicit confirmation window may a consensus of credible reporting be used."
            )

        if split_recommendation.get("should_split"):
            lines.append(
                "If geographic scope and attribution both carry core resolution meaning, this draft should be split into a narrower market."
            )

        return " ".join(lines)

    if category in {"esports", "sports"} or "game 2" in text or "dragon" in text:
        lines = []

        lines.append(
            'This market resolves to "Yes" if the defined in-game event occurs during the specified match context. Otherwise, it resolves to "No".'
        )

        lines.append(
            "Only the explicitly qualifying in-game events count toward resolution."
        )

        if needs_sources:
            lines.append(
                "The primary resolution source should be the official match result source first, with fallback reporting only if the official source is unavailable within the explicit verification window."
            )

        if any(a.get("action_type") == "consolidate_neutral_outcome_branches" for a in top_actions):
            lines.append(
                'If the match does not validly occur under the defined conditions, the market should resolve to one explicit fallback outcome only.'
            )

        return " ".join(lines)

    lines = []
    lines.append(
        'This market resolves to "Yes" only if the explicitly defined qualifying event occurs within the stated scope and timeframe. Otherwise, it resolves to "No".'
    )

    if needs_time:
        lines.append("Use one explicit time standard and one explicit confirmation window.")
    if needs_scope:
        lines.append("Use one narrower scope definition and avoid overlapping carveouts.")
    if needs_sources:
        lines.append("Use a strict official-primary source hierarchy before any fallback reporting.")
    if needs_attribution:
        lines.append("Keep attribution standards explicit and testable.")

    return " ".join(lines)


def build_split_recommendation(design_eval: Dict[str, Any], problem_clauses: List[Dict[str, Any]]) -> Dict[str, Any]:
    high_interpretive = design_eval.get("interpretive_complexity") == "high"
    high_risk = design_eval.get("launch_readiness") == "high_resolution_risk"

    clause_flag_counts = {}
    for c in problem_clauses:
        for f in c.get("flags") or []:
            clause_flag_counts[f] = clause_flag_counts.get(f, 0) + 1

    should_split = False
    rationale = []

    if high_interpretive and high_risk:
        should_split = True
        rationale.append("High interpretive complexity combined with high resolution risk.")

    if clause_flag_counts.get("territorial_carveout_complexity", 0) >= 1 and clause_flag_counts.get("actor_attribution_complexity", 0) >= 1:
        should_split = True
        rationale.append("Geographic scope and actor attribution are both carrying resolution meaning.")

    if clause_flag_counts.get("neutral_outcome_branch", 0) >= 3:
        rationale.append("Too many neutral fallback branches may justify separating edge-case logic.")

    return {
        "should_split": should_split,
        "rationale": rationale,
        "suggested_split_type": "narrower_market_scope" if should_split else None,
    }


def build_revised_title(title: str, design_eval: Dict[str, Any]) -> str:
    return title.strip()


def build_revised_description(
    description: str,
    design_eval: Dict[str, Any],
    rewrite_actions: List[Dict[str, Any]],
    structured_rewrite_actions: List[Dict[str, Any]],
) -> str:
    revised = description.strip()

    if any(a["action"] == "tighten_source_hierarchy" for a in rewrite_actions):
        revised = re.sub(
            r"(The primary resolution source.*?)(If the date/time|If the date or time|$)",
            "The primary resolution source will be official government or military statements first. Only if those are unavailable within the stated confirmation window may a consensus of credible reporting be used. \\2",
            revised,
            flags=re.IGNORECASE | re.DOTALL,
        )

    if any(a["action"] == "standardize_time_reference" for a in rewrite_actions):
        revised = revised.replace(
            "listed date in Israel Time (GMT+2)",
            "listed date using Israel Time (GMT+2) as the sole time standard"
        )

    if any(a["action"] == "consolidate_neutral_outcome_branches" for a in rewrite_actions):
        revised = re.sub(
            r"(If .*?50-50\.\s*){2,}",
            "If the event does not validly occur under the defined match conditions, this market will resolve to 50-50. ",
            revised,
            flags=re.IGNORECASE,
        )

    sentences = _split_sentences(revised)
    replacements = {
        row["target_clause_index"]: row
        for row in structured_rewrite_actions
        if row.get("after_text")
    }

    rebuilt: List[str] = []
    for idx, sentence in enumerate(sentences, start=1):
        replacement = replacements.get(idx)
        if replacement and replacement.get("after_text") and replacement["after_text"] != sentence:
            rebuilt.append(replacement["after_text"])
        else:
            rebuilt.append(sentence)

    return " ".join(rebuilt).strip()


def build_summary(title: str, design_eval: Dict[str, Any], rewrite_actions: List[Dict[str, Any]], split_recommendation: Dict[str, Any]) -> str:
    readiness = design_eval.get("launch_readiness")
    priority = "high" if readiness == "high_resolution_risk" else "medium" if readiness == "revise_before_launch" else "low"

    if split_recommendation.get("should_split"):
        return (
            f"{title} should be rewritten before launch and may be better expressed as a narrower market. "
            f"Rewrite priority is {priority}."
        )

    if rewrite_actions:
        return (
            f"{title} is usable as a draft but should be tightened before launch. "
            f"Rewrite priority is {priority}."
        )

    return (
        f"{title} requires only light wording cleanup before launch. "
        f"Rewrite priority is {priority}."
    )


def persist_rewrite_evaluation(payload: Dict[str, Any], result: Dict[str, Any], input_hash: str) -> Dict[str, Any]:
    q = """
    INSERT INTO public.market_design_rewrite_intelligence_evaluations (
        input_hash,
        title,
        description,
        category,
        market_type,
        protocol,
        oracle_family,
        rewrite_readiness,
        rewrite_priority,
        problem_clauses,
        rewrite_actions,
        structured_rewrite_actions,
        top_rewrite_actions,
        suppressed_rewrite_actions,
        primary_rewrite_strategy,
        revised_title,
        revised_description,
        revised_description_compact,
        split_recommendation,
        consistency_status,
        consistency_score,
        consistency_issues,
        rule_hits,
        threshold_hits,
        decision_trace,
        engine_versions_snapshot,
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
        %(rewrite_readiness)s,
        %(rewrite_priority)s,
        %(problem_clauses)s,
        %(rewrite_actions)s,
        %(structured_rewrite_actions)s,
        %(top_rewrite_actions)s,
        %(suppressed_rewrite_actions)s,
        %(primary_rewrite_strategy)s,
        %(revised_title)s,
        %(revised_description)s,
        %(revised_description_compact)s,
        %(split_recommendation)s,
        %(consistency_status)s,
        %(consistency_score)s,
        %(consistency_issues)s,
        %(rule_hits)s,
        %(threshold_hits)s,
        %(decision_trace)s,
        %(engine_versions_snapshot)s,
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
        rewrite_readiness = EXCLUDED.rewrite_readiness,
        rewrite_priority = EXCLUDED.rewrite_priority,
        problem_clauses = EXCLUDED.problem_clauses,
        rewrite_actions = EXCLUDED.rewrite_actions,
        structured_rewrite_actions = EXCLUDED.structured_rewrite_actions,
        top_rewrite_actions = EXCLUDED.top_rewrite_actions,
        suppressed_rewrite_actions = EXCLUDED.suppressed_rewrite_actions,
        primary_rewrite_strategy = EXCLUDED.primary_rewrite_strategy,
        revised_title = EXCLUDED.revised_title,
        revised_description = EXCLUDED.revised_description,
        revised_description_compact = EXCLUDED.revised_description_compact,
        split_recommendation = EXCLUDED.split_recommendation,
        consistency_status = EXCLUDED.consistency_status,
        consistency_score = EXCLUDED.consistency_score,
        consistency_issues = EXCLUDED.consistency_issues,
        rule_hits = EXCLUDED.rule_hits,
        threshold_hits = EXCLUDED.threshold_hits,
        decision_trace = EXCLUDED.decision_trace,
        engine_versions_snapshot = EXCLUDED.engine_versions_snapshot,
        summary = EXCLUDED.summary,
        engine_version = EXCLUDED.engine_version,
        updated_at = NOW()
    RETURNING id
    """

    params = {
        "input_hash": input_hash,
        "title": payload.get("title"),
        "description": payload.get("description"),
        "category": payload.get("category"),
        "market_type": payload.get("market_type"),
        "protocol": payload.get("protocol") or "polymarket",
        "oracle_family": payload.get("oracle_family") or "uma_oo",
        "rewrite_readiness": result["rewrite_readiness"],
        "rewrite_priority": result["rewrite_priority"],
        "problem_clauses": Json(result.get("problem_clauses") or []),
        "rewrite_actions": Json(result.get("rewrite_actions") or []),
        "structured_rewrite_actions": Json(result.get("structured_rewrite_actions") or []),
        "top_rewrite_actions": Json(result.get("top_rewrite_actions") or []),
        "suppressed_rewrite_actions": Json(result.get("suppressed_rewrite_actions") or []),
        "primary_rewrite_strategy": result.get("primary_rewrite_strategy"),
        "revised_title": result.get("revised_title"),
        "revised_description": result.get("revised_description"),
        "revised_description_compact": result.get("revised_description_compact"),
        "split_recommendation": Json(result.get("split_recommendation") or {}),
        "consistency_status": result.get("consistency_status"),
        "consistency_score": result.get("consistency_score"),
        "consistency_issues": Json(result.get("consistency_issues") or []),
        "rule_hits": Json(result.get("rule_hits") or []),
        "threshold_hits": Json(result.get("threshold_hits") or []),
        "decision_trace": Json(result.get("decision_trace") or []),
        "engine_versions_snapshot": Json(result.get("engine_versions_snapshot") or {}),
        "summary": result.get("summary"),
        "engine_version": ENGINE_VERSION,
    }

    with psycopg.connect(get_db_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(q, params)
            row = cur.fetchone()
        conn.commit()

    return {"id": row[0]}

def evaluate_market_design_rewrite(payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_payload = {
        "title": _normalize_input_text(payload.get("title")),
        "description": _normalize_input_text(payload.get("description")),
        "category": _normalize_input_text(payload.get("category")),
        "market_type": _normalize_input_text(payload.get("market_type")),
        "protocol": _normalize_input_text(payload.get("protocol")) or "polymarket",
        "oracle_family": _normalize_input_text(payload.get("oracle_family")) or "uma_oo",
    }

    input_hash = _sha(normalized_payload)

    design_eval = evaluate_market_design(normalized_payload)

    problem_clauses = identify_problem_clauses(
        normalized_payload["title"] or "",
        normalized_payload["description"] or "",
        design_eval,
    )

    rewrite_actions = build_rewrite_actions(design_eval, problem_clauses)
    structured_rewrite_actions = build_structured_rewrite_actions(
        design_eval,
        problem_clauses,
    )

    structured_rewrite_actions = validate_structured_rewrite_actions(
        structured_rewrite_actions
    )

    prioritized = prioritize_rewrite_actions(structured_rewrite_actions)

    top_actions = prioritized["top_actions"]
    suppressed_actions = prioritized["suppressed_actions"]
    primary_strategy = prioritized["primary_rewrite_strategy"]
    split_recommendation = build_split_recommendation(design_eval, problem_clauses)

    rewrite_priority = (
        "high"
        if design_eval.get("launch_readiness") == "high_resolution_risk"
        else "medium"
        if design_eval.get("launch_readiness") == "revise_before_launch"
        else "low"
    )

    rewrite_readiness = (
        "rewrite_required"
        if rewrite_priority == "high"
        else "rewrite_recommended"
        if rewrite_priority == "medium" or bool(rewrite_actions)
        else "light_cleanup_only"
    )

    revised_title = build_revised_title(normalized_payload["title"] or "", design_eval)
    revised_description = build_revised_description(
        normalized_payload["description"] or "",
        design_eval,
        rewrite_actions,
        top_actions,
    )

    revised_description_compact = build_compact_market_draft(
        normalized_payload,
        design_eval,
        top_actions,
        split_recommendation,
    )

    rewrite_notes_compact = build_rewrite_notes_compact(
        top_actions,
        split_recommendation,
    )

    launch_decision_note = build_launch_decision_note(
        design_eval,
        split_recommendation,
    )

    summary = build_summary(
        normalized_payload["title"] or "",
        design_eval,
        rewrite_actions,
        split_recommendation,
    )

    result = {
        "rewrite_readiness": rewrite_readiness,
        "rewrite_priority": rewrite_priority,
        "problem_clauses": problem_clauses,
        "rewrite_actions": rewrite_actions,
        "structured_rewrite_actions": structured_rewrite_actions,
        "top_rewrite_actions": top_actions,
        "suppressed_rewrite_actions": suppressed_actions,
        "primary_rewrite_strategy": primary_strategy,
        "revised_title": revised_title,
        "revised_description": revised_description,
        "revised_description_compact": revised_description_compact,
        "rewrite_notes_compact": rewrite_notes_compact,
        "launch_decision_note": launch_decision_note,
        "split_recommendation": split_recommendation,
        "summary": summary,
        "design_intelligence": design_eval,
        "engine_version": ENGINE_VERSION,
    }

    consistency = validate_cross_layer_consistency(result)
    result.update(consistency)

    audit_trace = build_audit_trace(result)
    result.update(audit_trace)

    persisted = persist_rewrite_evaluation(normalized_payload, result, input_hash)

    return {
        "id": persisted["id"],
        "input_hash": input_hash,
        **result,
    }