export type LaunchCandidate = {
  market_id: string;
  title?: string | null;
  category?: string | null;
  url?: string | null;
  recommendation?: string | null;
  recommendation_reason?: string | null;
  participation_quality_score?: number | null;
  liquidity_durability_score?: number | null;
  flags?: string[] | null;
  engine_version?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type SocialCandidate = {
  market_id: string;
  title?: string | null;
  category?: string | null;
  url?: string | null;
  recommendation?: string | null;
  summary?: string | null;
  attention_score?: number | null;
  sentiment_score?: number | null;
  demand_score?: number | null;
  trend_velocity?: number | null;
  mention_count?: number | null;
  source_count?: number | null;
  confidence_score?: number | null;
  flags?: string[] | null;
  engine_version?: string | null;
  created_at?: string | null;
  updated_at?: string | null;
};

export type ScoreDiffDriverDelta = {
  metric: string;
  before: number | null;
  after: number | null;
  delta: number | null;
};

export type ScoreDiffFlagChanges = {
  added: string[];
  removed: string[];
};

export type ScoreDiffSnapshotSide = {
  social_day?: string | null;
  demand_state?: string | null;
  demand_score?: number | null;
  alignment_day?: string | null;
  alignment_state?: string | null;
  alignment_score?: number | null;
};

export type ScoreDiffDeltas = {
  demand_score_delta?: number | null;
  alignment_score_delta?: number | null;
  demand_state_changed?: boolean;
  alignment_state_changed?: boolean;
  driver_deltas?: ScoreDiffDriverDelta[];
  social_flags?: ScoreDiffFlagChanges | null;
  alignment_flags?: ScoreDiffFlagChanges | null;
};

export type ScoreDiffs = {
  before?: ScoreDiffSnapshotSide | null;
  after?: ScoreDiffSnapshotSide | null;
  deltas?: ScoreDiffDeltas | null;
  summary?: string | null;
};

export type AiContextDriver = {
  key: string;
  label: string;
  impact: "high" | "medium" | "low";
  direction: "positive" | "negative" | "fragility" | "uncertainty";
};

export type AiContext = {
  interpretation?: string | null;
  interpretation_type?:
    | "confirmed"
    | "conviction_mismatch"
    | "structure_led"
    | "weak"
    | "unclear"
    | "abstain"
    | null;
  confidence?: number | null;
  does_demand_reflect_conviction?: boolean | null;
  does_alignment_imply_convergence?: boolean | null;
  caution_flags?: string[] | null;
  abstain?: boolean | null;
  abstain_reason?: string | null;
  model_provider?: string | null;
  model_name?: string | null;
  drivers?: AiContextDriver[] | null;
};

export type MarketSnapshot = {
  market_id: string;
  question: string | null;

  structural_score?: number | null;
  social_score?: number | null;

  structural_state?: string | null;
  social_state?: string | null;
  alignment_state?: string | null;

  integrity_band?: string | null;
  contextual_summary?: string | null;

  ai_context?: AiContext | null;

  audit_id?: string | number | null;
  cache_hit?: boolean | null;

  market?: Record<string, any> | null;
  social_intelligence?: Record<string, any> | null;
  alignment_intelligence?: Record<string, any> | null;
  opportunity_summary?: Record<string, any> | null;
  score_diffs?: ScoreDiffs | null;

  timeline?: any[] | null;

  incidents?: any[] | null;
  incident_events?: any[] | null;
  incident_effectiveness?: any[] | null;

  interventions?: any[] | null;
  interventions_effectiveness?: any[] | null;
  interventions_effectiveness_ui?: any[] | null;
  intervention_cumulative?: Record<string, any> | null;

  intervention_needed?: boolean | null;
  incentive_dependency?: string | null;
  activity_quality?: string | null;
  intervention_intelligence?: Record<string, any> | null;

  overrides?: any[] | null;

  traders?: {
    same_day?: Record<string, any> | null;
    rolling_window?: Record<string, any> | null;
  } | null;

  impact?: Record<string, any> | null;
  errors?: any[] | null;
  snapshot_meta?: Record<string, any> | null;
  coverage_summary?: Record<string, any> | null;

  is_mixed_horizon?: boolean | null;
};