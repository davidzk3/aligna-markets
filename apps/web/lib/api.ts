import { LaunchCandidate, MarketSnapshot, SocialCandidate } from "@/lib/types";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:8000";

const OPS_API_TOKEN = process.env.OPS_API_TOKEN || "";

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      ...(OPS_API_TOKEN ? { Authorization: `Bearer ${OPS_API_TOKEN}` } : {}),
    },
    cache: "no-store",
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${res.status} ${res.statusText}: ${text}`);
  }

  return res.json();
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(OPS_API_TOKEN ? { Authorization: `Bearer ${OPS_API_TOKEN}` } : {}),
    },
    body: JSON.stringify(body),
    cache: "no-store",
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${res.status} ${res.statusText}: ${text}`);
  }

  return res.json();
}

export type MarketExplorerRow = {
  market_id: string;
  question: string | null;
  title?: string | null;
  category: string | null;
  url: string | null;

  structural_day: string | null;
  social_day: string | null;
  alignment_day: string | null;

  structural_score: number | null;
  structural_state: string | null;

  social_score: number | null;
  social_state: string | null;

  alignment_state: string | null;
  contextual_summary: string | null;

  participation_quality_score: number | null;
  liquidity_durability_score: number | null;
  concentration_hhi: number | null;

  neutral_share: number | null;
  whale_share: number | null;
  speculative_share: number | null;

  flags: string[] | null;
};

export type MarketExplorerResponse = {
  day: string | null;
  rows: MarketExplorerRow[];
};

export async function getLaunchCandidates(limit = 10): Promise<LaunchCandidate[]> {
  return apiFetch<LaunchCandidate[]>(`/ops/launch/candidates?limit=${limit}`);
}

export async function getSocialCandidates(limit = 10): Promise<SocialCandidate[]> {
  return apiFetch<SocialCandidate[]>(`/ops/social/candidates?limit=${limit}`);
}

export async function getMarketSnapshot(
  marketId: string
): Promise<MarketSnapshot> {
  return apiFetch<MarketSnapshot>(`/ops/markets/${marketId}/snapshot`);
}

export async function getMarketExplorer(
  limit = 30
): Promise<MarketExplorerResponse> {
  return apiFetch<MarketExplorerResponse>(`/ops/markets/explorer?limit=${limit}`);
}

export async function fetchIntegrityHistory(marketId: string) {
  const res = await fetch(
    `http://localhost:8000/ops/markets/${marketId}/integrity-history`
  );

  if (!res.ok) {
    throw new Error("Failed to fetch integrity history");
  }

  return res.json();
}

export type DesignReviewInput = {
  title: string;
  description: string;
  category: string;
  market_type: string;
  protocol: string;
  oracle_family: string;
};

export type DesignIntelligenceResponse = {
  title?: string | null;
  category?: string | null;
  resolution_risk_band?: string | null;
  dispute_propensity_band?: string | null;
  operator_decision_band?: string | null;
  decision_rationale?: string | null;
  design_clarity_score?: number | null;
  expected_resolution_risk_score?: number | null;
  expected_dispute_propensity_score?: number | null;
};

export type RewriteIntelligenceResponse = {
  rewrite_readiness?: string | null;
  rewrite_priority?: string | null;
  primary_rewrite_strategy?: string | null;
  launch_decision_note?: string | null;
  revised_description_compact?: string | null;
  rewrite_notes_compact?: string[] | null;
  top_rewrite_actions?: Array<{
    target_clause_index?: number | null;
    target_problem_type?: string | null;
    action_type?: string | null;
    before_text?: string | null;
    after_text?: string | null;
    reason?: string | null;
  }> | null;
  split_recommendation?: {
    should_split?: boolean | null;
    suggested_split_type?: string | null;
    rationale?: string[] | null;
  } | null;
  consistency_status?: string | null;
  consistency_score?: number | null;
};

export async function runDesignIntelligence(
  input: DesignReviewInput
): Promise<DesignIntelligenceResponse> {
  return apiPost<DesignIntelligenceResponse>("/ops/design/intelligence", input);
}

export async function runRewriteIntelligence(
  input: DesignReviewInput
): Promise<RewriteIntelligenceResponse> {
  return apiPost<RewriteIntelligenceResponse>(
    "/ops/design/rewrite_intelligence",
    input
  );
}

export type ResolutionSimulationResponse = {
  simulation_run_id?: number | null;
  engine_version?: string | null;
  simulation?: {
    resolution_path?: string | null;
    expected_settlement_hours?: number | null;
    confidence?: number | null;
  } | null;
  design_scores?: Record<string, unknown> | null;
  rewrite_eval?: {
    rewrite_readiness?: string | null;
    rewrite_priority?: string | null;
    primary_rewrite_strategy?: string | null;
    launch_decision_note?: string | null;
    revised_description_compact?: string | null;
    rewrite_notes_compact?: string[] | null;
    top_rewrite_actions?: Array<{
      target_clause_index?: number | null;
      target_problem_type?: string | null;
      action_type?: string | null;
      before_text?: string | null;
      after_text?: string | null;
      reason?: string | null;
    }> | null;
    split_recommendation?: {
      should_split?: boolean | null;
      suggested_split_type?: string | null;
      rationale?: string[] | null;
    } | null;
    consistency_status?: string | null;
    consistency_score?: number | null;
  } | null;
  rewrite_actions?: Array<{
    target_clause_index?: number | null;
    target_problem_type?: string | null;
    action_type?: string | null;
    before_text?: string | null;
    after_text?: string | null;
    reason?: string | null;
  }> | null;
};

export async function runResolutionSimulation(
  input: DesignReviewInput
): Promise<ResolutionSimulationResponse> {
  return apiPost<ResolutionSimulationResponse>(
    "/ops/design/resolution_simulation",
    input
  );
}