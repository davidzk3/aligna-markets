CREATE TABLE IF NOT EXISTS public.market_design_intelligence_evaluations (
    id SERIAL PRIMARY KEY,

    input_hash TEXT NOT NULL UNIQUE,

    title TEXT NOT NULL,
    description TEXT NULL,
    category TEXT NULL,
    market_type TEXT NULL,
    protocol TEXT NOT NULL DEFAULT 'polymarket',
    oracle_family TEXT NOT NULL DEFAULT 'uma_oo',

    word_count INTEGER NOT NULL DEFAULT 0,
    exclusion_count INTEGER NOT NULL DEFAULT 0,
    source_reference_count INTEGER NOT NULL DEFAULT 0,
    geography_constraint_count INTEGER NOT NULL DEFAULT 0,
    neutral_outcome_branch_count INTEGER NOT NULL DEFAULT 0,

    has_time_ambiguity BOOLEAN NOT NULL DEFAULT FALSE,
    has_wording_ambiguity BOOLEAN NOT NULL DEFAULT FALSE,
    has_source_fragility BOOLEAN NOT NULL DEFAULT FALSE,
    has_bulletin_board_dependency BOOLEAN NOT NULL DEFAULT FALSE,
    has_timezone_complexity BOOLEAN NOT NULL DEFAULT FALSE,
    has_official_source_priority BOOLEAN NOT NULL DEFAULT FALSE,
    has_consensus_fallback BOOLEAN NOT NULL DEFAULT FALSE,
    has_video_evidence_fallback BOOLEAN NOT NULL DEFAULT FALSE,
    has_remake_logic BOOLEAN NOT NULL DEFAULT FALSE,
    has_cancellation_logic BOOLEAN NOT NULL DEFAULT FALSE,
    has_series_dependency_logic BOOLEAN NOT NULL DEFAULT FALSE,
    has_proxy_actor_exclusion BOOLEAN NOT NULL DEFAULT FALSE,
    has_territorial_carveout BOOLEAN NOT NULL DEFAULT FALSE,
    has_confirmation_window BOOLEAN NOT NULL DEFAULT FALSE,

    event_container_specificity_score DOUBLE PRECISION NULL,
    edge_case_branching_score DOUBLE PRECISION NULL,
    interpretive_ambiguity_score DOUBLE PRECISION NULL,
    source_fragility_score DOUBLE PRECISION NULL,
    time_confirmation_fragility_score DOUBLE PRECISION NULL,
    closed_world_resolvability_score DOUBLE PRECISION NULL,

    design_clarity_score DOUBLE PRECISION NULL,
    expected_resolution_risk_score DOUBLE PRECISION NULL,
    expected_dispute_propensity_score DOUBLE PRECISION NULL,

    operational_complexity TEXT NULL,
    interpretive_complexity TEXT NULL,
    evidence_dependence_tier TEXT NULL,
    design_complexity TEXT NULL,
    expected_resolution_state TEXT NULL,
    launch_readiness TEXT NULL,

    flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    suggestions JSONB NOT NULL DEFAULT '[]'::jsonb,
    historical_priors_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    extracted_features_json JSONB NOT NULL DEFAULT '{}'::jsonb,

    summary TEXT NULL,
    engine_version TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_market_design_intelligence_evaluations_created_at
    ON public.market_design_intelligence_evaluations (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_design_intelligence_evaluations_launch_readiness
    ON public.market_design_intelligence_evaluations (launch_readiness, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_market_design_intelligence_evaluations_design_complexity
    ON public.market_design_intelligence_evaluations (design_complexity, created_at DESC);
