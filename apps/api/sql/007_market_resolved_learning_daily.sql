CREATE TABLE IF NOT EXISTS public.market_resolved_learning_daily (
    market_id TEXT NOT NULL,
    resolution_day DATE NOT NULL,

    linked_uma_request_id TEXT NULL,
    link_method TEXT NULL,
    link_confidence DOUBLE PRECISION NULL,

    structural_state_pre TEXT NULL,
    structural_score_pre DOUBLE PRECISION NULL,
    social_state_pre TEXT NULL,
    social_score_pre DOUBLE PRECISION NULL,
    alignment_state_pre TEXT NULL,
    alignment_score_pre DOUBLE PRECISION NULL,

    unique_traders_pre DOUBLE PRECISION NULL,
    trades_pre DOUBLE PRECISION NULL,
    volume_pre DOUBLE PRECISION NULL,
    concentration_hhi_pre DOUBLE PRECISION NULL,

    resolution_state TEXT NULL,
    resolution_clarity_score DOUBLE PRECISION NULL,
    oracle_risk_score DOUBLE PRECISION NULL,
    dispute_propensity_score DOUBLE PRECISION NULL,
    disputed BOOLEAN NOT NULL DEFAULT FALSE,
    settled BOOLEAN NOT NULL DEFAULT FALSE,
    request_to_settlement_hours DOUBLE PRECISION NULL,
    dispute_to_settlement_hours DOUBLE PRECISION NULL,
    resolution_complexity TEXT NULL,
    resolution_flags JSONB NOT NULL DEFAULT '[]'::jsonb,

    learning_summary TEXT NULL,
    engine_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (market_id, resolution_day)
);

CREATE INDEX IF NOT EXISTS idx_market_resolved_learning_daily_day
    ON public.market_resolved_learning_daily (resolution_day DESC);

CREATE INDEX IF NOT EXISTS idx_market_resolved_learning_daily_market
    ON public.market_resolved_learning_daily (market_id);

CREATE INDEX IF NOT EXISTS idx_market_resolved_learning_daily_resolution_state
    ON public.market_resolved_learning_daily (resolution_state);