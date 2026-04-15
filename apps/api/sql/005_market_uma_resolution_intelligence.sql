CREATE TABLE IF NOT EXISTS public.market_uma_resolution_intelligence_daily (
    market_id TEXT NOT NULL,
    day DATE NOT NULL,

    oracle_family TEXT NOT NULL DEFAULT 'uma_oo',

    resolution_clarity_score DOUBLE PRECISION NULL,
    oracle_risk_score DOUBLE PRECISION NULL,
    dispute_propensity_score DOUBLE PRECISION NULL,

    resolution_complexity TEXT NULL,
    resolution_state TEXT NULL,

    disputed BOOLEAN NOT NULL DEFAULT FALSE,
    settled BOOLEAN NOT NULL DEFAULT FALSE,

    requested_time TIMESTAMPTZ NULL,
    proposed_time TIMESTAMPTZ NULL,
    disputed_time TIMESTAMPTZ NULL,
    settled_time TIMESTAMPTZ NULL,

    request_to_proposal_hours DOUBLE PRECISION NULL,
    proposal_to_dispute_hours DOUBLE PRECISION NULL,
    dispute_to_settlement_hours DOUBLE PRECISION NULL,
    request_to_settlement_hours DOUBLE PRECISION NULL,

    has_time_ambiguity BOOLEAN NOT NULL DEFAULT FALSE,
    has_wording_ambiguity BOOLEAN NOT NULL DEFAULT FALSE,
    has_source_fragility BOOLEAN NOT NULL DEFAULT FALSE,
    has_bulletin_board_dependency BOOLEAN NOT NULL DEFAULT FALSE,

    flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    summary TEXT NULL,
    engine_version TEXT NOT NULL,
    raw_inputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (market_id, day)
);

CREATE INDEX IF NOT EXISTS idx_market_uma_resolution_intelligence_daily_day
    ON public.market_uma_resolution_intelligence_daily (day DESC);

CREATE INDEX IF NOT EXISTS idx_market_uma_resolution_intelligence_daily_state
    ON public.market_uma_resolution_intelligence_daily (day DESC, resolution_state);

CREATE INDEX IF NOT EXISTS idx_market_uma_resolution_intelligence_daily_disputed
    ON public.market_uma_resolution_intelligence_daily (day DESC, disputed);