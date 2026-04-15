CREATE TABLE IF NOT EXISTS public.market_oracle_link_candidates (
    id SERIAL PRIMARY KEY,

    market_id TEXT NOT NULL,
    condition_id TEXT NULL,
    slug TEXT NULL,

    candidate_uma_request_id TEXT NOT NULL,
    oracle_family TEXT NOT NULL DEFAULT 'uma_oo',

    link_method TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,

    is_selected BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (market_id, candidate_uma_request_id, link_method)
);

CREATE INDEX IF NOT EXISTS idx_market_oracle_link_candidates_market_id
    ON public.market_oracle_link_candidates (market_id);

CREATE INDEX IF NOT EXISTS idx_market_oracle_link_candidates_selected
    ON public.market_oracle_link_candidates (market_id, is_selected);

CREATE INDEX IF NOT EXISTS idx_market_oracle_link_candidates_confidence
    ON public.market_oracle_link_candidates (market_id, confidence DESC);