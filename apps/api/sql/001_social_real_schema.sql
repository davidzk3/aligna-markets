create table if not exists public.market_topic_map (
  market_id text not null,
  as_of_date date not null,
  title text not null,
  category text,
  primary_topic text,
  topic_bundle_json jsonb not null,
  query_bundle_json jsonb not null,
  mapping_confidence numeric,
  created_at timestamptz default now(),
  primary key (market_id, as_of_date)
);

create table if not exists public.market_social_raw (
  id bigserial primary key,
  market_id text not null,
  source text not null,
  source_type text,
  query text,
  external_id text,
  author text,
  source_name text,
  title text,
  body text,
  url text,
  language text,
  country text,
  published_at timestamptz not null,
  ingested_at timestamptz default now(),
  engagement_score numeric,
  comment_count integer,
  upvote_ratio numeric,
  metadata_json jsonb,
  dedupe_hash text
);

create index if not exists idx_market_social_raw_market_time
  on public.market_social_raw (market_id, published_at desc);

create index if not exists idx_market_social_raw_source_time
  on public.market_social_raw (source, published_at desc);

create index if not exists idx_market_social_raw_dedupe_hash
  on public.market_social_raw (dedupe_hash);

create table if not exists public.market_social_features_daily (
  market_id text not null,
  day date not null,
  mention_count integer,
  source_count integer,
  unique_author_count integer,
  source_diversity numeric,
  engagement_total numeric,
  engagement_per_mention numeric,
  reddit_post_count integer,
  news_article_count integer,
  recency_weighted_volume numeric,
  trend_velocity_1d numeric,
  trend_velocity_3d numeric,
  attention_durability_score numeric,
  source_concentration numeric,
  sentiment_score numeric,
  sentiment_dispersion numeric,
  hype_score numeric,
  confidence_score numeric,
  demand_score numeric,
  demand_state text,
  features_json jsonb,
  created_at timestamptz default now(),
  primary key (market_id, day)
);

create index if not exists idx_market_social_features_daily_day
  on public.market_social_features_daily (day desc);