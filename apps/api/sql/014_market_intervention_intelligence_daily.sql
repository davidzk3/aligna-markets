--
-- PostgreSQL database dump
--

\restrict Tmn7CEs8wrf0qD26CnTVW59DpdXFM8ix5vIYwRIeMnUzqJnOs7XpuNTB9dIhsIT

-- Dumped from database version 16.11 (Debian 16.11-1.pgdg13+1)
-- Dumped by pg_dump version 16.11 (Debian 16.11-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: market_intervention_intelligence_daily; Type: TABLE; Schema: public; Owner: pmops
--

CREATE TABLE public.market_intervention_intelligence_daily (
    id bigint NOT NULL,
    market_id text NOT NULL,
    day date NOT NULL,
    intervention_needed boolean,
    recommended_intervention text,
    recommended_action text,
    action_priority text,
    action_reason text,
    incentive_dependency text,
    activity_quality text,
    organic_participation_ratio double precision,
    distortion_risk text,
    expected_failure_mode text,
    intervention_effectiveness_estimate double precision,
    confidence double precision,
    inputs jsonb DEFAULT '{}'::jsonb NOT NULL,
    engine_version text DEFAULT 'market_intervention_intelligence_v1'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.market_intervention_intelligence_daily OWNER TO pmops;

--
-- Name: market_intervention_intelligence_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: pmops
--

CREATE SEQUENCE public.market_intervention_intelligence_daily_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.market_intervention_intelligence_daily_id_seq OWNER TO pmops;

--
-- Name: market_intervention_intelligence_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: pmops
--

ALTER SEQUENCE public.market_intervention_intelligence_daily_id_seq OWNED BY public.market_intervention_intelligence_daily.id;


--
-- Name: market_intervention_intelligence_daily id; Type: DEFAULT; Schema: public; Owner: pmops
--

ALTER TABLE ONLY public.market_intervention_intelligence_daily ALTER COLUMN id SET DEFAULT nextval('public.market_intervention_intelligence_daily_id_seq'::regclass);


--
-- Name: market_intervention_intelligence_daily market_intervention_intelligen_market_id_day_engine_version_key; Type: CONSTRAINT; Schema: public; Owner: pmops
--

ALTER TABLE ONLY public.market_intervention_intelligence_daily
    ADD CONSTRAINT market_intervention_intelligen_market_id_day_engine_version_key UNIQUE (market_id, day, engine_version);


--
-- Name: market_intervention_intelligence_daily market_intervention_intelligence_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: pmops
--

ALTER TABLE ONLY public.market_intervention_intelligence_daily
    ADD CONSTRAINT market_intervention_intelligence_daily_pkey PRIMARY KEY (id);


--
-- Name: ix_market_intervention_intelligence_daily_created_at; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX ix_market_intervention_intelligence_daily_created_at ON public.market_intervention_intelligence_daily USING btree (created_at DESC);


--
-- Name: ix_market_intervention_intelligence_daily_market_day; Type: INDEX; Schema: public; Owner: pmops
--

CREATE INDEX ix_market_intervention_intelligence_daily_market_day ON public.market_intervention_intelligence_daily USING btree (market_id, day DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict Tmn7CEs8wrf0qD26CnTVW59DpdXFM8ix5vIYwRIeMnUzqJnOs7XpuNTB9dIhsIT

