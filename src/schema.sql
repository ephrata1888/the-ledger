-- The Ledger (TRP1) · Canonical Event Schema
-- Never store PII in the payload without encryption.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS events (
  event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  stream_id TEXT NOT NULL,
  stream_position BIGINT NOT NULL,
  global_position BIGINT GENERATED ALWAYS AS IDENTITY,
  event_type TEXT NOT NULL,
  event_version SMALLINT NOT NULL DEFAULT 1,
  payload JSONB NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT uq_stream_position UNIQUE (stream_id, stream_position)
);

-- Canonical indices
CREATE INDEX IF NOT EXISTS idx_events_stream_id ON events (stream_id, stream_position);
CREATE INDEX IF NOT EXISTS idx_events_global_pos ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_recorded ON events (recorded_at);
CREATE INDEX IF NOT EXISTS brin_events_recorded_at ON events USING BRIN (recorded_at);

CREATE TABLE IF NOT EXISTS event_streams (
  stream_id TEXT PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  current_version BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  archived_at TIMESTAMPTZ,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_event_streams_type_active ON event_streams (aggregate_type, archived_at);

CREATE TABLE IF NOT EXISTS projection_checkpoints (
  projection_name TEXT PRIMARY KEY,
  last_position BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id UUID NOT NULL REFERENCES events(event_id),
  destination TEXT NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ,
  attempts SMALLINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_outbox_unpublished ON outbox (published_at) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------------
-- Applicant Registry (TRP1 datagen / regulatory reference data)
-- Data boundary: schema applicant_registry, table companies
-- ---------------------------------------------------------------------------

DROP SCHEMA IF EXISTS applicant_registry CASCADE;

CREATE SCHEMA applicant_registry;

CREATE TABLE IF NOT EXISTS applicant_registry.companies (
  company_id TEXT PRIMARY KEY,
  legal_name TEXT NOT NULL,
  variant TEXT NOT NULL,
  legal_type TEXT NOT NULL DEFAULT 'LLC',
  sector TEXT NOT NULL DEFAULT '',
  jurisdiction TEXT NOT NULL DEFAULT 'US',
  founded_year INTEGER NOT NULL DEFAULT 2010,
  revenue_y1 DOUBLE PRECISION,
  revenue_y2 DOUBLE PRECISION,
  revenue_y3 DOUBLE PRECISION,
  net_income_y1 DOUBLE PRECISION,
  net_income_y2 DOUBLE PRECISION,
  net_income_y3 DOUBLE PRECISION,
  compliance_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_applicant_registry_companies_variant ON applicant_registry.companies (variant);
CREATE INDEX IF NOT EXISTS idx_applicant_registry_companies_jurisdiction ON applicant_registry.companies (jurisdiction);

CREATE TABLE IF NOT EXISTS applicant_registry.compliance_flags (
  id BIGSERIAL PRIMARY KEY,
  company_id TEXT NOT NULL REFERENCES applicant_registry.companies(company_id) ON DELETE CASCADE,
  flag_type TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (company_id, flag_type)
);

CREATE INDEX IF NOT EXISTS idx_applicant_registry_flags_company ON applicant_registry.compliance_flags (company_id);

-- ---------------------------------------------------------------------------
-- Phase 3: CQRS read models (projections)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS projection_application_summary (
  application_id TEXT PRIMARY KEY,
  state TEXT,
  applicant_id TEXT,
  requested_amount_usd DOUBLE PRECISION,
  risk_tier TEXT,
  fraud_score DOUBLE PRECISION,
  compliance_status TEXT NOT NULL DEFAULT 'UNKNOWN',
  last_event_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_projection_app_summary_state ON projection_application_summary (state);

CREATE TABLE IF NOT EXISTS projection_agent_performance (
  model_version TEXT PRIMARY KEY,
  analyses_completed BIGINT NOT NULL DEFAULT 0,
  sum_confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
  sum_duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  decision_approve_count BIGINT NOT NULL DEFAULT 0,
  decision_decline_count BIGINT NOT NULL DEFAULT 0,
  decision_refer_count BIGINT NOT NULL DEFAULT 0,
  human_review_total BIGINT NOT NULL DEFAULT 0,
  human_override_true_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS compliance_audit_timeline (
  application_id TEXT NOT NULL,
  global_position BIGINT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL,
  compliance_state JSONB NOT NULL,
  PRIMARY KEY (application_id, global_position)
);

CREATE INDEX IF NOT EXISTS idx_compliance_audit_app_time ON compliance_audit_timeline (application_id, recorded_at DESC);

CREATE TABLE IF NOT EXISTS projection_dead_letter (
  id BIGSERIAL PRIMARY KEY,
  projection_name TEXT NOT NULL,
  global_position BIGINT NOT NULL,
  event_type TEXT NOT NULL,
  stream_id TEXT NOT NULL,
  error_message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;

