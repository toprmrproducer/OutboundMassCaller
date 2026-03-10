-- =============================================================
-- RapidXAI Outbound Caller - Unified Supabase Schema
-- Run once in Supabase SQL Editor on a fresh project.
-- =============================================================

-- 1) Required extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS vector;

-- 2) Core entities
CREATE TABLE IF NOT EXISTS businesses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  description TEXT,
  website TEXT,
  timezone TEXT DEFAULT 'Asia/Kolkata',
  whatsapp_instance TEXT,
  whatsapp_token TEXT,
  -- Integrations/settings
  hubspot_api_key TEXT,
  hubspot_sync_enabled BOOLEAN DEFAULT false,
  gcal_calendar_id TEXT,
  gcal_sync_enabled BOOLEAN DEFAULT false,
  slack_webhook_url TEXT,
  slack_notify_on TEXT[] DEFAULT ARRAY['booking', 'campaign_complete', 'budget_alert', 'error']::text[],
  sms_provider TEXT,
  sms_api_key TEXT,
  sms_sender_id TEXT,
  reminder_enabled BOOLEAN DEFAULT true,
  reminder_hours_before INT[] DEFAULT ARRAY[24,2]::int[],
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sip_trunks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  trunk_id TEXT NOT NULL UNIQUE,
  phone_number TEXT,
  number_pool JSONB DEFAULT '[]'::jsonb,
  max_concurrent_calls INT DEFAULT 5,
  calls_per_minute INT DEFAULT 10,
  is_active BOOLEAN DEFAULT true,
  -- Caller ID rotation / spam health
  rotation_strategy TEXT DEFAULT 'round_robin',
  last_used_number_index INT DEFAULT 0,
  calls_per_number_limit INT DEFAULT 50,
  -- Inbound routing
  inbound_enabled BOOLEAN DEFAULT false,
  inbound_agent_id UUID,
  inbound_fallback_message TEXT DEFAULT 'Thank you for calling. Our team will get back to you shortly.',
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  subtitle TEXT,
  stt_provider TEXT DEFAULT 'sarvam',
  stt_language TEXT DEFAULT 'hi-IN',
  stt_model TEXT DEFAULT 'saaras:v3',
  llm_provider TEXT DEFAULT 'openai',
  llm_model TEXT DEFAULT 'gpt-4.1-mini',
  llm_base_url TEXT,
  llm_temperature FLOAT DEFAULT 0.7,
  llm_max_tokens INT DEFAULT 60,
  tts_provider TEXT DEFAULT 'sarvam',
  tts_voice TEXT DEFAULT 'rohan',
  tts_language TEXT DEFAULT 'hi-IN',
  system_prompt TEXT NOT NULL DEFAULT '',
  first_line TEXT NOT NULL DEFAULT '',
  agent_instructions TEXT DEFAULT '',
  max_turns INT DEFAULT 25,
  silence_threshold_seconds INT DEFAULT 20,
  max_nudges INT DEFAULT 2,
  is_active BOOLEAN DEFAULT false,
  -- Group A/D/G/F fields
  inbound_speaks_first BOOLEAN DEFAULT true,
  consent_disclosure TEXT DEFAULT NULL,
  consent_disclosure_language TEXT DEFAULT 'hi-IN',
  voicemail_message TEXT,
  handoff_enabled BOOLEAN DEFAULT false,
  handoff_sip_address TEXT,
  handoff_trigger_phrases TEXT[] DEFAULT ARRAY['speak to human', 'talk to agent', 'transfer me', 'real person', 'manager', 'supervisor']::text[],
  dtmf_menu JSONB DEFAULT NULL,
  auto_detect_language BOOLEAN DEFAULT false,
  supported_languages TEXT[] DEFAULT ARRAY['hi-IN', 'en-IN', 'ta-IN', 'te-IN', 'kn-IN', 'mr-IN']::text[],
  llm_fallback_provider TEXT,
  llm_fallback_model TEXT,
  memory_enabled BOOLEAN DEFAULT true,
  memory_lookback_calls INT DEFAULT 3,
  persona TEXT DEFAULT 'professional',
  sms_followup_enabled BOOLEAN DEFAULT false,
  sms_template_interested TEXT,
  sms_template_booked TEXT,
  email_followup_enabled BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now()
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_sip_trunks_inbound_agent'
  ) THEN
    ALTER TABLE sip_trunks
      ADD CONSTRAINT fk_sip_trunks_inbound_agent
      FOREIGN KEY (inbound_agent_id) REFERENCES agents(id) ON DELETE SET NULL;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS campaigns (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
  sip_trunk_id UUID REFERENCES sip_trunks(id) ON DELETE SET NULL,
  name TEXT NOT NULL,
  description TEXT,
  objective TEXT,
  status TEXT DEFAULT 'draft',
  calls_per_minute INT DEFAULT 3,
  max_concurrent_calls INT DEFAULT 5,
  retry_failed BOOLEAN DEFAULT true,
  max_retries INT DEFAULT 2,
  retry_delay_minutes INT DEFAULT 60,
  call_window_start TEXT DEFAULT '09:00',
  call_window_end TEXT DEFAULT '20:00',
  timezone TEXT DEFAULT 'Asia/Kolkata',
  custom_script TEXT,
  skip_sundays BOOLEAN DEFAULT true,
  retry_strategy JSONB DEFAULT '{"no_answer": {"delay_minutes": 30, "max_attempts": 3}, "busy": {"delay_minutes": 15, "max_attempts": 5}, "failed": {"delay_minutes": 60, "max_attempts": 2}, "voicemail": {"delay_minutes": 240, "max_attempts": 1}, "best_time_window": {"start": "10:00", "end": "18:00"}}'::jsonb,
  scheduled_start_at TIMESTAMPTZ,
  budget_cap_usd NUMERIC(10,2) DEFAULT NULL,
  requires_approval BOOLEAN DEFAULT false,
  approval_status TEXT DEFAULT 'not_required',
  approved_by TEXT,
  approval_notes TEXT,
  approved_at TIMESTAMPTZ,
  paused_at TIMESTAMPTZ,
  pause_reason TEXT,
  calls_made_before_pause INT DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS call_scripts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  system_prompt TEXT NOT NULL,
  first_line TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS leads (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id UUID REFERENCES campaigns(id) ON DELETE CASCADE,
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  phone TEXT NOT NULL,
  name TEXT,
  email TEXT,
  language TEXT DEFAULT 'hi-IN',
  custom_data JSONB DEFAULT '{}'::jsonb,
  status TEXT DEFAULT 'pending',
  call_attempts INT DEFAULT 0,
  max_call_attempts INT DEFAULT 3,
  last_call_at TIMESTAMPTZ,
  next_call_at TIMESTAMPTZ,
  scheduled_script_id UUID REFERENCES call_scripts(id) ON DELETE SET NULL,
  notes TEXT,
  -- lead intelligence
  score INT DEFAULT 50,
  score_factors JSONB DEFAULT '{}'::jsonb,
  score_updated_at TIMESTAMPTZ,
  temperature TEXT DEFAULT 'warm',
  tags TEXT[] DEFAULT ARRAY[]::text[],
  country_code TEXT,
  phone_e164 TEXT,
  email_verified BOOLEAN DEFAULT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS calls (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id UUID REFERENCES leads(id) ON DELETE CASCADE,
  campaign_id UUID REFERENCES campaigns(id) ON DELETE CASCADE,
  agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
  sip_trunk_id UUID REFERENCES sip_trunks(id) ON DELETE SET NULL,
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  phone TEXT NOT NULL,
  room_id TEXT UNIQUE,
  status TEXT DEFAULT 'initiated',
  call_attempt_number INT DEFAULT 1,
  duration_seconds INT DEFAULT 0,
  transcript TEXT,
  summary TEXT,
  sentiment TEXT,
  disposition TEXT,
  recording_url TEXT,
  was_booked BOOLEAN DEFAULT false,
  interrupt_count INT DEFAULT 0,
  estimated_cost_usd NUMERIC(10,4) DEFAULT 0,
  whatsapp_sent BOOLEAN DEFAULT false,
  call_date DATE,
  call_hour INT,
  -- advanced/expansion fields
  call_direction TEXT DEFAULT 'outbound',
  consent_disclosed BOOLEAN DEFAULT false,
  was_voicemail BOOLEAN DEFAULT false,
  amd_result TEXT,
  supervisor_joined BOOLEAN DEFAULT false,
  supervisor_mode TEXT,
  detected_language TEXT,
  quality_score INT,
  sms_sent BOOLEAN DEFAULT false,
  email_sent BOOLEAN DEFAULT false,
  key_points JSONB DEFAULT '[]'::jsonb,
  follow_up_action TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS call_transcript_lines (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  room_id TEXT NOT NULL,
  phone TEXT,
  role TEXT,
  content TEXT,
  turn_number INT DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bookings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  call_id UUID REFERENCES calls(id) ON DELETE CASCADE,
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  caller_name TEXT,
  caller_phone TEXT NOT NULL,
  caller_email TEXT,
  start_time TIMESTAMPTZ NOT NULL,
  notes TEXT,
  status TEXT DEFAULT 'confirmed',
  gcal_event_id TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS active_calls (
  room_id TEXT PRIMARY KEY,
  phone TEXT,
  campaign_id UUID,
  agent_id UUID,
  business_id UUID,
  sip_trunk_id UUID,
  status TEXT DEFAULT 'active',
  started_at TIMESTAMPTZ DEFAULT now(),
  last_updated TIMESTAMPTZ DEFAULT now(),
  turn_count INT DEFAULT 0,
  last_user_utterance TEXT,
  live_sentiment TEXT DEFAULT 'neutral'
);

CREATE TABLE IF NOT EXISTS dnc_list (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  phone TEXT NOT NULL,
  reason TEXT,
  added_by TEXT DEFAULT 'system',
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (business_id, phone)
);

CREATE TABLE IF NOT EXISTS knowledge_base (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  agent_id UUID REFERENCES agents(id) ON DELETE CASCADE,
  category TEXT,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  embedding vector(1536),
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scheduled_callbacks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id UUID REFERENCES leads(id) ON DELETE CASCADE,
  campaign_id UUID REFERENCES campaigns(id) ON DELETE CASCADE,
  script_id UUID REFERENCES call_scripts(id) ON DELETE SET NULL,
  scheduled_for TIMESTAMPTZ NOT NULL,
  reason TEXT,
  status TEXT DEFAULT 'pending',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 3) Campaign intelligence
CREATE TABLE IF NOT EXISTS campaign_agent_variants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  campaign_id UUID REFERENCES campaigns(id) ON DELETE CASCADE,
  agent_id UUID REFERENCES agents(id) ON DELETE CASCADE,
  weight INT DEFAULT 50,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS campaign_templates (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  description TEXT,
  config JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS campaign_sequences (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS campaign_sequence_steps (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sequence_id UUID REFERENCES campaign_sequences(id) ON DELETE CASCADE,
  campaign_id UUID REFERENCES campaigns(id) ON DELETE CASCADE,
  step_order INT NOT NULL,
  trigger TEXT DEFAULT 'previous_complete',
  delay_days INT DEFAULT 0,
  filter_disposition TEXT DEFAULT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 4) Holidays / number health / reminders / surveys
CREATE TABLE IF NOT EXISTS holidays (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  date DATE NOT NULL,
  country_code TEXT DEFAULT 'IN',
  is_recurring BOOLEAN DEFAULT true,
  skip_calls BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (business_id, date)
);

CREATE TABLE IF NOT EXISTS number_health (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sip_trunk_id UUID REFERENCES sip_trunks(id) ON DELETE CASCADE,
  phone_number TEXT NOT NULL,
  spam_score INT DEFAULT 0,
  spam_label TEXT,
  last_checked_at TIMESTAMPTZ,
  calls_today INT DEFAULT 0,
  calls_this_week INT DEFAULT 0,
  is_paused BOOLEAN DEFAULT false,
  pause_reason TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (sip_trunk_id, phone_number)
);

CREATE TABLE IF NOT EXISTS reminder_calls (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  booking_id UUID REFERENCES bookings(id) ON DELETE CASCADE,
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  lead_id UUID REFERENCES leads(id) ON DELETE CASCADE,
  agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
  phone TEXT NOT NULL,
  scheduled_for TIMESTAMPTZ NOT NULL,
  hours_before INT NOT NULL,
  status TEXT DEFAULT 'pending',
  call_id UUID REFERENCES calls(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS surveys (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_id UUID REFERENCES agents(id) ON DELETE CASCADE,
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  question TEXT NOT NULL,
  response_type TEXT DEFAULT 'numeric',
  valid_responses TEXT[],
  send_via TEXT DEFAULT 'whatsapp',
  send_delay_minutes INT DEFAULT 2,
  trigger_dispositions TEXT[] DEFAULT ARRAY['interested','booked','callback_requested']::text[],
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS survey_responses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  survey_id UUID REFERENCES surveys(id) ON DELETE CASCADE,
  call_id UUID REFERENCES calls(id) ON DELETE CASCADE,
  lead_id UUID REFERENCES leads(id) ON DELETE CASCADE,
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  phone TEXT NOT NULL,
  response TEXT,
  response_received_at TIMESTAMPTZ,
  status TEXT DEFAULT 'sent',
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 5) Security / access / audit
CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  email TEXT NOT NULL UNIQUE,
  hashed_password TEXT NOT NULL,
  role TEXT DEFAULT 'viewer',
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS user_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES users(id) ON DELETE CASCADE,
  token TEXT NOT NULL UNIQUE,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  key_prefix TEXT NOT NULL,
  scopes TEXT[] DEFAULT ARRAY['read', 'write']::text[],
  last_used_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ DEFAULT NULL,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit_log (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID,
  user_id UUID,
  action TEXT NOT NULL,
  resource_type TEXT NOT NULL,
  resource_id TEXT,
  old_value JSONB,
  new_value JSONB,
  ip_address TEXT,
  user_agent TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 6) AI runtime support
CREATE TABLE IF NOT EXISTS objection_handlers (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_id UUID REFERENCES agents(id) ON DELETE CASCADE,
  trigger_phrases TEXT[] NOT NULL,
  response TEXT NOT NULL,
  priority INT DEFAULT 1,
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pronunciation_guide (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_id UUID REFERENCES agents(id) ON DELETE CASCADE,
  word TEXT NOT NULL,
  pronunciation TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 7) Generic webhooks + scheduled reports + notifications
CREATE TABLE IF NOT EXISTS webhooks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  events TEXT[] NOT NULL,
  secret TEXT,
  is_active BOOLEAN DEFAULT true,
  last_triggered_at TIMESTAMPTZ,
  failure_count INT DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  webhook_id UUID REFERENCES webhooks(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  response_status INT,
  response_body TEXT,
  success BOOLEAN DEFAULT false,
  attempted_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS scheduled_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  report_type TEXT NOT NULL,
  frequency TEXT DEFAULT 'daily',
  send_to TEXT[] NOT NULL,
  campaign_id UUID,
  is_active BOOLEAN DEFAULT true,
  last_sent_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notifications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  resource_type TEXT,
  resource_id UUID,
  is_read BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- 8a) Compatibility backfills for partially-migrated databases
-- Safe to run on fresh and existing deployments.
ALTER TABLE leads
  ADD COLUMN IF NOT EXISTS next_call_at TIMESTAMPTZ DEFAULT NULL;

ALTER TABLE campaigns
  ADD COLUMN IF NOT EXISTS scheduled_start_at TIMESTAMPTZ DEFAULT NULL;

-- Legacy compatibility: some older releases expected this on calls.
ALTER TABLE calls
  ADD COLUMN IF NOT EXISTS next_call_at TIMESTAMPTZ DEFAULT NULL;

-- 8) Indexes
CREATE INDEX IF NOT EXISTS idx_leads_campaign_status ON leads(campaign_id, status);
CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
CREATE INDEX IF NOT EXISTS idx_leads_next_call_at_scheduled ON leads(next_call_at) WHERE status = 'scheduled';
CREATE INDEX IF NOT EXISTS idx_leads_campaign_score_created ON leads(campaign_id, score DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_leads_tags_gin ON leads USING GIN(tags);
CREATE INDEX IF NOT EXISTS idx_leads_business_phone ON leads(business_id, phone);

CREATE INDEX IF NOT EXISTS idx_calls_campaign_date ON calls(campaign_id, call_date);
CREATE INDEX IF NOT EXISTS idx_calls_lead_id ON calls(lead_id);
CREATE INDEX IF NOT EXISTS idx_calls_room_id ON calls(room_id);
CREATE INDEX IF NOT EXISTS idx_calls_business_created ON calls(business_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_calls_business_direction_created ON calls(business_id, call_direction, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_call_transcript_lines_room_created ON call_transcript_lines(room_id, created_at);
CREATE INDEX IF NOT EXISTS idx_active_calls_business_status ON active_calls(business_id, status, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_dnc_phone ON dnc_list(phone);
CREATE INDEX IF NOT EXISTS idx_dnc_business_phone ON dnc_list(business_id, phone);

CREATE INDEX IF NOT EXISTS idx_scheduled_callbacks_due ON scheduled_callbacks(scheduled_for, status);
CREATE INDEX IF NOT EXISTS idx_campaign_variants_campaign ON campaign_agent_variants(campaign_id);
CREATE INDEX IF NOT EXISTS idx_campaign_sequence_steps_sequence_order ON campaign_sequence_steps(sequence_id, step_order);

CREATE INDEX IF NOT EXISTS idx_holidays_business_date ON holidays(business_id, date);
CREATE INDEX IF NOT EXISTS idx_number_health_trunk_phone ON number_health(sip_trunk_id, phone_number);
CREATE INDEX IF NOT EXISTS idx_number_health_spam_paused ON number_health(spam_score, is_paused);
CREATE INDEX IF NOT EXISTS idx_reminder_calls_due ON reminder_calls(scheduled_for, status) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_survey_responses_phone ON survey_responses(phone, status);
CREATE INDEX IF NOT EXISTS idx_survey_responses_survey ON survey_responses(survey_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_sessions_token ON user_sessions(token);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user ON user_sessions(user_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_keys_business_active ON api_keys(business_id, is_active, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_business_created ON audit_log(business_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_webhooks_business_active ON webhooks(business_id, is_active, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_hook_attempted ON webhook_deliveries(webhook_id, attempted_at DESC);
CREATE INDEX IF NOT EXISTS idx_scheduled_reports_business_active ON scheduled_reports(business_id, is_active, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_business_read ON notifications(business_id, is_read, created_at DESC);

-- Vector similarity index (optional but recommended for scale)
CREATE INDEX IF NOT EXISTS idx_knowledge_base_embedding
  ON knowledge_base
  USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 100);

-- 9) Helpful constraints
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'uq_campaign_sequence_step'
  ) THEN
    ALTER TABLE campaign_sequence_steps
      ADD CONSTRAINT uq_campaign_sequence_step UNIQUE (sequence_id, step_order);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'chk_campaign_variant_weight'
  ) THEN
    ALTER TABLE campaign_agent_variants
      ADD CONSTRAINT chk_campaign_variant_weight CHECK (weight >= 0 AND weight <= 100);
  END IF;
END $$;

-- =============================================================
-- Done.
-- =============================================================
