import json
import logging
import os
import random
import hashlib
import secrets
import uuid
from datetime import datetime, timedelta

import psycopg2
import pytz
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger("db")


def get_conn():
    """Open a new PostgreSQL connection for each operation."""
    database_url = os.environ["DATABASE_URL"]
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)


def release_conn(conn):
    """Close a connection."""
    try:
        conn.close()
    except Exception as e:
        logger.warning("[DB] release_conn failed: %s", e)


def _dict(row):
    return dict(row) if row else None


def _list(rows):
    return [dict(r) for r in (rows or [])]


def _vector_literal(embedding: list[float]) -> str:
    return "[" + ",".join(str(float(x)) for x in embedding) + "]"


def _clean_phone(phone: str) -> str:
    return (phone or "").strip().replace(" ", "")


def _update_statement(data: dict, allowed: set[str]) -> tuple[str, list]:
    update_data = {k: v for k, v in data.items() if k in allowed and v is not None}
    if not update_data:
        return "", []
    cols = []
    vals = []
    for k, v in update_data.items():
        cols.append(f"{k}=%s")
        vals.append(v)
    return ", ".join(cols), vals


def initdb():
    # NOTE: pgcrypto and vector must be enabled in Supabase Dashboard first:
    # Dashboard -> Database -> Extensions -> enable pgcrypto + vector
    # Each extension is attempted separately - a failure logs WARNING only
    # and does NOT block table creation or crash the app.
    _extensions = [
        "CREATE EXTENSION IF NOT EXISTS pgcrypto",
        "CREATE EXTENSION IF NOT EXISTS vector",
    ]

    _tables_sql = """
    CREATE TABLE IF NOT EXISTS businesses (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name TEXT NOT NULL,
      description TEXT,
      website TEXT,
      timezone TEXT DEFAULT 'Asia/Kolkata',
      whatsapp_instance TEXT,
      whatsapp_token TEXT,
      sip_uri TEXT,
      sip_username TEXT,
      sip_password TEXT,
      sip_caller_id TEXT,
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
      created_at TIMESTAMPTZ DEFAULT now()
    );

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
      created_at TIMESTAMPTZ DEFAULT now(),
      started_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ
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
      scheduled_script_id UUID,
      notes TEXT,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_leads_campaign_status ON leads(campaign_id, status);
    CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
    CREATE INDEX IF NOT EXISTS idx_leads_next_call_at_scheduled ON leads(next_call_at) WHERE status = 'scheduled';

    CREATE TABLE IF NOT EXISTS call_scripts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      system_prompt TEXT NOT NULL,
      first_line TEXT NOT NULL,
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
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_calls_campaign_date ON calls(campaign_id, call_date);
    CREATE INDEX IF NOT EXISTS idx_calls_lead_id ON calls(lead_id);
    CREATE INDEX IF NOT EXISTS idx_calls_room_id ON calls(room_id);

    CREATE TABLE IF NOT EXISTS call_transcript_lines (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      room_id TEXT NOT NULL,
      phone TEXT,
      role TEXT,
      content TEXT,
      turn_number INT DEFAULT 0,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_call_transcript_lines_room_created ON call_transcript_lines(room_id, created_at);

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
      last_updated TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS dnc_list (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
      phone TEXT NOT NULL,
      reason TEXT,
      added_by TEXT DEFAULT 'system',
      created_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(business_id, phone)
    );
    CREATE INDEX IF NOT EXISTS idx_dnc_phone ON dnc_list(phone);

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
    CREATE INDEX IF NOT EXISTS idx_scheduled_callbacks_due ON scheduled_callbacks(scheduled_for, status);

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

    CREATE TABLE IF NOT EXISTS holidays (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      business_id UUID REFERENCES businesses(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      date DATE NOT NULL,
      country_code TEXT DEFAULT 'IN',
      is_recurring BOOLEAN DEFAULT true,
      skip_calls BOOLEAN DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT now(),
      UNIQUE(business_id, date)
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
      UNIQUE(sip_trunk_id, phone_number)
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
    CREATE INDEX IF NOT EXISTS idx_reminder_calls_due
      ON reminder_calls(scheduled_for, status)
      WHERE status = 'pending';

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
    CREATE INDEX IF NOT EXISTS idx_survey_responses_phone
      ON survey_responses(phone, status);

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
    CREATE INDEX IF NOT EXISTS idx_audit_log_business_created
      ON audit_log(business_id, created_at DESC);

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
    CREATE INDEX IF NOT EXISTS idx_notifications_business_read
      ON notifications(business_id, is_read, created_at DESC);
    """

    conn = None
    try:
        conn = get_conn()

        for ext_stmt in _extensions:
            try:
                with conn.cursor() as cur:
                    cur.execute(ext_stmt)
                conn.commit()
                logger.info("[DB] Extension ready: %s", ext_stmt)
            except Exception as ext_err:
                conn.rollback()
                logger.warning(
                    "[DB] Extension skipped - enable in Supabase Dashboard "
                    "-> Database -> Extensions: %s", ext_err
                )

        # Run each schema statement independently so one bad index on an old
        # partially-migrated table cannot roll back all core table creation.
        schema_statements = [s.strip() for s in _tables_sql.split(";") if s.strip()]
        for stmt in schema_statements:
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
                conn.commit()
            except Exception as stmt_err:
                conn.rollback()
                stmt_upper = stmt.lstrip().upper()
                if stmt_upper.startswith("CREATE INDEX"):
                    logger.warning("[DB] Optional index skipped during initdb: %s", stmt_err)
                    continue
                logger.exception("[DB] Critical schema statement failed: %s", stmt_err)
                raise

        # Post-table migrations: run individually and continue on failures.
        # This avoids bootstrap deadlocks on older schemas missing new columns.
        post_migrations = [
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS next_call_at TIMESTAMPTZ DEFAULT NULL",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS scheduled_start_at TIMESTAMPTZ DEFAULT NULL",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS skip_sundays BOOLEAN DEFAULT true",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS budget_cap_usd NUMERIC(10,2) DEFAULT NULL",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS requires_approval BOOLEAN DEFAULT false",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS approval_status TEXT DEFAULT 'not_required'",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS approved_by TEXT",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS approval_notes TEXT",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS pause_reason TEXT",
            "ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS calls_made_before_pause INT DEFAULT 0",
            """
            ALTER TABLE campaigns
            ADD COLUMN IF NOT EXISTS retry_strategy JSONB DEFAULT
            '{"no_answer": {"delay_minutes": 30, "max_attempts": 3}, "busy": {"delay_minutes": 15, "max_attempts": 5}, "failed": {"delay_minutes": 60, "max_attempts": 2}, "voicemail": {"delay_minutes": 240, "max_attempts": 1}, "best_time_window": {"start": "10:00", "end": "18:00"}}'::jsonb
            """,
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS reminder_enabled BOOLEAN DEFAULT true",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS reminder_hours_before INT[] DEFAULT ARRAY[24,2]::int[]",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS sip_uri TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS sip_username TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS sip_password TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS sip_caller_id TEXT",
            "ALTER TABLE sip_trunks ADD COLUMN IF NOT EXISTS rotation_strategy TEXT DEFAULT 'round_robin'",
            "ALTER TABLE sip_trunks ADD COLUMN IF NOT EXISTS last_used_number_index INT DEFAULT 0",
            "ALTER TABLE sip_trunks ADD COLUMN IF NOT EXISTS calls_per_number_limit INT DEFAULT 50",
            "ALTER TABLE sip_trunks ADD COLUMN IF NOT EXISTS inbound_enabled BOOLEAN DEFAULT false",
            "ALTER TABLE sip_trunks ADD COLUMN IF NOT EXISTS inbound_agent_id UUID",
            "ALTER TABLE sip_trunks ADD COLUMN IF NOT EXISTS inbound_fallback_message TEXT DEFAULT 'Thank you for calling. Our team will get back to you shortly.'",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS call_direction TEXT DEFAULT 'outbound'",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS consent_disclosed BOOLEAN DEFAULT false",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS was_voicemail BOOLEAN DEFAULT false",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS amd_result TEXT",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS supervisor_joined BOOLEAN DEFAULT false",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS supervisor_mode TEXT",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS detected_language TEXT",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS quality_score INT",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS sms_sent BOOLEAN DEFAULT false",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS email_sent BOOLEAN DEFAULT false",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS key_points JSONB DEFAULT '[]'::jsonb",
            "ALTER TABLE calls ADD COLUMN IF NOT EXISTS follow_up_action TEXT",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS inbound_speaks_first BOOLEAN DEFAULT true",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS consent_disclosure TEXT DEFAULT NULL",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS consent_disclosure_language TEXT DEFAULT 'hi-IN'",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS voicemail_message TEXT",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS handoff_enabled BOOLEAN DEFAULT false",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS handoff_sip_address TEXT",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS handoff_trigger_phrases TEXT[] DEFAULT ARRAY['speak to human', 'talk to agent', 'transfer me', 'real person', 'manager', 'supervisor']::text[]",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS dtmf_menu JSONB DEFAULT NULL",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS auto_detect_language BOOLEAN DEFAULT false",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS supported_languages TEXT[] DEFAULT ARRAY['hi-IN', 'en-IN', 'ta-IN', 'te-IN', 'kn-IN', 'mr-IN']::text[]",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS llm_fallback_provider TEXT",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS llm_fallback_model TEXT",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS memory_enabled BOOLEAN DEFAULT true",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS memory_lookback_calls INT DEFAULT 3",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS persona TEXT DEFAULT 'professional'",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS sms_followup_enabled BOOLEAN DEFAULT false",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS sms_template_interested TEXT",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS sms_template_booked TEXT",
            "ALTER TABLE agents ADD COLUMN IF NOT EXISTS email_followup_enabled BOOLEAN DEFAULT false",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS score INT DEFAULT 50",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS score_factors JSONB DEFAULT '{}'::jsonb",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS score_updated_at TIMESTAMPTZ",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS temperature TEXT DEFAULT 'warm'",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS tags TEXT[] DEFAULT ARRAY[]::text[]",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS country_code TEXT",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS phone_e164 TEXT",
            "ALTER TABLE leads ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT NULL",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS hubspot_api_key TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS hubspot_sync_enabled BOOLEAN DEFAULT false",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS gcal_calendar_id TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS gcal_sync_enabled BOOLEAN DEFAULT false",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS slack_webhook_url TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS slack_notify_on TEXT[] DEFAULT ARRAY['booking', 'campaign_complete', 'budget_alert', 'error']::text[]",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS sms_provider TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS sms_api_key TEXT",
            "ALTER TABLE businesses ADD COLUMN IF NOT EXISTS sms_sender_id TEXT",
            "ALTER TABLE bookings ADD COLUMN IF NOT EXISTS gcal_event_id TEXT",
            "ALTER TABLE active_calls ADD COLUMN IF NOT EXISTS turn_count INT DEFAULT 0",
            "ALTER TABLE active_calls ADD COLUMN IF NOT EXISTS last_user_utterance TEXT",
            "ALTER TABLE active_calls ADD COLUMN IF NOT EXISTS live_sentiment TEXT DEFAULT 'neutral'",
            "CREATE INDEX IF NOT EXISTS idx_leads_next_call_at_scheduled ON leads(next_call_at) WHERE status = 'scheduled'",
        ]
        for stmt in post_migrations:
            try:
                with conn.cursor() as cur:
                    cur.execute(stmt)
                conn.commit()
            except Exception as mig_err:
                conn.rollback()
                logger.warning("[DB] Migration skipped: %s", mig_err)

        logger.info("[DB] Tables initialized")
    except Exception as e:
        logger.exception("[DB] initdb failed: %s", e)
        raise
    finally:
        if conn is not None:
            release_conn(conn)
# Businesses

def create_business(name, description=None, website=None, timezone="Asia/Kolkata", whatsapp_instance=None, whatsapp_token=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO businesses (name, description, website, timezone, whatsapp_instance, whatsapp_token)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (name, description, website, timezone, whatsapp_instance, whatsapp_token),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_business failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_businesses():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM businesses ORDER BY created_at DESC")
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_businesses failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_business(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM businesses WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_business failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def save_sip_config(business_id, sip_uri, username, password, caller_id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE businesses
                SET sip_uri=%s,
                    sip_username=%s,
                    sip_password=%s,
                    sip_caller_id=%s
                WHERE id=%s::uuid
                RETURNING id AS business_id, sip_uri, sip_username AS username, sip_password AS password, sip_caller_id AS caller_id
                """,
                (sip_uri, username, password, caller_id, business_id),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("save_sip_config failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_sip_config(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id AS business_id,
                       sip_uri,
                       sip_username AS username,
                       sip_password AS password,
                       sip_caller_id AS caller_id
                FROM businesses
                WHERE id=%s::uuid
                LIMIT 1
                """,
                (business_id,),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("get_sip_config failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def update_business(id, **kwargs):
    allowed = {
        "name",
        "description",
        "website",
        "timezone",
        "whatsapp_instance",
        "whatsapp_token",
        "hubspot_api_key",
        "hubspot_sync_enabled",
        "gcal_calendar_id",
        "gcal_sync_enabled",
        "slack_webhook_url",
        "slack_notify_on",
        "sms_provider",
        "sms_api_key",
        "sms_sender_id",
        "reminder_enabled",
        "reminder_hours_before",
        "sip_uri",
        "sip_username",
        "sip_password",
        "sip_caller_id",
    }
    set_sql, vals = _update_statement(kwargs, allowed)
    if not set_sql:
        return {}

    if "slack_notify_on" in kwargs and kwargs.get("slack_notify_on") is not None:
        keys = [k for k in kwargs.keys() if k in allowed and kwargs[k] is not None]
        vals = []
        set_parts = []
        for k in keys:
            if k == "slack_notify_on":
                set_parts.append("slack_notify_on=%s::text[]")
                vals.append(list(kwargs[k] or []))
            elif k == "reminder_hours_before":
                set_parts.append("reminder_hours_before=%s::int[]")
                vals.append([int(x) for x in (kwargs[k] or [])])
            else:
                set_parts.append(f"{k}=%s")
                vals.append(kwargs[k])
        set_sql = ", ".join(set_parts)
    elif "reminder_hours_before" in kwargs and kwargs.get("reminder_hours_before") is not None:
        keys = [k for k in kwargs.keys() if k in allowed and kwargs[k] is not None]
        vals = []
        set_parts = []
        for k in keys:
            if k == "reminder_hours_before":
                set_parts.append("reminder_hours_before=%s::int[]")
                vals.append([int(x) for x in (kwargs[k] or [])])
            else:
                set_parts.append(f"{k}=%s")
                vals.append(kwargs[k])
        set_sql = ", ".join(set_parts)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE businesses SET {set_sql} WHERE id=%s::uuid RETURNING *", vals + [id])
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("update_business failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_business(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM businesses WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_business failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# SIP Trunks

def create_sip_trunk(business_id, name, trunk_id, phone_number=None, number_pool=None, max_concurrent_calls=5, calls_per_minute=10):
    pool = number_pool or []
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sip_trunks
                (business_id, name, trunk_id, phone_number, number_pool, max_concurrent_calls, calls_per_minute)
                VALUES (%s::uuid, %s, %s, %s, %s::jsonb, %s, %s)
                RETURNING *
                """,
                (business_id, name, trunk_id, phone_number, json.dumps(pool), max_concurrent_calls, calls_per_minute),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_sip_trunk failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_sip_trunks(business_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute("SELECT * FROM sip_trunks WHERE business_id=%s::uuid ORDER BY created_at DESC", (business_id,))
            else:
                cur.execute("SELECT * FROM sip_trunks ORDER BY created_at DESC")
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_sip_trunks failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_sip_trunk(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sip_trunks WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_sip_trunk failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def update_sip_trunk(id, **kwargs):
    allowed = {
        "name",
        "phone_number",
        "number_pool",
        "max_concurrent_calls",
        "calls_per_minute",
        "is_active",
        "rotation_strategy",
        "last_used_number_index",
        "calls_per_number_limit",
        "inbound_enabled",
        "inbound_agent_id",
        "inbound_fallback_message",
    }
    set_sql, vals = _update_statement(kwargs, allowed)
    if not set_sql:
        return {}

    if "number_pool" in kwargs and kwargs["number_pool"] is not None:
        # rewrite value with json serialization to keep jsonb type stable
        keys = [k for k in kwargs.keys() if k in allowed and kwargs[k] is not None]
        vals = []
        set_parts = []
        for k in keys:
            if k == "number_pool":
                set_parts.append("number_pool=%s::jsonb")
                vals.append(json.dumps(kwargs[k]))
            else:
                set_parts.append(f"{k}=%s")
                vals.append(kwargs[k])
        set_sql = ", ".join(set_parts)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE sip_trunks SET {set_sql} WHERE id=%s::uuid RETURNING *", vals + [id])
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("update_sip_trunk failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_sip_trunk(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM sip_trunks WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_sip_trunk failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def pick_mask_number(trunk_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            # trunk_id here is expected to be table UUID id
            try:
                cur.execute("SELECT number_pool, phone_number FROM sip_trunks WHERE id=%s::uuid LIMIT 1", (trunk_id,))
                row = cur.fetchone()
            except Exception:
                row = None
            if not row:
                cur.execute("SELECT number_pool, phone_number FROM sip_trunks WHERE trunk_id=%s LIMIT 1", (trunk_id,))
                row = cur.fetchone()

            if not row:
                return None
            pool = row.get("number_pool") or []
            if isinstance(pool, str):
                try:
                    pool = json.loads(pool)
                except Exception:
                    pool = []
            if isinstance(pool, list) and len(pool) > 0:
                return random.choice(pool)
            return row.get("phone_number")
    except Exception as e:
        logger.exception("pick_mask_number failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


# Agents

def create_agent(**kwargs):
    defaults = {
        "business_id": None,
        "name": "Agent",
        "subtitle": None,
        "stt_provider": "sarvam",
        "stt_language": "hi-IN",
        "stt_model": "saaras:v3",
        "llm_provider": "openai",
        "llm_model": "gpt-4.1-mini",
        "llm_base_url": None,
        "llm_temperature": 0.7,
        "llm_max_tokens": 60,
        "tts_provider": "sarvam",
        "tts_voice": "rohan",
        "tts_language": "hi-IN",
        "system_prompt": "",
        "first_line": "",
        "agent_instructions": "",
        "max_turns": 25,
        "silence_threshold_seconds": 20,
        "max_nudges": 2,
        "inbound_speaks_first": True,
        "consent_disclosure": None,
        "consent_disclosure_language": "hi-IN",
        "voicemail_message": None,
        "handoff_enabled": False,
        "handoff_sip_address": None,
        "handoff_trigger_phrases": [
            "speak to human",
            "talk to agent",
            "transfer me",
            "real person",
            "manager",
            "supervisor",
        ],
        "dtmf_menu": None,
        "auto_detect_language": False,
        "supported_languages": ["hi-IN", "en-IN", "ta-IN", "te-IN", "kn-IN", "mr-IN"],
        "llm_fallback_provider": None,
        "llm_fallback_model": None,
        "memory_enabled": True,
        "memory_lookback_calls": 3,
        "persona": "professional",
        "sms_followup_enabled": False,
        "sms_template_interested": None,
        "sms_template_booked": None,
        "email_followup_enabled": False,
        "is_active": False,
    }
    defaults.update({k: v for k, v in kwargs.items() if v is not None})

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agents
                (business_id, name, subtitle, stt_provider, stt_language, stt_model,
                 llm_provider, llm_model, llm_base_url, llm_temperature, llm_max_tokens,
                 tts_provider, tts_voice, tts_language,
                 system_prompt, first_line, agent_instructions,
                 max_turns, silence_threshold_seconds, max_nudges, inbound_speaks_first,
                 consent_disclosure, consent_disclosure_language,
                 voicemail_message, handoff_enabled, handoff_sip_address, handoff_trigger_phrases,
                 dtmf_menu, auto_detect_language, supported_languages,
                 llm_fallback_provider, llm_fallback_model, memory_enabled, memory_lookback_calls,
                 persona, sms_followup_enabled, sms_template_interested, sms_template_booked,
                 email_followup_enabled, is_active)
                VALUES
                (%s::uuid, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s::text[], %s::jsonb, %s, %s::text[],
                 %s, %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s)
                RETURNING *
                """,
                (
                    defaults["business_id"],
                    defaults["name"],
                    defaults["subtitle"],
                    defaults["stt_provider"],
                    defaults["stt_language"],
                    defaults["stt_model"],
                    defaults["llm_provider"],
                    defaults["llm_model"],
                    defaults["llm_base_url"],
                    defaults["llm_temperature"],
                    defaults["llm_max_tokens"],
                    defaults["tts_provider"],
                    defaults["tts_voice"],
                    defaults["tts_language"],
                    defaults["system_prompt"],
                    defaults["first_line"],
                    defaults["agent_instructions"],
                    defaults["max_turns"],
                    defaults["silence_threshold_seconds"],
                    defaults["max_nudges"],
                    defaults["inbound_speaks_first"],
                    defaults["consent_disclosure"],
                    defaults["consent_disclosure_language"],
                    defaults["voicemail_message"],
                    defaults["handoff_enabled"],
                    defaults["handoff_sip_address"],
                    list(defaults["handoff_trigger_phrases"] or []),
                    json.dumps(defaults["dtmf_menu"]) if defaults.get("dtmf_menu") is not None else None,
                    defaults["auto_detect_language"],
                    list(defaults["supported_languages"] or []),
                    defaults["llm_fallback_provider"],
                    defaults["llm_fallback_model"],
                    defaults["memory_enabled"],
                    defaults["memory_lookback_calls"],
                    defaults["persona"],
                    defaults["sms_followup_enabled"],
                    defaults["sms_template_interested"],
                    defaults["sms_template_booked"],
                    defaults["email_followup_enabled"],
                    defaults["is_active"],
                ),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_agent failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_agents(business_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute("SELECT * FROM agents WHERE business_id=%s::uuid ORDER BY created_at DESC", (business_id,))
            else:
                cur.execute("SELECT * FROM agents ORDER BY created_at DESC")
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_agents failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_agent(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM agents WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_agent failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def get_active_agent(business_id: str | None = None) -> dict | None:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if business_id:
                cur.execute(
                    "SELECT * FROM agents WHERE is_active = true AND business_id = %s LIMIT 1",
                    (business_id,),
                )
            else:
                cur.execute("SELECT * FROM agents WHERE is_active = true LIMIT 1")
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as e:
        logger.error(f"get_active_agent error: {e}")
        return None
    finally:
        release_conn(conn)


def update_agent(id, **kwargs):
    if "dtmf_menu" in kwargs and kwargs.get("dtmf_menu") is not None and not isinstance(kwargs.get("dtmf_menu"), str):
        kwargs["dtmf_menu"] = json.dumps(kwargs.get("dtmf_menu"))
    allowed = {
        "name", "subtitle", "stt_provider", "stt_language", "stt_model",
        "llm_provider", "llm_model", "llm_base_url", "llm_temperature", "llm_max_tokens",
        "tts_provider", "tts_voice", "tts_language", "system_prompt", "first_line",
        "agent_instructions", "max_turns", "silence_threshold_seconds", "max_nudges", "is_active",
        "inbound_speaks_first", "consent_disclosure", "consent_disclosure_language",
        "voicemail_message", "handoff_enabled", "handoff_sip_address", "handoff_trigger_phrases",
        "dtmf_menu", "auto_detect_language", "supported_languages",
        "llm_fallback_provider", "llm_fallback_model", "memory_enabled", "memory_lookback_calls",
        "persona", "sms_followup_enabled", "sms_template_interested", "sms_template_booked",
        "email_followup_enabled",
    }
    set_sql, vals = _update_statement(kwargs, allowed)
    if not set_sql:
        return {}

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE agents SET {set_sql} WHERE id=%s::uuid RETURNING *", vals + [id])
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("update_agent failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_agent(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM agents WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_agent failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def set_active_agent(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT business_id FROM agents WHERE id=%s::uuid", (id,))
            row = cur.fetchone()
            if not row:
                return False
            business_id = row["business_id"]
            cur.execute("UPDATE agents SET is_active=false WHERE business_id=%s::uuid", (business_id,))
            cur.execute("UPDATE agents SET is_active=true WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("set_active_agent failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# Campaigns

def create_campaign(**kwargs):
    defaults = {
        "business_id": None,
        "agent_id": None,
        "sip_trunk_id": None,
        "name": "Campaign",
        "description": None,
        "objective": None,
        "status": "draft",
        "calls_per_minute": 3,
        "max_concurrent_calls": 5,
        "retry_failed": True,
        "max_retries": 2,
        "retry_delay_minutes": 60,
        "call_window_start": "09:00",
        "call_window_end": "20:00",
        "timezone": "Asia/Kolkata",
        "custom_script": None,
        "skip_sundays": True,
        "retry_strategy": None,
        "budget_cap_usd": None,
        "requires_approval": False,
        "approval_status": "not_required",
        "approved_by": None,
        "approval_notes": None,
        "approved_at": None,
        "paused_at": None,
        "pause_reason": None,
        "calls_made_before_pause": 0,
        "scheduled_start_at": None,
        "started_at": None,
        "completed_at": None,
    }
    defaults.update({k: v for k, v in kwargs.items() if v is not None})

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaigns
                (business_id, agent_id, sip_trunk_id, name, description, objective,
                 status, calls_per_minute, max_concurrent_calls, retry_failed,
                 max_retries, retry_delay_minutes, call_window_start, call_window_end,
                 timezone, custom_script, skip_sundays, retry_strategy,
                 budget_cap_usd, requires_approval, approval_status, approved_by,
                 approval_notes, approved_at, paused_at, pause_reason, calls_made_before_pause,
                 scheduled_start_at, started_at, completed_at)
                VALUES
                (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s, %s::jsonb,
                 %s, %s, %s, %s, %s, %s, %s, %s, %s,
                 %s, %s, %s)
                RETURNING *
                """,
                (
                    defaults["business_id"], defaults["agent_id"], defaults["sip_trunk_id"],
                    defaults["name"], defaults["description"], defaults["objective"],
                    defaults["status"], defaults["calls_per_minute"], defaults["max_concurrent_calls"], defaults["retry_failed"],
                    defaults["max_retries"], defaults["retry_delay_minutes"], defaults["call_window_start"], defaults["call_window_end"],
                    defaults["timezone"], defaults["custom_script"], defaults["skip_sundays"],
                    json.dumps(defaults["retry_strategy"]) if defaults["retry_strategy"] else None,
                    defaults["budget_cap_usd"],
                    defaults["requires_approval"],
                    defaults["approval_status"],
                    defaults["approved_by"],
                    defaults["approval_notes"],
                    defaults["approved_at"],
                    defaults["paused_at"],
                    defaults["pause_reason"],
                    defaults["calls_made_before_pause"],
                    defaults["scheduled_start_at"], defaults["started_at"], defaults["completed_at"],
                ),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_campaign failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaigns(business_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute("SELECT * FROM campaigns WHERE business_id=%s::uuid ORDER BY created_at DESC", (business_id,))
            else:
                cur.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_campaigns failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM campaigns WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_campaign failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def update_campaign(id, **kwargs):
    allowed = {
        "name", "description", "objective", "agent_id", "sip_trunk_id", "status",
        "calls_per_minute", "max_concurrent_calls", "retry_failed", "max_retries",
        "retry_delay_minutes", "call_window_start", "call_window_end", "timezone",
        "custom_script", "skip_sundays", "retry_strategy",
        "budget_cap_usd", "requires_approval", "approval_status", "approved_by", "approval_notes", "approved_at",
        "paused_at", "pause_reason", "calls_made_before_pause",
        "scheduled_start_at", "started_at", "completed_at",
    }
    update_data = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not update_data:
        return {}

    cols = []
    vals = []
    for k, v in update_data.items():
        if isinstance(v, str) and v.lower() == "now()":
            cols.append(f"{k}=NOW()")
        elif k == "retry_strategy":
            cols.append("retry_strategy=%s::jsonb")
            vals.append(json.dumps(v))
        else:
            cols.append(f"{k}=%s")
            vals.append(v)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE campaigns SET {', '.join(cols)} WHERE id=%s::uuid RETURNING *", vals + [id])
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("update_campaign failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaigns_due_to_start():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM campaigns
                WHERE status='scheduled'
                  AND scheduled_start_at IS NOT NULL
                  AND scheduled_start_at <= NOW()
                ORDER BY scheduled_start_at ASC
                """
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_campaigns_due_to_start failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaigns_active():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM campaigns WHERE status='active' ORDER BY started_at DESC NULLS LAST, created_at DESC")
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_campaigns_active failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_retry_config(campaign_id, disposition):
    default_cfg = {"delay_minutes": 60, "max_attempts": 2}
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT retry_strategy FROM campaigns WHERE id=%s::uuid LIMIT 1", (campaign_id,))
            row = cur.fetchone()
            if not row:
                return default_cfg
            strategy = row.get("retry_strategy") or {}
            if isinstance(strategy, str):
                try:
                    strategy = json.loads(strategy)
                except Exception:
                    strategy = {}
            return strategy.get(disposition) or strategy.get("failed") or default_cfg
    except Exception as e:
        logger.exception("get_retry_config failed: %s", e)
        return default_cfg
    finally:
        if conn is not None:
            release_conn(conn)


def delete_campaign(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM campaigns WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_campaign failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign_stats(campaign_id):
    empty = {
        "total": 0,
        "pending": 0,
        "calling": 0,
        "completed": 0,
        "failed": 0,
        "no_answer": 0,
        "dnc": 0,
        "scheduled": 0,
        "booked_count": 0,
        "avg_duration_seconds": 0,
        "total_cost_usd": 0,
    }
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH lead_counts AS (
                    SELECT
                      COUNT(*)::int AS total,
                      COUNT(*) FILTER (WHERE status='pending')::int AS pending,
                      COUNT(*) FILTER (WHERE status='calling')::int AS calling,
                      COUNT(*) FILTER (WHERE status='completed')::int AS completed,
                      COUNT(*) FILTER (WHERE status='failed')::int AS failed,
                      COUNT(*) FILTER (WHERE status='no_answer')::int AS no_answer,
                      COUNT(*) FILTER (WHERE status='dnc')::int AS dnc,
                      COUNT(*) FILTER (WHERE status='scheduled')::int AS scheduled
                    FROM leads
                    WHERE campaign_id=%s::uuid
                ),
                call_metrics AS (
                    SELECT
                      COUNT(*) FILTER (WHERE was_booked=true)::int AS booked_count,
                      COALESCE(AVG(duration_seconds),0)::int AS avg_duration_seconds,
                      COALESCE(SUM(estimated_cost_usd),0)::numeric(10,4) AS total_cost_usd
                    FROM calls
                    WHERE campaign_id=%s::uuid
                )
                SELECT * FROM lead_counts CROSS JOIN call_metrics
                """,
                (campaign_id, campaign_id),
            )
            row = _dict(cur.fetchone())
            return {**empty, **(row or {})}
    except Exception as e:
        logger.exception("get_campaign_stats failed: %s", e)
        return empty
    finally:
        if conn is not None:
            release_conn(conn)


def set_campaign_variants(campaign_id, variants):
    conn = None
    try:
        total_weight = sum(int(v.get("weight") or 0) for v in variants or [])
        if total_weight != 100:
            logger.error("set_campaign_variants failed: weights must sum to 100")
            return False
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM campaign_agent_variants WHERE campaign_id=%s::uuid", (campaign_id,))
            values = [(campaign_id, v["agent_id"], int(v["weight"])) for v in variants]
            execute_values(
                cur,
                """
                INSERT INTO campaign_agent_variants (campaign_id, agent_id, weight)
                VALUES %s
                """,
                values,
                template="(%s::uuid,%s::uuid,%s)",
            )
            return True
    except Exception as e:
        logger.exception("set_campaign_variants failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign_variants(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cav.*, a.name AS agent_name
                FROM campaign_agent_variants cav
                LEFT JOIN agents a ON a.id = cav.agent_id
                WHERE cav.campaign_id=%s::uuid
                ORDER BY cav.created_at ASC
                """,
                (campaign_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_campaign_variants failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def pick_variant_agent(campaign_id):
    conn = None
    try:
        variants = get_campaign_variants(campaign_id)
        if variants:
            total = sum(max(int(v.get("weight") or 0), 0) for v in variants)
            if total > 0:
                pick = random.randint(1, total)
                running = 0
                for variant in variants:
                    running += max(int(variant.get("weight") or 0), 0)
                    if pick <= running:
                        return str(variant["agent_id"])
        campaign = get_campaign(campaign_id)
        if campaign and campaign.get("agent_id"):
            return str(campaign["agent_id"])
        return ""
    except Exception as e:
        logger.exception("pick_variant_agent failed: %s", e)
        return ""
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign_variant_stats(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.agent_id,
                    COALESCE(a.name, 'Unknown') AS agent_name,
                    COUNT(*)::int AS total_calls,
                    COUNT(*) FILTER (WHERE c.was_booked = true)::int AS booked_count,
                    COALESCE(AVG(c.duration_seconds), 0)::int AS avg_duration,
                    CASE WHEN COUNT(*) = 0 THEN 0
                         ELSE ROUND((COUNT(*) FILTER (WHERE c.was_booked = true)::numeric / COUNT(*)::numeric) * 100, 2)
                    END AS conversion_rate
                FROM calls c
                LEFT JOIN agents a ON a.id = c.agent_id
                WHERE c.campaign_id=%s::uuid
                GROUP BY c.agent_id, a.name
                ORDER BY total_calls DESC
                """,
                (campaign_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_campaign_variant_stats failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_hourly_connection_rates(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  EXTRACT(HOUR FROM created_at AT TIME ZONE 'Asia/Kolkata')::int AS hour,
                  COUNT(*)::int AS total_calls,
                  COUNT(*) FILTER (WHERE status='completed')::int AS connected,
                  ROUND(
                    (COUNT(*) FILTER (WHERE status='completed')::numeric / NULLIF(COUNT(*), 0)) * 100,
                    2
                  ) AS connection_rate
                FROM calls
                WHERE campaign_id=%s::uuid
                GROUP BY 1
                ORDER BY hour ASC
                """,
                (campaign_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_hourly_connection_rates failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def create_campaign_template(business_id, name, description, config):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaign_templates (business_id, name, description, config)
                VALUES (%s::uuid, %s, %s, %s::jsonb)
                RETURNING *
                """,
                (business_id, name, description, json.dumps(config or {})),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_campaign_template failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign_templates(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM campaign_templates WHERE business_id=%s::uuid ORDER BY created_at DESC",
                (business_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_campaign_templates failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def delete_campaign_template(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM campaign_templates WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_campaign_template failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def clone_campaign(campaign_id, new_name, include_leads=False):
    conn = None
    try:
        source = get_campaign(campaign_id)
        if not source:
            return {}
        payload = {
            "business_id": source.get("business_id"),
            "agent_id": source.get("agent_id"),
            "sip_trunk_id": source.get("sip_trunk_id"),
            "name": new_name,
            "description": source.get("description"),
            "objective": source.get("objective"),
            "status": "draft",
            "calls_per_minute": source.get("calls_per_minute"),
            "max_concurrent_calls": source.get("max_concurrent_calls"),
            "retry_failed": source.get("retry_failed"),
            "max_retries": source.get("max_retries"),
            "retry_delay_minutes": source.get("retry_delay_minutes"),
            "call_window_start": source.get("call_window_start"),
            "call_window_end": source.get("call_window_end"),
            "timezone": source.get("timezone"),
            "custom_script": source.get("custom_script"),
            "skip_sundays": source.get("skip_sundays", True),
            "retry_strategy": source.get("retry_strategy"),
            "budget_cap_usd": source.get("budget_cap_usd"),
            "requires_approval": source.get("requires_approval", False),
            "approval_status": "not_required",
            "calls_made_before_pause": 0,
        }
        cloned = create_campaign(**payload)
        if not cloned:
            return {}

        if include_leads:
            source_leads = get_leads(campaign_id, status=None, limit=50000, offset=0)
            insert_rows = []
            for lead in source_leads:
                if str(lead.get("status") or "") not in {"pending", "scheduled"}:
                    continue
                insert_rows.append(
                    {
                        "phone": lead.get("phone"),
                        "name": lead.get("name"),
                        "email": lead.get("email"),
                        "language": lead.get("language") or "hi-IN",
                        "custom_data": lead.get("custom_data") or {},
                    }
                )
            if insert_rows:
                bulk_create_leads(str(cloned["id"]), str(cloned["business_id"]), insert_rows)

        return cloned
    except Exception as e:
        logger.exception("clone_campaign failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign_spend(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(estimated_cost_usd), 0)::numeric(12,4) AS total
                FROM calls
                WHERE campaign_id=%s::uuid
                  AND status != 'initiated'
                """,
                (campaign_id,),
            )
            row = _dict(cur.fetchone()) or {}
            return float(row.get("total") or 0.0)
    except Exception as e:
        logger.exception("get_campaign_spend failed: %s", e)
        return 0.0
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign_call_count(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*)::int AS c FROM calls WHERE campaign_id=%s::uuid", (campaign_id,))
            return int((_dict(cur.fetchone()) or {}).get("c") or 0)
    except Exception as e:
        logger.exception("get_campaign_call_count failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def reset_campaign_calling_leads(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE leads SET status='pending' WHERE campaign_id=%s::uuid AND status='calling'",
                (campaign_id,),
            )
            return cur.rowcount
    except Exception as e:
        logger.exception("reset_campaign_calling_leads failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def approve_campaign(id, approved_by, notes):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaigns
                SET approval_status='approved',
                    approved_by=%s,
                    approval_notes=%s,
                    approved_at=NOW(),
                    status=CASE WHEN status='pending_approval' THEN 'active' ELSE status END
                WHERE id=%s::uuid
                RETURNING *
                """,
                (approved_by, notes, id),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("approve_campaign failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def reject_campaign(id, approved_by, notes):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaigns
                SET approval_status='rejected',
                    approved_by=%s,
                    approval_notes=%s,
                    approved_at=NOW(),
                    status='paused'
                WHERE id=%s::uuid
                RETURNING *
                """,
                (approved_by, notes, id),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("reject_campaign failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_pending_approval_campaigns(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM campaigns
                WHERE business_id=%s::uuid
                  AND approval_status='pending'
                ORDER BY created_at DESC
                """,
                (business_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_pending_approval_campaigns failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def create_sequence(business_id, name):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaign_sequences (business_id, name)
                VALUES (%s::uuid, %s)
                RETURNING *
                """,
                (business_id, name),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("create_sequence failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def add_sequence_step(sequence_id, campaign_id, step_order, trigger="previous_complete", delay_days=0, filter_disposition=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaign_sequence_steps
                (sequence_id, campaign_id, step_order, trigger, delay_days, filter_disposition)
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s)
                RETURNING *
                """,
                (sequence_id, campaign_id, int(step_order), trigger, int(delay_days or 0), filter_disposition),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("add_sequence_step failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_sequence_steps(sequence_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT css.*, c.name AS campaign_name
                FROM campaign_sequence_steps css
                LEFT JOIN campaigns c ON c.id = css.campaign_id
                WHERE css.sequence_id=%s::uuid
                ORDER BY css.step_order ASC
                """,
                (sequence_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_sequence_steps failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_campaign_sequence(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  cs.id AS sequence_id,
                  cs.business_id,
                  cs.name AS sequence_name,
                  css.id AS step_id,
                  css.step_order,
                  css.trigger,
                  css.delay_days,
                  css.filter_disposition
                FROM campaign_sequence_steps css
                JOIN campaign_sequences cs ON cs.id = css.sequence_id
                WHERE css.campaign_id=%s::uuid
                LIMIT 1
                """,
                (campaign_id,),
            )
            row = cur.fetchone()
            return _dict(row) if row else None
    except Exception as e:
        logger.exception("get_campaign_sequence failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


# Leads

def create_lead(**kwargs):
    defaults = {
        "campaign_id": None,
        "business_id": None,
        "phone": "",
        "name": None,
        "email": None,
        "language": "hi-IN",
        "custom_data": {},
        "status": "pending",
        "call_attempts": 0,
        "max_call_attempts": 3,
        "last_call_at": None,
        "next_call_at": None,
        "scheduled_script_id": None,
        "notes": None,
        "score": 50,
        "score_factors": {},
        "score_updated_at": None,
        "temperature": "warm",
        "tags": [],
        "country_code": None,
        "phone_e164": None,
        "email_verified": None,
    }
    defaults.update({k: v for k, v in kwargs.items() if v is not None})
    phone = _clean_phone(defaults["phone"])
    if not phone:
        logger.error("create_lead failed: phone required")
        return {}

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO leads
                (campaign_id, business_id, phone, name, email, language, custom_data, status,
                 call_attempts, max_call_attempts, last_call_at, next_call_at, scheduled_script_id, notes,
                 score, score_factors, score_updated_at, temperature, tags, country_code, phone_e164, email_verified)
                VALUES
                (%s::uuid, %s::uuid, %s, %s, %s, %s, %s::jsonb, %s,
                 %s, %s, %s, %s, %s::uuid, %s,
                 %s, %s::jsonb, %s, %s, %s::text[], %s, %s, %s)
                RETURNING *
                """,
                (
                    defaults["campaign_id"], defaults["business_id"], phone,
                    defaults["name"], defaults["email"], defaults["language"],
                    json.dumps(defaults["custom_data"] or {}), defaults["status"],
                    defaults["call_attempts"], defaults["max_call_attempts"],
                    defaults["last_call_at"], defaults["next_call_at"],
                    defaults["scheduled_script_id"], defaults["notes"],
                    defaults["score"], json.dumps(defaults["score_factors"] or {}),
                    defaults["score_updated_at"], defaults["temperature"],
                    list(defaults["tags"] or []), defaults["country_code"],
                    defaults["phone_e164"], defaults["email_verified"],
                ),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_lead failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def bulk_create_leads(campaign_id, business_id, leads):
    if not leads:
        return 0

    deduped = {}
    for lead in leads:
        phone = _clean_phone(str(lead.get("phone", "")))
        if not phone:
            continue
        if phone in deduped:
            continue
        custom_data = lead.get("custom_data")
        if isinstance(custom_data, str):
            try:
                custom_data = json.loads(custom_data)
            except Exception:
                custom_data = {"raw": custom_data}
        deduped[phone] = {
            "campaign_id": campaign_id,
            "business_id": business_id,
            "phone": phone,
            "name": lead.get("name"),
            "email": lead.get("email"),
            "language": lead.get("language") or "hi-IN",
            "custom_data": custom_data or {},
            "status": lead.get("status") or "pending",
            "call_attempts": int(lead.get("call_attempts") or 0),
            "max_call_attempts": int(lead.get("max_call_attempts") or 3),
            "last_call_at": lead.get("last_call_at"),
            "next_call_at": lead.get("next_call_at"),
            "scheduled_script_id": lead.get("scheduled_script_id"),
            "notes": lead.get("notes"),
            "score": int(lead.get("score") or 50),
            "score_factors": lead.get("score_factors") or {},
            "score_updated_at": lead.get("score_updated_at"),
            "temperature": lead.get("temperature") or "warm",
            "tags": list(lead.get("tags") or []),
            "country_code": lead.get("country_code"),
            "phone_e164": lead.get("phone_e164"),
            "email_verified": lead.get("email_verified"),
        }

    rows = list(deduped.values())
    if not rows:
        return 0

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT phone FROM leads WHERE campaign_id=%s::uuid AND phone = ANY(%s)",
                (campaign_id, list(deduped.keys())),
            )
            existing = {r["phone"] for r in cur.fetchall()}

            to_insert = [r for r in rows if r["phone"] not in existing]
            if not to_insert:
                return 0

            values = [
                (
                    r["campaign_id"], r["business_id"], r["phone"], r["name"], r["email"], r["language"],
                    json.dumps(r["custom_data"]), r["status"], r["call_attempts"], r["max_call_attempts"],
                    r["last_call_at"], r["next_call_at"], r["scheduled_script_id"], r["notes"],
                    r["score"], json.dumps(r["score_factors"]), r["score_updated_at"], r["temperature"],
                    list(r["tags"]), r["country_code"], r["phone_e164"], r["email_verified"],
                )
                for r in to_insert
            ]

            execute_values(
                cur,
                """
                INSERT INTO leads
                (campaign_id, business_id, phone, name, email, language,
                 custom_data, status, call_attempts, max_call_attempts,
                 last_call_at, next_call_at, scheduled_script_id, notes,
                 score, score_factors, score_updated_at, temperature, tags,
                 country_code, phone_e164, email_verified)
                VALUES %s
                """,
                values,
                template="(%s::uuid,%s::uuid,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s::uuid,%s,%s,%s::jsonb,%s,%s,%s::text[],%s,%s,%s)",
            )
            return len(to_insert)
    except Exception as e:
        logger.exception("bulk_create_leads failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def get_leads(campaign_id, status=None, limit=100, offset=0):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if status:
                cur.execute(
                    """
                    SELECT * FROM leads
                    WHERE campaign_id=%s::uuid AND status=%s
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (campaign_id, status, limit, offset),
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM leads
                    WHERE campaign_id=%s::uuid
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (campaign_id, limit, offset),
                )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_leads failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_lead(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM leads WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_lead failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def update_lead(id, **kwargs):
    allowed = {
        "phone", "name", "email", "language", "custom_data", "status", "call_attempts",
        "max_call_attempts", "last_call_at", "next_call_at", "scheduled_script_id", "notes",
        "score", "score_factors", "score_updated_at", "temperature", "tags",
        "country_code", "phone_e164", "email_verified",
    }
    update_data = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not update_data:
        return False

    parts = []
    vals = []
    for k, v in update_data.items():
        if k == "custom_data":
            parts.append("custom_data=%s::jsonb")
            vals.append(json.dumps(v))
        elif k == "score_factors":
            parts.append("score_factors=%s::jsonb")
            vals.append(json.dumps(v or {}))
        elif k == "tags":
            parts.append("tags=%s::text[]")
            vals.append(list(v or []))
        elif k == "scheduled_script_id":
            parts.append("scheduled_script_id=%s::uuid")
            vals.append(v)
        else:
            parts.append(f"{k}=%s")
            vals.append(v)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE leads SET {', '.join(parts)} WHERE id=%s::uuid", vals + [id])
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_lead failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def update_lead_status(id, status):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE leads SET status=%s WHERE id=%s::uuid", (status, id))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_lead_status failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_next_pending_leads(campaign_id, limit=10):
    """
    Fetch next pending leads and atomically mark them as 'calling'
    to prevent double-dialing on Supabase transaction pooler.

    Uses UPDATE ... RETURNING instead of FOR UPDATE SKIP LOCKED
    because Supabase's PgBouncer transaction pooler (port 6543)
    drops session-level locks between statements.
    """
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH candidates AS (
                    SELECT id FROM leads
                    WHERE campaign_id = %s::uuid
                      AND status = 'pending'
                      AND (next_call_at IS NULL OR next_call_at <= NOW())
                    ORDER BY score DESC NULLS LAST, created_at ASC
                    LIMIT %s
                )
                UPDATE leads
                SET status = 'calling',
                    last_call_at = NOW(),
                    call_attempts = call_attempts + 1
                WHERE id IN (SELECT id FROM candidates)
                RETURNING *
                """,
                (campaign_id, limit),
            )
            rows = cur.fetchall()
            return _list(rows)
    except Exception as e:
        logger.exception("get_next_pending_leads failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def reschedule_lead(lead_id, next_call_at, script_id=None, reason=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE leads
                SET status='scheduled', next_call_at=%s::timestamptz,
                    scheduled_script_id=%s::uuid,
                    notes=COALESCE(notes,'') || CASE WHEN %s IS NULL THEN '' ELSE ('\n[rescheduled] ' || %s) END
                WHERE id=%s::uuid
                """,
                (next_call_at, script_id, reason, reason, lead_id),
            )
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("reschedule_lead failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# DNC Registry

def add_to_dnc(business_id, phone, reason=None, added_by="system"):
    conn = None
    try:
        clean_phone = _clean_phone(phone)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dnc_list (business_id, phone, reason, added_by)
                VALUES (%s::uuid, %s, %s, %s)
                ON CONFLICT (business_id, phone)
                DO UPDATE SET reason=EXCLUDED.reason, added_by=EXCLUDED.added_by
                RETURNING *
                """,
                (business_id, clean_phone, reason, added_by),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("add_to_dnc failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def remove_from_dnc(business_id, phone):
    conn = None
    try:
        clean_phone = _clean_phone(phone)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM dnc_list WHERE business_id=%s::uuid AND phone=%s", (business_id, clean_phone))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("remove_from_dnc failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def is_dnc(business_id, phone):
    conn = None
    try:
        clean_phone = _clean_phone(phone)
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM dnc_list WHERE business_id=%s::uuid AND phone=%s LIMIT 1",
                (business_id, clean_phone),
            )
            return cur.fetchone() is not None
    except Exception as e:
        logger.exception("is_dnc failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def bulk_add_dnc(business_id, phones, reason=None):
    conn = None
    try:
        clean_phones = []
        seen = set()
        for p in phones or []:
            cp = _clean_phone(p)
            if cp and cp not in seen:
                clean_phones.append(cp)
                seen.add(cp)
        if not clean_phones:
            return 0

        conn = get_conn()
        with conn, conn.cursor() as cur:
            values = [(business_id, p, reason, "bulk_upload") for p in clean_phones]
            execute_values(
                cur,
                """
                INSERT INTO dnc_list (business_id, phone, reason, added_by)
                VALUES %s
                ON CONFLICT (business_id, phone) DO NOTHING
                """,
                values,
                template="(%s::uuid,%s,%s,%s)",
            )
            return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
    except Exception as e:
        logger.exception("bulk_add_dnc failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def get_dnc_list(business_id, limit=100, offset=0):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM dnc_list
                WHERE business_id=%s::uuid
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (business_id, limit, offset),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_dnc_list failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


# Call Scripts

def create_script(business_id, name, system_prompt, first_line):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO call_scripts (business_id, name, system_prompt, first_line)
                VALUES (%s::uuid, %s, %s, %s)
                RETURNING *
                """,
                (business_id, name, system_prompt, first_line),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_script failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_scripts(business_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute("SELECT * FROM call_scripts WHERE business_id=%s::uuid ORDER BY created_at DESC", (business_id,))
            else:
                cur.execute("SELECT * FROM call_scripts ORDER BY created_at DESC")
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_scripts failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_script(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM call_scripts WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_script failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def update_script(id, **kwargs):
    allowed = {"name", "system_prompt", "first_line"}
    set_sql, vals = _update_statement(kwargs, allowed)
    if not set_sql:
        return {}

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE call_scripts SET {set_sql} WHERE id=%s::uuid RETURNING *", vals + [id])
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("update_script failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_script(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM call_scripts WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_script failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# Calls

def create_call(
    lead_id,
    campaign_id,
    agent_id,
    sip_trunk_id,
    phone,
    room_id,
    business_id=None,
    call_attempt_number=1,
    call_direction="outbound",
):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO calls
                (lead_id, campaign_id, agent_id, sip_trunk_id, business_id,
                 phone, room_id, call_attempt_number, status, call_date, call_hour, call_direction)
                VALUES
                (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                 %s, %s, %s, 'initiated', %s, %s, %s)
                RETURNING *
                """,
                (
                    lead_id, campaign_id, agent_id, sip_trunk_id, business_id,
                    _clean_phone(phone), room_id, call_attempt_number,
                    now.date().isoformat(), now.hour, call_direction,
                ),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_call failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def update_call(room_id, **kwargs):
    allowed = {
        "status", "duration_seconds", "transcript", "summary", "sentiment", "disposition",
        "recording_url", "was_booked", "interrupt_count", "estimated_cost_usd", "whatsapp_sent",
        "call_date", "call_hour", "call_direction", "consent_disclosed",
        "was_voicemail", "amd_result", "supervisor_joined", "supervisor_mode",
        "detected_language", "quality_score",
        "lead_id", "campaign_id", "agent_id", "sip_trunk_id", "business_id",
    }
    update_data = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not update_data:
        return False

    parts = []
    vals = []
    for k, v in update_data.items():
        if k in {"lead_id", "campaign_id", "agent_id", "sip_trunk_id", "business_id"}:
            parts.append(f"{k}=%s::uuid")
            vals.append(v)
        else:
            parts.append(f"{k}=%s")
            vals.append(v)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE calls SET {', '.join(parts)} WHERE room_id=%s", vals + [room_id])
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_call failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_calls(campaign_id=None, business_id=None, limit=50, offset=0):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if campaign_id and business_id:
                cur.execute(
                    """
                    SELECT * FROM calls
                    WHERE campaign_id=%s::uuid AND business_id=%s::uuid
                    ORDER BY created_at DESC
                    LIMIT %s OFFSET %s
                    """,
                    (campaign_id, business_id, limit, offset),
                )
            elif campaign_id:
                cur.execute(
                    "SELECT * FROM calls WHERE campaign_id=%s::uuid ORDER BY created_at DESC LIMIT %s OFFSET %s",
                    (campaign_id, limit, offset),
                )
            elif business_id:
                cur.execute(
                    "SELECT * FROM calls WHERE business_id=%s::uuid ORDER BY created_at DESC LIMIT %s OFFSET %s",
                    (business_id, limit, offset),
                )
            else:
                cur.execute("SELECT * FROM calls ORDER BY created_at DESC LIMIT %s OFFSET %s", (limit, offset))
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_calls failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_call_by_room(room_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM calls WHERE room_id=%s LIMIT 1", (room_id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_call_by_room failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def get_call(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM calls WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_call failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def get_call_history_for_lead(lead_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM calls WHERE lead_id=%s::uuid ORDER BY created_at DESC", (lead_id,))
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_call_history_for_lead failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


# Transcript lines

def append_transcript_line(room_id, phone, role, content, turn_number):
    conn = None
    try:
        safe_content = content
        if os.environ.get("PII_MASKING_ENABLED", "false").lower() == "true":
            try:
                from privacy.masker import mask_pii

                safe_content = mask_pii(content or "")
            except Exception:
                safe_content = content
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO call_transcript_lines (room_id, phone, role, content, turn_number)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (room_id, _clean_phone(phone or ""), role, safe_content, int(turn_number or 0)),
            )
            return True
    except Exception as e:
        logger.exception("append_transcript_line failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_transcript(room_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM call_transcript_lines
                WHERE room_id=%s
                ORDER BY turn_number ASC, created_at ASC
                """,
                (room_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_transcript failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


# Active calls

def upsert_active_call(room_id, phone, campaign_id, agent_id, business_id, sip_trunk_id, status):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO active_calls
                (room_id, phone, campaign_id, agent_id, business_id, sip_trunk_id, status, started_at, last_updated)
                VALUES
                (%s, %s, %s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, NOW(), NOW())
                ON CONFLICT (room_id)
                DO UPDATE SET
                    phone=EXCLUDED.phone,
                    campaign_id=EXCLUDED.campaign_id,
                    agent_id=EXCLUDED.agent_id,
                    business_id=EXCLUDED.business_id,
                    sip_trunk_id=EXCLUDED.sip_trunk_id,
                    status=EXCLUDED.status,
                    last_updated=NOW()
                """,
                (room_id, _clean_phone(phone), campaign_id, agent_id, business_id, sip_trunk_id, status),
            )
            return True
    except Exception as e:
        logger.exception("upsert_active_call failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def remove_active_call(room_id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM active_calls WHERE room_id=%s", (room_id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("remove_active_call failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_active_calls(business_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute("SELECT * FROM active_calls WHERE business_id=%s::uuid ORDER BY started_at DESC", (business_id,))
            else:
                cur.execute("SELECT * FROM active_calls ORDER BY started_at DESC")
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_active_calls failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def update_active_call_turn(room_id, role, content, live_sentiment=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            if role == "user":
                cur.execute(
                    """
                    UPDATE active_calls
                    SET turn_count = COALESCE(turn_count, 0) + 1,
                        last_user_utterance = %s,
                        live_sentiment = COALESCE(%s, live_sentiment),
                        last_updated = NOW()
                    WHERE room_id = %s
                    """,
                    ((content or "")[:200], live_sentiment, room_id),
                )
            else:
                cur.execute(
                    """
                    UPDATE active_calls
                    SET turn_count = COALESCE(turn_count, 0) + 1,
                        last_updated = NOW()
                    WHERE room_id = %s
                    """,
                    (room_id,),
                )
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_active_call_turn failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_recent_transcript_lines(since_created_at=None, limit=200):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if since_created_at:
                cur.execute(
                    """
                    SELECT id, room_id, role, content, turn_number, created_at
                    FROM call_transcript_lines
                    WHERE created_at > %s::timestamptz
                    ORDER BY created_at ASC
                    LIMIT %s
                    """,
                    (since_created_at, int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT id, room_id, role, content, turn_number, created_at
                    FROM call_transcript_lines
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (int(limit),),
                )
            rows = _list(cur.fetchall())
            if not since_created_at:
                rows.reverse()
            return rows
    except Exception as e:
        logger.exception("get_recent_transcript_lines failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


# Bookings

def create_booking(call_id, business_id, caller_name, caller_phone, caller_email, start_time, notes):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bookings
                (call_id, business_id, caller_name, caller_phone, caller_email, start_time, notes)
                VALUES
                (%s::uuid, %s::uuid, %s, %s, %s, %s::timestamptz, %s)
                RETURNING *
                """,
                (call_id, business_id, caller_name, _clean_phone(caller_phone), caller_email, start_time, notes),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_booking failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_bookings(business_id=None, limit=50):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute(
                    "SELECT * FROM bookings WHERE business_id=%s::uuid ORDER BY created_at DESC LIMIT %s",
                    (business_id, limit),
                )
            else:
                cur.execute("SELECT * FROM bookings ORDER BY created_at DESC LIMIT %s", (limit,))
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_bookings failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


# Scheduled callbacks

def create_callback(lead_id, campaign_id, scheduled_for, script_id=None, reason=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scheduled_callbacks
                (lead_id, campaign_id, script_id, scheduled_for, reason, status)
                VALUES
                (%s::uuid, %s::uuid, %s::uuid, %s::timestamptz, %s, 'pending')
                RETURNING *
                """,
                (lead_id, campaign_id, script_id, scheduled_for, reason),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_callback failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_due_callbacks():
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sc.*, l.phone, l.business_id, l.name AS lead_name
                FROM scheduled_callbacks sc
                JOIN leads l ON l.id = sc.lead_id
                WHERE sc.scheduled_for <= NOW() AND sc.status='pending'
                ORDER BY sc.scheduled_for ASC
                """
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_due_callbacks failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def update_callback(id, status):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE scheduled_callbacks SET status=%s WHERE id=%s::uuid", (status, id))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_callback failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# Knowledge base

def insert_knowledge(business_id, agent_id, category, title, content, embedding):
    conn = None
    try:
        vector = _vector_literal(embedding)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO knowledge_base
                (business_id, agent_id, category, title, content, embedding)
                VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s::vector)
                RETURNING *
                """,
                (business_id, agent_id, category, title, content, vector),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("insert_knowledge failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def search_knowledge(agent_id, query_embedding, limit=3):
    conn = None
    try:
        vector = _vector_literal(query_embedding)
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, business_id, agent_id, category, title, content, created_at,
                       (embedding <-> %s::vector) AS distance
                FROM knowledge_base
                WHERE agent_id=%s::uuid
                ORDER BY embedding <-> %s::vector
                LIMIT %s
                """,
                (vector, agent_id, vector, limit),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("search_knowledge failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_knowledge_items(agent_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, business_id, agent_id, category, title, content, created_at FROM knowledge_base WHERE agent_id=%s::uuid ORDER BY created_at DESC",
                (agent_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_knowledge_items failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def delete_knowledge_item(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM knowledge_base WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_knowledge_item failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# Analytics

def get_campaign_hourly_stats(campaign_id, date):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    EXTRACT(HOUR FROM created_at)::int AS hour,
                    COUNT(*)::int AS calls
                FROM calls
                WHERE campaign_id=%s::uuid
                  AND DATE(created_at)=%s::date
                GROUP BY 1
                ORDER BY 1
                """,
                (campaign_id, date),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_campaign_hourly_stats failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_disposition_breakdown(campaign_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(disposition, 'unknown') AS disposition, COUNT(*)::int AS count
                FROM calls
                WHERE campaign_id=%s::uuid
                GROUP BY 1
                ORDER BY count DESC
                """,
                (campaign_id,),
            )
            rows = _list(cur.fetchall())
            return {r["disposition"]: r["count"] for r in rows}
    except Exception as e:
        logger.exception("get_disposition_breakdown failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_conversion_funnel(campaign_id):
    empty = {
        "total_leads": 0,
        "dialed": 0,
        "connected": 0,
        "interested": 0,
        "booked": 0,
        "conversion_rate_pct": 0.0,
    }
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH lead_totals AS (
                    SELECT COUNT(*)::int AS total_leads
                    FROM leads WHERE campaign_id=%s::uuid
                ),
                call_totals AS (
                    SELECT
                        COUNT(*)::int AS dialed,
                        COUNT(*) FILTER (WHERE status IN ('active','completed'))::int AS connected,
                        COUNT(*) FILTER (WHERE disposition IN ('interested','callback_requested','booked'))::int AS interested,
                        COUNT(*) FILTER (WHERE was_booked=true)::int AS booked
                    FROM calls WHERE campaign_id=%s::uuid
                )
                SELECT * FROM lead_totals CROSS JOIN call_totals
                """,
                (campaign_id, campaign_id),
            )
            row = _dict(cur.fetchone()) or {}
            result = {**empty, **row}
            total_leads = int(result.get("total_leads") or 0)
            booked = int(result.get("booked") or 0)
            result["conversion_rate_pct"] = round((booked / total_leads) * 100, 2) if total_leads else 0.0
            return result
    except Exception as e:
        logger.exception("get_conversion_funnel failed: %s", e)
        return empty
    finally:
        if conn is not None:
            release_conn(conn)


def get_agent_performance(business_id, days=7):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.agent_id,
                    COALESCE(a.name, 'Unknown') AS agent_name,
                    COUNT(*)::int AS total_calls,
                    COALESCE(AVG(c.duration_seconds), 0)::int AS avg_duration,
                    COUNT(*) FILTER (WHERE c.was_booked=true)::int AS booked_count,
                    COUNT(*) FILTER (WHERE c.sentiment='positive')::int AS positive_count,
                    COUNT(*) FILTER (WHERE c.sentiment='neutral')::int AS neutral_count,
                    COUNT(*) FILTER (WHERE c.sentiment='negative')::int AS negative_count
                FROM calls c
                LEFT JOIN agents a ON a.id = c.agent_id
                WHERE c.business_id=%s::uuid
                  AND c.created_at >= NOW() - (%s || ' days')::interval
                GROUP BY c.agent_id, a.name
                ORDER BY total_calls DESC
                """,
                (business_id, int(days)),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_agent_performance failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_cost_report(business_id, from_date, to_date):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.campaign_id,
                    COALESCE(cp.name, 'Unknown') AS campaign_name,
                    COUNT(*)::int AS total_calls,
                    COUNT(*) FILTER (WHERE c.was_booked=true)::int AS booked_count,
                    COALESCE(SUM(c.estimated_cost_usd), 0)::numeric(10,4) AS total_cost_usd
                FROM calls c
                LEFT JOIN campaigns cp ON cp.id = c.campaign_id
                WHERE c.business_id=%s::uuid
                  AND DATE(c.created_at) BETWEEN %s::date AND %s::date
                GROUP BY c.campaign_id, cp.name
                ORDER BY total_cost_usd DESC
                """,
                (business_id, from_date, to_date),
            )
            rows = _list(cur.fetchall())
            total_cost = sum(float(r.get("total_cost_usd") or 0) for r in rows)
            total_calls = sum(int(r.get("total_calls") or 0) for r in rows)
            total_bookings = sum(int(r.get("booked_count") or 0) for r in rows)
            return {
                "total_cost_usd": round(total_cost, 4),
                "cost_per_call": round(total_cost / total_calls, 4) if total_calls else 0.0,
                "cost_per_booking": round(total_cost / total_bookings, 4) if total_bookings else 0.0,
                "by_campaign": rows,
            }
    except Exception as e:
        logger.exception("get_cost_report failed: %s", e)
        return {"total_cost_usd": 0.0, "cost_per_call": 0.0, "cost_per_booking": 0.0, "by_campaign": []}
    finally:
        if conn is not None:
            release_conn(conn)


def get_live_monitor_snapshot(business_id=None):
    conn = None
    try:
        active_calls = get_active_calls(business_id=business_id)
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute(
                    """
                    SELECT COUNT(*)::int AS count
                    FROM calls
                    WHERE business_id=%s::uuid
                      AND created_at >= NOW() - INTERVAL '60 seconds'
                    """,
                    (business_id,),
                )
                calls_per_minute_now = int((cur.fetchone() or {}).get("count") or 0)

                cur.execute(
                    "SELECT id, status FROM campaigns WHERE business_id=%s::uuid ORDER BY created_at DESC",
                    (business_id,),
                )
                campaigns = _list(cur.fetchall())

                cur.execute(
                    """
                    SELECT COUNT(*)::int AS queue_depth
                    FROM leads l
                    JOIN campaigns c ON c.id = l.campaign_id
                    WHERE c.business_id=%s::uuid
                      AND c.status='active'
                      AND l.status='pending'
                    """,
                    (business_id,),
                )
                queue_depth = int((cur.fetchone() or {}).get("queue_depth") or 0)

                cur.execute(
                    """
                    SELECT COUNT(*)::int AS booked_today
                    FROM calls
                    WHERE business_id=%s::uuid
                      AND was_booked=true
                      AND DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
                    """,
                    (business_id,),
                )
                booked_today = int((cur.fetchone() or {}).get("booked_today") or 0)
            else:
                cur.execute("SELECT COUNT(*)::int AS count FROM calls WHERE created_at >= NOW() - INTERVAL '60 seconds'")
                calls_per_minute_now = int((cur.fetchone() or {}).get("count") or 0)

                cur.execute("SELECT id, status FROM campaigns ORDER BY created_at DESC")
                campaigns = _list(cur.fetchall())

                cur.execute(
                    """
                    SELECT COUNT(*)::int AS queue_depth
                    FROM leads l
                    JOIN campaigns c ON c.id = l.campaign_id
                    WHERE c.status='active'
                      AND l.status='pending'
                    """
                )
                queue_depth = int((cur.fetchone() or {}).get("queue_depth") or 0)

                cur.execute(
                    """
                    SELECT COUNT(*)::int AS booked_today
                    FROM calls
                    WHERE was_booked=true
                      AND DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
                    """
                )
                booked_today = int((cur.fetchone() or {}).get("booked_today") or 0)

        return {
            "active_calls": active_calls,
            "calls_per_minute_now": calls_per_minute_now,
            "campaign_statuses": {str(r["id"]): r["status"] for r in campaigns},
            "queue_depth": queue_depth,
            "booked_today": booked_today,
        }
    except Exception as e:
        logger.exception("get_live_monitor_snapshot failed: %s", e)
        return {
            "active_calls": [],
            "calls_per_minute_now": 0,
            "campaign_statuses": {},
            "queue_depth": 0,
            "booked_today": 0,
        }
    finally:
        if conn is not None:
            release_conn(conn)


# Stats

def get_platform_stats(business_id=None):
    empty = {
        "total_calls_today": 0,
        "active_calls_now": 0,
        "total_leads": 0,
        "active_campaigns": 0,
        "bookings_today": 0,
        "total_cost_today_usd": 0.0,
        "calls_per_hour_last_24h": [],
    }

    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if business_id:
                cur.execute(
                    """
                    SELECT
                      (SELECT COUNT(*)::int FROM calls WHERE business_id=%s::uuid
                        AND DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')) AS total_calls_today,
                      (SELECT COUNT(*)::int FROM active_calls WHERE business_id=%s::uuid) AS active_calls_now,
                      (SELECT COUNT(*)::int FROM leads WHERE business_id=%s::uuid) AS total_leads,
                      (SELECT COUNT(*)::int FROM campaigns WHERE business_id=%s::uuid AND status='active') AS active_campaigns,
                      (SELECT COUNT(*)::int FROM bookings WHERE business_id=%s::uuid
                        AND DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')) AS bookings_today,
                      (SELECT COALESCE(SUM(estimated_cost_usd),0)::numeric(10,4) FROM calls WHERE business_id=%s::uuid
                        AND DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')) AS total_cost_today_usd
                    """,
                    (business_id, business_id, business_id, business_id, business_id, business_id),
                )
                header = _dict(cur.fetchone()) or {}
                cur.execute(
                    """
                    SELECT to_char(date_trunc('hour', created_at), 'YYYY-MM-DD HH24:00') AS hour,
                           COUNT(*)::int AS calls
                    FROM calls
                    WHERE business_id=%s::uuid
                      AND created_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (business_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT
                      (SELECT COUNT(*)::int FROM calls
                        WHERE DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')) AS total_calls_today,
                      (SELECT COUNT(*)::int FROM active_calls) AS active_calls_now,
                      (SELECT COUNT(*)::int FROM leads) AS total_leads,
                      (SELECT COUNT(*)::int FROM campaigns WHERE status='active') AS active_campaigns,
                      (SELECT COUNT(*)::int FROM bookings
                        WHERE DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')) AS bookings_today,
                      (SELECT COALESCE(SUM(estimated_cost_usd),0)::numeric(10,4) FROM calls
                        WHERE DATE(created_at AT TIME ZONE 'Asia/Kolkata')=DATE(NOW() AT TIME ZONE 'Asia/Kolkata')) AS total_cost_today_usd
                    """
                )
                header = _dict(cur.fetchone()) or {}
                cur.execute(
                    """
                    SELECT to_char(date_trunc('hour', created_at), 'YYYY-MM-DD HH24:00') AS hour,
                           COUNT(*)::int AS calls
                    FROM calls
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )

            per_hour = _list(cur.fetchall())
            merged = {**empty, **header, "calls_per_hour_last_24h": per_hour}
            if merged.get("total_cost_today_usd") is None:
                merged["total_cost_today_usd"] = 0.0
            return merged
    except Exception as e:
        logger.exception("get_platform_stats failed: %s", e)
        return empty
    finally:
        if conn is not None:
            release_conn(conn)


def reset_inflight_leads():
    """Reset leads stuck in `calling` state during shutdown."""
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE leads SET status='pending' WHERE status='calling'")
            return cur.rowcount
    except Exception as e:
        logger.exception("reset_inflight_leads failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def close_pool():
    """Compatibility no-op for pool-based variants."""
    return None


# Group J: Holidays

def seed_holidays(business_id, holidays):
    conn = None
    try:
        if not holidays:
            return 0
        rows = []
        for h in holidays:
            rows.append(
                (
                    business_id,
                    h.get("name"),
                    h.get("date"),
                    h.get("country_code", "IN"),
                    bool(h.get("is_recurring", True)),
                    bool(h.get("skip_calls", True)),
                )
            )
        conn = get_conn()
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO holidays (business_id, name, date, country_code, is_recurring, skip_calls)
                VALUES %s
                ON CONFLICT (business_id, date)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    country_code = EXCLUDED.country_code,
                    is_recurring = EXCLUDED.is_recurring,
                    skip_calls = EXCLUDED.skip_calls
                """,
                rows,
                template="(%s::uuid,%s,%s::date,%s,%s,%s)",
            )
            return len(rows)
    except Exception as e:
        logger.exception("seed_holidays failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def get_holidays(business_id, year=None):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if year:
                cur.execute(
                    """
                    SELECT * FROM holidays
                    WHERE business_id=%s::uuid
                      AND EXTRACT(YEAR FROM date)=%s::int
                    ORDER BY date ASC
                    """,
                    (business_id, int(year)),
                )
            else:
                cur.execute(
                    "SELECT * FROM holidays WHERE business_id=%s::uuid ORDER BY date ASC",
                    (business_id,),
                )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_holidays failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def add_holiday(business_id, name, date, is_recurring=True, skip_calls=True, country_code="IN"):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO holidays (business_id, name, date, country_code, is_recurring, skip_calls)
                VALUES (%s::uuid, %s, %s::date, %s, %s, %s)
                ON CONFLICT (business_id, date)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    country_code = EXCLUDED.country_code,
                    is_recurring = EXCLUDED.is_recurring,
                    skip_calls = EXCLUDED.skip_calls
                RETURNING *
                """,
                (business_id, name, date, country_code, bool(is_recurring), bool(skip_calls)),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("add_holiday failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_holiday(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM holidays WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_holiday failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def is_holiday(business_id, date_str):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM holidays
                WHERE business_id=%s::uuid
                  AND date=%s::date
                  AND skip_calls=true
                LIMIT 1
                """,
                (business_id, date_str),
            )
            return cur.fetchone() is not None
    except Exception as e:
        logger.exception("is_holiday failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# Group J: Number health and inbound helpers

def get_number_health(sip_trunk_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM number_health
                WHERE sip_trunk_id=%s::uuid
                ORDER BY created_at DESC
                """,
                (sip_trunk_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_number_health failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def upsert_number_health(sip_trunk_id, phone_number, spam_score=0, spam_label=None, is_paused=False, pause_reason=None):
    conn = None
    try:
        phone_number = _clean_phone(phone_number)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO number_health
                (sip_trunk_id, phone_number, spam_score, spam_label, is_paused, pause_reason, last_checked_at)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (sip_trunk_id, phone_number)
                DO UPDATE SET
                    spam_score = EXCLUDED.spam_score,
                    spam_label = EXCLUDED.spam_label,
                    is_paused = EXCLUDED.is_paused,
                    pause_reason = EXCLUDED.pause_reason,
                    last_checked_at = NOW()
                RETURNING *
                """,
                (sip_trunk_id, phone_number, int(spam_score or 0), spam_label, bool(is_paused), pause_reason),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("upsert_number_health failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def set_number_pause(sip_trunk_id, phone_number, is_paused, reason=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE number_health
                SET is_paused=%s, pause_reason=%s
                WHERE sip_trunk_id=%s::uuid AND phone_number=%s
                """,
                (bool(is_paused), reason, sip_trunk_id, _clean_phone(phone_number)),
            )
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("set_number_pause failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def increment_number_usage(sip_trunk_id, phone_number):
    conn = None
    try:
        phone_number = _clean_phone(phone_number)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO number_health (sip_trunk_id, phone_number, calls_today, calls_this_week)
                VALUES (%s::uuid, %s, 1, 1)
                ON CONFLICT (sip_trunk_id, phone_number)
                DO UPDATE SET
                    calls_today = number_health.calls_today + 1,
                    calls_this_week = number_health.calls_this_week + 1
                """,
                (sip_trunk_id, phone_number),
            )
            return True
    except Exception as e:
        logger.exception("increment_number_usage failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_trunk_by_phone_number(phone_number: str):
    conn = None
    try:
        phone = _clean_phone(phone_number)
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM sip_trunks
                WHERE phone_number = %s
                   OR number_pool ? %s
                LIMIT 1
                """,
                (phone, phone),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_trunk_by_phone_number failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def get_lead_by_phone_and_business(phone: str, business_id: str):
    conn = None
    try:
        cleaned = _clean_phone(phone)
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM leads
                WHERE business_id=%s::uuid AND phone=%s
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (business_id, cleaned),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_lead_by_phone_and_business failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def get_inbound_calls(business_id, limit=50, offset=0):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM calls
                WHERE business_id=%s::uuid
                  AND call_direction='inbound'
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (business_id, int(limit), int(offset)),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_inbound_calls failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_inbound_stats(business_id, days=7):
    default = {"total_inbound": 0, "answered": 0, "missed": 0, "avg_duration": 0.0, "top_callback_hour": None}
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total_inbound,
                    COUNT(*) FILTER (WHERE status IN ('active','completed'))::int AS answered,
                    COUNT(*) FILTER (WHERE status NOT IN ('active','completed'))::int AS missed,
                    COALESCE(AVG(duration_seconds), 0)::float AS avg_duration
                FROM calls
                WHERE business_id=%s::uuid
                  AND call_direction='inbound'
                  AND created_at >= NOW() - (%s || ' days')::interval
                """,
                (business_id, int(days)),
            )
            row = _dict(cur.fetchone()) or {}
            cur.execute(
                """
                SELECT EXTRACT(HOUR FROM created_at)::int AS hour, COUNT(*)::int AS cnt
                FROM calls
                WHERE business_id=%s::uuid
                  AND call_direction='inbound'
                  AND created_at >= NOW() - (%s || ' days')::interval
                GROUP BY 1
                ORDER BY cnt DESC
                LIMIT 1
                """,
                (business_id, int(days)),
            )
            top = _dict(cur.fetchone())
            result = {**default, **row}
            result["top_callback_hour"] = top.get("hour") if top else None
            return result
    except Exception as e:
        logger.exception("get_inbound_stats failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


# Group J: Reminder calls

def create_reminder_calls_for_booking(booking_id, business_id, lead_id, agent_id, phone, hours_list):
    conn = None
    try:
        if not hours_list:
            return []
        conn = get_conn()
        with conn, conn.cursor() as cur:
            created = []
            for hours_before in hours_list:
                cur.execute(
                    """
                    INSERT INTO reminder_calls
                    (booking_id, business_id, lead_id, agent_id, phone, scheduled_for, hours_before, status)
                    SELECT
                      %s::uuid, %s::uuid, %s::uuid, %s::uuid, %s,
                      b.start_time - (%s::int || ' hours')::interval,
                      %s::int,
                      'pending'
                    FROM bookings b
                    WHERE b.id=%s::uuid
                      AND b.start_time - (%s::int || ' hours')::interval > NOW() + INTERVAL '5 minutes'
                    RETURNING *
                    """,
                    (
                        booking_id,
                        business_id,
                        lead_id,
                        agent_id,
                        _clean_phone(phone),
                        int(hours_before),
                        int(hours_before),
                        booking_id,
                        int(hours_before),
                    ),
                )
                row = cur.fetchone()
                if row:
                    created.append(dict(row))
            return created
    except Exception as e:
        logger.exception("create_reminder_calls_for_booking failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_due_reminders(buffer_minutes=5):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT rc.*, b.start_time AS booking_start_time, b.notes AS booking_notes,
                       l.name AS lead_name, l.custom_data, c.name AS campaign_name
                FROM reminder_calls rc
                LEFT JOIN bookings b ON b.id = rc.booking_id
                LEFT JOIN leads l ON l.id = rc.lead_id
                LEFT JOIN campaigns c ON c.id = l.campaign_id
                WHERE rc.status='pending'
                  AND rc.scheduled_for <= NOW() + (%s::int || ' minutes')::interval
                ORDER BY rc.scheduled_for ASC
                """,
                (int(buffer_minutes),),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_due_reminders failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def update_reminder_status(id, status, call_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            if call_id:
                cur.execute(
                    "UPDATE reminder_calls SET status=%s, call_id=%s::uuid WHERE id=%s::uuid",
                    (status, call_id, id),
                )
            else:
                cur.execute("UPDATE reminder_calls SET status=%s WHERE id=%s::uuid", (status, id))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_reminder_status failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_reminders_for_booking(booking_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM reminder_calls WHERE booking_id=%s::uuid ORDER BY scheduled_for ASC",
                (booking_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_reminders_for_booking failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def cancel_reminders_for_booking(booking_id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE reminder_calls SET status='cancelled' WHERE booking_id=%s::uuid AND status IN ('pending','dispatched')",
                (booking_id,),
            )
            return cur.rowcount
    except Exception as e:
        logger.exception("cancel_reminders_for_booking failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


# Group J: Surveys

def create_survey(
    business_id,
    agent_id,
    question,
    response_type="numeric",
    valid_responses=None,
    send_via="whatsapp",
    send_delay_minutes=2,
    trigger_dispositions=None,
):
    conn = None
    try:
        valid_responses = valid_responses or ["1", "2", "3", "4", "5"]
        trigger_dispositions = trigger_dispositions or ["interested", "booked", "callback_requested"]
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO surveys
                (business_id, agent_id, question, response_type, valid_responses, send_via,
                 send_delay_minutes, trigger_dispositions, is_active)
                VALUES
                (%s::uuid, %s::uuid, %s, %s, %s::text[], %s, %s, %s::text[], true)
                RETURNING *
                """,
                (
                    business_id,
                    agent_id,
                    question,
                    response_type,
                    list(valid_responses),
                    send_via,
                    int(send_delay_minutes),
                    list(trigger_dispositions),
                ),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_survey failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_surveys(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM surveys WHERE business_id=%s::uuid ORDER BY created_at DESC", (business_id,))
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_surveys failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_survey(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM surveys WHERE id=%s::uuid LIMIT 1", (id,))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_survey failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def update_survey(id, **kwargs):
    allowed = {
        "question",
        "response_type",
        "valid_responses",
        "send_via",
        "send_delay_minutes",
        "trigger_dispositions",
        "is_active",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not updates:
        return {}

    cols = []
    vals = []
    for k, v in updates.items():
        if k in {"valid_responses", "trigger_dispositions"}:
            cols.append(f"{k}=%s::text[]")
            vals.append(list(v))
        else:
            cols.append(f"{k}=%s")
            vals.append(v)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE surveys SET {', '.join(cols)} WHERE id=%s::uuid RETURNING *", vals + [id])
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("update_survey failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_survey(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM surveys WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_survey failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_active_survey_for_agent(agent_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM surveys
                WHERE agent_id=%s::uuid
                  AND is_active=true
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (agent_id,),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_active_survey_for_agent failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def create_survey_response(survey_id, call_id, lead_id, business_id, phone):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO survey_responses
                (survey_id, call_id, lead_id, business_id, phone, status)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, 'sent')
                RETURNING *
                """,
                (survey_id, call_id, lead_id, business_id, _clean_phone(phone)),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_survey_response failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_latest_sent_survey_for_phone(phone):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT sr.*, s.response_type, s.valid_responses, s.id AS survey_id_ref
                FROM survey_responses sr
                JOIN surveys s ON s.id = sr.survey_id
                WHERE sr.phone=%s
                  AND sr.status='sent'
                  AND sr.created_at >= NOW() - INTERVAL '24 hours'
                ORDER BY sr.created_at DESC
                LIMIT 1
                """,
                (_clean_phone(phone),),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_latest_sent_survey_for_phone failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def update_survey_response(phone, response, received_at):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE survey_responses
                SET response=%s,
                    response_received_at=%s::timestamptz,
                    status='responded'
                WHERE id = (
                    SELECT id FROM survey_responses
                    WHERE phone=%s
                      AND status='sent'
                      AND created_at >= NOW() - INTERVAL '24 hours'
                    ORDER BY created_at DESC
                    LIMIT 1
                )
                """,
                (response, received_at, _clean_phone(phone)),
            )
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_survey_response failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_survey_responses(survey_id, limit=50, offset=0):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM survey_responses
                WHERE survey_id=%s::uuid
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (survey_id, int(limit), int(offset)),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_survey_responses failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_survey_stats(survey_id):
    default = {
        "total_sent": 0,
        "total_responded": 0,
        "response_rate_pct": 0.0,
        "avg_numeric_score": None,
        "value_breakdown": {},
    }
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*)::int AS total_sent,
                  COUNT(*) FILTER (WHERE status='responded')::int AS total_responded,
                  AVG(CASE WHEN response ~ '^[0-9]+$' THEN response::int END)::float AS avg_numeric_score
                FROM survey_responses
                WHERE survey_id=%s::uuid
                """,
                (survey_id,),
            )
            row = _dict(cur.fetchone()) or {}

            cur.execute(
                """
                SELECT COALESCE(response, 'no_response') AS value, COUNT(*)::int AS count
                FROM survey_responses
                WHERE survey_id=%s::uuid
                GROUP BY 1
                ORDER BY count DESC
                """,
                (survey_id,),
            )
            breakdown_rows = _list(cur.fetchall())
            total_sent = int(row.get("total_sent") or 0)
            total_responded = int(row.get("total_responded") or 0)
            return {
                "total_sent": total_sent,
                "total_responded": total_responded,
                "response_rate_pct": round((total_responded / total_sent) * 100, 2) if total_sent else 0.0,
                "avg_numeric_score": row.get("avg_numeric_score"),
                "value_breakdown": {x["value"]: x["count"] for x in breakdown_rows},
            }
    except Exception as e:
        logger.exception("get_survey_stats failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


# Group J: Booking calendar helpers

def get_bookings_calendar(business_id, from_date, to_date):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  b.id,
                  COALESCE(b.caller_name, 'Unknown') AS caller_name,
                  b.caller_phone,
                  b.start_time,
                  b.status,
                  b.call_id,
                  b.notes,
                  cp.name AS campaign_name
                FROM bookings b
                LEFT JOIN calls c ON c.id = b.call_id
                LEFT JOIN campaigns cp ON cp.id = c.campaign_id
                WHERE b.business_id=%s::uuid
                  AND DATE(b.start_time) BETWEEN %s::date AND %s::date
                ORDER BY b.start_time ASC
                """,
                (business_id, from_date, to_date),
            )
            rows = _list(cur.fetchall())
            result = []
            for r in rows:
                result.append(
                    {
                        "id": str(r.get("id")),
                        "title": f"{r.get('caller_name')} — {r.get('caller_phone')}",
                        "start": str(r.get("start_time")),
                        "end": str(r.get("start_time") + timedelta(minutes=30)) if r.get("start_time") else None,
                        "status": r.get("status") or "confirmed",
                        "call_id": str(r.get("call_id")) if r.get("call_id") else None,
                        "campaign_name": r.get("campaign_name") or "",
                        "notes": r.get("notes") or "",
                        "color": "#22c55e" if (r.get("status") or "confirmed") != "cancelled" else "#ef4444",
                    }
                )
            return result
    except Exception as e:
        logger.exception("get_bookings_calendar failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def update_booking(id, status=None, notes=None, start_time=None):
    conn = None
    try:
        update_cols = []
        vals = []
        if status is not None:
            update_cols.append("status=%s")
            vals.append(status)
        if notes is not None:
            update_cols.append("notes=%s")
            vals.append(notes)
        if start_time is not None:
            update_cols.append("start_time=%s::timestamptz")
            vals.append(start_time)
        if not update_cols:
            return {}

        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE bookings SET {', '.join(update_cols)} WHERE id=%s::uuid RETURNING *",
                vals + [id],
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("update_booking failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def update_booking_gcal_event(id, gcal_event_id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE bookings SET gcal_event_id=%s WHERE id=%s::uuid RETURNING *",
                (gcal_event_id, id),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("update_booking_gcal_event failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_bookings_summary(business_id):
    default = {
        "today": 0,
        "this_week": 0,
        "this_month": 0,
        "upcoming_24h": [],
        "cancellation_rate_pct": 0.0,
    }
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE DATE(start_time)=CURRENT_DATE)::int AS today,
                  COUNT(*) FILTER (WHERE date_trunc('week', start_time)=date_trunc('week', NOW()))::int AS this_week,
                  COUNT(*) FILTER (WHERE date_trunc('month', start_time)=date_trunc('month', NOW()))::int AS this_month,
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status='cancelled')::int AS cancelled
                FROM bookings
                WHERE business_id=%s::uuid
                """,
                (business_id,),
            )
            header = _dict(cur.fetchone()) or {}
            cur.execute(
                """
                SELECT * FROM bookings
                WHERE business_id=%s::uuid
                  AND start_time BETWEEN NOW() AND NOW() + INTERVAL '24 hours'
                ORDER BY start_time ASC
                LIMIT 25
                """,
                (business_id,),
            )
            upcoming = _list(cur.fetchall())
            total = int(header.get("total") or 0)
            cancelled = int(header.get("cancelled") or 0)
            return {
                "today": int(header.get("today") or 0),
                "this_week": int(header.get("this_week") or 0),
                "this_month": int(header.get("this_month") or 0),
                "upcoming_24h": upcoming,
                "cancellation_rate_pct": round((cancelled / total) * 100, 2) if total else 0.0,
            }
    except Exception as e:
        logger.exception("get_bookings_summary failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


# Group D: RBAC/Auth, API keys, Audit, GDPR

def create_user(business_id, email, hashed_password, role="viewer"):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (business_id, email, hashed_password, role, is_active)
                VALUES (%s::uuid, %s, %s, %s, true)
                RETURNING id, business_id, email, role, is_active, created_at
                """,
                (business_id, email.lower().strip(), hashed_password, role),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_user failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_user_by_email(email):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE email=%s LIMIT 1", (email.lower().strip(),))
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_user_by_email failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def get_user(id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, business_id, email, role, is_active, created_at FROM users WHERE id=%s::uuid LIMIT 1",
                (id,),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_user failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def create_user_session(user_id, token, expires_at):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO user_sessions (user_id, token, expires_at)
                VALUES (%s::uuid, %s, %s::timestamptz)
                RETURNING *
                """,
                (user_id, token, expires_at),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("create_user_session failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_user_session(token):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.*, u.email, u.role, u.business_id, u.is_active
                FROM user_sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.token=%s
                  AND s.expires_at > NOW()
                LIMIT 1
                """,
                (token,),
            )
            return _dict(cur.fetchone())
    except Exception as e:
        logger.exception("get_user_session failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def delete_user_session(token):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM user_sessions WHERE token=%s", (token,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_user_session failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def create_api_key(business_id, name, scopes):
    conn = None
    try:
        raw = "rxai_" + secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(raw.encode()).hexdigest()
        key_prefix = raw[:8]
        scopes = scopes or ["read", "write"]
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO api_keys (business_id, name, key_hash, key_prefix, scopes)
                VALUES (%s::uuid, %s, %s, %s, %s::text[])
                RETURNING id, business_id, name, key_prefix, scopes, created_at
                """,
                (business_id, name, key_hash, key_prefix, list(scopes)),
            )
            row = _dict(cur.fetchone()) or {}
            row["key"] = raw
            return row
    except Exception as e:
        logger.exception("create_api_key failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def validate_api_key(raw_key):
    conn = None
    try:
        key_hash = hashlib.sha256((raw_key or "").encode()).hexdigest()
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, business_id, name, key_prefix, scopes, is_active, expires_at
                FROM api_keys
                WHERE key_hash=%s
                  AND is_active=true
                  AND (expires_at IS NULL OR expires_at > NOW())
                LIMIT 1
                """,
                (key_hash,),
            )
            row = _dict(cur.fetchone())
            if row:
                cur.execute("UPDATE api_keys SET last_used_at=NOW() WHERE id=%s::uuid", (row["id"],))
            return row
    except Exception as e:
        logger.exception("validate_api_key failed: %s", e)
        return None
    finally:
        if conn is not None:
            release_conn(conn)


def revoke_api_key(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE api_keys SET is_active=false WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("revoke_api_key failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_api_keys(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, business_id, name, key_prefix, scopes, last_used_at, expires_at, is_active, created_at
                FROM api_keys
                WHERE business_id=%s::uuid
                ORDER BY created_at DESC
                """,
                (business_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_api_keys failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def log_audit(
    business_id,
    user_id,
    action,
    resource_type,
    resource_id=None,
    old_value=None,
    new_value=None,
    ip_address=None,
    user_agent=None,
):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_log
                (business_id, user_id, action, resource_type, resource_id,
                 old_value, new_value, ip_address, user_agent)
                VALUES
                (%s::uuid, %s::uuid, %s, %s, %s,
                 %s::jsonb, %s::jsonb, %s, %s)
                """,
                (
                    business_id,
                    user_id,
                    action,
                    resource_type,
                    resource_id,
                    json.dumps(old_value) if old_value is not None else None,
                    json.dumps(new_value) if new_value is not None else None,
                    ip_address,
                    user_agent,
                ),
            )
    except Exception as e:
        logger.warning("log_audit failed: %s", e)
    finally:
        if conn is not None:
            release_conn(conn)


def get_audit_log(business_id, limit=50, offset=0):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM audit_log
                WHERE business_id=%s::uuid
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (business_id, int(limit), int(offset)),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_audit_log failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def export_lead_data(business_id, phone):
    conn = None
    try:
        clean = _clean_phone(phone)
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM leads WHERE business_id=%s::uuid AND phone=%s ORDER BY created_at DESC",
                (business_id, clean),
            )
            leads = _list(cur.fetchall())

            cur.execute(
                """
                SELECT id, lead_id, campaign_id, agent_id, sip_trunk_id, business_id, phone,
                       room_id, status, call_attempt_number, duration_seconds, summary,
                       sentiment, disposition, recording_url, was_booked, interrupt_count,
                       estimated_cost_usd, whatsapp_sent, call_date, call_hour, created_at
                FROM calls
                WHERE business_id=%s::uuid AND phone=%s
                ORDER BY created_at DESC
                """,
                (business_id, clean),
            )
            calls = _list(cur.fetchall())

            cur.execute(
                """
                SELECT b.*
                FROM bookings b
                JOIN calls c ON c.id = b.call_id
                WHERE b.business_id=%s::uuid AND c.phone=%s
                ORDER BY b.created_at DESC
                """,
                (business_id, clean),
            )
            bookings = _list(cur.fetchall())

            cur.execute(
                """
                SELECT sc.*
                FROM scheduled_callbacks sc
                JOIN leads l ON l.id = sc.lead_id
                WHERE l.business_id=%s::uuid AND l.phone=%s
                ORDER BY sc.created_at DESC
                """,
                (business_id, clean),
            )
            callbacks = _list(cur.fetchall())

            cur.execute(
                "SELECT 1 FROM dnc_list WHERE business_id=%s::uuid AND phone=%s LIMIT 1",
                (business_id, clean),
            )
            on_dnc = cur.fetchone() is not None

            return {
                "phone": clean,
                "leads": leads,
                "calls": calls,
                "bookings": bookings,
                "scheduled_callbacks": callbacks,
                "on_dnc": on_dnc,
            }
    except Exception as e:
        logger.exception("export_lead_data failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_lead_pii(business_id, phone):
    conn = None
    try:
        clean = _clean_phone(phone)
        phone_hash = hashlib.sha256(clean.encode()).hexdigest()[:8]
        replacement_phone = f"DELETED_{phone_hash}"

        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE leads
                SET phone=%s,
                    name='[DELETED]',
                    email='[DELETED]',
                    custom_data='{}'::jsonb,
                    notes=NULL
                WHERE business_id=%s::uuid AND phone=%s
                """,
                (replacement_phone, business_id, clean),
            )

            cur.execute(
                """
                UPDATE calls
                SET phone=%s,
                    transcript=NULL,
                    summary=NULL
                WHERE business_id=%s::uuid AND phone=%s
                """,
                (replacement_phone, business_id, clean),
            )

            cur.execute(
                """
                INSERT INTO dnc_list (business_id, phone, reason, added_by)
                VALUES (%s::uuid, %s, 'gdpr_erasure', 'system')
                ON CONFLICT (business_id, phone)
                DO UPDATE SET reason='gdpr_erasure', added_by='system'
                """,
                (business_id, replacement_phone),
            )
            return True
    except Exception as e:
        logger.exception("delete_lead_pii failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_data_retention_report(business_id):
    default = {
        "older_than_90_days": {"leads": 0, "calls": 0, "bookings": 0},
        "older_than_180_days": {"leads": 0, "calls": 0, "bookings": 0},
        "older_than_365_days": {"leads": 0, "calls": 0, "bookings": 0},
    }
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            for days in (90, 180, 365):
                cur.execute(
                    "SELECT COUNT(*)::int AS c FROM leads WHERE business_id=%s::uuid AND created_at < NOW() - (%s || ' days')::interval",
                    (business_id, days),
                )
                leads_c = int((_dict(cur.fetchone()) or {}).get("c") or 0)
                cur.execute(
                    "SELECT COUNT(*)::int AS c FROM calls WHERE business_id=%s::uuid AND created_at < NOW() - (%s || ' days')::interval",
                    (business_id, days),
                )
                calls_c = int((_dict(cur.fetchone()) or {}).get("c") or 0)
                cur.execute(
                    "SELECT COUNT(*)::int AS c FROM bookings WHERE business_id=%s::uuid AND created_at < NOW() - (%s || ' days')::interval",
                    (business_id, days),
                )
                bookings_c = int((_dict(cur.fetchone()) or {}).get("c") or 0)
                default[f"older_than_{days}_days"] = {
                    "leads": leads_c,
                    "calls": calls_c,
                    "bookings": bookings_c,
                }
            return default
    except Exception as e:
        logger.exception("get_data_retention_report failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


def get_voicemail_stats(campaign_id):
    default = {"total_voicemails": 0, "voicemail_rate_pct": 0.0}
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*)::int AS total_calls,
                  COUNT(*) FILTER (WHERE was_voicemail=true)::int AS total_voicemails
                FROM calls
                WHERE campaign_id=%s::uuid
                """,
                (campaign_id,),
            )
            row = _dict(cur.fetchone()) or {}
            total_calls = int(row.get("total_calls") or 0)
            total_voicemails = int(row.get("total_voicemails") or 0)
            rate = round((total_voicemails / total_calls) * 100, 2) if total_calls else 0.0
            return {"total_voicemails": total_voicemails, "voicemail_rate_pct": rate}
    except Exception as e:
        logger.exception("get_voicemail_stats failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


def get_transfer_stats(campaign_id):
    default = {"total_transfers": 0, "transfer_rate_pct": 0.0}
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*)::int AS total_calls,
                  COUNT(*) FILTER (
                    WHERE disposition='transferred' OR status='transferred'
                  )::int AS total_transfers
                FROM calls
                WHERE campaign_id=%s::uuid
                """,
                (campaign_id,),
            )
            row = _dict(cur.fetchone()) or {}
            total_calls = int(row.get("total_calls") or 0)
            total_transfers = int(row.get("total_transfers") or 0)
            rate = round((total_transfers / total_calls) * 100, 2) if total_calls else 0.0
            return {"total_transfers": total_transfers, "transfer_rate_pct": rate}
    except Exception as e:
        logger.exception("get_transfer_stats failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


def get_quality_distribution(campaign_id):
    default = {"excellent": 0, "good": 0, "fair": 0, "poor": 0}
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE quality_score BETWEEN 80 AND 100)::int AS excellent,
                  COUNT(*) FILTER (WHERE quality_score BETWEEN 60 AND 79)::int AS good,
                  COUNT(*) FILTER (WHERE quality_score BETWEEN 40 AND 59)::int AS fair,
                  COUNT(*) FILTER (WHERE quality_score BETWEEN 0 AND 39)::int AS poor
                FROM calls
                WHERE campaign_id=%s::uuid
                """,
                (campaign_id,),
            )
            row = _dict(cur.fetchone()) or {}
            return {
                "excellent": int(row.get("excellent") or 0),
                "good": int(row.get("good") or 0),
                "fair": int(row.get("fair") or 0),
                "poor": int(row.get("poor") or 0),
            }
    except Exception as e:
        logger.exception("get_quality_distribution failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


# Lead scoring / tags / timeline

def get_leads_by_temperature(campaign_id, temperature):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM leads
                WHERE campaign_id=%s::uuid AND temperature=%s
                ORDER BY score DESC NULLS LAST, created_at ASC
                """,
                (campaign_id, str(temperature or "").lower()),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_leads_by_temperature failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_score_distribution(campaign_id):
    default = {"hot": 0, "warm": 0, "cold": 0, "avg_score": 0.0}
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE temperature='hot')::int AS hot,
                  COUNT(*) FILTER (WHERE temperature='warm')::int AS warm,
                  COUNT(*) FILTER (WHERE temperature='cold')::int AS cold,
                  ROUND(COALESCE(AVG(score), 0)::numeric, 2) AS avg_score
                FROM leads
                WHERE campaign_id=%s::uuid
                """,
                (campaign_id,),
            )
            row = _dict(cur.fetchone()) or {}
            return {
                "hot": int(row.get("hot") or 0),
                "warm": int(row.get("warm") or 0),
                "cold": int(row.get("cold") or 0),
                "avg_score": float(row.get("avg_score") or 0.0),
            }
    except Exception as e:
        logger.exception("get_score_distribution failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


def add_lead_tag(lead_id, tag: str):
    conn = None
    try:
        clean_tag = str(tag or "").strip()
        if not clean_tag:
            return False
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE leads
                SET tags = CASE
                    WHEN tags IS NULL THEN ARRAY[%s]::text[]
                    WHEN NOT (%s = ANY(tags)) THEN array_append(tags, %s)
                    ELSE tags
                END
                WHERE id=%s::uuid
                """,
                (clean_tag, clean_tag, clean_tag, lead_id),
            )
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("add_lead_tag failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def remove_lead_tag(lead_id, tag: str):
    conn = None
    try:
        clean_tag = str(tag or "").strip()
        if not clean_tag:
            return False
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE leads
                SET tags = array_remove(COALESCE(tags, ARRAY[]::text[]), %s)
                WHERE id=%s::uuid
                """,
                (clean_tag, lead_id),
            )
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("remove_lead_tag failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_leads_by_tag(campaign_id, tag: str):
    conn = None
    try:
        clean_tag = str(tag or "").strip()
        if not clean_tag:
            return []
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM leads
                WHERE campaign_id=%s::uuid AND %s = ANY(COALESCE(tags, ARRAY[]::text[]))
                ORDER BY score DESC NULLS LAST, created_at ASC
                """,
                (campaign_id, clean_tag),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_leads_by_tag failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_all_tags(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT unnest(COALESCE(tags, ARRAY[]::text[])) AS tag
                FROM leads
                WHERE business_id=%s::uuid
                ORDER BY tag
                """,
                (business_id,),
            )
            return [str(r.get("tag")) for r in cur.fetchall() if r.get("tag")]
    except Exception as e:
        logger.exception("get_all_tags failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_lead_timeline(lead_id):
    conn = None
    try:
        conn = get_conn()
        events = []
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, room_id, status, disposition, duration_seconds, summary, sentiment, created_at
                FROM calls
                WHERE lead_id=%s::uuid
                ORDER BY created_at ASC
                """,
                (lead_id,),
            )
            calls = _list(cur.fetchall())
            room_ids = [c.get("room_id") for c in calls if c.get("room_id")]

            for call in calls:
                events.append(
                    {
                        "event_type": "call",
                        "timestamp": str(call.get("created_at")),
                        "data": call,
                    }
                )
                events.append(
                    {
                        "event_type": "status_change",
                        "timestamp": str(call.get("created_at")),
                        "data": {
                            "status": call.get("status"),
                            "disposition": call.get("disposition"),
                            "call_id": str(call.get("id")),
                            "room_id": call.get("room_id"),
                        },
                    }
                )

            if room_ids:
                cur.execute(
                    """
                    SELECT room_id, role, content, turn_number, created_at
                    FROM call_transcript_lines
                    WHERE room_id = ANY(%s)
                    ORDER BY created_at ASC
                    """,
                    (room_ids,),
                )
                for row in _list(cur.fetchall()):
                    events.append(
                        {
                            "event_type": "transcript",
                            "timestamp": str(row.get("created_at")),
                            "data": row,
                        }
                    )

            cur.execute(
                """
                SELECT b.*, c.room_id
                FROM bookings b
                JOIN calls c ON c.id = b.call_id
                WHERE c.lead_id=%s::uuid
                ORDER BY b.created_at ASC
                """,
                (lead_id,),
            )
            for row in _list(cur.fetchall()):
                events.append(
                    {
                        "event_type": "booking",
                        "timestamp": str(row.get("created_at")),
                        "data": row,
                    }
                )

            cur.execute(
                """
                SELECT *
                FROM scheduled_callbacks
                WHERE lead_id=%s::uuid
                ORDER BY created_at ASC
                """,
                (lead_id,),
            )
            for row in _list(cur.fetchall()):
                events.append(
                    {
                        "event_type": "callback",
                        "timestamp": str(row.get("created_at")),
                        "data": row,
                    }
                )

        events.sort(key=lambda x: str(x.get("timestamp") or ""))
        return events
    except Exception as e:
        logger.exception("get_lead_timeline failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def find_duplicate_leads_across_campaigns(business_id, phones):
    conn = None
    try:
        clean_phones = []
        seen = set()
        for p in phones or []:
            cp = _clean_phone(str(p))
            if cp and cp not in seen:
                clean_phones.append(cp)
                seen.add(cp)
        if not clean_phones:
            return {}
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT phone, campaign_id
                FROM leads
                WHERE business_id=%s::uuid
                  AND phone = ANY(%s)
                """,
                (business_id, clean_phones),
            )
            out = {}
            for row in _list(cur.fetchall()):
                phone = str(row.get("phone") or "")
                campaign_id = str(row.get("campaign_id") or "")
                if not phone or not campaign_id:
                    continue
                out.setdefault(phone, [])
                if campaign_id not in out[phone]:
                    out[phone].append(campaign_id)
            return out
    except Exception as e:
        logger.exception("find_duplicate_leads_across_campaigns failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_lead_across_campaigns(business_id, phone):
    conn = None
    try:
        clean = _clean_phone(phone)
        if not clean:
            return []
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT l.*, c.name AS campaign_name
                FROM leads l
                LEFT JOIN campaigns c ON c.id = l.campaign_id
                WHERE l.business_id=%s::uuid
                  AND l.phone=%s
                ORDER BY l.created_at DESC
                """,
                (business_id, clean),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_lead_across_campaigns failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


# Objection handlers / pronunciation guide

def create_objection_handler(agent_id, trigger_phrases, response, priority=1):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO objection_handlers (agent_id, trigger_phrases, response, priority)
                VALUES (%s::uuid, %s::text[], %s, %s)
                RETURNING *
                """,
                (agent_id, list(trigger_phrases or []), response, int(priority or 1)),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("create_objection_handler failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_objection_handlers(agent_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM objection_handlers
                WHERE agent_id=%s::uuid AND is_active=true
                ORDER BY priority DESC, created_at ASC
                """,
                (agent_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_objection_handlers failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def update_objection_handler(id, **kwargs):
    allowed = {"trigger_phrases", "response", "priority", "is_active"}
    update_data = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not update_data:
        return {}

    parts = []
    vals = []
    for k, v in update_data.items():
        if k == "trigger_phrases":
            parts.append("trigger_phrases=%s::text[]")
            vals.append(list(v or []))
        else:
            parts.append(f"{k}=%s")
            vals.append(v)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE objection_handlers SET {', '.join(parts)} WHERE id=%s::uuid RETURNING *",
                vals + [id],
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("update_objection_handler failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_objection_handler(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM objection_handlers WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_objection_handler failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def create_pronunciation_entry(agent_id, word, pronunciation):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pronunciation_guide (agent_id, word, pronunciation)
                VALUES (%s::uuid, %s, %s)
                RETURNING *
                """,
                (agent_id, word, pronunciation),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("create_pronunciation_entry failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_pronunciation_guide(agent_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM pronunciation_guide WHERE agent_id=%s::uuid ORDER BY created_at ASC",
                (agent_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_pronunciation_guide failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def delete_pronunciation_entry(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM pronunciation_guide WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_pronunciation_entry failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# Advanced analytics

def get_call_heatmap(business_id, days=30):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  ((EXTRACT(DOW FROM created_at AT TIME ZONE 'Asia/Kolkata') + 6)::int % 7) AS day_of_week,
                  EXTRACT(HOUR FROM created_at AT TIME ZONE 'Asia/Kolkata')::int AS hour,
                  COUNT(*)::int AS total_calls,
                  COUNT(*) FILTER (WHERE status='completed')::int AS connected,
                  ROUND(
                    COUNT(*) FILTER (WHERE status='completed')::numeric /
                    NULLIF(COUNT(*), 0) * 100, 1
                  ) AS connection_rate
                FROM calls
                WHERE business_id=%s::uuid
                  AND created_at >= NOW() - (%s || ' days')::interval
                GROUP BY 1, 2
                HAVING COUNT(*) >= 3
                ORDER BY connection_rate DESC
                """,
                (business_id, int(days or 30)),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_call_heatmap failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_agent_comparison(business_id, campaign_id=None, days=7):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if campaign_id:
                cur.execute(
                    """
                    SELECT
                      c.agent_id,
                      COALESCE(a.name, 'Unknown') AS agent_name,
                      COUNT(*)::int AS total_calls,
                      COALESCE(AVG(c.duration_seconds), 0)::int AS avg_duration_seconds,
                      COUNT(*) FILTER (WHERE c.was_booked=true)::int AS booked_count,
                      ROUND(COALESCE(AVG(c.quality_score), 0)::numeric, 2) AS avg_quality_score,
                      ROUND(COALESCE(AVG(c.interrupt_count), 0)::numeric, 2) AS avg_interrupts,
                      ROUND(
                        CASE WHEN COUNT(*) = 0 THEN 0
                             ELSE (COUNT(*) FILTER (WHERE c.was_booked=true)::numeric / COUNT(*)::numeric) * 100
                        END, 2
                      ) AS conversion_rate_pct,
                      ROUND(
                        CASE WHEN COUNT(*) = 0 THEN 0
                             ELSE (COUNT(*) FILTER (WHERE c.sentiment='positive')::numeric / COUNT(*)::numeric) * 100
                        END, 2
                      ) AS positive_sentiment_pct
                    FROM calls c
                    LEFT JOIN agents a ON a.id = c.agent_id
                    WHERE c.business_id=%s::uuid
                      AND c.campaign_id=%s::uuid
                      AND c.created_at >= NOW() - (%s || ' days')::interval
                    GROUP BY c.agent_id, a.name
                    ORDER BY conversion_rate_pct DESC, total_calls DESC
                    """,
                    (business_id, campaign_id, int(days or 7)),
                )
            else:
                cur.execute(
                    """
                    SELECT
                      c.agent_id,
                      COALESCE(a.name, 'Unknown') AS agent_name,
                      COUNT(*)::int AS total_calls,
                      COALESCE(AVG(c.duration_seconds), 0)::int AS avg_duration_seconds,
                      COUNT(*) FILTER (WHERE c.was_booked=true)::int AS booked_count,
                      ROUND(COALESCE(AVG(c.quality_score), 0)::numeric, 2) AS avg_quality_score,
                      ROUND(COALESCE(AVG(c.interrupt_count), 0)::numeric, 2) AS avg_interrupts,
                      ROUND(
                        CASE WHEN COUNT(*) = 0 THEN 0
                             ELSE (COUNT(*) FILTER (WHERE c.was_booked=true)::numeric / COUNT(*)::numeric) * 100
                        END, 2
                      ) AS conversion_rate_pct,
                      ROUND(
                        CASE WHEN COUNT(*) = 0 THEN 0
                             ELSE (COUNT(*) FILTER (WHERE c.sentiment='positive')::numeric / COUNT(*)::numeric) * 100
                        END, 2
                      ) AS positive_sentiment_pct
                    FROM calls c
                    LEFT JOIN agents a ON a.id = c.agent_id
                    WHERE c.business_id=%s::uuid
                      AND c.created_at >= NOW() - (%s || ' days')::interval
                    GROUP BY c.agent_id, a.name
                    ORDER BY conversion_rate_pct DESC, total_calls DESC
                    """,
                    (business_id, int(days or 7)),
                )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_agent_comparison failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def get_cost_per_acquisition(business_id, from_date, to_date):
    default = {
        "total_cost_usd": 0.0,
        "total_calls": 0,
        "total_bookings": 0,
        "cost_per_call": 0.0,
        "cost_per_booking": None,
        "cost_per_interested_lead": None,
        "by_campaign": [],
    }
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  COALESCE(SUM(estimated_cost_usd), 0)::numeric(12,4) AS total_cost,
                  COUNT(*)::int AS total_calls,
                  COUNT(*) FILTER (WHERE was_booked=true)::int AS total_bookings,
                  COUNT(*) FILTER (WHERE disposition='interested')::int AS interested_count
                FROM calls
                WHERE business_id=%s::uuid
                  AND created_at::date >= %s::date
                  AND created_at::date <= %s::date
                """,
                (business_id, from_date, to_date),
            )
            row = _dict(cur.fetchone()) or {}
            total_cost = float(row.get("total_cost") or 0.0)
            total_calls = int(row.get("total_calls") or 0)
            total_bookings = int(row.get("total_bookings") or 0)
            interested_count = int(row.get("interested_count") or 0)

            cur.execute(
                """
                SELECT
                  c.campaign_id,
                  COALESCE(cp.name, 'Unknown') AS campaign_name,
                  COALESCE(SUM(c.estimated_cost_usd), 0)::numeric(12,4) AS cost,
                  COUNT(*) FILTER (WHERE c.was_booked=true)::int AS bookings
                FROM calls c
                LEFT JOIN campaigns cp ON cp.id = c.campaign_id
                WHERE c.business_id=%s::uuid
                  AND c.created_at::date >= %s::date
                  AND c.created_at::date <= %s::date
                GROUP BY c.campaign_id, cp.name
                ORDER BY cost DESC
                """,
                (business_id, from_date, to_date),
            )
            by_campaign = []
            for r in _list(cur.fetchall()):
                cost = float(r.get("cost") or 0.0)
                bookings = int(r.get("bookings") or 0)
                by_campaign.append(
                    {
                        "campaign_id": str(r.get("campaign_id")) if r.get("campaign_id") else None,
                        "campaign_name": r.get("campaign_name"),
                        "cost": cost,
                        "bookings": bookings,
                        "cpa": (cost / bookings) if bookings > 0 else None,
                    }
                )

            return {
                "total_cost_usd": total_cost,
                "total_calls": total_calls,
                "total_bookings": total_bookings,
                "cost_per_call": (total_cost / total_calls) if total_calls > 0 else 0.0,
                "cost_per_booking": (total_cost / total_bookings) if total_bookings > 0 else None,
                "cost_per_interested_lead": (total_cost / interested_count) if interested_count > 0 else None,
                "by_campaign": by_campaign,
            }
    except Exception as e:
        logger.exception("get_cost_per_acquisition failed: %s", e)
        return default
    finally:
        if conn is not None:
            release_conn(conn)


def get_sentiment_trend(campaign_id, days=7):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  created_at::date AS date,
                  COUNT(*) FILTER (WHERE sentiment='positive')::int AS positive,
                  COUNT(*) FILTER (WHERE sentiment='neutral')::int AS neutral,
                  COUNT(*) FILTER (WHERE sentiment='negative')::int AS negative,
                  COUNT(*) FILTER (WHERE sentiment IS NULL OR sentiment NOT IN ('positive','neutral','negative'))::int AS unknown
                FROM calls
                WHERE campaign_id=%s::uuid
                  AND created_at >= NOW() - (%s || ' days')::interval
                GROUP BY 1
                ORDER BY 1 ASC
                """,
                (campaign_id, int(days or 7)),
            )
            out = []
            for row in _list(cur.fetchall()):
                out.append(
                    {
                        "date": str(row.get("date")),
                        "positive": int(row.get("positive") or 0),
                        "neutral": int(row.get("neutral") or 0),
                        "negative": int(row.get("negative") or 0),
                        "unknown": int(row.get("unknown") or 0),
                    }
                )
            return out
    except Exception as e:
        logger.exception("get_sentiment_trend failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def create_scheduled_report(business_id, report_type, frequency, send_to, campaign_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scheduled_reports (business_id, report_type, frequency, send_to, campaign_id)
                VALUES (%s::uuid, %s, %s, %s::text[], %s::uuid)
                RETURNING *
                """,
                (business_id, report_type, frequency, list(send_to or []), campaign_id),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("create_scheduled_report failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_scheduled_reports(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM scheduled_reports
                WHERE business_id=%s::uuid
                ORDER BY created_at DESC
                """,
                (business_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_scheduled_reports failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def update_report_last_sent(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE scheduled_reports SET last_sent_at=NOW() WHERE id=%s::uuid",
                (id,),
            )
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("update_report_last_sent failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def delete_scheduled_report(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM scheduled_reports WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_scheduled_report failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


# Integrations: webhooks

def create_webhook(business_id, name, url, events, secret=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO webhooks (business_id, name, url, events, secret)
                VALUES (%s::uuid, %s, %s, %s::text[], %s)
                RETURNING *
                """,
                (business_id, name, url, list(events or []), secret),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("create_webhook failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_webhooks(business_id):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM webhooks WHERE business_id=%s::uuid ORDER BY created_at DESC",
                (business_id,),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_webhooks failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def update_webhook(id, **kwargs):
    allowed = {"name", "url", "events", "secret", "is_active", "failure_count", "last_triggered_at"}
    update_data = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not update_data:
        return {}

    parts = []
    vals = []
    for k, v in update_data.items():
        if k == "events":
            parts.append("events=%s::text[]")
            vals.append(list(v or []))
        else:
            parts.append(f"{k}=%s")
            vals.append(v)

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE webhooks SET {', '.join(parts)} WHERE id=%s::uuid RETURNING *", vals + [id])
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("update_webhook failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def delete_webhook(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM webhooks WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("delete_webhook failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def get_webhook_deliveries(webhook_id, limit=20):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM webhook_deliveries
                WHERE webhook_id=%s::uuid
                ORDER BY attempted_at DESC
                LIMIT %s
                """,
                (webhook_id, limit),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_webhook_deliveries failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def log_webhook_delivery(webhook_id, event_type, payload, response_status=None, response_body=None, success=False):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO webhook_deliveries
                (webhook_id, event_type, payload, response_status, response_body, success)
                VALUES (%s::uuid, %s, %s::jsonb, %s, %s, %s)
                RETURNING *
                """,
                (webhook_id, event_type, json.dumps(payload or {}), response_status, response_body, bool(success)),
            )
            inserted = _dict(cur.fetchone()) or {}
            cur.execute(
                """
                UPDATE webhooks
                SET last_triggered_at=NOW(),
                    failure_count=CASE WHEN %s THEN 0 ELSE COALESCE(failure_count,0)+1 END
                WHERE id=%s::uuid
                """,
                (bool(success), webhook_id),
            )
            return inserted
    except Exception as e:
        logger.exception("log_webhook_delivery failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


# Notifications / search / activity / bulk actions

def create_notification(business_id, type, title, body, resource_type=None, resource_id=None):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO notifications
                (business_id, type, title, body, resource_type, resource_id)
                VALUES (%s::uuid, %s, %s, %s, %s, %s::uuid)
                RETURNING *
                """,
                (business_id, type, title, body, resource_type, resource_id),
            )
            return _dict(cur.fetchone()) or {}
    except Exception as e:
        logger.exception("create_notification failed: %s", e)
        return {}
    finally:
        if conn is not None:
            release_conn(conn)


def get_notifications(business_id, unread_only=False, limit=50):
    if business_id is None or str(business_id).strip() == "":
        return []
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if unread_only:
                cur.execute(
                    """
                    SELECT *
                    FROM notifications
                    WHERE business_id=%s::uuid AND is_read=false
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (business_id, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT *
                    FROM notifications
                    WHERE business_id=%s::uuid
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (business_id, limit),
                )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_notifications failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def mark_notification_read(id):
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE notifications SET is_read=true WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as e:
        logger.exception("mark_notification_read failed: %s", e)
        return False
    finally:
        if conn is not None:
            release_conn(conn)


def mark_all_read(business_id):
    if business_id is None or str(business_id).strip() == "":
        return None
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE notifications SET is_read=true WHERE business_id=%s::uuid AND is_read=false",
                (business_id,),
            )
            return int(cur.rowcount or 0)
    except Exception as e:
        logger.exception("mark_all_read failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def get_unread_count(business_id):
    if business_id is None or str(business_id).strip() == "":
        return 0
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*)::int AS c FROM notifications WHERE business_id=%s::uuid AND is_read=false",
                (business_id,),
            )
            row = _dict(cur.fetchone()) or {}
            return int(row.get("c") or 0)
    except Exception as e:
        logger.exception("get_unread_count failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def get_activity_feed(business_id, limit=50, offset=0):
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM (
                    SELECT 'call' AS type, c.id::text AS id, c.phone AS label,
                           c.status AS action, c.created_at, c.room_id AS ref_id
                    FROM calls c
                    WHERE c.business_id=%s::uuid

                    UNION ALL

                    SELECT 'booking' AS type, b.id::text AS id, b.caller_phone AS label,
                           'booked' AS action, b.created_at, b.call_id::text AS ref_id
                    FROM bookings b
                    WHERE b.business_id=%s::uuid

                    UNION ALL

                    SELECT 'campaign' AS type, cp.id::text AS id, cp.name AS label,
                           cp.status AS action, cp.created_at, cp.id::text AS ref_id
                    FROM campaigns cp
                    WHERE cp.business_id=%s::uuid
                      AND cp.status IN ('active','completed','paused','failed','pending_approval')

                    UNION ALL

                    SELECT 'notification' AS type, n.id::text AS id, n.title AS label,
                           n.type AS action, n.created_at, n.resource_id::text AS ref_id
                    FROM notifications n
                    WHERE n.business_id=%s::uuid
                ) feed
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                (business_id, business_id, business_id, business_id, limit, offset),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("get_activity_feed failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def global_search(business_id, query, limit=20):
    conn = None
    try:
        q = str(query or "").strip()
        if not q:
            return []
        like = f"%{q}%"
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM (
                    SELECT 'lead' AS type, l.id::text AS id,
                           COALESCE(l.name, l.phone) AS label,
                           l.phone AS subtitle,
                           ('/leads/' || l.id::text) AS url
                    FROM leads l
                    WHERE l.business_id=%s::uuid
                      AND (l.phone ILIKE %s OR COALESCE(l.name,'') ILIKE %s)
                    LIMIT 5
                ) a

                UNION ALL

                SELECT *
                FROM (
                    SELECT 'call' AS type, c.id::text AS id,
                           c.phone AS label,
                           COALESCE(c.summary, c.status) AS subtitle,
                           ('/calls/' || COALESCE(c.room_id, c.id::text)) AS url
                    FROM calls c
                    WHERE c.business_id=%s::uuid
                      AND (c.phone ILIKE %s OR COALESCE(c.transcript,'') ILIKE %s)
                    LIMIT 5
                ) b

                UNION ALL

                SELECT *
                FROM (
                    SELECT 'campaign' AS type, cp.id::text AS id,
                           cp.name AS label,
                           COALESCE(cp.description, cp.status) AS subtitle,
                           ('/campaigns/' || cp.id::text) AS url
                    FROM campaigns cp
                    WHERE cp.business_id=%s::uuid
                      AND cp.name ILIKE %s
                    LIMIT 5
                ) c

                UNION ALL

                SELECT *
                FROM (
                    SELECT 'agent' AS type, ag.id::text AS id,
                           ag.name AS label,
                           COALESCE(ag.subtitle, ag.llm_model) AS subtitle,
                           ('/agents/' || ag.id::text) AS url
                    FROM agents ag
                    WHERE ag.business_id=%s::uuid
                      AND ag.name ILIKE %s
                    LIMIT 5
                ) d

                LIMIT %s
                """,
                (business_id, like, like, business_id, like, like, business_id, like, business_id, like, int(limit or 20)),
            )
            return _list(cur.fetchall())
    except Exception as e:
        logger.exception("global_search failed: %s", e)
        return []
    finally:
        if conn is not None:
            release_conn(conn)


def bulk_update_lead_status(lead_ids, status):
    conn = None
    try:
        if not lead_ids:
            return 0
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE leads SET status=%s WHERE id = ANY(%s::uuid[])",
                (status, list(lead_ids)),
            )
            return int(cur.rowcount or 0)
    except Exception as e:
        logger.exception("bulk_update_lead_status failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def bulk_add_lead_tags(lead_ids, tag):
    conn = None
    try:
        if not lead_ids or not str(tag or "").strip():
            return 0
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE leads
                SET tags = CASE
                    WHEN tags IS NULL THEN ARRAY[%s]::text[]
                    WHEN NOT (%s = ANY(tags)) THEN array_append(tags, %s)
                    ELSE tags
                END
                WHERE id = ANY(%s::uuid[])
                """,
                (tag, tag, tag, list(lead_ids)),
            )
            return int(cur.rowcount or 0)
    except Exception as e:
        logger.exception("bulk_add_lead_tags failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def bulk_delete_leads(lead_ids):
    conn = None
    try:
        if not lead_ids:
            return 0
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM leads
                WHERE id = ANY(%s::uuid[])
                  AND COALESCE(status, 'pending') = 'pending'
                """,
                (list(lead_ids),),
            )
            return int(cur.rowcount or 0)
    except Exception as e:
        logger.exception("bulk_delete_leads failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def bulk_pause_campaigns(campaign_ids):
    conn = None
    try:
        if not campaign_ids:
            return 0
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaigns
                SET status='paused',
                    paused_at=NOW(),
                    pause_reason=COALESCE(pause_reason, 'bulk_pause')
                WHERE id = ANY(%s::uuid[])
                """,
                (list(campaign_ids),),
            )
            return int(cur.rowcount or 0)
    except Exception as e:
        logger.exception("bulk_pause_campaigns failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)


def bulk_resume_campaigns(campaign_ids):
    conn = None
    try:
        if not campaign_ids:
            return 0
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE campaigns
                SET status='active',
                    paused_at=NULL,
                    pause_reason=NULL
                WHERE id = ANY(%s::uuid[])
                """,
                (list(campaign_ids),),
            )
            return int(cur.rowcount or 0)
    except Exception as e:
        logger.exception("bulk_resume_campaigns failed: %s", e)
        return 0
    finally:
        if conn is not None:
            release_conn(conn)
