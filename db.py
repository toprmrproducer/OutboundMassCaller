import json
import logging
import os
import random
import uuid
from datetime import datetime

import psycopg2
import pytz
import threading
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor, execute_values

logger = logging.getLogger("db")


_pool: pg_pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()


def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _pool
    if _pool is not None:
        return _pool
    with _pool_lock:
        if _pool is not None:
            return _pool
        database_url = os.environ["DATABASE_URL"]
        _pool = pg_pool.ThreadedConnectionPool(
            minconn=int(os.environ.get("DB_POOL_MIN", "2")),
            maxconn=int(os.environ.get("DB_POOL_MAX", "20")),
            dsn=database_url,
            cursor_factory=RealDictCursor,
        )
        logger.info("[DB] Connection pool initialised (min=2, max=20)")
        return _pool


def get_conn():
    """Get a connection from the pool."""
    return _get_pool().getconn()


def release_conn(conn):
    """Return a connection to the pool."""
    try:
        _get_pool().putconn(conn)
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
    sql = """
    -- NOTE: pgcrypto and vector extensions must be enabled
    -- manually in Supabase Dashboard before first run.
    -- Dashboard -> Database -> Extensions -> enable:
    --   1. pgcrypto
    --   2. vector (pgvector)
    -- Do NOT attempt to create extensions from application code
    -- as Supabase restricts this to dashboard/superuser only.

    CREATE TABLE IF NOT EXISTS businesses (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      name TEXT NOT NULL,
      description TEXT,
      website TEXT,
      timezone TEXT DEFAULT 'Asia/Kolkata',
      whatsapp_instance TEXT,
      whatsapp_token TEXT,
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
    """
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(sql)
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


def update_business(id, **kwargs):
    allowed = {"name", "description", "website", "timezone", "whatsapp_instance", "whatsapp_token"}
    set_sql, vals = _update_statement(kwargs, allowed)
    if not set_sql:
        return {}

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
    allowed = {"name", "phone_number", "number_pool", "max_concurrent_calls", "calls_per_minute", "is_active"}
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
                 max_turns, silence_threshold_seconds, max_nudges, is_active)
                VALUES
                (%s::uuid, %s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s, %s)
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
    allowed = {
        "name", "subtitle", "stt_provider", "stt_language", "stt_model",
        "llm_provider", "llm_model", "llm_base_url", "llm_temperature", "llm_max_tokens",
        "tts_provider", "tts_voice", "tts_language", "system_prompt", "first_line",
        "agent_instructions", "max_turns", "silence_threshold_seconds", "max_nudges", "is_active",
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
                 timezone, custom_script, scheduled_start_at, started_at, completed_at)
                VALUES
                (%s::uuid, %s::uuid, %s::uuid, %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    defaults["business_id"], defaults["agent_id"], defaults["sip_trunk_id"],
                    defaults["name"], defaults["description"], defaults["objective"],
                    defaults["status"], defaults["calls_per_minute"], defaults["max_concurrent_calls"], defaults["retry_failed"],
                    defaults["max_retries"], defaults["retry_delay_minutes"], defaults["call_window_start"], defaults["call_window_end"],
                    defaults["timezone"], defaults["custom_script"], defaults["scheduled_start_at"], defaults["started_at"], defaults["completed_at"],
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
        "custom_script", "scheduled_start_at", "started_at", "completed_at",
    }
    update_data = {k: v for k, v in kwargs.items() if k in allowed and v is not None}
    if not update_data:
        return {}

    cols = []
    vals = []
    for k, v in update_data.items():
        if isinstance(v, str) and v.lower() == "now()":
            cols.append(f"{k}=NOW()")
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
                 call_attempts, max_call_attempts, last_call_at, next_call_at, scheduled_script_id, notes)
                VALUES
                (%s::uuid, %s::uuid, %s, %s, %s, %s, %s::jsonb, %s,
                 %s, %s, %s, %s, %s::uuid, %s)
                RETURNING *
                """,
                (
                    defaults["campaign_id"], defaults["business_id"], phone,
                    defaults["name"], defaults["email"], defaults["language"],
                    json.dumps(defaults["custom_data"] or {}), defaults["status"],
                    defaults["call_attempts"], defaults["max_call_attempts"],
                    defaults["last_call_at"], defaults["next_call_at"],
                    defaults["scheduled_script_id"], defaults["notes"],
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
                )
                for r in to_insert
            ]

            execute_values(
                cur,
                """
                INSERT INTO leads
                (campaign_id, business_id, phone, name, email, language,
                 custom_data, status, call_attempts, max_call_attempts,
                 last_call_at, next_call_at, scheduled_script_id, notes)
                VALUES %s
                """,
                values,
                template="(%s::uuid,%s::uuid,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s::uuid,%s)",
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
                    ORDER BY created_at ASC
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

def create_call(lead_id, campaign_id, agent_id, sip_trunk_id, phone, room_id, business_id=None, call_attempt_number=1):
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
                 phone, room_id, call_attempt_number, status, call_date, call_hour)
                VALUES
                (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s::uuid,
                 %s, %s, %s, 'initiated', %s, %s)
                RETURNING *
                """,
                (
                    lead_id, campaign_id, agent_id, sip_trunk_id, business_id,
                    _clean_phone(phone), room_id, call_attempt_number,
                    now.date().isoformat(), now.hour,
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
        "call_date", "call_hour", "lead_id", "campaign_id", "agent_id", "sip_trunk_id", "business_id",
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
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO call_transcript_lines (room_id, phone, role, content, turn_number)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (room_id, _clean_phone(phone or ""), role, content, int(turn_number or 0)),
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
