import json
import logging
import os
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_values

logger = logging.getLogger("db")


def get_conn():
    """Create a new psycopg2 connection from DATABASE_URL."""
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(database_url, cursor_factory=RealDictCursor)


def _normalize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [_normalize_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _normalize_value(v) for k, v in value.items()}
    return value


def _normalize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {k: _normalize_value(v) for k, v in row.items()}


def _normalize_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    if not rows:
        return []
    return [_normalize_row(r) for r in rows if r is not None]


def _filter_allowed(values: dict[str, Any], allowed: set[str]) -> dict[str, Any]:
    return {k: v for k, v in values.items() if k in allowed and v is not None}


def _build_update_clause(values: dict[str, Any], allow_now_literal: bool = False) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []

    for key, value in values.items():
        if allow_now_literal and isinstance(value, str) and value.upper() == "NOW()":
            parts.append(f"{key}=NOW()")
            continue
        parts.append(f"{key}=%s")
        params.append(value)

    return ", ".join(parts), params


def initdb() -> bool:
    """Create all platform tables and indexes."""
    sql = """
    CREATE EXTENSION IF NOT EXISTS pgcrypto;

    CREATE TABLE IF NOT EXISTS sip_trunks (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
        name TEXT NOT NULL,
        subtitle TEXT,
        stt_provider TEXT DEFAULT 'sarvam',
        stt_language TEXT DEFAULT 'hi-IN',
        stt_model TEXT DEFAULT 'saaras:v3',
        llm_provider TEXT DEFAULT 'openai',
        llm_model TEXT DEFAULT 'gpt-4o-mini',
        llm_base_url TEXT,
        llm_temperature FLOAT DEFAULT 0.7,
        llm_max_tokens INT DEFAULT 150,
        tts_provider TEXT DEFAULT 'sarvam',
        tts_voice TEXT DEFAULT 'rohan',
        tts_language TEXT DEFAULT 'hi-IN',
        system_prompt TEXT NOT NULL DEFAULT '',
        first_line TEXT NOT NULL DEFAULT '',
        opening_greeting TEXT DEFAULT '',
        agent_instructions TEXT DEFAULT '',
        max_turns INT DEFAULT 25,
        is_active BOOLEAN DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS campaigns (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name TEXT NOT NULL,
        agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
        sip_trunk_id UUID REFERENCES sip_trunks(id) ON DELETE SET NULL,
        status TEXT DEFAULT 'draft',
        calls_per_minute INT DEFAULT 5,
        retry_failed BOOLEAN DEFAULT true,
        max_retries INT DEFAULT 2,
        call_window_start TEXT DEFAULT '09:00',
        call_window_end TEXT DEFAULT '20:00',
        timezone TEXT DEFAULT 'Asia/Kolkata',
        created_at TIMESTAMPTZ DEFAULT now(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS leads (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        campaign_id UUID REFERENCES campaigns(id) ON DELETE CASCADE,
        phone TEXT NOT NULL,
        name TEXT,
        email TEXT,
        custom_data JSONB DEFAULT '{}'::jsonb,
        status TEXT DEFAULT 'pending',
        call_attempts INT DEFAULT 0,
        last_call_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_leads_campaign_status ON leads(campaign_id, status);
    CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);

    CREATE TABLE IF NOT EXISTS calls (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        lead_id UUID REFERENCES leads(id) ON DELETE CASCADE,
        campaign_id UUID REFERENCES campaigns(id) ON DELETE CASCADE,
        agent_id UUID REFERENCES agents(id) ON DELETE SET NULL,
        sip_trunk_id UUID REFERENCES sip_trunks(id) ON DELETE SET NULL,
        phone TEXT NOT NULL,
        room_id TEXT,
        status TEXT DEFAULT 'initiated',
        duration_seconds INT DEFAULT 0,
        transcript TEXT,
        summary TEXT,
        sentiment TEXT,
        recording_url TEXT,
        was_booked BOOLEAN DEFAULT false,
        interrupt_count INT DEFAULT 0,
        estimated_cost_usd NUMERIC(10,4) DEFAULT 0,
        call_date DATE,
        call_hour INT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_calls_campaign_created ON calls(campaign_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_calls_room_id ON calls(room_id);

    CREATE TABLE IF NOT EXISTS bookings (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        call_id UUID REFERENCES calls(id) ON DELETE CASCADE,
        caller_name TEXT,
        caller_phone TEXT,
        caller_email TEXT,
        start_time TIMESTAMPTZ NOT NULL,
        notes TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS call_transcript_lines (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        room_id TEXT NOT NULL,
        phone TEXT,
        role TEXT,
        content TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_transcript_room_created ON call_transcript_lines(room_id, created_at);

    CREATE TABLE IF NOT EXISTS active_calls (
        room_id TEXT PRIMARY KEY,
        phone TEXT,
        campaign_id UUID,
        agent_id UUID,
        sip_trunk_id UUID,
        status TEXT DEFAULT 'active',
        started_at TIMESTAMPTZ DEFAULT now(),
        last_updated TIMESTAMPTZ DEFAULT now()
    );
    """

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(sql)
        logger.info("initdb: schema ready")
        return True
    except Exception as exc:
        logger.exception("initdb failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


# SIP Trunks

def create_sip_trunk(
    name: str,
    trunk_id: str,
    phone_number: str | None = None,
    number_pool: list[str] | None = None,
    max_concurrent_calls: int = 5,
    calls_per_minute: int = 10,
) -> dict[str, Any]:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sip_trunks
                    (name, trunk_id, phone_number, number_pool, max_concurrent_calls, calls_per_minute)
                VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                RETURNING *
                """,
                (
                    name,
                    trunk_id,
                    phone_number,
                    json.dumps(number_pool or []),
                    max_concurrent_calls,
                    calls_per_minute,
                ),
            )
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("create_sip_trunk failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def get_sip_trunks() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sip_trunks ORDER BY created_at DESC")
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_sip_trunks failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


def get_sip_trunk(trunk_id: str) -> dict[str, Any] | None:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sip_trunks WHERE trunk_id=%s LIMIT 1", (trunk_id,))
            return _normalize_row(cur.fetchone())
    except Exception as exc:
        logger.exception("get_sip_trunk failed: %s", exc)
        return None
    finally:
        if conn is not None:
            conn.close()


def get_sip_trunk_by_id(trunk_uuid: str) -> dict[str, Any] | None:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sip_trunks WHERE id=%s::uuid LIMIT 1", (trunk_uuid,))
            return _normalize_row(cur.fetchone())
    except Exception as exc:
        logger.exception("get_sip_trunk_by_id failed: %s", exc)
        return None
    finally:
        if conn is not None:
            conn.close()


def update_sip_trunk(id: str, **kwargs) -> dict[str, Any]:
    allowed = {
        "name",
        "trunk_id",
        "phone_number",
        "number_pool",
        "max_concurrent_calls",
        "calls_per_minute",
        "is_active",
    }
    values = _filter_allowed(kwargs, allowed)
    if "number_pool" in values and not isinstance(values["number_pool"], str):
        values["number_pool"] = json.dumps(values["number_pool"])
    if not values:
        return {}

    conn = None
    try:
        clause, params = _build_update_clause(values)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                f"UPDATE sip_trunks SET {clause} WHERE id=%s::uuid RETURNING *",
                params + [id],
            )
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("update_sip_trunk failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def delete_sip_trunk(id: str) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM sip_trunks WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as exc:
        logger.exception("delete_sip_trunk failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


# Agents

def create_agent(**kwargs) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "name": "Default Agent",
        "subtitle": "",
        "stt_provider": "sarvam",
        "stt_language": "hi-IN",
        "stt_model": "saaras:v3",
        "llm_provider": "openai",
        "llm_model": "gpt-4o-mini",
        "llm_base_url": None,
        "llm_temperature": 0.7,
        "llm_max_tokens": 150,
        "tts_provider": "sarvam",
        "tts_voice": "rohan",
        "tts_language": "hi-IN",
        "system_prompt": "",
        "first_line": "",
        "opening_greeting": "",
        "agent_instructions": "",
        "max_turns": 25,
        "is_active": False,
    }
    defaults.update({k: v for k, v in kwargs.items() if k in defaults and v is not None})

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agents
                (name, subtitle, stt_provider, stt_language, stt_model,
                 llm_provider, llm_model, llm_base_url, llm_temperature, llm_max_tokens,
                 tts_provider, tts_voice, tts_language,
                 system_prompt, first_line, opening_greeting, agent_instructions,
                 max_turns, is_active)
                VALUES
                (%s, %s, %s, %s, %s,
                 %s, %s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s, %s,
                 %s, %s)
                RETURNING *
                """,
                (
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
                    defaults["opening_greeting"],
                    defaults["agent_instructions"],
                    defaults["max_turns"],
                    defaults["is_active"],
                ),
            )
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("create_agent failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def get_agents() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM agents ORDER BY created_at DESC")
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_agents failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


def get_agent(id: str) -> dict[str, Any] | None:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM agents WHERE id=%s::uuid LIMIT 1", (id,))
            return _normalize_row(cur.fetchone())
    except Exception as exc:
        logger.exception("get_agent failed: %s", exc)
        return None
    finally:
        if conn is not None:
            conn.close()


def get_active_agent() -> dict[str, Any] | None:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM agents WHERE is_active=true ORDER BY created_at DESC LIMIT 1")
            return _normalize_row(cur.fetchone())
    except Exception as exc:
        logger.exception("get_active_agent failed: %s", exc)
        return None
    finally:
        if conn is not None:
            conn.close()


def update_agent(id: str, **kwargs) -> dict[str, Any]:
    allowed = {
        "name",
        "subtitle",
        "stt_provider",
        "stt_language",
        "stt_model",
        "llm_provider",
        "llm_model",
        "llm_base_url",
        "llm_temperature",
        "llm_max_tokens",
        "tts_provider",
        "tts_voice",
        "tts_language",
        "system_prompt",
        "first_line",
        "opening_greeting",
        "agent_instructions",
        "max_turns",
        "is_active",
    }
    values = _filter_allowed(kwargs, allowed)
    if not values:
        return {}

    conn = None
    try:
        clause, params = _build_update_clause(values)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE agents SET {clause} WHERE id=%s::uuid RETURNING *", params + [id])
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("update_agent failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def delete_agent(id: str) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM agents WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as exc:
        logger.exception("delete_agent failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


def set_active_agent(id: str) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE agents SET is_active=false WHERE is_active=true")
            cur.execute("UPDATE agents SET is_active=true WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as exc:
        logger.exception("set_active_agent failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


# Campaigns

def create_campaign(**kwargs) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "name": "New Campaign",
        "agent_id": None,
        "sip_trunk_id": None,
        "status": "draft",
        "calls_per_minute": 5,
        "retry_failed": True,
        "max_retries": 2,
        "call_window_start": "09:00",
        "call_window_end": "20:00",
        "timezone": "Asia/Kolkata",
        "started_at": None,
        "completed_at": None,
    }
    defaults.update({k: v for k, v in kwargs.items() if k in defaults and v is not None})

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO campaigns
                (name, agent_id, sip_trunk_id, status, calls_per_minute, retry_failed, max_retries,
                 call_window_start, call_window_end, timezone, started_at, completed_at)
                VALUES (%s, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (
                    defaults["name"],
                    defaults["agent_id"],
                    defaults["sip_trunk_id"],
                    defaults["status"],
                    defaults["calls_per_minute"],
                    defaults["retry_failed"],
                    defaults["max_retries"],
                    defaults["call_window_start"],
                    defaults["call_window_end"],
                    defaults["timezone"],
                    defaults["started_at"],
                    defaults["completed_at"],
                ),
            )
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("create_campaign failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def get_campaigns() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_campaigns failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


def get_campaign(id: str) -> dict[str, Any] | None:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM campaigns WHERE id=%s::uuid LIMIT 1", (id,))
            return _normalize_row(cur.fetchone())
    except Exception as exc:
        logger.exception("get_campaign failed: %s", exc)
        return None
    finally:
        if conn is not None:
            conn.close()


def update_campaign(id: str, **kwargs) -> dict[str, Any]:
    allowed = {
        "name",
        "agent_id",
        "sip_trunk_id",
        "status",
        "calls_per_minute",
        "retry_failed",
        "max_retries",
        "call_window_start",
        "call_window_end",
        "timezone",
        "started_at",
        "completed_at",
    }
    values = _filter_allowed(kwargs, allowed)
    if not values:
        return {}

    conn = None
    try:
        clause, params = _build_update_clause(values, allow_now_literal=True)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE campaigns SET {clause} WHERE id=%s::uuid RETURNING *", params + [id])
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("update_campaign failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def delete_campaign(id: str) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM campaigns WHERE id=%s::uuid", (id,))
            return cur.rowcount > 0
    except Exception as exc:
        logger.exception("delete_campaign failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


def get_campaign_stats(campaign_id: str) -> dict[str, int]:
    empty = {"total": 0, "pending": 0, "calling": 0, "completed": 0, "failed": 0, "dnc": 0}
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total,
                    COUNT(*) FILTER (WHERE status='pending')::int AS pending,
                    COUNT(*) FILTER (WHERE status='calling')::int AS calling,
                    COUNT(*) FILTER (WHERE status='completed')::int AS completed,
                    COUNT(*) FILTER (WHERE status='failed')::int AS failed,
                    COUNT(*) FILTER (WHERE status='dnc')::int AS dnc
                FROM leads
                WHERE campaign_id=%s::uuid
                """,
                (campaign_id,),
            )
            row = _normalize_row(cur.fetchone())
            return {**empty, **(row or {})}
    except Exception as exc:
        logger.exception("get_campaign_stats failed: %s", exc)
        return empty
    finally:
        if conn is not None:
            conn.close()


# Leads

def create_lead(**kwargs) -> dict[str, Any]:
    defaults = {
        "campaign_id": None,
        "phone": "",
        "name": "",
        "email": "",
        "custom_data": {},
        "status": "pending",
        "call_attempts": 0,
        "last_call_at": None,
    }
    defaults.update({k: v for k, v in kwargs.items() if k in defaults and v is not None})

    if not defaults["campaign_id"] or not defaults["phone"]:
        logger.error("create_lead requires campaign_id and phone")
        return {}

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO leads
                (campaign_id, phone, name, email, custom_data, status, call_attempts, last_call_at)
                VALUES (%s::uuid, %s, %s, %s, %s::jsonb, %s, %s, %s)
                RETURNING *
                """,
                (
                    defaults["campaign_id"],
                    defaults["phone"],
                    defaults["name"],
                    defaults["email"],
                    json.dumps(defaults["custom_data"] or {}),
                    defaults["status"],
                    defaults["call_attempts"],
                    defaults["last_call_at"],
                ),
            )
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("create_lead failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def bulk_create_leads(campaign_id: str, leads: list[dict[str, Any]]) -> int:
    if not leads:
        return 0

    records: list[tuple[Any, ...]] = []
    for lead in leads:
        phone = (str(lead.get("phone", "")).strip())
        if not phone:
            continue

        custom_data = lead.get("custom_data", {})
        if isinstance(custom_data, str):
            try:
                custom_data = json.loads(custom_data)
            except Exception:
                custom_data = {"raw": custom_data}

        records.append(
            (
                campaign_id,
                phone,
                lead.get("name"),
                lead.get("email"),
                Json(custom_data or {}),
                lead.get("status", "pending"),
                int(lead.get("call_attempts", 0) or 0),
                lead.get("last_call_at"),
            )
        )

    if not records:
        return 0

    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO leads
                (campaign_id, phone, name, email, custom_data, status, call_attempts, last_call_at)
                VALUES %s
                """,
                records,
                template="(%s::uuid, %s, %s, %s, %s, %s, %s, %s)",
            )
        return len(records)
    except Exception as exc:
        logger.exception("bulk_create_leads failed: %s", exc)
        return 0
    finally:
        if conn is not None:
            conn.close()


def get_leads(campaign_id: str, status: str | None = None, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
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
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_leads failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


def update_lead_status(id: str, status: str) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            if status == "calling":
                cur.execute(
                    """
                    UPDATE leads
                    SET status=%s, call_attempts=call_attempts+1, last_call_at=now()
                    WHERE id=%s::uuid
                    """,
                    (status, id),
                )
            else:
                cur.execute("UPDATE leads SET status=%s WHERE id=%s::uuid", (status, id))
            return cur.rowcount > 0
    except Exception as exc:
        logger.exception("update_lead_status failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


def get_next_pending_leads(campaign_id: str, limit: int = 10) -> list[dict[str, Any]]:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM leads
                WHERE campaign_id=%s::uuid AND status='pending'
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (campaign_id, limit),
            )
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_next_pending_leads failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


# Calls

def create_call(
    lead_id: str | None,
    campaign_id: str | None,
    agent_id: str | None,
    sip_trunk_id: str | None,
    phone: str,
    room_id: str,
) -> dict[str, Any]:
    conn = None
    try:
        now_ist = datetime.now()
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO calls
                (lead_id, campaign_id, agent_id, sip_trunk_id, phone, room_id,
                 status, call_date, call_hour)
                VALUES (%s::uuid, %s::uuid, %s::uuid, %s::uuid, %s, %s,
                        'initiated', %s, %s)
                RETURNING *
                """,
                (
                    lead_id,
                    campaign_id,
                    agent_id,
                    sip_trunk_id,
                    phone,
                    room_id,
                    now_ist.date().isoformat(),
                    now_ist.hour,
                ),
            )
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("create_call failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def update_call(room_id: str, **kwargs) -> bool:
    allowed = {
        "lead_id",
        "campaign_id",
        "agent_id",
        "sip_trunk_id",
        "phone",
        "status",
        "duration_seconds",
        "transcript",
        "summary",
        "sentiment",
        "recording_url",
        "was_booked",
        "interrupt_count",
        "estimated_cost_usd",
        "call_date",
        "call_hour",
    }
    values = _filter_allowed(kwargs, allowed)
    if not values:
        return False

    conn = None
    try:
        clause, params = _build_update_clause(values)
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(f"UPDATE calls SET {clause} WHERE room_id=%s", params + [room_id])
            return cur.rowcount > 0
    except Exception as exc:
        logger.exception("update_call failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


def get_calls(campaign_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            if campaign_id:
                cur.execute(
                    """
                    SELECT * FROM calls
                    WHERE campaign_id=%s::uuid
                    ORDER BY created_at DESC
                    LIMIT %s
                    """,
                    (campaign_id, limit),
                )
            else:
                cur.execute("SELECT * FROM calls ORDER BY created_at DESC LIMIT %s", (limit,))
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_calls failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


def get_call_by_room(room_id: str) -> dict[str, Any] | None:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM calls WHERE room_id=%s LIMIT 1", (room_id,))
            return _normalize_row(cur.fetchone())
    except Exception as exc:
        logger.exception("get_call_by_room failed: %s", exc)
        return None
    finally:
        if conn is not None:
            conn.close()


# Transcript lines

def append_transcript_line(room_id: str, phone: str | None, role: str, content: str) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO call_transcript_lines (room_id, phone, role, content)
                VALUES (%s, %s, %s, %s)
                """,
                (room_id, phone, role, content),
            )
            return True
    except Exception as exc:
        logger.exception("append_transcript_line failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


# Active calls

def upsert_active_call(
    room_id: str,
    phone: str,
    campaign_id: str | None,
    agent_id: str | None,
    status: str,
    sip_trunk_id: str | None = None,
) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO active_calls (room_id, phone, campaign_id, agent_id, sip_trunk_id, status, started_at, last_updated)
                VALUES (%s, %s, %s::uuid, %s::uuid, %s::uuid, %s, now(), now())
                ON CONFLICT (room_id)
                DO UPDATE SET
                    phone=EXCLUDED.phone,
                    campaign_id=EXCLUDED.campaign_id,
                    agent_id=EXCLUDED.agent_id,
                    sip_trunk_id=EXCLUDED.sip_trunk_id,
                    status=EXCLUDED.status,
                    last_updated=now()
                """,
                (room_id, phone, campaign_id, agent_id, sip_trunk_id, status),
            )
            return True
    except Exception as exc:
        logger.exception("upsert_active_call failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


def remove_active_call(room_id: str) -> bool:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM active_calls WHERE room_id=%s", (room_id,))
            return cur.rowcount > 0
    except Exception as exc:
        logger.exception("remove_active_call failed: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


def get_active_calls() -> list[dict[str, Any]]:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM active_calls ORDER BY started_at DESC")
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_active_calls failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


# Bookings

def create_booking(
    call_id: str,
    caller_name: str,
    caller_phone: str,
    caller_email: str,
    start_time: str,
    notes: str,
) -> dict[str, Any]:
    conn = None
    try:
        conn = get_conn()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bookings
                (call_id, caller_name, caller_phone, caller_email, start_time, notes)
                VALUES (%s::uuid, %s, %s, %s, %s::timestamptz, %s)
                RETURNING *
                """,
                (call_id, caller_name, caller_phone, caller_email, start_time, notes),
            )
            return _normalize_row(cur.fetchone()) or {}
    except Exception as exc:
        logger.exception("create_booking failed: %s", exc)
        return {}
    finally:
        if conn is not None:
            conn.close()


def get_bookings(limit: int = 50) -> list[dict[str, Any]]:
    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM bookings ORDER BY created_at DESC LIMIT %s", (limit,))
            return _normalize_rows(cur.fetchall())
    except Exception as exc:
        logger.exception("get_bookings failed: %s", exc)
        return []
    finally:
        if conn is not None:
            conn.close()


# Stats

def get_platform_stats() -> dict[str, int]:
    empty = {
        "total_calls": 0,
        "active_calls": 0,
        "total_leads": 0,
        "total_campaigns": 0,
        "bookings_today": 0,
    }

    conn = None
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*)::int FROM calls) AS total_calls,
                    (SELECT COUNT(*)::int FROM active_calls WHERE status IN ('active','ringing','calling')) AS active_calls,
                    (SELECT COUNT(*)::int FROM leads) AS total_leads,
                    (SELECT COUNT(*)::int FROM campaigns) AS total_campaigns,
                    (
                        SELECT COUNT(*)::int
                        FROM bookings
                        WHERE DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
                    ) AS bookings_today
                """
            )
            row = _normalize_row(cur.fetchone())
            return {**empty, **(row or {})}
    except Exception as exc:
        logger.exception("get_platform_stats failed: %s", exc)
        return empty
    finally:
        if conn is not None:
            conn.close()
