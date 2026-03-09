import asyncio
import csv
import io
import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, File, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from livekit import api as lkapi
from livekit.api import AccessToken, VideoGrants
from pydantic import BaseModel, Field

import db
from db import get_conn, initdb
from dialer import start_campaign_dialer, stop_campaign_dialer

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ui-server")

try:
    if initdb():
        logging.info("STARTUP: DB init complete")
    else:
        logging.warning("STARTUP: DB init failed")
except Exception as e:
    logging.warning(f"STARTUP: DB init failed: {e}")

app = FastAPI(title="RapidXAI Cold Calling Platform")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SIPTrunkCreate(BaseModel):
    name: str
    trunk_id: str
    phone_number: str | None = None
    number_pool: list[str] = Field(default_factory=list)
    max_concurrent_calls: int = 5
    calls_per_minute: int = 10


class SIPTrunkUpdate(BaseModel):
    name: str | None = None
    trunk_id: str | None = None
    phone_number: str | None = None
    number_pool: list[str] | None = None
    max_concurrent_calls: int | None = None
    calls_per_minute: int | None = None
    is_active: bool | None = None


class AgentCreate(BaseModel):
    name: str
    subtitle: str | None = None
    stt_provider: str = "sarvam"
    stt_language: str = "hi-IN"
    stt_model: str = "saaras:v3"
    llm_provider: str = "openai"
    llm_model: str = "gpt-4o-mini"
    llm_base_url: str | None = None
    llm_temperature: float = 0.7
    llm_max_tokens: int = 150
    tts_provider: str = "sarvam"
    tts_voice: str = "rohan"
    tts_language: str = "hi-IN"
    system_prompt: str = ""
    first_line: str = ""
    opening_greeting: str = ""
    agent_instructions: str = ""
    max_turns: int = 25
    is_active: bool = False


class AgentUpdate(BaseModel):
    name: str | None = None
    subtitle: str | None = None
    stt_provider: str | None = None
    stt_language: str | None = None
    stt_model: str | None = None
    llm_provider: str | None = None
    llm_model: str | None = None
    llm_base_url: str | None = None
    llm_temperature: float | None = None
    llm_max_tokens: int | None = None
    tts_provider: str | None = None
    tts_voice: str | None = None
    tts_language: str | None = None
    system_prompt: str | None = None
    first_line: str | None = None
    opening_greeting: str | None = None
    agent_instructions: str | None = None
    max_turns: int | None = None
    is_active: bool | None = None


class CampaignCreate(BaseModel):
    name: str
    agent_id: str | None = None
    sip_trunk_id: str | None = None
    status: str = "draft"
    calls_per_minute: int = 5
    retry_failed: bool = True
    max_retries: int = 2
    call_window_start: str = "09:00"
    call_window_end: str = "20:00"
    timezone: str = "Asia/Kolkata"


class CampaignUpdate(BaseModel):
    name: str | None = None
    agent_id: str | None = None
    sip_trunk_id: str | None = None
    status: str | None = None
    calls_per_minute: int | None = None
    retry_failed: bool | None = None
    max_retries: int | None = None
    call_window_start: str | None = None
    call_window_end: str | None = None
    timezone: str | None = None
    started_at: str | None = None
    completed_at: str | None = None


class LeadStatusUpdate(BaseModel):
    status: str


class DemoStartRequest(BaseModel):
    identity: str = "demo-user"
    name: str = "Demo User"


class CallTriggerRequest(BaseModel):
    phone: str
    agent_id: str | None = None
    campaign_id: str | None = None
    sip_trunk_id: str | None = None


def _lk_client() -> lkapi.LiveKitAPI:
    url = os.getenv("LIVEKIT_URL", "").strip()
    key = os.getenv("LIVEKIT_API_KEY", "").strip()
    secret = os.getenv("LIVEKIT_API_SECRET", "").strip()

    if not all([url, key, secret]):
        raise RuntimeError("LIVEKIT_URL/LIVEKIT_API_KEY/LIVEKIT_API_SECRET are required")

    return lkapi.LiveKitAPI(url=url, api_key=key, api_secret=secret)


def _find_sip_trunk_by_id(trunk_id: str) -> dict[str, Any] | None:
    trunks = db.get_sip_trunks()
    for trunk in trunks:
        if str(trunk.get("id")) == str(trunk_id):
            return trunk
    return None


async def _dispatch_outbound_call(
    phone: str,
    agent_id: str | None,
    campaign_id: str | None,
    sip_trunk: dict[str, Any],
) -> dict[str, Any]:
    lk = _lk_client()
    room_name = f"call-{phone.replace('+', '')}-{random.randint(1000, 9999)}"
    dispatch_id = ""

    metadata = json.dumps(
        {
            "phone_number": phone,
            "campaign_id": campaign_id,
            "agent_id": agent_id,
            "sip_trunk_id": str(sip_trunk.get("id")),
            "sip_trunk_livekit_id": sip_trunk.get("trunk_id"),
        }
    )

    try:
        await lk.sip.create_sip_participant(
            lkapi.CreateSIPParticipantRequest(
                room_name=room_name,
                sip_trunk_id=sip_trunk["trunk_id"],
                sip_call_to=phone,
                participant_identity=f"sip_{phone.replace('+', '')}",
                participant_name=f"Lead {phone[-4:]}",
                wait_until_answered=False,
            )
        )

        dispatch = await lk.agent_dispatch.create_dispatch(
            lkapi.CreateAgentDispatchRequest(
                agent_name="outbound-caller",
                room=room_name,
                metadata=metadata,
            )
        )
        dispatch_id = dispatch.id
        return {
            "room": room_name,
            "dispatch_id": dispatch_id,
            "phone": phone,
        }
    finally:
        await lk.aclose()


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        conn = get_conn()
        conn.close()
        db_ok = True
    except Exception as exc:
        logger.warning("health db check failed: %s", exc)
        db_ok = False

    return {
        "status": "ok" if db_ok else "degraded",
        "db": db_ok,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# SIP Trunks

@app.post("/api/sip-trunks")
def create_sip_trunk(payload: SIPTrunkCreate):
    try:
        row = db.create_sip_trunk(**payload.model_dump())
        if not row:
            raise HTTPException(status_code=400, detail="Failed to create SIP trunk")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/sip-trunks failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/sip-trunks")
def get_sip_trunks():
    try:
        return db.get_sip_trunks()
    except Exception as exc:
        logger.exception("GET /api/sip-trunks failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/api/sip-trunks/{id}")
def update_sip_trunk(id: str, payload: SIPTrunkUpdate):
    try:
        row = db.update_sip_trunk(id, **payload.model_dump(exclude_none=True))
        if not row:
            raise HTTPException(status_code=404, detail="SIP trunk not found or no updates")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("PUT /api/sip-trunks/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/api/sip-trunks/{id}")
def delete_sip_trunk(id: str):
    try:
        ok = db.delete_sip_trunk(id)
        if not ok:
            raise HTTPException(status_code=404, detail="SIP trunk not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("DELETE /api/sip-trunks/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


# Agents

@app.post("/api/agents")
def create_agent(payload: AgentCreate):
    try:
        row = db.create_agent(**payload.model_dump())
        if not row:
            raise HTTPException(status_code=400, detail="Failed to create agent")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/agents failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/agents")
def get_agents():
    try:
        return db.get_agents()
    except Exception as exc:
        logger.exception("GET /api/agents failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/agents/{id}")
def get_agent(id: str):
    try:
        row = db.get_agent(id)
        if not row:
            raise HTTPException(status_code=404, detail="Agent not found")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("GET /api/agents/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/api/agents/{id}")
def update_agent(id: str, payload: AgentUpdate):
    try:
        row = db.update_agent(id, **payload.model_dump(exclude_none=True))
        if not row:
            raise HTTPException(status_code=404, detail="Agent not found or no updates")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("PUT /api/agents/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/api/agents/{id}")
def delete_agent(id: str):
    try:
        ok = db.delete_agent(id)
        if not ok:
            raise HTTPException(status_code=404, detail="Agent not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("DELETE /api/agents/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/agents/{id}/activate")
def activate_agent(id: str):
    try:
        ok = db.set_active_agent(id)
        if not ok:
            raise HTTPException(status_code=404, detail="Agent not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/agents/{id}/activate failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


# Campaigns

@app.post("/api/campaigns")
def create_campaign(payload: CampaignCreate):
    try:
        row = db.create_campaign(**payload.model_dump())
        if not row:
            raise HTTPException(status_code=400, detail="Failed to create campaign")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/campaigns failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/campaigns")
def get_campaigns():
    try:
        return db.get_campaigns()
    except Exception as exc:
        logger.exception("GET /api/campaigns failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/campaigns/{id}")
def get_campaign(id: str):
    try:
        row = db.get_campaign(id)
        if not row:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("GET /api/campaigns/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/campaigns/{id}/stats")
def get_campaign_stats(id: str):
    try:
        campaign = db.get_campaign(id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return db.get_campaign_stats(id)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("GET /api/campaigns/{id}/stats failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/api/campaigns/{id}")
def update_campaign(id: str, payload: CampaignUpdate):
    try:
        row = db.update_campaign(id, **payload.model_dump(exclude_none=True))
        if not row:
            raise HTTPException(status_code=404, detail="Campaign not found or no updates")
        return row
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("PUT /api/campaigns/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/campaigns/{id}/start")
async def start_campaign(id: str):
    try:
        campaign = db.get_campaign(id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        updated = db.update_campaign(id, status="active", started_at=datetime.now(timezone.utc).isoformat(), completed_at=None)
        if not updated:
            raise HTTPException(status_code=400, detail="Failed to activate campaign")

        await start_campaign_dialer(id)
        return {"success": True, "campaign_id": id, "status": "active"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/campaigns/{id}/start failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/campaigns/{id}/pause")
async def pause_campaign(id: str):
    try:
        campaign = db.get_campaign(id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        updated = db.update_campaign(id, status="paused")
        if not updated:
            raise HTTPException(status_code=400, detail="Failed to pause campaign")

        await stop_campaign_dialer(id)
        return {"success": True, "campaign_id": id, "status": "paused"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/campaigns/{id}/pause failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.delete("/api/campaigns/{id}")
def delete_campaign(id: str):
    try:
        ok = db.delete_campaign(id)
        if not ok:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("DELETE /api/campaigns/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


# Leads

@app.post("/api/campaigns/{id}/leads/upload")
async def upload_leads(id: str, file: UploadFile = File(...)):
    try:
        campaign = db.get_campaign(id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")

        content = await file.read()
        if not content:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        text = content.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))

        leads: list[dict[str, Any]] = []
        for row in reader:
            phone = (row.get("phone") or row.get("Phone") or row.get("mobile") or row.get("Mobile") or "").strip()
            if not phone:
                continue

            lead = {
                "phone": phone,
                "name": (row.get("name") or row.get("Name") or "").strip() or None,
                "email": (row.get("email") or row.get("Email") or "").strip() or None,
            }

            custom_data = {
                k: v
                for k, v in row.items()
                if k not in {"phone", "Phone", "mobile", "Mobile", "name", "Name", "email", "Email"}
            }
            if custom_data:
                lead["custom_data"] = custom_data

            leads.append(lead)

        inserted = db.bulk_create_leads(id, leads)
        return {"success": True, "inserted": inserted}
    except UnicodeDecodeError as exc:
        logger.exception("CSV decode failed: %s", exc)
        raise HTTPException(status_code=400, detail="Unable to decode CSV as UTF-8")
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/campaigns/{id}/leads/upload failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/campaigns/{id}/leads")
def get_leads(
    id: str,
    status: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
):
    try:
        campaign = db.get_campaign(id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return db.get_leads(id, status=status, limit=limit, offset=offset)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("GET /api/campaigns/{id}/leads failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.put("/api/leads/{id}")
def update_lead(id: str, payload: LeadStatusUpdate):
    try:
        ok = db.update_lead_status(id, payload.status)
        if not ok:
            raise HTTPException(status_code=404, detail="Lead not found")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("PUT /api/leads/{id} failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


# Calls

@app.get("/api/calls")
def get_calls(campaign_id: str | None = Query(default=None), limit: int = Query(default=50, ge=1, le=500)):
    try:
        return db.get_calls(campaign_id=campaign_id, limit=limit)
    except Exception as exc:
        logger.exception("GET /api/calls failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


# Live / Stats

@app.get("/api/stats")
def get_stats():
    try:
        return db.get_platform_stats()
    except Exception as exc:
        logger.exception("GET /api/stats failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/api/active-calls")
def get_active_calls():
    try:
        return db.get_active_calls()
    except Exception as exc:
        logger.exception("GET /api/active-calls failed: %s", exc)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.websocket("/ws/calls")
async def websocket_calls(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            await ws.send_json(db.get_active_calls())
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected: /ws/calls")
    except Exception as exc:
        logger.exception("WebSocket /ws/calls failed: %s", exc)
        await ws.close()


# Demo

@app.post("/api/demo/start")
async def demo_start(payload: DemoStartRequest):
    lk = None
    try:
        api_key = os.getenv("LIVEKIT_API_KEY", "")
        api_secret = os.getenv("LIVEKIT_API_SECRET", "")
        livekit_url = os.getenv("LIVEKIT_URL", "")
        if not all([api_key, api_secret, livekit_url]):
            raise HTTPException(status_code=500, detail="LiveKit credentials are missing")

        room_name = f"demo-{random.randint(10000, 99999)}"
        token = (
            AccessToken(api_key, api_secret)
            .with_identity(payload.identity)
            .with_name(payload.name)
            .with_grants(VideoGrants(room_join=True, room=room_name))
            .with_ttl(3600)
            .to_jwt()
        )

        lk = _lk_client()
        await lk.agent_dispatch.create_dispatch(
            lkapi.CreateAgentDispatchRequest(
                agent_name="outbound-caller",
                room=room_name,
                metadata="demo",
            )
        )

        return {
            "room": room_name,
            "token": token,
            "url": livekit_url,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/demo/start failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to start demo")
    finally:
        if lk is not None:
            await lk.aclose()


# Single outbound call trigger

@app.post("/api/calls/trigger")
async def trigger_call(payload: CallTriggerRequest):
    phone = payload.phone.strip()
    if not phone.startswith("+"):
        raise HTTPException(status_code=400, detail="phone must be E.164 format, e.g. +919999999999")

    try:
        if payload.sip_trunk_id:
            trunk = _find_sip_trunk_by_id(payload.sip_trunk_id)
        else:
            trunks = [t for t in db.get_sip_trunks() if t.get("is_active")]
            trunk = trunks[0] if trunks else None

        if not trunk:
            raise HTTPException(status_code=400, detail="No SIP trunk available")

        dispatch = await _dispatch_outbound_call(
            phone=phone,
            agent_id=payload.agent_id,
            campaign_id=payload.campaign_id,
            sip_trunk=trunk,
        )

        call = db.create_call(
            lead_id=None,
            campaign_id=payload.campaign_id,
            agent_id=payload.agent_id,
            sip_trunk_id=str(trunk.get("id")),
            phone=phone,
            room_id=dispatch["room"],
        )

        db.upsert_active_call(
            room_id=dispatch["room"],
            phone=phone,
            campaign_id=payload.campaign_id,
            agent_id=payload.agent_id,
            status="ringing",
            sip_trunk_id=str(trunk.get("id")),
        )

        return {
            "success": True,
            "dispatch_id": dispatch["dispatch_id"],
            "room": dispatch["room"],
            "call": call,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("POST /api/calls/trigger failed: %s", exc)
        raise HTTPException(status_code=500, detail="Failed to trigger call")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("ui_server:app", host="0.0.0.0", port=8000, reload=False)
