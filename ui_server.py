import asyncio
import csv
import io
import json
import logging
import os
import re
import uuid
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import File, FastAPI, HTTPException, Query, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from livekit import api as lk_api
from livekit.api import AccessToken, VideoGrants
from pydantic import BaseModel

load_dotenv()

import db

app = FastAPI(title="RapidXAI Cold Calling API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    try:
        db.initdb()
        logging.info("STARTUP: DB init complete")
    except Exception as e:
        logging.warning(f"STARTUP: DB init failed: {e}")


class BusinessCreate(BaseModel):
    name: str
    description: Optional[str] = None
    website: Optional[str] = None
    timezone: str = "Asia/Kolkata"
    whatsapp_instance: Optional[str] = None
    whatsapp_token: Optional[str] = None


class BusinessUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    website: Optional[str] = None
    timezone: Optional[str] = None
    whatsapp_instance: Optional[str] = None
    whatsapp_token: Optional[str] = None


class SIPTrunkCreate(BaseModel):
    business_id: str
    name: str
    trunk_id: str
    phone_number: Optional[str] = None
    number_pool: List[str] = []
    max_concurrent_calls: int = 5
    calls_per_minute: int = 10


class SIPTrunkUpdate(BaseModel):
    name: Optional[str] = None
    phone_number: Optional[str] = None
    number_pool: Optional[List[str]] = None
    max_concurrent_calls: Optional[int] = None
    calls_per_minute: Optional[int] = None
    is_active: Optional[bool] = None


class AgentCreate(BaseModel):
    business_id: str
    name: str
    subtitle: Optional[str] = None
    stt_provider: str = "sarvam"
    stt_language: str = "hi-IN"
    stt_model: str = "saaras:v3"
    llm_provider: str = "openai"
    llm_model: str = "gpt-4.1-mini"
    llm_base_url: Optional[str] = None
    llm_temperature: float = 0.7
    llm_max_tokens: int = 60
    tts_provider: str = "sarvam"
    tts_voice: str = "rohan"
    tts_language: str = "hi-IN"
    system_prompt: str
    first_line: str
    agent_instructions: Optional[str] = None
    max_turns: int = 25
    silence_threshold_seconds: int = 20
    max_nudges: int = 2


class AgentUpdate(BaseModel):
    name: Optional[str] = None
    subtitle: Optional[str] = None
    stt_provider: Optional[str] = None
    stt_language: Optional[str] = None
    stt_model: Optional[str] = None
    llm_model: Optional[str] = None
    llm_temperature: Optional[float] = None
    llm_max_tokens: Optional[int] = None
    tts_provider: Optional[str] = None
    tts_voice: Optional[str] = None
    tts_language: Optional[str] = None
    system_prompt: Optional[str] = None
    first_line: Optional[str] = None
    agent_instructions: Optional[str] = None
    max_turns: Optional[int] = None
    is_active: Optional[bool] = None


class CampaignCreate(BaseModel):
    business_id: str
    agent_id: str
    sip_trunk_id: str
    name: str
    description: Optional[str] = None
    objective: Optional[str] = None
    calls_per_minute: int = 3
    max_concurrent_calls: int = 5
    retry_failed: bool = True
    max_retries: int = 2
    retry_delay_minutes: int = 60
    call_window_start: str = "09:00"
    call_window_end: str = "20:00"
    timezone: str = "Asia/Kolkata"
    custom_script: Optional[str] = None
    scheduled_start_at: Optional[str] = None


class CampaignUpdate(BaseModel):
    name: Optional[str] = None
    agent_id: Optional[str] = None
    sip_trunk_id: Optional[str] = None
    objective: Optional[str] = None
    calls_per_minute: Optional[int] = None
    max_concurrent_calls: Optional[int] = None
    retry_failed: Optional[bool] = None
    max_retries: Optional[int] = None
    call_window_start: Optional[str] = None
    call_window_end: Optional[str] = None
    custom_script: Optional[str] = None
    status: Optional[str] = None


class TriggerCallBody(BaseModel):
    phone: str
    agent_id: str
    campaign_id: Optional[str] = None
    sip_trunk_id: str
    business_id: str
    script_override: Optional[str] = None
    lead_name: Optional[str] = None


class TestCallBatch(BaseModel):
    phones: List[str]
    agent_id: str
    sip_trunk_id: str
    business_id: str
    campaign_id: Optional[str] = None


class RescheduleLeadBody(BaseModel):
    lead_id: str
    next_call_at: str
    script_id: Optional[str] = None
    reason: Optional[str] = None


class KnowledgeCreate(BaseModel):
    business_id: str
    agent_id: str
    category: str
    title: str
    content: str


class ScriptCreate(BaseModel):
    business_id: str
    name: str
    system_prompt: str
    first_line: str


class ScriptUpdate(BaseModel):
    name: Optional[str] = None
    system_prompt: Optional[str] = None
    first_line: Optional[str] = None


class LeadUpdate(BaseModel):
    phone: Optional[str] = None
    name: Optional[str] = None
    email: Optional[str] = None
    language: Optional[str] = None
    custom_data: Optional[dict] = None
    status: Optional[str] = None
    call_attempts: Optional[int] = None
    max_call_attempts: Optional[int] = None
    next_call_at: Optional[str] = None
    notes: Optional[str] = None


class DemoStartBody(BaseModel):
    agent_id: str
    business_id: str


def _lk_client() -> lk_api.LiveKitAPI:
    return lk_api.LiveKitAPI(
        url=os.environ["LIVEKIT_URL"],
        api_key=os.environ["LIVEKIT_API_KEY"],
        api_secret=os.environ["LIVEKIT_API_SECRET"],
    )


def _normalize_phone(phone: str) -> str:
    return (phone or "").strip().replace(" ", "")


async def dispatch_outbound_call(
    phone: str,
    agent_id: str,
    sip_trunk_id: str,
    business_id: str,
    campaign_id: Optional[str],
    lead_id: Optional[str],
    script_override: Optional[str],
    call_attempt_number: int = 1,
) -> dict:
    lk = _lk_client()
    try:
        trunk = db.get_sip_trunk(sip_trunk_id)
        if not trunk:
            raise RuntimeError("SIP trunk not found")
        mask_number = db.pick_mask_number(sip_trunk_id)
        room_name = f"_{phone}_{uuid.uuid4().hex[:8]}"

        await lk.room.create_room(lk_api.CreateRoomRequest(name=room_name))

        metadata = json.dumps(
            {
                "phone_number": phone,
                "from_number": mask_number or trunk.get("phone_number"),
                "agent_id": agent_id,
                "campaign_id": campaign_id,
                "lead_id": lead_id,
                "business_id": business_id,
                "sip_trunk_id": sip_trunk_id,
                "script_override": script_override,
                "call_attempt_number": call_attempt_number,
            }
        )

        await lk.sip.create_sip_participant(
            lk_api.CreateSIPParticipantRequest(
                room_name=room_name,
                sip_trunk_id=trunk["trunk_id"],
                sip_call_to=phone,
                participant_identity=f"sip_{phone.replace('+', '')}",
                participant_name="Caller",
                wait_until_answered=True,
            )
        )

        await lk.agent_dispatch.create_dispatch(
            lk_api.CreateAgentDispatchRequest(room=room_name, agent_name="outbound-caller", metadata=metadata)
        )

        return {"room_id": room_name, "status": "dispatched"}
    finally:
        await lk.aclose()


def _split_chunks(text: str, words_per_chunk: int = 300) -> list[str]:
    words = (text or "").split()
    chunks = []
    for i in range(0, len(words), words_per_chunk):
        chunk = " ".join(words[i : i + words_per_chunk]).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


def _extract_pdf_text(data: bytes) -> str:
    content = data.decode("latin-1", errors="ignore")
    parts = re.findall(r"\(([^\)]{1,500})\)\s*Tj", content)
    if parts:
        return "\n".join(parts)
    return ""


# Businesses

@app.post("/api/businesses")
def create_business(body: BusinessCreate):
    row = db.create_business(**body.model_dump())
    if not row:
        raise HTTPException(status_code=400, detail="Failed to create business")
    return row


@app.get("/api/businesses")
def get_businesses():
    return db.get_businesses()


@app.get("/api/businesses/{id}")
def get_business(id: str):
    row = db.get_business(id)
    if not row:
        raise HTTPException(status_code=404, detail="Business not found")
    return row


@app.put("/api/businesses/{id}")
def update_business(id: str, body: BusinessUpdate):
    row = db.update_business(id, **body.model_dump(exclude_none=True))
    if not row:
        raise HTTPException(status_code=404, detail="Business not found or no updates")
    return row


@app.delete("/api/businesses/{id}")
def delete_business(id: str):
    ok = db.delete_business(id)
    if not ok:
        raise HTTPException(status_code=404, detail="Business not found")
    return {"success": True}


# SIP Trunks

@app.post("/api/sip-trunks")
def create_sip_trunk(body: SIPTrunkCreate):
    row = db.create_sip_trunk(**body.model_dump())
    if not row:
        raise HTTPException(status_code=400, detail="Failed to create SIP trunk")
    return row


@app.get("/api/sip-trunks")
def get_sip_trunks(business_id: Optional[str] = Query(default=None)):
    return db.get_sip_trunks(business_id=business_id)


@app.get("/api/sip-trunks/{id}")
def get_sip_trunk(id: str):
    row = db.get_sip_trunk(id)
    if not row:
        raise HTTPException(status_code=404, detail="SIP trunk not found")
    return row


@app.put("/api/sip-trunks/{id}")
def update_sip_trunk(id: str, body: SIPTrunkUpdate):
    row = db.update_sip_trunk(id, **body.model_dump(exclude_none=True))
    if not row:
        raise HTTPException(status_code=404, detail="SIP trunk not found or no updates")
    return row


@app.delete("/api/sip-trunks/{id}")
def delete_sip_trunk(id: str):
    ok = db.delete_sip_trunk(id)
    if not ok:
        raise HTTPException(status_code=404, detail="SIP trunk not found")
    return {"success": True}


# Agents

@app.post("/api/agents")
def create_agent(body: AgentCreate):
    row = db.create_agent(**body.model_dump())
    if not row:
        raise HTTPException(status_code=400, detail="Failed to create agent")
    return row


@app.get("/api/agents")
def get_agents(business_id: Optional[str] = Query(default=None)):
    return db.get_agents(business_id=business_id)


@app.get("/api/agents/{id}")
def get_agent(id: str):
    row = db.get_agent(id)
    if not row:
        raise HTTPException(status_code=404, detail="Agent not found")
    return row


@app.put("/api/agents/{id}")
def update_agent(id: str, body: AgentUpdate):
    row = db.update_agent(id, **body.model_dump(exclude_none=True))
    if not row:
        raise HTTPException(status_code=404, detail="Agent not found or no updates")
    return row


@app.delete("/api/agents/{id}")
def delete_agent(id: str):
    ok = db.delete_agent(id)
    if not ok:
        raise HTTPException(status_code=404, detail="Agent not found")
    return {"success": True}


@app.post("/api/agents/{id}/activate")
def activate_agent(id: str):
    ok = db.set_active_agent(id)
    if not ok:
        raise HTTPException(status_code=404, detail="Agent not found")
    return {"success": True}


# Campaigns

@app.post("/api/campaigns")
def create_campaign(body: CampaignCreate):
    row = db.create_campaign(**body.model_dump())
    if not row:
        raise HTTPException(status_code=400, detail="Failed to create campaign")
    return row


@app.get("/api/campaigns")
def get_campaigns(business_id: Optional[str] = Query(default=None)):
    return db.get_campaigns(business_id=business_id)


@app.get("/api/campaigns/{id}")
def get_campaign(id: str):
    row = db.get_campaign(id)
    if not row:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return row


@app.get("/api/campaigns/{id}/stats")
def get_campaign_stats(id: str):
    campaign = db.get_campaign(id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return db.get_campaign_stats(id)


@app.put("/api/campaigns/{id}")
def update_campaign(id: str, body: CampaignUpdate):
    row = db.update_campaign(id, **body.model_dump(exclude_none=True))
    if not row:
        raise HTTPException(status_code=404, detail="Campaign not found or no updates")
    return row


@app.delete("/api/campaigns/{id}")
def delete_campaign(id: str):
    ok = db.delete_campaign(id)
    if not ok:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return {"success": True}


@app.post("/api/campaigns/{id}/start")
async def start_campaign(id: str):
    campaign = db.get_campaign(id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    db.update_campaign(id, status="active", started_at=datetime_now_iso())
    import dialer

    asyncio.create_task(dialer.start_campaign(id))
    return {"status": "started"}


@app.post("/api/campaigns/{id}/pause")
async def pause_campaign(id: str):
    campaign = db.get_campaign(id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    db.update_campaign(id, status="paused")
    import dialer

    asyncio.create_task(dialer.stop_campaign(id))
    return {"status": "paused"}


@app.post("/api/campaigns/{id}/stop")
async def stop_campaign(id: str):
    campaign = db.get_campaign(id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    db.update_campaign(id, status="completed", completed_at=datetime_now_iso())
    import dialer

    asyncio.create_task(dialer.stop_campaign(id))
    return {"status": "stopped"}


# Leads

@app.post("/api/campaigns/{id}/leads/upload")
async def upload_leads(id: str, file: UploadFile = File(...)):
    campaign = db.get_campaign(id)
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")

    text = raw.decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    leads = []

    for row in reader:
        phone = _normalize_phone(row.get("phone") or row.get("Phone") or "")
        if not phone:
            continue
        custom_data = row.get("custom_data")
        parsed_custom = {}
        if custom_data:
            try:
                parsed_custom = json.loads(custom_data)
            except Exception:
                parsed_custom = {"raw": custom_data}

        leads.append(
            {
                "phone": phone,
                "name": row.get("name"),
                "email": row.get("email"),
                "language": row.get("language") or "hi-IN",
                "custom_data": parsed_custom,
            }
        )

    inserted = db.bulk_create_leads(id, campaign["business_id"], leads)
    return {"inserted": inserted}


@app.get("/api/campaigns/{id}/leads")
def get_campaign_leads(id: str, status: Optional[str] = Query(default=None), limit: int = 100, offset: int = 0):
    return db.get_leads(id, status=status, limit=limit, offset=offset)


@app.put("/api/leads/{id}")
def update_lead(id: str, body: LeadUpdate):
    ok = db.update_lead(id, **body.model_dump(exclude_none=True))
    if not ok:
        raise HTTPException(status_code=404, detail="Lead not found or no updates")
    return {"success": True}


@app.post("/api/leads/reschedule")
def reschedule_lead(body: RescheduleLeadBody):
    ok = db.reschedule_lead(body.lead_id, body.next_call_at, script_id=body.script_id, reason=body.reason)
    if not ok:
        raise HTTPException(status_code=400, detail="Failed to reschedule lead")
    return {"success": True}


# Calls

@app.get("/api/calls")
def get_calls(
    campaign_id: Optional[str] = Query(default=None),
    business_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    return db.get_calls(campaign_id=campaign_id, business_id=business_id, limit=limit, offset=offset)


@app.get("/api/calls/{room_id}/transcript")
def get_call_transcript(room_id: str):
    return db.get_transcript(room_id)


@app.get("/api/calls/{room_id}")
def get_call(room_id: str):
    row = db.get_call_by_room(room_id)
    if not row:
        raise HTTPException(status_code=404, detail="Call not found")
    return row


@app.get("/api/leads/{id}/calls")
def get_lead_calls(id: str):
    return db.get_call_history_for_lead(id)


# Trigger calls

@app.post("/api/calls/trigger")
async def trigger_call(body: TriggerCallBody):
    phone = _normalize_phone(body.phone)
    if not phone.startswith("+"):
        raise HTTPException(status_code=400, detail="Phone must be E.164 format")

    agent_cfg = db.get_agent(body.agent_id)
    if not agent_cfg:
        raise HTTPException(status_code=404, detail="Agent not found")

    trunk = db.get_sip_trunk(body.sip_trunk_id)
    if not trunk:
        raise HTTPException(status_code=404, detail="SIP trunk not found")

    lead_id = None
    if body.campaign_id:
        lead = db.create_lead(
            campaign_id=body.campaign_id,
            business_id=body.business_id,
            phone=phone,
            name=body.lead_name,
            status="calling",
        )
        lead_id = str(lead.get("id")) if lead else None

    result = await dispatch_outbound_call(
        phone=phone,
        agent_id=body.agent_id,
        sip_trunk_id=body.sip_trunk_id,
        business_id=body.business_id,
        campaign_id=body.campaign_id,
        lead_id=lead_id,
        script_override=body.script_override,
        call_attempt_number=1,
    )

    room_id = result["room_id"]
    db.create_call(
        lead_id=lead_id,
        campaign_id=body.campaign_id,
        agent_id=body.agent_id,
        sip_trunk_id=body.sip_trunk_id,
        business_id=body.business_id,
        phone=phone,
        room_id=room_id,
        call_attempt_number=1,
    )
    db.upsert_active_call(room_id, phone, body.campaign_id, body.agent_id, body.business_id, body.sip_trunk_id, "active")

    return {"room_id": room_id, "status": "dispatched"}


@app.post("/api/calls/test-batch")
async def test_batch(body: TestCallBatch):
    phones = [_normalize_phone(p) for p in body.phones if _normalize_phone(p)]
    if len(phones) > 10:
        raise HTTPException(status_code=400, detail="Max 10 phones allowed")

    dispatched = []
    for phone in phones:
        if not phone.startswith("+"):
            continue
        result = await dispatch_outbound_call(
            phone=phone,
            agent_id=body.agent_id,
            sip_trunk_id=body.sip_trunk_id,
            business_id=body.business_id,
            campaign_id=body.campaign_id,
            lead_id=None,
            script_override=None,
            call_attempt_number=1,
        )
        db.create_call(
            lead_id=None,
            campaign_id=body.campaign_id,
            agent_id=body.agent_id,
            sip_trunk_id=body.sip_trunk_id,
            business_id=body.business_id,
            phone=phone,
            room_id=result["room_id"],
            call_attempt_number=1,
        )
        db.upsert_active_call(result["room_id"], phone, body.campaign_id, body.agent_id, body.business_id, body.sip_trunk_id, "active")
        dispatched.append(phone)
        await asyncio.sleep(1)

    return {"dispatched": len(dispatched), "phones": dispatched}


# Scripts

@app.post("/api/scripts")
def create_script(body: ScriptCreate):
    row = db.create_script(**body.model_dump())
    if not row:
        raise HTTPException(status_code=400, detail="Failed to create script")
    return row


@app.get("/api/scripts")
def get_scripts(business_id: Optional[str] = Query(default=None)):
    return db.get_scripts(business_id=business_id)


@app.get("/api/scripts/{id}")
def get_script(id: str):
    row = db.get_script(id)
    if not row:
        raise HTTPException(status_code=404, detail="Script not found")
    return row


@app.put("/api/scripts/{id}")
def update_script(id: str, body: ScriptUpdate):
    row = db.update_script(id, **body.model_dump(exclude_none=True))
    if not row:
        raise HTTPException(status_code=404, detail="Script not found or no updates")
    return row


@app.delete("/api/scripts/{id}")
def delete_script(id: str):
    ok = db.delete_script(id)
    if not ok:
        raise HTTPException(status_code=404, detail="Script not found")
    return {"success": True}


# Knowledge

@app.post("/api/knowledge")
async def create_knowledge(body: KnowledgeCreate):
    from openai import AsyncOpenAI

    try:
        client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
        emb = await client.embeddings.create(model="text-embedding-3-small", input=body.content)
        vector = emb.data[0].embedding
        row = db.insert_knowledge(body.business_id, body.agent_id, body.category, body.title, body.content, vector)
        if not row:
            raise HTTPException(status_code=400, detail="Failed to save knowledge")
        return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Knowledge embed failed: {e}")


@app.get("/api/knowledge")
def get_knowledge(agent_id: str = Query(...)):
    return db.get_knowledge_items(agent_id)


@app.delete("/api/knowledge/{id}")
def delete_knowledge(id: str):
    ok = db.delete_knowledge_item(id)
    if not ok:
        raise HTTPException(status_code=404, detail="Knowledge item not found")
    return {"success": True}


@app.post("/api/knowledge/upload-file")
async def upload_knowledge_file(
    business_id: str = Query(...),
    agent_id: str = Query(...),
    category: str = Query("faq"),
    file: UploadFile = File(...),
):
    from openai import AsyncOpenAI

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    text = ""
    if (file.filename or "").lower().endswith(".txt"):
        text = content.decode("utf-8-sig", errors="ignore")
    elif (file.filename or "").lower().endswith(".pdf"):
        text = _extract_pdf_text(content)
    else:
        raise HTTPException(status_code=400, detail="Only .txt or .pdf supported")

    chunks = _split_chunks(text, words_per_chunk=300)
    if not chunks:
        raise HTTPException(status_code=400, detail="No readable text found")

    client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
    inserted = 0
    for i, chunk in enumerate(chunks, start=1):
        emb = await client.embeddings.create(model="text-embedding-3-small", input=chunk)
        vector = emb.data[0].embedding
        title = f"{file.filename} - chunk {i}"
        saved = db.insert_knowledge(business_id, agent_id, category, title, chunk, vector)
        if saved:
            inserted += 1

    return {"chunks_inserted": inserted}


# Bookings

@app.get("/api/bookings")
def get_bookings(business_id: Optional[str] = Query(default=None)):
    return db.get_bookings(business_id=business_id)


# Live / Stats

@app.get("/api/stats")
def get_stats(business_id: Optional[str] = Query(default=None)):
    return db.get_platform_stats(business_id=business_id)


@app.get("/api/active-calls")
def get_active_calls(business_id: Optional[str] = Query(default=None)):
    return db.get_active_calls(business_id=business_id)


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            active = db.get_active_calls()
            stats = db.get_platform_stats()
            await ws.send_json({"active_calls": active, "stats": stats})
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        return
    except Exception:
        try:
            await ws.close()
        except Exception:
            return


# Demo

@app.post("/api/demo/start")
async def demo_start(body: DemoStartBody):
    room_name = f"demo-{uuid.uuid4().hex[:8]}"
    lk = _lk_client()
    try:
        await lk.room.create_room(lk_api.CreateRoomRequest(name=room_name))

        token = (
            AccessToken(os.environ["LIVEKIT_API_KEY"], os.environ["LIVEKIT_API_SECRET"])
            .with_identity(f"demo-{uuid.uuid4().hex[:6]}")
            .with_name("Demo Caller")
            .with_grants(VideoGrants(room_join=True, room=room_name))
            .with_ttl(3600)
            .to_jwt()
        )

        await lk.agent_dispatch.create_dispatch(
            lk_api.CreateAgentDispatchRequest(
                room=room_name,
                agent_name="outbound-caller",
                metadata="demo",
            )
        )

        return {"room_name": room_name, "token": token, "livekit_url": os.environ["LIVEKIT_URL"]}
    finally:
        await lk.aclose()


def datetime_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


@app.get("/health")
def health():
    return JSONResponse({"status": "ok"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("ui_server:app", host="0.0.0.0", port=8000, reload=False)
