import asyncio
import csv
import io
import json
import logging
import os
import re
import signal
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import File, FastAPI, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from livekit import api as lk_api
from livekit.api import AccessToken, VideoGrants
from pydantic import BaseModel

load_dotenv()

from config.validator import assert_valid_env
from jobs.queue import job_queue
from logging_config import configure_logging

configure_logging()
assert_valid_env()

import db

app = FastAPI(title="RapidXAI Cold Calling API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


_live_clients: set[WebSocket] = set()
_live_filters: dict[WebSocket, Optional[str]] = {}
_live_broadcast_task: Optional[asyncio.Task] = None
_scheduled_campaigns_task: Optional[asyncio.Task] = None
_import_jobs: dict[str, dict] = {}
_reminder_task: Optional[asyncio.Task] = None
_spam_monitor_task: Optional[asyncio.Task] = None
_simulator_cleanup_task: Optional[asyncio.Task] = None
_simulator_sessions: dict[str, dict] = {}
_last_transcript_cursor: Optional[str] = None
_shutdown_event = asyncio.Event()
_signal_handlers_registered = False


def _success(data):
    return {"data": data, "error": None}


def _error(message: str, status_code: int = 400):
    return JSONResponse(status_code=status_code, content={"data": None, "error": message})


def register_signal_handlers():
    global _signal_handlers_registered
    if _signal_handlers_registered:
        return
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(on_shutdown()))
        except NotImplementedError:
            # add_signal_handler is not available on some platforms (e.g. Windows)
            continue
    _signal_handlers_registered = True


@app.on_event("startup")
async def startup_event():
    global _live_broadcast_task, _scheduled_campaigns_task, _reminder_task, _spam_monitor_task, _simulator_cleanup_task
    register_signal_handlers()
    _shutdown_event.clear()
    try:
        import campaign_runner

        campaign_runner.clear_shutdown()
    except Exception:
        pass
    try:
        db.initdb()
        logging.info("STARTUP: DB init complete")
    except Exception as e:
        logging.warning(f"STARTUP: DB init failed: {e}")
    await job_queue.start()
    logging.info("STARTUP: Job queue started")
    if _live_broadcast_task is None or _live_broadcast_task.done():
        _live_broadcast_task = asyncio.create_task(_live_broadcast_loop())
    if _scheduled_campaigns_task is None or _scheduled_campaigns_task.done():
        _scheduled_campaigns_task = asyncio.create_task(_scheduled_campaign_autostart_loop())
    if _reminder_task is None or _reminder_task.done():
        from reminders.reminder_scheduler import run_reminder_dispatch_loop

        _reminder_task = asyncio.create_task(run_reminder_dispatch_loop())
    if _spam_monitor_task is None or _spam_monitor_task.done():
        _spam_monitor_task = asyncio.create_task(_spam_monitor_loop())
    if _simulator_cleanup_task is None or _simulator_cleanup_task.done():
        _simulator_cleanup_task = asyncio.create_task(_simulator_cleanup_loop())


@app.on_event("shutdown")
async def on_shutdown():
    if _shutdown_event.is_set():
        return
    _shutdown_event.set()
    try:
        import campaign_runner

        campaign_runner.request_shutdown()
    except Exception:
        pass
    logging.info("[SHUTDOWN] Shutdown signal received")
    try:
        import dialer

        for campaign in db.get_campaigns_active():
            cid = str(campaign["id"])
            await dialer.stop_campaign(cid)
    except Exception as e:
        logging.warning("[SHUTDOWN] Unable to stop dialers cleanly: %s", e)

    for task in (_live_broadcast_task, _scheduled_campaigns_task, _reminder_task, _spam_monitor_task, _simulator_cleanup_task):
        if task and not task.done():
            task.cancel()

    try:
        reset_count = db.reset_inflight_leads()
        logging.info("[SHUTDOWN] Reset %s in-flight leads to pending", reset_count)
    except Exception as e:
        logging.warning("[SHUTDOWN] Failed to reset in-flight leads: %s", e)

    await asyncio.sleep(1)
    db.close_pool()
    logging.info("[SHUTDOWN] Graceful shutdown complete.")


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
    inbound_speaks_first: bool = True


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
    inbound_speaks_first: Optional[bool] = None


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


class DNCAddBody(BaseModel):
    business_id: str
    phone: str
    reason: Optional[str] = None
    added_by: str = "api"


class DNCBulkBody(BaseModel):
    business_id: str
    phones: List[str]
    reason: Optional[str] = None


class VariantItem(BaseModel):
    agent_id: str
    weight: int


class VariantSetBody(BaseModel):
    business_id: str
    variants: List[VariantItem]


class HolidayCreateBody(BaseModel):
    business_id: str
    name: str
    date: str
    is_recurring: bool = True
    skip_calls: bool = True


class SeedHolidaysBody(BaseModel):
    business_id: str


class SurveyCreateBody(BaseModel):
    business_id: str
    agent_id: str
    question: str
    response_type: str = "numeric"
    valid_responses: List[str] = ["1", "2", "3", "4", "5"]
    send_via: str = "whatsapp"
    send_delay_minutes: int = 2
    trigger_dispositions: List[str] = ["interested", "booked", "callback_requested"]


class SurveyUpdateBody(BaseModel):
    question: Optional[str] = None
    response_type: Optional[str] = None
    valid_responses: Optional[List[str]] = None
    send_via: Optional[str] = None
    send_delay_minutes: Optional[int] = None
    trigger_dispositions: Optional[List[str]] = None
    is_active: Optional[bool] = None


class SimulatorStartBody(BaseModel):
    agent_id: str
    mock_lead: Optional[dict] = None


class SimulatorMessageBody(BaseModel):
    content: str


class BookingPatchBody(BaseModel):
    status: Optional[str] = None
    notes: Optional[str] = None
    start_time: Optional[str] = None


def _lk_client() -> lk_api.LiveKitAPI:
    return lk_api.LiveKitAPI(
        url=os.environ["LIVEKIT_URL"],
        api_key=os.environ["LIVEKIT_API_KEY"],
        api_secret=os.environ["LIVEKIT_API_SECRET"],
    )


def _normalize_phone(phone: str) -> str:
    return (phone or "").strip().replace(" ", "")


_PHONE_RE = re.compile(r"^\+?[1-9]\d{7,14}$")


def _is_valid_phone(phone: str) -> bool:
    return bool(_PHONE_RE.match(phone or ""))


async def dispatch_outbound_call(
    phone: str,
    agent_id: str,
    sip_trunk_id: str,
    business_id: str,
    campaign_id: Optional[str],
    lead_id: Optional[str],
    script_override: Optional[str],
    call_attempt_number: int = 1,
    from_number: Optional[str] = None,
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
                "from_number": from_number or mask_number or trunk.get("phone_number"),
                "agent_id": agent_id,
                "campaign_id": campaign_id,
                "lead_id": lead_id,
                "business_id": business_id,
                "sip_trunk_id": sip_trunk_id,
                "script_override": script_override,
                "call_attempt_number": call_attempt_number,
            }
        )

        sip_req = lk_api.CreateSIPParticipantRequest(
            room_name=room_name,
            sip_trunk_id=trunk["trunk_id"],
            sip_call_to=phone,
            participant_identity=f"sip_{phone.replace('+', '')}",
            participant_name="Caller",
            wait_until_answered=True,
        )
        if from_number:
            sip_req.sip_number = from_number

        await lk.sip.create_sip_participant(sip_req)

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


async def _live_broadcast_loop():
    global _last_transcript_cursor
    while not _shutdown_event.is_set():
        try:
            if not _live_clients:
                await asyncio.sleep(2)
                continue
            stale = []
            for ws in list(_live_clients):
                business_id = _live_filters.get(ws)
                payload = db.get_live_monitor_snapshot(business_id=business_id)
                try:
                    await ws.send_json(payload)
                except Exception:
                    stale.append(ws)
            # Piggyback transcript events on the same websocket channel.
            lines = db.get_recent_transcript_lines(_last_transcript_cursor, limit=200)
            for line in lines:
                event = {
                    "event": "transcript_line",
                    "room_id": line.get("room_id"),
                    "role": "agent" if line.get("role") == "assistant" else line.get("role"),
                    "content": line.get("content"),
                    "turn_number": line.get("turn_number"),
                    "timestamp": str(line.get("created_at")),
                }
                _last_transcript_cursor = str(line.get("created_at"))
                for ws in list(_live_clients):
                    try:
                        await ws.send_json(event)
                    except Exception:
                        stale.append(ws)
            for ws in stale:
                _live_clients.discard(ws)
                _live_filters.pop(ws, None)
            await asyncio.sleep(2)
        except Exception as e:
            logging.error("[WS] live broadcast loop error: %s", e)
            await asyncio.sleep(2)


async def _scheduled_campaign_autostart_loop():
    while not _shutdown_event.is_set():
        try:
            due = db.get_campaigns_due_to_start()
            if due:
                import dialer

                for campaign in due:
                    cid = str(campaign["id"])
                    db.update_campaign(cid, status="active", started_at=datetime_now_iso())
                    asyncio.create_task(dialer.start_campaign(cid))
                    logging.info("[SCHEDULER] Auto-started campaign %s", cid)
        except Exception as e:
            logging.error("[SCHEDULER] auto-start loop error: %s", e)
        await asyncio.sleep(60)


async def _spam_monitor_loop():
    from number_health.spam_checker import check_spam_score

    while not _shutdown_event.is_set():
        try:
            trunks = db.get_sip_trunks()
            for trunk in trunks:
                pool = trunk.get("number_pool") or []
                if isinstance(pool, str):
                    try:
                        pool = json.loads(pool)
                    except Exception:
                        pool = []
                numbers = list({*(pool or []), trunk.get("phone_number")})
                for phone in [p for p in numbers if p]:
                    existing = next(
                        (r for r in db.get_number_health(str(trunk["id"])) if str(r.get("phone_number")) == str(phone)),
                        None,
                    ) or {}
                    result = await check_spam_score(
                        phone_number=str(phone),
                        calls_today=int(existing.get("calls_today") or 0),
                        calls_per_number_limit=int(trunk.get("calls_per_number_limit") or 50),
                    )
                    is_paused = int(result.get("spam_score") or 0) >= 80
                    pause_reason = "spam_flagged" if is_paused else None
                    db.upsert_number_health(
                        str(trunk["id"]),
                        str(phone),
                        spam_score=int(result.get("spam_score") or 0),
                        spam_label=result.get("spam_label"),
                        is_paused=is_paused,
                        pause_reason=pause_reason,
                    )
                    if is_paused:
                        logging.warning("[SPAM] Number %s flagged as spam. Paused.", phone)
        except Exception as e:
            logging.error("[SPAM] monitor loop error: %s", e)
        await asyncio.sleep(3600)


async def _simulator_cleanup_loop():
    while not _shutdown_event.is_set():
        try:
            now_ts = datetime.utcnow().timestamp()
            stale = []
            for sid, payload in _simulator_sessions.items():
                created = float(payload.get("created_ts") or now_ts)
                if now_ts - created > 30 * 60:
                    stale.append(sid)
            for sid in stale:
                _simulator_sessions.pop(sid, None)
        except Exception as e:
            logging.warning("[SIM] cleanup loop error: %s", e)
        await asyncio.sleep(60)


# Businesses

@app.post("/api/businesses")
def create_business(body: BusinessCreate):
    row = db.create_business(**body.model_dump())
    if not row:
        raise HTTPException(status_code=400, detail="Failed to create business")
    try:
        from holidays.holiday_engine import seed_default_holidays

        seed_default_holidays(str(row["id"]))
    except Exception as e:
        logging.warning("[HOLIDAY] default seed skipped for business=%s error=%s", row.get("id"), e)
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


@app.get("/api/campaigns/{id}/variants")
def get_campaign_variants(id: str, business_id: str = Query(...)):
    logging.info("[AB] get variants campaign_id=%s business_id=%s", id, business_id)
    campaign = db.get_campaign(id)
    if not campaign or str(campaign.get("business_id")) != str(business_id):
        return _error("Campaign not found", 404)
    return _success(db.get_campaign_variants(id))


@app.post("/api/campaigns/{id}/variants")
def set_campaign_variants(id: str, body: VariantSetBody):
    logging.info("[AB] set variants campaign_id=%s business_id=%s", id, body.business_id)
    campaign = db.get_campaign(id)
    if not campaign or str(campaign.get("business_id")) != str(body.business_id):
        return _error("Campaign not found", 404)
    ok = db.set_campaign_variants(id, [v.model_dump() for v in body.variants])
    if not ok:
        return _error("Failed to set variants; weights must sum to 100", 400)
    return _success({"saved": True})


@app.get("/api/campaigns/{id}/variant-stats")
def get_variant_stats(id: str, business_id: str = Query(...)):
    logging.info("[AB] variant-stats campaign_id=%s business_id=%s", id, business_id)
    campaign = db.get_campaign(id)
    if not campaign or str(campaign.get("business_id")) != str(business_id):
        return _error("Campaign not found", 404)
    return _success(db.get_campaign_variant_stats(id))


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


@app.post("/api/campaigns/{campaign_id}/leads/import")
async def import_leads(campaign_id: str, business_id: str = Query(...), file: UploadFile = File(...)):
    logging.info("[LEADS] import requested campaign_id=%s business_id=%s", campaign_id, business_id)
    campaign = db.get_campaign(campaign_id)
    if not campaign:
        return _error("Campaign not found", 404)

    content = await file.read()
    if len(content) > 10 * 1024 * 1024:
        return _error("File too large (max 10MB)", 400)

    text = content.decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    errors = []
    imported_rows = []
    seen_csv = set()
    duplicates_skipped = 0
    dnc_skipped = 0
    invalid_skipped = 0

    existing_phones = {(_normalize_phone(x.get("phone")) or "") for x in db.get_leads(campaign_id, limit=50000, offset=0)}
    row_idx = 1
    for row in reader:
        row_idx += 1
        if row_idx > 50001:
            return _error("Max 50,000 rows allowed", 400)

        phone = _normalize_phone(row.get("phone") or "")
        if not phone or not _is_valid_phone(phone):
            invalid_skipped += 1
            errors.append({"row": row_idx, "phone": phone, "reason": "invalid_phone"})
            continue
        if phone in seen_csv or phone in existing_phones:
            duplicates_skipped += 1
            continue
        if db.is_dnc(str(business_id), phone):
            dnc_skipped += 1
            continue
        seen_csv.add(phone)

        custom_data = {}
        raw_custom = row.get("custom_data")
        if raw_custom:
            try:
                custom_data = json.loads(raw_custom)
            except Exception:
                custom_data = {"raw": raw_custom}
        imported_rows.append(
            {
                "phone": phone,
                "name": row.get("name"),
                "email": row.get("email"),
                "language": row.get("language") or "hi-IN",
                "custom_data": custom_data,
            }
        )

    inserted = db.bulk_create_leads(campaign_id, business_id, imported_rows)
    result = {
        "imported": inserted,
        "duplicates_skipped": duplicates_skipped,
        "dnc_skipped": dnc_skipped,
        "invalid_skipped": invalid_skipped,
        "errors": errors,
    }
    job_id = uuid.uuid4().hex
    _import_jobs[job_id] = {"status": "completed", "result": result, "created_at": datetime.utcnow().isoformat()}
    result["job_id"] = job_id
    return _success(result)


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str, business_id: str = Query(...)):
    _ = business_id
    row = _import_jobs.get(job_id)
    if not row:
        return _error("Job not found", 404)
    return _success(row)


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


# DNC Registry

@app.post("/api/dnc")
def add_dnc(body: DNCAddBody):
    logging.info("[DNC] add phone=%s business_id=%s", body.phone, body.business_id)
    row = db.add_to_dnc(body.business_id, body.phone, body.reason, body.added_by)
    if not row:
        return _error("Failed to add DNC", 400)
    return _success(row)


@app.post("/api/dnc/bulk")
def add_dnc_bulk(body: DNCBulkBody):
    logging.info("[DNC] bulk add business_id=%s count=%s", body.business_id, len(body.phones))
    count = db.bulk_add_dnc(body.business_id, body.phones, body.reason)
    return _success({"added": count})


@app.get("/api/dnc")
def list_dnc(business_id: str = Query(...), limit: int = 100, offset: int = 0):
    logging.info("[DNC] list business_id=%s", business_id)
    rows = db.get_dnc_list(business_id, limit=limit, offset=offset)
    return _success(rows)


@app.delete("/api/dnc/{phone}")
def remove_dnc(phone: str, business_id: str = Query(...)):
    logging.info("[DNC] remove phone=%s business_id=%s", phone, business_id)
    ok = db.remove_from_dnc(business_id, phone)
    if not ok:
        return _error("DNC entry not found", 404)
    return _success({"removed": True})


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
def get_call(
    room_id: str,
    business_id: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    if room_id == "inbound":
        if not business_id:
            raise HTTPException(status_code=400, detail="business_id is required for inbound calls")
        return _success(db.get_inbound_calls(business_id, limit=limit, offset=offset))
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


@app.post("/api/calls/dispatch")
async def dispatch_call_alias(body: TriggerCallBody):
    result = await trigger_call(body)
    if isinstance(result, dict):
        return _success(result)
    return result


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


@app.get("/api/analytics/funnel")
def analytics_funnel(campaign_id: str = Query(...), business_id: str = Query(...)):
    logging.info("[ANALYTICS] funnel campaign_id=%s business_id=%s", campaign_id, business_id)
    campaign = db.get_campaign(campaign_id)
    if not campaign or str(campaign.get("business_id")) != str(business_id):
        return _error("Campaign not found", 404)
    return _success(db.get_conversion_funnel(campaign_id))


@app.get("/api/analytics/hourly")
def analytics_hourly(campaign_id: str = Query(...), date: str = Query(...), business_id: str = Query(...)):
    logging.info("[ANALYTICS] hourly campaign_id=%s date=%s business_id=%s", campaign_id, date, business_id)
    campaign = db.get_campaign(campaign_id)
    if not campaign or str(campaign.get("business_id")) != str(business_id):
        return _error("Campaign not found", 404)
    return _success(db.get_campaign_hourly_stats(campaign_id, date))


@app.get("/api/analytics/disposition")
def analytics_disposition(campaign_id: str = Query(...), business_id: str = Query(...)):
    logging.info("[ANALYTICS] disposition campaign_id=%s business_id=%s", campaign_id, business_id)
    campaign = db.get_campaign(campaign_id)
    if not campaign or str(campaign.get("business_id")) != str(business_id):
        return _error("Campaign not found", 404)
    return _success(db.get_disposition_breakdown(campaign_id))


@app.get("/api/analytics/agent-performance")
def analytics_agent_performance(business_id: str = Query(...), days: int = Query(7, ge=1, le=90)):
    logging.info("[ANALYTICS] agent-performance business_id=%s days=%s", business_id, days)
    return _success(db.get_agent_performance(business_id, days=days))


@app.get("/api/analytics/cost")
def analytics_cost(
    business_id: str = Query(...),
    from_date: str = Query(..., alias="from"),
    to_date: str = Query(..., alias="to"),
):
    logging.info("[ANALYTICS] cost business_id=%s from=%s to=%s", business_id, from_date, to_date)
    return _success(db.get_cost_report(business_id, from_date, to_date))


# Group J: Holidays

@app.get("/api/holidays")
def get_holidays_api(business_id: str = Query(...), year: Optional[int] = Query(default=None)):
    logging.info("[HOLIDAY] list business_id=%s year=%s", business_id, year)
    return _success(db.get_holidays(business_id, year=year))


@app.post("/api/holidays")
def add_holiday_api(body: HolidayCreateBody):
    logging.info("[HOLIDAY] add business_id=%s date=%s", body.business_id, body.date)
    row = db.add_holiday(body.business_id, body.name, body.date, body.is_recurring, body.skip_calls)
    if not row:
        return _error("Failed to add holiday", 400)
    return _success(row)


@app.delete("/api/holidays/{id}")
def delete_holiday_api(id: str):
    ok = db.delete_holiday(id)
    if not ok:
        return _error("Holiday not found", 404)
    return _success({"deleted": True})


@app.post("/api/holidays/seed-defaults")
def seed_holidays_api(body: SeedHolidaysBody):
    from holidays.holiday_engine import seed_default_holidays

    count = seed_default_holidays(body.business_id)
    return _success({"inserted": count})


@app.get("/api/holidays/check-today")
def check_holiday_today_api(business_id: str = Query(...)):
    from holidays.holiday_engine import is_holiday_today

    is_holiday, name = is_holiday_today(business_id)
    return _success({"is_holiday": is_holiday, "name": name or None})


# Group J: Number health

@app.get("/api/sip-trunks/{id}/number-health")
async def trunk_number_health(id: str):
    return _success(db.get_number_health(id))


@app.post("/api/sip-trunks/{id}/number-health/{phone}/pause")
def pause_number(id: str, phone: str):
    ok = db.set_number_pause(id, phone, True, reason="manual_pause")
    if not ok:
        return _error("Number not found", 404)
    return _success({"paused": True})


@app.post("/api/sip-trunks/{id}/number-health/{phone}/resume")
def resume_number(id: str, phone: str):
    ok = db.set_number_pause(id, phone, False, reason=None)
    if not ok:
        return _error("Number not found", 404)
    return _success({"paused": False})


@app.post("/api/sip-trunks/{id}/number-health/check-all")
async def check_all_numbers(id: str):
    from number_health.spam_checker import check_spam_score

    trunk = db.get_sip_trunk(id)
    if not trunk:
        return _error("SIP trunk not found", 404)
    pool = trunk.get("number_pool") or []
    if isinstance(pool, str):
        try:
            pool = json.loads(pool)
        except Exception:
            pool = []
    numbers = list({*(pool or []), trunk.get("phone_number")})
    checked = 0
    for phone in [x for x in numbers if x]:
        existing = next((r for r in db.get_number_health(id) if str(r.get("phone_number")) == str(phone)), {}) or {}
        result = await check_spam_score(
            str(phone),
            calls_today=int(existing.get("calls_today") or 0),
            calls_per_number_limit=int(trunk.get("calls_per_number_limit") or 50),
        )
        db.upsert_number_health(
            id,
            str(phone),
            spam_score=int(result.get("spam_score") or 0),
            spam_label=result.get("spam_label"),
            is_paused=int(result.get("spam_score") or 0) >= 80,
            pause_reason="spam_flagged" if int(result.get("spam_score") or 0) >= 80 else None,
        )
        checked += 1
    return _success({"checked": checked})


# Group J: Surveys

@app.get("/api/surveys")
def list_surveys(business_id: str = Query(...)):
    return _success(db.get_surveys(business_id))


@app.post("/api/surveys")
def create_survey_api(body: SurveyCreateBody):
    row = db.create_survey(**body.model_dump())
    if not row:
        return _error("Failed to create survey", 400)
    return _success(row)


@app.put("/api/surveys/{id}")
def update_survey_api(id: str, body: SurveyUpdateBody):
    row = db.update_survey(id, **body.model_dump(exclude_none=True))
    if not row:
        return _error("Survey not found or no updates", 404)
    return _success(row)


@app.delete("/api/surveys/{id}")
def delete_survey_api(id: str):
    ok = db.delete_survey(id)
    if not ok:
        return _error("Survey not found", 404)
    return _success({"deleted": True})


@app.get("/api/surveys/{id}/responses")
def survey_responses_api(id: str, limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    return _success(db.get_survey_responses(id, limit=limit, offset=offset))


@app.get("/api/surveys/{id}/stats")
def survey_stats_api(id: str):
    return _success(db.get_survey_stats(id))


# Group J: Reminders

@app.get("/api/bookings/{booking_id}/reminders")
def reminders_for_booking(booking_id: str):
    return _success(db.get_reminders_for_booking(booking_id))


@app.delete("/api/bookings/{booking_id}/reminders")
def cancel_booking_reminders(booking_id: str):
    count = db.cancel_reminders_for_booking(booking_id)
    return _success({"cancelled": count})


@app.post("/api/bookings/{booking_id}/reminders/send-now")
async def send_booking_reminder_now(booking_id: str):
    reminders = db.get_reminders_for_booking(booking_id)
    if not reminders:
        return _error("No reminders found for booking", 404)
    reminder = reminders[0]
    lead = db.get_lead(str(reminder.get("lead_id"))) if reminder.get("lead_id") else None
    campaign = db.get_campaign(str(lead.get("campaign_id"))) if lead and lead.get("campaign_id") else None
    if not campaign:
        return _error("Cannot resolve campaign for reminder", 400)
    prompt = (
        f"You are calling {(lead or {}).get('name') or 'the customer'} to remind them about their appointment. "
        "Confirm they will attend and collect any reschedule request politely."
    )
    result = await dispatch_outbound_call(
        phone=reminder.get("phone"),
        agent_id=str(reminder.get("agent_id") or campaign.get("agent_id")),
        sip_trunk_id=str(campaign.get("sip_trunk_id")),
        business_id=str(reminder.get("business_id")),
        campaign_id=str(campaign.get("id")),
        lead_id=str(reminder.get("lead_id")) if reminder.get("lead_id") else None,
        script_override=prompt,
        call_attempt_number=1,
    )
    call_row = db.create_call(
        lead_id=str(reminder.get("lead_id")) if reminder.get("lead_id") else None,
        campaign_id=str(campaign.get("id")),
        agent_id=str(reminder.get("agent_id") or campaign.get("agent_id")),
        sip_trunk_id=str(campaign.get("sip_trunk_id")),
        business_id=str(reminder.get("business_id")),
        phone=reminder.get("phone"),
        room_id=result.get("room_id"),
        call_attempt_number=1,
        call_direction="outbound",
    )
    db.update_reminder_status(str(reminder["id"]), "dispatched", call_id=str(call_row.get("id")) if call_row else None)
    return _success({"queued": True, "reminder_id": str(reminder["id"]), "room_id": result.get("room_id")})


# Group J: Inbound

@app.post("/webhook/livekit-sip-inbound")
async def livekit_sip_inbound_webhook(request: Request):
    from inbound.inbound_handler import on_inbound_call_webhook

    payload = await request.json()
    return JSONResponse(content=await on_inbound_call_webhook(payload))


@app.get("/api/analytics/inbound-stats")
def inbound_stats_api(business_id: str = Query(...), days: int = Query(7, ge=1, le=90)):
    return _success(db.get_inbound_stats(business_id, days=days))


# Group J: Booking calendar

@app.get("/api/bookings/calendar")
def bookings_calendar_api(
    business_id: str = Query(...),
    from_date: str = Query(..., alias="from"),
    to_date: str = Query(..., alias="to"),
):
    return _success(db.get_bookings_calendar(business_id, from_date, to_date))


@app.patch("/api/bookings/{id}")
def patch_booking(id: str, body: BookingPatchBody):
    row = db.update_booking(id, **body.model_dump(exclude_none=True))
    if not row:
        return _error("Booking not found or no updates", 404)
    if body.status == "cancelled":
        db.cancel_reminders_for_booking(id)
    return _success(row)


@app.get("/api/bookings/summary")
def booking_summary_api(business_id: str = Query(...)):
    return _success(db.get_bookings_summary(business_id))


# Group J: Agent Simulator

@app.post("/api/simulator/start")
async def simulator_start(body: SimulatorStartBody):
    from simulator.agent_simulator import AgentSimulator

    agent_cfg = db.get_agent(body.agent_id)
    if not agent_cfg:
        return _error("Agent not found", 404)
    session_id = uuid.uuid4().hex
    simulator = AgentSimulator(agent_cfg, lead_context=body.mock_lead or {})
    _simulator_sessions[session_id] = {
        "simulator": simulator,
        "agent_id": body.agent_id,
        "created_ts": datetime.utcnow().timestamp(),
    }
    return _success({"session_id": session_id, "conversation": simulator.get_conversation()})


@app.post("/api/simulator/{session_id}/message")
async def simulator_message(session_id: str, body: SimulatorMessageBody):
    payload = _simulator_sessions.get(session_id)
    if not payload:
        return _error("Simulator session not found", 404)
    sim = payload["simulator"]
    handlers = []
    response, objection = await sim.send_message(body.content, objection_handlers=handlers)
    return _success(
        {
            "response": response,
            "turn_number": len(sim.get_conversation()),
            "objection_triggered": bool(objection),
        }
    )


@app.get("/api/simulator/{session_id}/conversation")
def simulator_conversation(session_id: str):
    payload = _simulator_sessions.get(session_id)
    if not payload:
        return _error("Simulator session not found", 404)
    return _success(payload["simulator"].get_conversation())


@app.post("/api/simulator/{session_id}/reset")
def simulator_reset(session_id: str):
    payload = _simulator_sessions.get(session_id)
    if not payload:
        return _error("Simulator session not found", 404)
    payload["simulator"].reset()
    payload["created_ts"] = datetime.utcnow().timestamp()
    return _success({"reset": True})


@app.delete("/api/simulator/{session_id}")
def simulator_delete(session_id: str):
    existed = _simulator_sessions.pop(session_id, None)
    if not existed:
        return _error("Simulator session not found", 404)
    return _success({"deleted": True})


@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    business_id = ws.query_params.get("business_id")
    _live_clients.add(ws)
    _live_filters[ws] = business_id
    try:
        await ws.send_json(db.get_live_monitor_snapshot(business_id=business_id))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        _live_clients.discard(ws)
        _live_filters.pop(ws, None)


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


@app.post("/webhook/whatsapp")
async def whatsapp_webhook(request: Request):
    payload = await request.json()
    business_id = str(payload.get("business_id") or request.query_params.get("business_id") or "")
    if not business_id:
        return _error("business_id is required", 400)

    text = (
        payload.get("message")
        or payload.get("text")
        or ((payload.get("data") or {}).get("message") if isinstance(payload.get("data"), dict) else "")
        or ""
    )
    phone = (
        payload.get("phone")
        or payload.get("from")
        or ((payload.get("data") or {}).get("from") if isinstance(payload.get("data"), dict) else "")
        or ""
    )
    phone = _normalize_phone(phone).replace("@s.whatsapp.net", "")
    if phone and not phone.startswith("+"):
        phone = f"+{phone}"

    if "stop" in str(text).lower() and phone:
        row = db.add_to_dnc(business_id, phone, "whatsapp_stop", "whatsapp_webhook")
        if row:
            logging.info("[WHATSAPP] STOP received and added to DNC phone=%s business_id=%s", phone, business_id)
            return _success({"dnc_added": True, "phone": phone})
    try:
        from surveys.survey_engine import handle_incoming_survey_response

        handled = await handle_incoming_survey_response(phone, str(text))
        if handled:
            return _success({"dnc_added": False, "survey_response_recorded": True})
    except Exception as e:
        logging.warning("[WHATSAPP] survey response parse failed: %s", e)
    return _success({"dnc_added": False, "survey_response_recorded": False})


@app.post("/webhook/sms")
async def sms_webhook(request: Request):
    payload = await request.json()
    phone = _normalize_phone(str(payload.get("phone") or payload.get("from") or ""))
    if phone and not phone.startswith("+"):
        phone = f"+{phone}"
    message = str(payload.get("message") or payload.get("text") or "")
    try:
        from surveys.survey_engine import handle_incoming_survey_response

        handled = await handle_incoming_survey_response(phone, message)
        return _success({"survey_response_recorded": bool(handled)})
    except Exception as e:
        logging.warning("[SMS] survey webhook failed: %s", e)
        return _success({"survey_response_recorded": False})


def datetime_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


@app.get("/health")
def health():
    return _success({"status": "ok", "timestamp": datetime_now_iso()})


@app.get("/health/live")
def liveness_check():
    return _success({"alive": True, "timestamp": datetime_now_iso()})


@app.get("/health/ready")
async def readiness_check():
    checks = {"db": "ok", "redis": "warn", "livekit": "ok"}
    status_code = 200
    status = "ready"

    # DB check (critical)
    try:
        conn = db.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        finally:
            db.release_conn(conn)
    except Exception as e:
        checks["db"] = f"fail: {e}"
        status = "unhealthy"
        status_code = 503

    # Redis check (optional)
    try:
        if os.environ.get("REDIS_URL"):
            from cache.redis_cache import get_redis

            r = await get_redis()
            await r.ping()
            checks["redis"] = "ok"
        else:
            checks["redis"] = "warn"
    except Exception:
        checks["redis"] = "warn"

    # LiveKit config check (critical)
    if not os.environ.get("LIVEKIT_URL"):
        checks["livekit"] = "fail"
        status = "unhealthy"
        status_code = 503

    if status != "unhealthy" and checks["redis"] == "warn":
        status = "degraded"

    return JSONResponse(
        status_code=status_code,
        content={
            "data": {
                "status": status,
                "checks": checks,
                "timestamp": datetime_now_iso(),
            },
            "error": None,
        },
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("ui_server:app", host="0.0.0.0", port=8000, reload=False)
