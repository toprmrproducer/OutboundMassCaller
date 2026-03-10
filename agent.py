import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime

import pytz
from dotenv import load_dotenv

load_dotenv()

from config.validator import assert_valid_env
from logging_config import configure_logging

configure_logging()
assert_valid_env()

import db
from livekit import agents
from livekit.agents import Agent, AgentSession, function_tool
from livekit.plugins import openai as lk_openai
from livekit.plugins import sarvam, silero

logger = logging.getLogger("agent")

try:
    db.initdb()
    logger.info("STARTUP: DB init complete")
except Exception as e:
    logger.warning(f"STARTUP: DB init failed: {e}")

FILLERS_THINKING = ["Umm...", "Let me see...", "Acha..."]
FILLERS_AGREEMENT = ["Haan ji,", "Bilkul,", "Theek hai,"]
FILLERS_EMPATHY = ["Samajh sakta hoon,", "Haan, woh toh hai,"]
PERSONA_INSTRUCTIONS = {
    "professional": "Maintain a formal, professional tone. Be concise and clear.",
    "friendly": "Be warm, conversational and friendly. Use casual language.",
    "assertive": "Be confident and direct. Gently push for commitment.",
    "empathetic": "Show understanding and empathy. Listen actively and validate feelings.",
}


def build_stt(cfg: dict):
    provider = cfg.get("stt_provider", "sarvam")
    language = cfg.get("stt_language", "hi-IN")
    model = cfg.get("stt_model", "saaras:v3")
    if provider == "deepgram":
        from livekit.plugins import deepgram

        return deepgram.STT(model="nova-3", language=language)
    return sarvam.STT(language=language, model=model)


def build_tts(cfg: dict):
    provider = cfg.get("tts_provider", "sarvam")
    voice = cfg.get("tts_voice", "rohan")
    language = cfg.get("tts_language", "hi-IN")
    if provider == "elevenlabs":
        from livekit.plugins import elevenlabs

        return elevenlabs.TTS(voice_id=voice, model="eleven_flash_v2_5")
    return sarvam.TTS(speaker=voice, language=language)


def build_llm(cfg: dict):
    temperature = cfg.get("llm_temperature", 0.7)
    kwargs = {"model": cfg.get("llm_model") or "gpt-4.1-mini", "temperature": temperature}
    if cfg.get("llm_base_url"):
        kwargs["base_url"] = cfg["llm_base_url"]
    return lk_openai.LLM(**kwargs)


def create_llm_with_fallback(agent_cfg: dict):
    try:
        return build_llm(agent_cfg)
    except Exception as primary_err:
        fallback_provider = agent_cfg.get("llm_fallback_provider")
        fallback_model = agent_cfg.get("llm_fallback_model")
        if not fallback_provider and not fallback_model:
            raise
        logging.warning(
            "[LLM] Primary %s failed. Switching to fallback.",
            agent_cfg.get("llm_provider", "openai"),
        )
        fallback_cfg = dict(agent_cfg)
        if fallback_provider:
            fallback_cfg["llm_provider"] = fallback_provider
        if fallback_model:
            fallback_cfg["llm_model"] = fallback_model
        try:
            return build_llm(fallback_cfg)
        except Exception:
            raise primary_err


def inject_lead_context(template: str, lead: dict, campaign: dict) -> str:
    result = str(template or "")
    lead = lead or {}
    campaign = campaign or {}
    result = result.replace("{{lead_name}}", str(lead.get("name") or "the customer"))
    result = result.replace("{{lead_phone}}", str(lead.get("phone") or ""))
    result = result.replace("{{lead_language}}", str(lead.get("language") or "hi-IN"))
    result = result.replace("{{campaign_name}}", str(campaign.get("name") or ""))
    result = result.replace("{{campaign_objective}}", str(campaign.get("objective") or ""))
    custom_data = lead.get("custom_data") or {}
    if isinstance(custom_data, str):
        try:
            custom_data = json.loads(custom_data)
        except Exception:
            custom_data = {}
    if isinstance(custom_data, dict):
        for k, v in custom_data.items():
            result = result.replace(f"{{{{custom_{k}}}}}", str(v))
    return result


def apply_pronunciation_guide(text: str, guide: list[dict]) -> str:
    out = str(text or "")
    for entry in guide or []:
        word = str(entry.get("word") or "").strip()
        pronunciation = str(entry.get("pronunciation") or "").strip()
        if not word or not pronunciation:
            continue
        pattern = re.compile(r"\\b" + re.escape(word) + r"\\b", re.IGNORECASE)
        out = pattern.sub(pronunciation, out)
    return out


def compute_turn_sentiment(text: str) -> str:
    sample = str(text or "").lower()
    positive_keywords = [
        "interested",
        "yes",
        "ok",
        "sure",
        "good",
        "great",
        "proceed",
        "confirm",
        "book",
        "schedule",
        "happy",
        "excellent",
        "haan",
        "bilkul",
    ]
    negative_keywords = [
        "no",
        "not",
        "busy",
        "later",
        "stop",
        "cancel",
        "remove",
        "angry",
        "upset",
        "disturb",
        "wrong",
        "fraud",
        "scam",
        "nahi",
    ]
    if any(k in sample for k in negative_keywords):
        return "negative"
    if any(k in sample for k in positive_keywords):
        return "positive"
    return "neutral"


async def generate_call_summary(transcript: str, llm_client, model: str) -> dict:
    payload = {
        "summary": "",
        "disposition": "",
        "sentiment": "neutral",
        "key_points": [],
        "follow_up_action": "",
    }
    try:
        prompt = (
            "Analyze this call transcript and extract structured information as JSON with keys: "
            "summary, disposition, sentiment, key_points, follow_up_action.\n\nTranscript:\n"
            + str(transcript or "")[:5000]
        )
        resp = await llm_client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=220,
        )
        raw = (resp.choices[0].message.content or "").strip()
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            payload.update(parsed)
    except Exception:
        return payload
    return payload


def humanize_text(text: str, user_text: str = "") -> str:
    text = (text or "").strip()
    if not text:
        return text
    if len(text.split()) > 22:
        text = text.replace(" and ", ". And ")
    text = re.sub(r"\?", "...?", text)
    if random.random() < 0.30:
        text = re.sub(r"\. ([A-Z])", r"... \1", text)

    if len(text.split()) > 8:
        if "?" in (user_text or ""):
            filler_pool = FILLERS_THINKING
        elif any(w in (user_text or "").lower() for w in ["busy", "baad", "time", "abhi"]):
            filler_pool = FILLERS_EMPATHY
        else:
            filler_pool = FILLERS_AGREEMENT
        if random.random() < 0.40:
            text = f"{random.choice(filler_pool)} {text}"
    return text


def detect_answering_machine(audio_buffer_seconds: float, speech_started_at: float) -> str:
    """
    Heuristic AMD: if no speech detected within 3 seconds of connect,
    or initial speech is very long, classify as machine.
    """
    try:
        if float(audio_buffer_seconds or 0.0) > 5.0:
            return "machine"
        if float(speech_started_at or 0.0) > 3.0:
            return "machine"
        return "human"
    except Exception:
        return "unknown"


def detect_language_from_text(text: str) -> str:
    sample = (text or "").strip()
    if not sample:
        return ""
    # Lightweight heuristic to avoid external latency/cost.
    if re.search(r"[\u0900-\u097F]", sample):
        return "hi-IN"
    lowered = sample.lower()
    hindi_roman = ["haan", "nahi", "achha", "theek", "aap", "kyu", "kaise", "baad", "zaroor"]
    if any(token in lowered for token in hindi_roman):
        return "hi-IN"
    return "en-IN"


async def initiate_warm_transfer(room_name: str, sip_trunk_id: str, sip_address: str) -> bool:
    try:
        from livekit import api as lk_api

        lk = lk_api.LiveKitAPI(
            url=os.environ["LIVEKIT_URL"],
            api_key=os.environ["LIVEKIT_API_KEY"],
            api_secret=os.environ["LIVEKIT_API_SECRET"],
        )
        try:
            await lk.sip.create_sip_participant(
                lk_api.CreateSIPParticipantRequest(
                    room_name=room_name,
                    sip_trunk_id=sip_trunk_id,
                    sip_call_to=sip_address,
                    participant_identity=f"transfer_{int(time.time())}",
                    participant_name="Human Agent",
                    wait_until_answered=False,
                )
            )
            return True
        finally:
            await lk.aclose()
    except Exception as e:
        logger.warning("[HANDOFF] transfer API failed: %s", e)
        return False


def infer_sentiment(transcript_text: str) -> str:
    # TODO: Replace with LLM-based sentiment via post_call.py
    # This keyword approach fails on Hindi/Hinglish.
    # post_call.py will overwrite this with gpt-4.1-nano result.
    text = (transcript_text or "").lower()
    negative_keywords = [
        "angry",
        "frustrated",
        "not interested",
        "stop calling",
        "bad",
        "nahi chahiye",
        "mat karo",
        "hatao",
        "band karo",
    ]
    positive_keywords = [
        "thanks",
        "great",
        "perfect",
        "book",
        "yes",
        "haan",
        "theek hai",
        "bilkul",
        "zaroor",
        "confirm",
        "appointment",
    ]
    if any(k in text for k in negative_keywords):
        return "negative"
    if any(k in text for k in positive_keywords):
        return "positive"
    return "neutral"


async def say_humanized(
    session: AgentSession,
    text: str,
    user_text: str = "",
    allow_interruptions: bool = True,
    pronunciation_guide: list[dict] | None = None,
):
    spoken = humanize_text(text, user_text)
    if pronunciation_guide:
        spoken = apply_pronunciation_guide(spoken, pronunciation_guide)
    await session.say(spoken, allow_interruptions=allow_interruptions)


def build_system_prompt(agent_cfg: dict, lead: dict = None, campaign: dict = None, script_override: str = None) -> str:
    ist = pytz.timezone("Asia/Kolkata")
    now_str = datetime.now(ist).strftime("%I:%M %p, %A")

    campaign = campaign or {}
    lead = lead or {}

    base_prompt = script_override or campaign.get("custom_script") or agent_cfg.get("system_prompt", "")
    caller_name = lead.get("name", "the caller")
    caller_phone = lead.get("phone", "")
    custom_data = json.dumps(lead.get("custom_data", {}), ensure_ascii=False)
    objective = campaign.get("objective", "have a helpful conversation")

    return f"""
{base_prompt}

CURRENT CALL CONTEXT:
- Caller name: {caller_name}
- Caller phone: {caller_phone}
- Campaign objective: {objective}
- Caller data: {custom_data}
- Current time (IST): {now_str}

CONVERSATION RULES (ABSOLUTE — NEVER BREAK):
- You are a human agent. Never say you are AI, bot, or assistant.
- If asked "are you a bot/AI?":
  First time: "Main? Nahi nahi, main ek real agent hoon. Waise, aapke baare mein batao..."
  Second time: "Arre, main toh real mein baat kar raha hoon. Anyway, [pivot to objective]"
  Third time: "[laugh] Sab yahi poochte hain. Mujhe batao, [ask genuine question about their need]"
- Max 2 sentences per response. Hard limit: 40 words total.
- Ask ONE question at a time. Never dump multiple questions.
- Use natural fillers: umm, acha, theek hai, haan ji, dekhiye.
  Maximum ONE filler per response.
- When caller shows interest → move toward booking immediately.
- When caller gives hard rejection → thank warmly and end call.
- Never repeat the same sentence twice in a conversation.
- Match the caller's language (Hindi/English/mixed).

ENDING TRIGGERS:
- Call end_call tool when: caller says goodbye, confirmed appointment,
  confirmed not interested, or after max_turns.
- Call book_appointment tool when: caller agrees to meet/visit.
- Call reschedule_call tool when: caller says "call later" or "baad mein".
- Call transfer_to_human tool when: caller insists on speaking with manager or senior.
- Call send_whatsapp tool when: caller asks for details on WhatsApp.
"""


def parse_phone_from_context(ctx, metadata: str) -> str:
    room = ctx.room.name
    if room.startswith("_+"):
        parts = room.split("_")
        for part in parts:
            if part.startswith("+"):
                return part
    try:
        meta = json.loads(metadata)
        return meta.get("phone_number", "unknown")
    except Exception:
        return "unknown"


class CallTools:
    def __init__(self, session: AgentSession, ctx, phone: str, call_id: str, lead_id: str, campaign_id: str, business_id: str):
        self.session = session
        self.ctx = ctx
        self.phone = phone
        self.call_id = call_id
        self.lead_id = lead_id
        self.campaign_id = campaign_id
        self.business_id = business_id
        self._agent_id = ""

    @function_tool
    async def end_call(self, reason: str = "conversation_complete") -> str:
        """End the current call. Use when conversation is complete, caller is not interested, or caller says goodbye."""
        logging.info(f"[TOOL] end_call: {reason}")
        if self.lead_id:
            db.update_lead_status(self.lead_id, "completed")
        db.update_call(self.ctx.room.name, disposition=reason, status="completed")
        await asyncio.sleep(1)
        await self.ctx.room.disconnect()
        return "Call ended"

    @function_tool
    async def book_appointment(self, caller_name: str, caller_phone: str, date_time: str, notes: str = "") -> str:
        """Book an appointment for the caller. date_time format: YYYY-MM-DD HH:MM"""
        try:
            normalized = date_time.replace(" ", "T")
            start_time = datetime.fromisoformat(normalized)
            booking = db.create_booking(
                call_id=self.call_id,
                business_id=self.business_id,
                caller_name=caller_name,
                caller_phone=caller_phone,
                caller_email="",
                start_time=start_time.isoformat(),
                notes=notes,
            )
            db.update_call(self.ctx.room.name, was_booked=True, disposition="booked")
            if self.lead_id:
                db.update_lead_status(self.lead_id, "completed")
            try:
                business = db.get_business(self.business_id)
                lead = db.get_lead(self.lead_id) if self.lead_id else {"phone": caller_phone, "name": caller_name}
                if business and bool(business.get("reminder_enabled", True)) and booking:
                    from reminders.reminder_scheduler import schedule_reminders_for_booking

                    created = schedule_reminders_for_booking(
                        booking=booking,
                        business=business,
                        lead=lead or {"phone": caller_phone, "name": caller_name},
                        agent_id=self._agent_id or str(self.business_id),
                        db_conn=None,
                    )
                    logging.info("[REMINDER] Scheduled %s reminder calls for booking %s", len(created), booking.get("id"))
            except Exception as reminder_err:
                logging.warning("[REMINDER] Failed to schedule reminders: %s", reminder_err)
            logging.info(f"[TOOL] Booking created: {date_time}")
            return f"Appointment booked for {date_time}"
        except Exception as e:
            logging.error(f"[TOOL] book_appointment error: {e}")
            return "Booking failed, please try manually"

    @function_tool
    async def reschedule_call(self, when: str, reason: str = "") -> str:
        """Reschedule this lead for a callback. when: ISO datetime string like 2026-03-10T15:00:00"""
        try:
            if self.lead_id:
                db.reschedule_lead(self.lead_id, when, reason=reason)
                db.create_callback(self.lead_id, self.campaign_id, when, reason=reason)
            db.update_call(self.ctx.room.name, disposition="callback_requested")
            return f"Lead scheduled for callback at {when}"
        except Exception as e:
            logging.error(f"[TOOL] reschedule_call error: {e}")
            return "Reschedule failed"

    @function_tool
    async def send_whatsapp(self, message: str) -> str:
        """Send a WhatsApp message to the caller with details."""
        try:
            from whatsapp import send_whatsapp_message

            business = db.get_business(self.business_id)
            if business and business.get("whatsapp_instance"):
                sent = await send_whatsapp_message(
                    instance_id=business["whatsapp_instance"],
                    token=business.get("whatsapp_token", ""),
                    phone=self.phone,
                    message=message,
                )
                if sent:
                    db.update_call(self.ctx.room.name, whatsapp_sent=True)
                    return "WhatsApp message sent"
                return "WhatsApp send failed"
            return "WhatsApp not configured for this business"
        except Exception as e:
            logging.error(f"[TOOL] send_whatsapp error: {e}")
            return "WhatsApp send failed"

    @function_tool
    async def transfer_to_human(self, reason: str = "") -> str:
        """Transfer call to a human agent."""
        logging.info(f"[TOOL] transfer_to_human: {reason}")
        if self.lead_id:
            db.update_lead_status(self.lead_id, "transferred")
        db.update_call(self.ctx.room.name, disposition="transferred", status="transferred")
        await asyncio.sleep(1)
        await self.ctx.room.disconnect()
        return "Transferring"

    @function_tool
    async def lookup_knowledge(self, query: str) -> str:
        """Look up product info, pricing, FAQs, or treatment details from the knowledge base."""
        try:
            from openai import AsyncOpenAI

            client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
            resp = await client.embeddings.create(model="text-embedding-3-small", input=query)
            embedding = resp.data[0].embedding
            results = db.search_knowledge(self._agent_id, embedding, limit=2)
            if not results:
                return "No specific information found."
            return "\n".join([f"{r['title']}: {r['content'][:250]}" for r in results])
        except Exception as e:
            logging.error(f"[TOOL] lookup_knowledge error: {e}")
            return "Knowledge lookup failed"


async def silence_watchdog(
    session: AgentSession,
    ctx,
    activity_state: dict,
    threshold: int = 20,
    max_nudges: int = 2,
    pronunciation_guide: list[dict] | None = None,
):
    nudge_count = 0
    while True:
        await asyncio.sleep(5)
        if time.time() - activity_state["last_activity"] > threshold:
            nudge_count += 1
            if nudge_count > max_nudges:
                await say_humanized(
                    session,
                    "It seems you're not available right now. I'll try again later. Goodbye!",
                    pronunciation_guide=pronunciation_guide,
                )
                await asyncio.sleep(3)
                await ctx.room.disconnect()
                return
            await say_humanized(session, "Are you still there?", pronunciation_guide=pronunciation_guide)
            activity_state["last_activity"] = time.time()
            await asyncio.sleep(threshold)


async def post_call_handler(room_id: str, call_id: str, lead_id: str, agent_cfg: dict):
    try:
        from post_call import run_post_call

        await run_post_call(room_id, call_id, lead_id, agent_cfg)
    except Exception as e:
        logging.error(f"[POST-CALL] Error: {e}")


async def entrypoint(ctx: agents.JobContext):
    try:
        await ctx.connect()

        metadata_raw = ctx.job.metadata or ""
        is_demo = metadata_raw == "demo" or metadata_raw.startswith("demo")

        meta = {}
        try:
            meta = json.loads(metadata_raw)
        except Exception:
            meta = {}

        phone = parse_phone_from_context(ctx, metadata_raw)
        campaign_id = meta.get("campaign_id")
        lead_id = meta.get("lead_id")
        business_id = meta.get("business_id")
        sip_trunk_id = meta.get("sip_trunk_id")
        script_override = meta.get("script_override")
        call_attempt = int(meta.get("call_attempt_number", 1) or 1)
        call_direction = str(meta.get("call_direction") or "outbound").lower()
        inbound_first_line = meta.get("inbound_first_line")

        try:
            amd_audio_seconds = float(meta.get("amd_audio_buffer_seconds") or meta.get("audio_buffer_seconds") or 0.0)
        except Exception:
            amd_audio_seconds = 0.0
        try:
            amd_speech_started_at = float(meta.get("amd_speech_started_at") or meta.get("speech_started_at") or 0.0)
        except Exception:
            amd_speech_started_at = 0.0
        amd_result = detect_answering_machine(amd_audio_seconds, amd_speech_started_at)

        business_id_from_meta = meta.get("business_id")
        agent_cfg = db.get_active_agent(business_id=business_id_from_meta)
        if not agent_cfg:
            logging.error("[AGENT] No active agent found. Aborting.")
            return

        handoff_enabled = bool(meta.get("handoff_enabled")) if "handoff_enabled" in meta else bool(
            agent_cfg.get("handoff_enabled", False)
        )
        handoff_sip_address = str(meta.get("handoff_sip_address") or agent_cfg.get("handoff_sip_address") or "").strip()
        handoff_phrases = agent_cfg.get("handoff_trigger_phrases") or [
            "speak to human",
            "talk to agent",
            "transfer me",
            "real person",
            "manager",
            "supervisor",
        ]
        if isinstance(handoff_phrases, str):
            handoff_phrases = [handoff_phrases]
        handoff_phrases = [str(x).lower() for x in handoff_phrases if str(x).strip()]

        auto_detect_language = bool(agent_cfg.get("auto_detect_language", False))
        supported_languages = agent_cfg.get("supported_languages") or ["hi-IN", "en-IN", "ta-IN", "te-IN", "kn-IN", "mr-IN"]
        if isinstance(supported_languages, str):
            supported_languages = [supported_languages]
        supported_languages = [str(x) for x in supported_languages]
        detected_language_state = {"value": ""}
        handoff_state = {"in_progress": False}

        lead = db.get_lead(lead_id) if lead_id else {}
        campaign = db.get_campaign(campaign_id) if campaign_id else {}

        active_stt = build_stt(agent_cfg)
        active_tts = build_tts(agent_cfg)
        active_llm = create_llm_with_fallback(agent_cfg)
        vad = silero.VAD.load()
        objection_handlers = db.get_objection_handlers(str(agent_cfg.get("id")))
        pronunciation_guide = db.get_pronunciation_guide(str(agent_cfg.get("id")))

        call_record = db.create_call(
            lead_id=lead_id,
            campaign_id=campaign_id,
            agent_id=str(agent_cfg["id"]),
            sip_trunk_id=sip_trunk_id,
            business_id=business_id or str(agent_cfg.get("business_id", "")),
            phone=phone,
            room_id=ctx.room.name,
            call_attempt_number=call_attempt,
            call_direction=call_direction,
        )
        call_id = str(call_record.get("id", ""))
        db.update_call(ctx.room.name, amd_result=amd_result)
        logging.info("[AMD] %s detected as %s", phone, amd_result)

        tools_instance = CallTools(
            session=None,
            ctx=ctx,
            phone=phone,
            call_id=call_id,
            lead_id=lead_id or "",
            campaign_id=campaign_id or "",
            business_id=business_id or str(agent_cfg.get("business_id", "")),
        )
        tools_instance._agent_id = str(agent_cfg["id"])

        system_prompt = build_system_prompt(agent_cfg, lead, campaign, script_override)
        system_prompt = inject_lead_context(system_prompt, lead or {}, campaign or {})
        persona_key = str(agent_cfg.get("persona") or "professional")
        persona_suffix = PERSONA_INSTRUCTIONS.get(persona_key, "")
        if persona_suffix:
            system_prompt = f"{system_prompt}\n\nPERSONA:\n{persona_suffix}"

        if bool(agent_cfg.get("memory_enabled", True)) and lead_id:
            lookback = int(agent_cfg.get("memory_lookback_calls") or 3)
            history = db.get_call_history_for_lead(lead_id)[: max(1, lookback)]
            if history:
                memory_lines = []
                for h in history:
                    memory_lines.append(
                        f"- Call on {h.get('created_at')}: Duration {h.get('duration_seconds')}s. "
                        f"Disposition: {h.get('disposition')}. Summary: {h.get('summary')}. "
                        f"Sentiment: {h.get('sentiment')}."
                    )
                system_prompt = f"{system_prompt}\n\nCUSTOMER HISTORY:\n" + "\n".join(memory_lines)

        voice_agent = Agent(
            instructions=system_prompt,
            tools=[
                tools_instance.end_call,
                tools_instance.book_appointment,
                tools_instance.reschedule_call,
                tools_instance.send_whatsapp,
                tools_instance.transfer_to_human,
                tools_instance.lookup_knowledge,
            ],
        )

        session = AgentSession(stt=active_stt, llm=active_llm, tts=active_tts, vad=vad, turn_detection=active_stt)
        tools_instance.session = session

        activity_state = {"last_activity": time.time(), "last_user_text": ""}
        turn_counter = {"n": 0}

        async def _handle_handoff(user_text: str):
            if handoff_state["in_progress"]:
                return
            handoff_state["in_progress"] = True
            await say_humanized(
                session,
                "Of course! Please hold while I transfer you.",
                user_text=user_text,
                allow_interruptions=False,
                pronunciation_guide=pronunciation_guide,
            )
            transferred = False
            if sip_trunk_id and handoff_sip_address:
                transferred = await initiate_warm_transfer(ctx.room.name, str(sip_trunk_id), handoff_sip_address)
            if transferred:
                if lead_id:
                    db.update_lead_status(lead_id, "transferred")
                db.update_call(ctx.room.name, disposition="transferred", status="transferred")
                logging.info("[HANDOFF] %s transfer initiated to %s", phone, handoff_sip_address)
            else:
                logging.warning("[HANDOFF] Transfer failed for %s to %s", phone, handoff_sip_address)
                handoff_state["in_progress"] = False

        @session.on("user_speech_committed")
        def _on_user_speech(ev):
            text = (getattr(ev, "user_transcript", "") or "").strip()
            if not text:
                return
            turn_counter["n"] += 1
            activity_state["last_activity"] = time.time()
            activity_state["last_user_text"] = text
            db.append_transcript_line(ctx.room.name, phone, "user", text, turn_counter["n"])
            live_sentiment = compute_turn_sentiment(text)
            db.update_active_call_turn(ctx.room.name, "user", text, live_sentiment=live_sentiment)
            lowered = text.lower()

            if auto_detect_language and not detected_language_state["value"]:
                detected = detect_language_from_text(text)
                if detected:
                    detected_language_state["value"] = detected
                    db.update_call(ctx.room.name, detected_language=detected)
                    if lead_id:
                        db.update_lead(lead_id, language=detected)
                    if detected in supported_languages and detected != str(agent_cfg.get("stt_language") or ""):
                        logging.info("[LANG] Switched to %s for %s", detected, phone)

            if handoff_phrases and any(phrase in lowered for phrase in handoff_phrases):
                if handoff_enabled and handoff_sip_address:
                    asyncio.create_task(_handle_handoff(text))
                else:
                    asyncio.create_task(
                        say_humanized(
                            session,
                            "I'm handling your request directly. Let me help you with that.",
                            user_text=text,
                            pronunciation_guide=pronunciation_guide,
                        )
                    )

            for handler in objection_handlers:
                phrases = handler.get("trigger_phrases") or []
                if isinstance(phrases, str):
                    phrases = [phrases]
                matched = any(str(p).lower() in lowered for p in phrases if str(p).strip())
                if matched:
                    asyncio.create_task(
                        say_humanized(
                            session,
                            str(handler.get("response") or "I understand your concern. Let me help."),
                            user_text=text,
                            pronunciation_guide=pronunciation_guide,
                        )
                    )
                    break

        @session.on("agent_speech_committed")
        def _on_agent_speech(ev):
            text = (getattr(ev, "agent_transcript", "") or getattr(ev, "transcript", "") or "").strip()
            if not text:
                return
            turn_counter["n"] += 1
            activity_state["last_activity"] = time.time()
            db.append_transcript_line(ctx.room.name, phone, "assistant", text, turn_counter["n"])
            db.update_active_call_turn(ctx.room.name, "assistant", text)

        await session.start(room=ctx.room, agent=voice_agent)

        voicemail_text = (agent_cfg.get("voicemail_message") or "").strip()
        if amd_result == "machine" and voicemail_text:
            await say_humanized(session, voicemail_text, allow_interruptions=False, pronunciation_guide=pronunciation_guide)
            db.update_call(
                ctx.room.name,
                was_voicemail=True,
                amd_result="machine",
                status="completed",
                disposition="voicemail",
            )
            if lead_id:
                db.update_lead_status(lead_id, "voicemail")
                lead_row = db.get_lead(lead_id) or {}
                current_attempts = int(lead_row.get("call_attempts") or 0)
                if current_attempts > 0:
                    db.update_lead(lead_id, call_attempts=max(current_attempts - 1, 0))
            await asyncio.sleep(1)
            await ctx.room.disconnect()
            return

        greeting_template = inbound_first_line if call_direction == "inbound" else (
            agent_cfg.get("first_line") or "Hello! How can I help you today?"
        )
        greeting_text = inject_lead_context(greeting_template, lead or {}, campaign or {})
        if is_demo and not agent_cfg.get("first_line"):
            greeting_text = "Hello! Welcome to the demo. How can I help you today?"
        consent_text = (agent_cfg.get("consent_disclosure") or "").strip()
        if consent_text:
            await say_humanized(session, consent_text, allow_interruptions=False, pronunciation_guide=pronunciation_guide)
            await asyncio.sleep(0.5)
            db.update_call(ctx.room.name, consent_disclosed=True)
        inbound_speaks_first = bool(agent_cfg.get("inbound_speaks_first", True))
        if call_direction != "inbound" or inbound_speaks_first:
            await say_humanized(session, greeting_text, allow_interruptions=True, pronunciation_guide=pronunciation_guide)

        dtmf_menu = agent_cfg.get("dtmf_menu")
        if isinstance(dtmf_menu, str):
            try:
                dtmf_menu = json.loads(dtmf_menu)
            except Exception:
                dtmf_menu = None
        if isinstance(dtmf_menu, dict) and dtmf_menu.get("prompt"):
            await say_humanized(
                session,
                str(dtmf_menu.get("prompt")),
                allow_interruptions=False,
                pronunciation_guide=pronunciation_guide,
            )
            timeout_seconds = int(dtmf_menu.get("timeout_seconds") or 5)
            await asyncio.sleep(max(1, min(timeout_seconds, 20)))
            pressed_key = str(meta.get("dtmf_digit") or "").strip()
            options = dtmf_menu.get("options") if isinstance(dtmf_menu.get("options"), dict) else {}
            if pressed_key and pressed_key in options:
                logging.info("[DTMF] %s pressed %s", phone, pressed_key)
                script = db.get_script(str(options.get(pressed_key)))
                if script:
                    routed_line = inject_lead_context(script.get("first_line") or "Thank you, connecting you now.", lead or {}, campaign or {})
                    await say_humanized(session, routed_line, pronunciation_guide=pronunciation_guide)

        asyncio.create_task(
            asyncio.to_thread(
                db.upsert_active_call,
                ctx.room.name,
                phone,
                campaign_id,
                str(agent_cfg["id"]),
                business_id or str(agent_cfg.get("business_id", "")),
                sip_trunk_id,
                "active",
            )
        )

        threshold = int(agent_cfg.get("silence_threshold_seconds", 20) or 20)
        max_nudges = int(agent_cfg.get("max_nudges", 2) or 2)
        watchdog_task = asyncio.create_task(
            silence_watchdog(
                session,
                ctx,
                activity_state,
                threshold,
                max_nudges,
                pronunciation_guide=pronunciation_guide,
            )
        )

        await session.wait()

        watchdog_task.cancel()
        try:
            await watchdog_task
        except asyncio.CancelledError:
            logger.debug("[WATCHDOG] Cancelled")

        asyncio.create_task(post_call_handler(ctx.room.name, call_id, lead_id or "", agent_cfg))

    except Exception as e:
        logging.exception(f"[AGENT] Unhandled exception: {e}")
        try:
            await ctx.room.disconnect()
        except Exception:
            return


if __name__ == "__main__":
    agents.cli.run_app(agents.WorkerOptions(entrypoint_fnc=entrypoint, agent_name="outbound-caller"))
