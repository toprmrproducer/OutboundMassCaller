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

import db
from livekit import agents
from livekit.agents import Agent, AgentSession, function_tool
from livekit.plugins import openai as lk_openai
from livekit.plugins import sarvam, silero

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agent")

try:
    db.initdb()
    logger.info("STARTUP: DB init complete")
except Exception as e:
    logger.warning(f"STARTUP: DB init failed: {e}")

FILLERS_THINKING = ["Umm...", "Let me see...", "Acha..."]
FILLERS_AGREEMENT = ["Haan ji,", "Bilkul,", "Theek hai,"]
FILLERS_EMPATHY = ["Samajh sakta hoon,", "Haan, woh toh hai,"]


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


async def say_humanized(session: AgentSession, text: str, user_text: str = "", allow_interruptions: bool = True):
    await session.say(humanize_text(text, user_text), allow_interruptions=allow_interruptions)


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
            db.create_booking(
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
                    instance=business["whatsapp_instance"],
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
        db.update_call(self.ctx.room.name, disposition="transfer_requested")
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


async def silence_watchdog(session: AgentSession, ctx, activity_state: dict, threshold: int = 20, max_nudges: int = 2):
    nudge_count = 0
    while True:
        await asyncio.sleep(5)
        if time.time() - activity_state["last_activity"] > threshold:
            nudge_count += 1
            if nudge_count > max_nudges:
                await say_humanized(session, "It seems you're not available right now. I'll try again later. Goodbye!")
                await asyncio.sleep(3)
                await ctx.room.disconnect()
                return
            await say_humanized(session, "Are you still there?")
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

        business_id_from_meta = meta.get("business_id")
        agent_cfg = db.get_active_agent(business_id=business_id_from_meta)
        if not agent_cfg:
            logging.error("[AGENT] No active agent found. Aborting.")
            return

        lead = db.get_lead(lead_id) if lead_id else {}
        campaign = db.get_campaign(campaign_id) if campaign_id else {}

        active_stt = build_stt(agent_cfg)
        active_tts = build_tts(agent_cfg)
        active_llm = build_llm(agent_cfg)
        vad = silero.VAD.load()

        call_record = db.create_call(
            lead_id=lead_id,
            campaign_id=campaign_id,
            agent_id=str(agent_cfg["id"]),
            sip_trunk_id=sip_trunk_id,
            business_id=business_id or str(agent_cfg.get("business_id", "")),
            phone=phone,
            room_id=ctx.room.name,
            call_attempt_number=call_attempt,
        )
        call_id = str(call_record.get("id", ""))

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

        @session.on("user_speech_committed")
        def _on_user_speech(ev):
            text = (getattr(ev, "user_transcript", "") or "").strip()
            if not text:
                return
            turn_counter["n"] += 1
            activity_state["last_activity"] = time.time()
            activity_state["last_user_text"] = text
            db.append_transcript_line(ctx.room.name, phone, "user", text, turn_counter["n"])

        @session.on("agent_speech_committed")
        def _on_agent_speech(ev):
            text = (getattr(ev, "agent_transcript", "") or getattr(ev, "transcript", "") or "").strip()
            if not text:
                return
            turn_counter["n"] += 1
            activity_state["last_activity"] = time.time()
            db.append_transcript_line(ctx.room.name, phone, "assistant", text, turn_counter["n"])

        await session.start(room=ctx.room, agent=voice_agent)

        greeting_text = agent_cfg.get("first_line") or "Hello! How can I help you today?"
        if is_demo and not agent_cfg.get("first_line"):
            greeting_text = "Hello! Welcome to the demo. How can I help you today?"
        await say_humanized(session, greeting_text, allow_interruptions=True)

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
        watchdog_task = asyncio.create_task(silence_watchdog(session, ctx, activity_state, threshold, max_nudges))

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
