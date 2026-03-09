import asyncio
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Any

import certifi
from dotenv import load_dotenv
from livekit import api
from livekit.agents import Agent, AgentSession, JobContext, WorkerOptions, cli, llm
from livekit.plugins import silero

import db

os.environ["SSL_CERT_FILE"] = certifi.where()
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("voice-agent")

try:
    if db.initdb():
        logger.info("STARTUP: DB init complete")
    else:
        logger.warning("STARTUP: DB init failed")
except Exception as exc:
    logger.warning("STARTUP: DB init failed: %s", exc)


FILLERS = ["Umm,", "Hmm,", "Acha,", "Dekhiye,"]


def humanize_text(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return text
    if len(text.split()) > 22:
        text = text.replace(" and ", ". And ")
    text = re.sub(r"\?", "...?", text)
    if len(text.split()) > 8 and random.random() < 0.40:
        text = f"{random.choice(FILLERS)} {text}"
    return text


async def say_humanized(session: AgentSession, text: str, allow_interruptions: bool = True) -> None:
    try:
        await session.say(humanize_text(text), allow_interruptions=allow_interruptions)
    except Exception as exc:
        logger.exception("session.say failed: %s", exc)


def parse_metadata(metadata: str) -> dict[str, Any]:
    if not metadata:
        return {}
    try:
        parsed = json.loads(metadata)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def parse_phone_from_context(ctx: JobContext, metadata: str) -> str:
    phone = "unknown"
    meta = parse_metadata(metadata)
    phone = (meta.get("phone") or meta.get("phone_number") or "").strip() or phone

    if phone == "unknown" and metadata.startswith("+"):
        phone = metadata.strip()

    for identity, participant in ctx.room.remote_participants.items():
        attrs = participant.attributes or {}
        p = attrs.get("sip.phoneNumber") or attrs.get("phoneNumber") or ""
        if p:
            phone = p
            break
        if "+" in identity:
            match = re.search(r"\+\d{7,15}", identity)
            if match:
                phone = match.group(0)
                break

    return phone


def build_system_prompt(agent_cfg: dict[str, Any], phone: str) -> str:
    now_ist = datetime.now(timezone.utc).astimezone()
    base = (agent_cfg.get("system_prompt") or "").strip()
    instructions = (agent_cfg.get("agent_instructions") or "").strip()
    return "\n\n".join(
        x
        for x in [
            base,
            instructions,
            f"Caller phone: {phone}",
            f"Current datetime: {now_ist.isoformat()}",
            "Keep responses concise and conversational. Use tools when needed.",
        ]
        if x
    )


def build_stt(cfg: dict[str, Any]):
    provider = (cfg.get("stt_provider") or "sarvam").lower()
    language = cfg.get("stt_language") or "hi-IN"

    if provider == "deepgram":
        from livekit.plugins import deepgram

        if not language.lower().startswith("en"):
            language = "en"
        return deepgram.STT(model="nova-3", language=language)

    from livekit.plugins import sarvam

    return sarvam.STT(language=language, model=cfg.get("stt_model", "saaras:v3"))


def build_tts(cfg: dict[str, Any]):
    provider = (cfg.get("tts_provider") or "sarvam").lower()

    if provider == "elevenlabs":
        from livekit.plugins import elevenlabs

        return elevenlabs.TTS(voice_id=cfg.get("tts_voice"), model="eleven_flash_v2_5")

    from livekit.plugins import sarvam

    return sarvam.TTS(speaker=cfg.get("tts_voice", "rohan"), language=cfg.get("tts_language", "hi-IN"))


def build_llm(cfg: dict[str, Any]):
    from livekit.plugins import openai as lk_openai

    kwargs: dict[str, Any] = {
        "model": cfg.get("llm_model") or "gpt-4o-mini",
        "temperature": cfg.get("llm_temperature", 0.7),
    }
    if cfg.get("llm_base_url"):
        kwargs["base_url"] = cfg["llm_base_url"]
    return lk_openai.LLM(**kwargs)


def find_sip_identity(ctx: JobContext) -> str | None:
    for identity, participant in ctx.room.remote_participants.items():
        attrs = participant.attributes or {}
        if identity.startswith("sip_") or attrs.get("sip.callID"):
            return identity
    return None


async def start_recording(room_name: str) -> str | None:
    livekit_url = os.getenv("LIVEKIT_URL", "")
    livekit_key = os.getenv("LIVEKIT_API_KEY", "")
    livekit_secret = os.getenv("LIVEKIT_API_SECRET", "")
    r2_bucket = os.getenv("R2_BUCKET", "")
    r2_endpoint = os.getenv("R2_ENDPOINT", "")
    r2_access_key = os.getenv("R2_ACCESS_KEY", "")
    r2_secret_key = os.getenv("R2_SECRET_KEY", "")

    if not all([livekit_url, livekit_key, livekit_secret, r2_bucket, r2_endpoint, r2_access_key, r2_secret_key]):
        logger.info("Recording skipped: missing R2 or LiveKit env vars")
        return None

    lk = api.LiveKitAPI(url=livekit_url, api_key=livekit_key, api_secret=livekit_secret)
    try:
        req = api.RoomCompositeEgressRequest(
            room_name=room_name,
            audio_only=True,
            file_outputs=[
                api.EncodedFileOutput(
                    file_type=api.EncodedFileType.OGG,
                    filepath=f"recordings/{room_name}.ogg",
                    s3=api.S3Upload(
                        access_key=r2_access_key,
                        secret=r2_secret_key,
                        bucket=r2_bucket,
                        region=os.getenv("R2_REGION", "auto"),
                        endpoint=r2_endpoint,
                        force_path_style=True,
                    ),
                )
            ],
        )
        response = await lk.egress.start_room_composite_egress(req)
        return response.egress_id
    except Exception as exc:
        logger.exception("start_recording failed: %s", exc)
        return None
    finally:
        await lk.aclose()


async def stop_recording(egress_id: str | None, room_name: str) -> str:
    if not egress_id:
        return ""

    livekit_url = os.getenv("LIVEKIT_URL", "")
    livekit_key = os.getenv("LIVEKIT_API_KEY", "")
    livekit_secret = os.getenv("LIVEKIT_API_SECRET", "")

    if not all([livekit_url, livekit_key, livekit_secret]):
        return ""

    lk = api.LiveKitAPI(url=livekit_url, api_key=livekit_key, api_secret=livekit_secret)
    try:
        await lk.egress.stop_egress(api.StopEgressRequest(egress_id=egress_id))
    except Exception as exc:
        logger.exception("stop_recording failed: %s", exc)
    finally:
        await lk.aclose()

    endpoint = os.getenv("R2_PUBLIC_BASE_URL", "").strip() or os.getenv("R2_ENDPOINT", "").strip()
    bucket = os.getenv("R2_BUCKET", "").strip()
    if not endpoint or not bucket:
        return ""
    return f"{endpoint.rstrip('/')}/{bucket}/recordings/{room_name}.ogg"


def infer_sentiment(transcript_text: str) -> str:
    text = (transcript_text or "").lower()
    if any(k in text for k in ["angry", "frustrated", "not interested", "stop calling", "bad"]):
        return "negative"
    if any(k in text for k in ["thanks", "great", "perfect", "book", "yes"]):
        return "positive"
    return "neutral"


class CallTools:
    def __init__(self, session: AgentSession | None, room, phone: str, call_id: str | None):
        self.session = session
        self.room = room
        self.phone = phone
        self.call_id = call_id
        self.ctx_api = None
        self.sip_participant_identity: str | None = None

    @llm.function_tool
    async def end_call(self) -> str:
        """End the current call gracefully."""
        try:
            if self.session is not None:
                await say_humanized(self.session, "Thank you for your time. Goodbye!")
                await asyncio.sleep(0.8)
            await self.room.disconnect()
            return "Call ended gracefully."
        except Exception as exc:
            logger.exception("end_call tool failed: %s", exc)
            return "I could not end the call right now."

    @llm.function_tool
    async def book_appointment(self, name: str, phone: str, date_time: str, notes: str = "") -> str:
        """Book an appointment for the caller."""
        if not self.call_id:
            logger.error("book_appointment called without call_id")
            return "I cannot book this right now due to missing call context."

        try:
            parsed = datetime.fromisoformat(date_time.replace("Z", "+00:00"))
            booking = db.create_booking(
                call_id=self.call_id,
                caller_name=name,
                caller_phone=phone,
                caller_email="",
                start_time=parsed.isoformat(),
                notes=notes,
            )
            if not booking:
                return "I could not confirm that booking right now."

            db.update_call(self.room.name, was_booked=True, summary=f"Booking created for {parsed.isoformat()}")
            return f"Appointment booked for {name} at {parsed.isoformat()}."
        except Exception as exc:
            logger.exception("book_appointment tool failed: %s", exc)
            return "I could not create the booking."

    @llm.function_tool
    async def transfer_call(self, transfer_to: str) -> str:
        """Transfer the call to a human agent."""
        if not self.ctx_api or not self.sip_participant_identity:
            logger.error("transfer_call tool missing SIP context")
            return "Transfer is not available for this call."

        destination = transfer_to.strip()
        if destination and not destination.startswith("sip:") and not destination.startswith("tel:"):
            destination = f"tel:{destination}"

        try:
            await self.ctx_api.sip.transfer_sip_participant(
                api.TransferSIPParticipantRequest(
                    room_name=self.room.name,
                    participant_identity=self.sip_participant_identity,
                    transfer_to=destination,
                    play_dialtone=False,
                )
            )
            return "Transfer initiated."
        except Exception as exc:
            logger.exception("transfer_call tool failed: %s", exc)
            return "I could not transfer the call right now."


async def silence_watchdog(session: AgentSession, ctx: JobContext, activity: dict[str, float]) -> None:
    silence_threshold = 20
    max_nudges = 2
    nudge_count = 0

    while True:
        await asyncio.sleep(5)
        if time.time() - activity["last"] <= silence_threshold:
            continue

        nudge_count += 1
        if nudge_count > max_nudges:
            await say_humanized(session, "It seems you're not available right now. I'll try again later. Goodbye!")
            await asyncio.sleep(3)
            await ctx.room.disconnect()
            return

        await say_humanized(session, "Are you still there?")
        activity["last"] = time.time()


async def post_call_cleanup(
    room_id: str,
    call_id: str | None,
    transcript_lines: list[tuple[str, str]],
    call_started_at: datetime,
    egress_id: str | None,
) -> None:
    try:
        duration = max(0, int((datetime.now(timezone.utc) - call_started_at).total_seconds()))
        transcript_text = "\n".join(f"[{role.upper()}] {content}" for role, content in transcript_lines)
        sentiment = infer_sentiment(transcript_text)
        recording_url = await stop_recording(egress_id, room_id)

        summary = "Call completed"
        if transcript_lines:
            summary = transcript_lines[-1][1][:200]

        now_ist = datetime.now()
        update_payload: dict[str, Any] = {
            "status": "completed",
            "duration_seconds": duration,
            "transcript": transcript_text,
            "summary": summary,
            "sentiment": sentiment,
            "recording_url": recording_url,
            "estimated_cost_usd": round((duration / 60.0) * 0.01, 4),
            "call_date": now_ist.date().isoformat(),
            "call_hour": now_ist.hour,
        }
        db.update_call(room_id, **update_payload)
        db.remove_active_call(room_id)

        if call_id:
            call_row = db.get_call_by_room(room_id)
            if call_row and call_row.get("status") == "completed" and call_row.get("lead_id"):
                db.update_lead_status(str(call_row["lead_id"]), "completed")
    except Exception as exc:
        logger.exception("post_call_cleanup failed: %s", exc)


async def entrypoint(ctx: JobContext):
    call_record: dict[str, Any] = {}
    transcript_lines: list[tuple[str, str]] = []
    egress_id: str | None = None

    try:
        await ctx.connect()

        metadata = ctx.job.metadata or ""
        meta = parse_metadata(metadata)
        is_demo = metadata == "demo" or metadata.startswith("demo") or bool(meta.get("is_demo"))

        agent_cfg = db.get_active_agent()
        if not agent_cfg:
            logger.error("No active agent in DB — cannot proceed")
            return

        stt = build_stt(agent_cfg)
        tts = build_tts(agent_cfg)
        llm_engine = build_llm(agent_cfg)
        vad = silero.VAD.load()

        phone = parse_phone_from_context(ctx, metadata)

        call_record = db.create_call(
            lead_id=meta.get("lead_id"),
            campaign_id=meta.get("campaign_id"),
            agent_id=str(agent_cfg.get("id")),
            sip_trunk_id=meta.get("sip_trunk_id"),
            phone=phone,
            room_id=ctx.room.name,
        )

        system_prompt = build_system_prompt(agent_cfg, phone)

        call_tools = CallTools(session=None, room=ctx.room, phone=phone, call_id=call_record.get("id"))
        call_tools.ctx_api = ctx.api
        call_tools.sip_participant_identity = find_sip_identity(ctx)
        toolset = llm.find_function_tools(call_tools)

        voice_agent = Agent(instructions=system_prompt, tools=toolset)

        session = AgentSession(
            stt=stt,
            llm=llm_engine,
            tts=tts,
            vad=vad,
            turn_detection=stt,
        )
        call_tools.session = session

        activity = {"last": time.time()}
        call_started_at = datetime.now(timezone.utc)

        @session.on("user_speech_committed")
        def _on_user_speech(ev):
            try:
                text = (getattr(ev, "user_transcript", "") or "").strip()
                if not text:
                    return
                activity["last"] = time.time()
                transcript_lines.append(("user", text))
                db.append_transcript_line(ctx.room.name, phone, "user", text)
            except Exception as exc:
                logger.exception("user transcript handler failed: %s", exc)

        @session.on("agent_speech_committed")
        def _on_agent_speech(ev):
            try:
                text = (
                    getattr(ev, "agent_transcript", "")
                    or getattr(ev, "transcript", "")
                    or ""
                ).strip()
                if not text:
                    return
                activity["last"] = time.time()
                transcript_lines.append(("assistant", text))
                db.append_transcript_line(ctx.room.name, phone, "assistant", text)
            except Exception as exc:
                logger.exception("agent transcript handler failed: %s", exc)

        await session.start(room=ctx.room, agent=voice_agent)

        greeting = (
            agent_cfg.get("first_line")
            or agent_cfg.get("opening_greeting")
            or "Hello! How can I help you today?"
        )
        if is_demo:
            greeting = greeting or "Welcome to the demo. How can I help you today?"

        await say_humanized(session, greeting, allow_interruptions=True)

        asyncio.create_task(
            asyncio.to_thread(
                db.upsert_active_call,
                ctx.room.name,
                phone,
                meta.get("campaign_id"),
                str(agent_cfg.get("id")),
                "active",
                meta.get("sip_trunk_id"),
            )
        )

        egress_id = await start_recording(ctx.room.name)
        watchdog_task = asyncio.create_task(silence_watchdog(session, ctx, activity))

        await session.wait()

        watchdog_task.cancel()
        try:
            await watchdog_task
        except asyncio.CancelledError:
            pass

        await post_call_cleanup(
            room_id=ctx.room.name,
            call_id=call_record.get("id"),
            transcript_lines=transcript_lines,
            call_started_at=call_started_at,
            egress_id=egress_id,
        )

    except Exception as exc:
        logger.exception("entrypoint failed: %s", exc)
        room_name = getattr(getattr(ctx, "room", None), "name", "")
        if room_name:
            db.update_call(room_name, status="failed", summary=str(exc))
            db.remove_active_call(room_name)


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint, agent_name="outbound-caller"))
