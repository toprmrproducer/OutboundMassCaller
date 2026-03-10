import json
import logging
import os
from datetime import datetime, timedelta

import pytz
from openai import AsyncOpenAI

import db
from whatsapp import build_followup_message, send_whatsapp_message

SUMMARY_MODEL = "gpt-4.1-nano"
SUMMARY_MAX_TOKENS = 150


async def run_post_call(room_id: str, call_id: str, lead_id: str, agent_cfg: dict):
    client = AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])

    try:
        lines = db.get_transcript(room_id)
        if not lines:
            logging.warning(f"[POST-CALL] No transcript for {room_id}")
            db.update_call(room_id, status="completed")
            return

        transcript_text = "\n".join([f"{l['role'].upper()}: {l['content']}" for l in lines])

        duration = len(lines) * 8
        total_chars = sum(len(l.get("content", "")) for l in lines if l.get("role") == "assistant")
        estimated_cost = (total_chars / 1000.0) * 0.003

        summary_prompt = f"""
Analyze this call transcript and return JSON with these fields:
- summary: 2 sentence summary of the call
- sentiment: one of: positive | neutral | negative
- disposition: one of: interested | not_interested | callback_requested | booked | do_not_call | no_answer | wrong_number
- key_points: list of 3 bullet points max

Transcript:
{transcript_text[:3000]}

Return ONLY valid JSON. No markdown.
"""

        resp = await client.chat.completions.create(
            model=SUMMARY_MODEL,
            messages=[{"role": "user", "content": summary_prompt}],
            max_tokens=SUMMARY_MAX_TOKENS,
            temperature=0.3,
        )

        result = {}
        raw = (resp.choices[0].message.content or "").strip()
        try:
            result = json.loads(raw)
        except Exception:
            result = {
                "summary": raw[:200],
                "sentiment": "neutral",
                "disposition": "interested",
            }

        if os.environ.get("PII_MASKING_ENABLED", "false").lower() == "true":
            try:
                from privacy.masker import mask_pii

                transcript_text = mask_pii(transcript_text)
                if result.get("summary"):
                    result["summary"] = mask_pii(str(result.get("summary")))
            except Exception as mask_err:
                logging.warning("[POST-CALL] PII masking failed: %s", mask_err)

        db.update_call(
            room_id,
            status="completed",
            transcript=transcript_text,
            summary=result.get("summary", ""),
            sentiment=result.get("sentiment", "neutral"),
            disposition=result.get("disposition", ""),
            duration_seconds=duration,
            estimated_cost_usd=estimated_cost,
        )

        disposition = (result.get("disposition", "") or "").strip().lower()
        if disposition == "not_interested":
            disposition = "do_not_call"

        call_row = db.get_call_by_room(room_id) or {}
        business_id = call_row.get("business_id")
        campaign_id = call_row.get("campaign_id")
        lead = db.get_lead(lead_id) if lead_id else {}
        lead_phone = (lead or {}).get("phone")
        lead_name = (lead or {}).get("name") or "there"

        # Auto DNC on explicit opt-out phrases or do_not_call disposition.
        lowered_transcript = transcript_text.lower()
        stop_phrases = [
            "remove me from your list",
            "do not call",
            "don't call",
            "stop calling",
            "unsubscribe",
            "mat call karo",
            "mujhe call mat karo",
        ]
        if business_id and lead_phone and (disposition == "do_not_call" or any(p in lowered_transcript for p in stop_phrases)):
            db.add_to_dnc(str(business_id), lead_phone, "agent_auto_optout", "agent_auto")

        # Lead status update
        if lead_id:
            if disposition == "do_not_call":
                db.update_lead_status(lead_id, "dnc")
            elif disposition == "callback_requested":
                db.update_lead_status(lead_id, "scheduled")
            else:
                db.update_lead_status(lead_id, "completed")

        # Smart retry scheduling
        if lead_id and campaign_id and disposition in {"no_answer", "busy", "failed", "voicemail"}:
            retry_cfg = db.get_retry_config(str(campaign_id), disposition)
            max_attempts = int(retry_cfg.get("max_attempts", 1) or 1)
            delay_minutes = int(retry_cfg.get("delay_minutes", 60) or 60)
            attempts = int((lead or {}).get("call_attempts") or 0)
            if attempts < max_attempts:
                next_call_at = datetime.utcnow() + timedelta(minutes=delay_minutes)
                campaign = db.get_campaign(str(campaign_id)) or {}
                strategy = campaign.get("retry_strategy") or {}
                best_window = strategy.get("best_time_window") or {}
                tz_name = campaign.get("timezone", "Asia/Kolkata")
                try:
                    tz = pytz.timezone(tz_name)
                    local_next = pytz.utc.localize(next_call_at).astimezone(tz)
                    start_s = best_window.get("start", "10:00")
                    end_s = best_window.get("end", "18:00")
                    sh, sm = [int(x) for x in start_s.split(":")]
                    eh, em = [int(x) for x in end_s.split(":")]
                    start_dt = local_next.replace(hour=sh, minute=sm, second=0, microsecond=0)
                    end_dt = local_next.replace(hour=eh, minute=em, second=0, microsecond=0)
                    if local_next < start_dt:
                        local_next = start_dt
                    elif local_next > end_dt:
                        local_next = (start_dt + timedelta(days=1))
                    next_call_at = local_next.astimezone(pytz.utc).replace(tzinfo=None)
                except Exception:
                    pass
                db.reschedule_lead(lead_id, next_call_at.isoformat(), reason=f"auto_retry_{disposition}")
            else:
                db.update_lead_status(lead_id, disposition if disposition in {"failed", "no_answer", "dnc"} else "failed")

        # Automated WhatsApp follow-up
        send_followup = bool(call_row.get("was_booked")) or disposition in {"interested", "callback_requested"}
        if send_followup and business_id and lead_phone:
            business = db.get_business(str(business_id))
            if business and business.get("whatsapp_instance") and business.get("whatsapp_token"):
                booking_time = None
                if call_row.get("was_booked"):
                    bookings = db.get_bookings(str(business_id), limit=100)
                    for b in bookings:
                        if str(b.get("call_id")) == str(call_id):
                            booking_time = str(b.get("start_time"))
                            break
                message = build_followup_message(
                    "booked" if call_row.get("was_booked") else disposition,
                    lead_name,
                    booking_time=booking_time,
                )
                sent = await send_whatsapp_message(
                    instance_id=business["whatsapp_instance"],
                    token=business["whatsapp_token"],
                    phone=lead_phone,
                    message=message,
                )
                if sent:
                    db.update_call(room_id, whatsapp_sent=True)
                else:
                    logging.error("[POST-CALL] WhatsApp send failed for room_id=%s", room_id)

        # Trigger post-call survey (non-blocking)
        try:
            if business_id:
                business = db.get_business(str(business_id))
            else:
                business = None
            if business and call_row:
                from surveys.survey_engine import trigger_survey_for_call

                await trigger_survey_for_call(call_row, lead or {}, business)
        except Exception as survey_err:
            logging.warning("[POST-CALL] Survey scheduling failed for room_id=%s error=%s", room_id, survey_err)

        logging.info(f"[POST-CALL] Completed: {room_id} | disposition={disposition} | cost=${estimated_cost:.4f}")

    except Exception as e:
        logging.error(f"[POST-CALL] Error for {room_id}: {e}")
        db.update_call(room_id, status="completed")
    finally:
        db.remove_active_call(room_id)
