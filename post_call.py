import json
import logging
import os

from openai import AsyncOpenAI

import db

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

        disposition = result.get("disposition", "")
        if lead_id:
            if disposition == "do_not_call":
                db.update_lead_status(lead_id, "dnc")
            elif disposition == "callback_requested":
                db.update_lead_status(lead_id, "scheduled")
            else:
                db.update_lead_status(lead_id, "completed")

        logging.info(f"[POST-CALL] Completed: {room_id} | disposition={disposition} | cost=${estimated_cost:.4f}")

    except Exception as e:
        logging.error(f"[POST-CALL] Error for {room_id}: {e}")
        db.update_call(room_id, status="completed")
    finally:
        db.remove_active_call(room_id)
