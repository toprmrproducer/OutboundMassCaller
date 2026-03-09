import logging
import os

import aiohttp

EVOLUTION_BASE_URL = os.environ.get("EVOLUTION_API_URL", "http://localhost:8080")


def build_followup_message(disposition: str, lead_name: str, booking_time: str | None = None) -> str:
    name = (lead_name or "there").strip() or "there"
    disp = (disposition or "").strip().lower()
    if disp == "booked":
        time_text = booking_time or "your scheduled time"
        return f"Hi {name}! Your appointment is confirmed for {time_text}. Reply STOP to opt out."
    if disp == "callback_requested":
        return (
            f"Hi {name}! We've noted your callback request and will call you back at your preferred time. "
            "Reply STOP to opt out."
        )
    return f"Hi {name}! Thank you for your interest. Our team will follow up with you shortly. Reply STOP to opt out."


async def send_whatsapp_message(instance_id: str, token: str, phone: str, message: str) -> bool:
    """Send WhatsApp message via the configured HTTP API."""
    phone_clean = phone.replace("+", "").replace(" ", "")
    jid = f"{phone_clean}@s.whatsapp.net"

    url = f"{EVOLUTION_BASE_URL}/message/sendText/{instance_id}"
    headers = {"Content-Type": "application/json", "apikey": token}
    payload = {"number": jid, "text": message}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 201):
                    logging.info("[WHATSAPP] Sent to %s", phone)
                    return True
                body = await resp.text()
                logging.error(f"[WHATSAPP] Failed {resp.status}: {body}")
                return False
    except Exception as e:
        logging.error(f"[WHATSAPP] Error: {e}")
        return False


async def send_whatsapp_template(instance: str, token: str, phone: str, template_name: str, variables: list[str]) -> bool:
    """Send WhatsApp template message."""
    phone_clean = phone.replace("+", "").replace(" ", "")
    jid = f"{phone_clean}@s.whatsapp.net"

    url = f"{EVOLUTION_BASE_URL}/message/sendTemplate/{instance}"
    headers = {"Content-Type": "application/json", "apikey": token}
    payload = {
        "number": jid,
        "template": {
            "name": template_name,
            "language": {"code": "en"},
            "components": [
                {
                    "type": "body",
                    "parameters": [{"type": "text", "text": v} for v in variables],
                }
            ],
        },
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                return resp.status in (200, 201)
    except Exception as e:
        logging.error(f"[WHATSAPP] Template error: {e}")
        return False
