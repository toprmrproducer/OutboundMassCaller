import logging
import os

import aiohttp

EVOLUTION_BASE_URL = os.environ.get("EVOLUTION_API_URL", "http://localhost:8080")


async def send_whatsapp_message(instance: str, token: str, phone: str, message: str) -> bool:
    """Send WhatsApp message via Evolution API."""
    phone_clean = phone.replace("+", "").replace(" ", "")
    jid = f"{phone_clean}@s.whatsapp.net"

    url = f"{EVOLUTION_BASE_URL}/message/sendText/{instance}"
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
                    logging.info(f"[WHATSAPP] Sent to {phone}: {message[:50]}")
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
