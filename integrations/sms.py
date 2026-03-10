from __future__ import annotations

import logging
import os

import aiohttp


async def send_sms(provider, api_key, sender_id, phone, message) -> bool:
    p = str(provider or "").lower()
    if p == "msg91":
        if not api_key:
            return False
        url = "https://api.msg91.com/api/v2/sendsms"
        payload = {
            "sender": sender_id or "RAPIDXAI",
            "route": "4",
            "country": "91",
            "sms": [{"message": message, "to": [str(phone).replace("+", "")]}],
        }
        headers = {"authkey": api_key, "Content-Type": "application/json"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    return resp.status in (200, 201)
        except Exception as e:
            logging.warning("[SMS] MSG91 failed: %s", e)
            return False

    if p == "twilio":
        # api_key should be SID:TOKEN or fallback to env vars
        sid = os.environ.get("TWILIO_ACCOUNT_SID")
        token = os.environ.get("TWILIO_AUTH_TOKEN")
        if api_key and ":" in str(api_key):
            sid, token = str(api_key).split(":", 1)
        if not sid or not token or not sender_id:
            return False
        url = f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json"
        data = {"From": sender_id, "To": phone, "Body": message}
        try:
            async with aiohttp.ClientSession(auth=aiohttp.BasicAuth(login=sid, password=token)) as session:
                async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    return resp.status in (200, 201)
        except Exception as e:
            logging.warning("[SMS] Twilio failed: %s", e)
            return False

    return False
