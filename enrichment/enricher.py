from __future__ import annotations

import os
from typing import Any

import aiohttp
import phonenumbers
from phonenumbers import NumberParseException


def normalize_phone_e164(phone: str, default_country: str = "IN") -> str:
    """
    Normalize various phone formats to E.164. Returns original on parse failure.
    """
    raw = str(phone or "").strip().replace(" ", "")
    if not raw:
        return raw
    try:
        parsed = phonenumbers.parse(raw, default_country)
        if not phonenumbers.is_valid_number(parsed):
            return raw
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except NumberParseException:
        return raw


def _infer_carrier_hint(phone_e164: str) -> str | None:
    # Lightweight prefix heuristic for Indian mobile ranges.
    p = str(phone_e164 or "")
    if p.startswith("+91") and len(p) >= 6:
        prefix = p[3:6]
        if prefix[0] in {"9", "8", "7", "6"}:
            return "mobile"
        return "unknown"
    return None


async def _verify_email_hunter(email: str, api_key: str) -> bool | None:
    if not email or not api_key:
        return None
    url = "https://api.hunter.io/v2/email-verifier"
    params = {"email": email, "api_key": api_key}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                payload = await resp.json()
                status = (((payload or {}).get("data") or {}).get("status") or "").lower()
                if status in {"valid", "accept_all"}:
                    return True
                if status in {"invalid", "webmail", "disposable"}:
                    return False
                return None
    except Exception:
        return None


async def enrich_lead(phone: str, existing_data: dict) -> dict:
    """
    Enrich lead data without overwriting non-null existing values.
    """
    existing = existing_data or {}
    out: dict[str, Any] = {}

    normalized = normalize_phone_e164(phone)
    if normalized and not existing.get("phone_e164") and normalized != phone:
        out["phone_e164"] = normalized

    country_code = None
    try:
        parsed = phonenumbers.parse(normalized or phone, "IN")
        country_code = f"+{parsed.country_code}" if parsed.country_code else None
    except Exception:
        country_code = None

    if country_code and not existing.get("country_code"):
        out["country_code"] = country_code

    carrier_hint = _infer_carrier_hint(normalized or phone)
    if carrier_hint:
        base_custom = existing.get("custom_data") if isinstance(existing.get("custom_data"), dict) else {}
        merged_custom = dict(base_custom)
        if merged_custom.get("carrier_hint") is None:
            merged_custom["carrier_hint"] = carrier_hint
            out["custom_data"] = merged_custom

    if existing.get("email") and existing.get("email_verified") is None:
        hunter_key = os.environ.get("HUNTER_KEY")
        verified = await _verify_email_hunter(str(existing.get("email")), hunter_key) if hunter_key else None
        if verified is not None:
            out["email_verified"] = verified

    return out
