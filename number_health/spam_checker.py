import logging
import os
import random
from datetime import datetime

import aiohttp

logger = logging.getLogger("number_health")


async def check_spam_score(phone_number: str, calls_today: int = 0, calls_per_number_limit: int = 50) -> dict:
    api_key = os.environ.get("NUMVERIFY_API_KEY")
    if api_key:
        try:
            url = "http://apilayer.net/api/validate"
            params = {"access_key": api_key, "number": phone_number}
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    body = await resp.json(content_type=None)
            valid = bool(body.get("valid"))
            line_type = (body.get("line_type") or "").lower()
            score = 10
            label = "clean"
            if not valid:
                score = 85
                label = "invalid_number"
            elif line_type in {"voip", "special_services"}:
                score = 65
                label = "potential_spam_risk"
            return {
                "phone": phone_number,
                "spam_score": score,
                "spam_label": label,
                "checked_at": datetime.utcnow().isoformat(),
            }
        except Exception as e:
            logger.warning("[SPAM] external check failed for %s: %s", phone_number, e)

    # Fallback internal heuristic
    score = 10
    label = "clean"
    if calls_today > (calls_per_number_limit * 2):
        score = 85
        label = "flagged_overused"
    elif calls_today > calls_per_number_limit:
        score = 60
        label = "warning_overused"

    return {
        "phone": phone_number,
        "spam_score": score,
        "spam_label": label,
        "checked_at": datetime.utcnow().isoformat(),
    }


def pick_best_number(sip_trunk: dict, number_pool: list, number_health_records: list) -> str:
    primary = sip_trunk.get("phone_number")
    pool = [str(p).strip() for p in (number_pool or []) if str(p).strip()]
    if not pool:
        return primary

    health_by_phone = {str(r.get("phone_number")): r for r in (number_health_records or [])}
    eligible = []
    for phone in pool:
        health = health_by_phone.get(phone, {})
        if health.get("is_paused"):
            continue
        if int(health.get("spam_score") or 0) >= 80:
            continue
        eligible.append(phone)

    if not eligible:
        return primary or pool[0]

    strategy = (sip_trunk.get("rotation_strategy") or "round_robin").lower()
    if strategy == "random":
        return random.choice(eligible)

    if strategy == "least_used":
        def usage(p: str) -> int:
            return int((health_by_phone.get(p) or {}).get("calls_today") or 0)

        eligible.sort(key=usage)
        return eligible[0]

    # round_robin
    last_index = int(sip_trunk.get("last_used_number_index") or 0)
    idx = last_index % len(eligible)
    return eligible[idx]
