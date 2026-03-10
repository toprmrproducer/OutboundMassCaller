from __future__ import annotations


def compute_lead_score(lead: dict, call_history: list[dict]) -> tuple[int, dict]:
    """
    Returns (score: int 0-100, factors: dict explaining score).

    Rules:
    +20 if lead has name
    +10 if lead has email
    +15 if any call sentiment='positive'
    -10 if any call disposition='not_interested'
    +20 if any call was_booked=true
    +10 if call_history count >= 2
    -15 if lead.call_attempts >= lead.max_call_attempts - 1
    +10 if lead.language != 'hi-IN'
    Clamp to 0-100.
    """
    factors: dict[str, int | bool] = {}
    score = 50

    if (lead or {}).get("name"):
        score += 20
        factors["has_name"] = 20
    else:
        factors["has_name"] = 0

    if (lead or {}).get("email"):
        score += 10
        factors["has_email"] = 10
    else:
        factors["has_email"] = 0

    sentiments = [str((c or {}).get("sentiment") or "").lower() for c in (call_history or [])]
    dispositions = [str((c or {}).get("disposition") or "").lower() for c in (call_history or [])]
    booked_any = any(bool((c or {}).get("was_booked")) for c in (call_history or []))

    if any(s == "positive" for s in sentiments):
        score += 15
        factors["positive_sentiment_history"] = 15
    else:
        factors["positive_sentiment_history"] = 0

    if any(d == "not_interested" for d in dispositions):
        score -= 10
        factors["not_interested_penalty"] = -10
    else:
        factors["not_interested_penalty"] = 0

    if booked_any:
        score += 20
        factors["booked_history"] = 20
    else:
        factors["booked_history"] = 0

    if len(call_history or []) >= 2:
        score += 10
        factors["multi_call_engagement"] = 10
    else:
        factors["multi_call_engagement"] = 0

    call_attempts = int((lead or {}).get("call_attempts") or 0)
    max_call_attempts = int((lead or {}).get("max_call_attempts") or 3)
    if call_attempts >= max(0, max_call_attempts - 1):
        score -= 15
        factors["attempts_penalty"] = -15
    else:
        factors["attempts_penalty"] = 0

    language = str((lead or {}).get("language") or "hi-IN")
    if language and language != "hi-IN":
        score += 10
        factors["non_default_language"] = 10
    else:
        factors["non_default_language"] = 0

    score = max(0, min(100, int(score)))
    factors["final_score"] = score
    return score, factors


def classify_lead_temperature(score: int) -> str:
    """
    Returns 'hot' (score >= 75), 'warm' (score 40-74), 'cold' (score < 40)
    """
    if int(score) >= 75:
        return "hot"
    if int(score) >= 40:
        return "warm"
    return "cold"
