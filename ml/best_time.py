from datetime import datetime
from typing import Optional

import pytz


def get_best_call_hours(campaign_id: str, db_conn: Optional[object] = None) -> list[int]:
    """
    Analyse historical connection rates and return ranked call hours.
    Falls back to a broad daytime range when history is too small.
    """
    _ = db_conn
    try:
        import db

        rows = db.get_hourly_connection_rates(campaign_id)
        total_calls = sum(int(r.get("total_calls") or 0) for r in rows)
        if total_calls < 20:
            return list(range(9, 21))
        ranked = sorted(
            rows,
            key=lambda r: (
                float(r.get("connection_rate") or 0.0),
                int(r.get("connected") or 0),
                int(r.get("total_calls") or 0),
            ),
            reverse=True,
        )
        return [int(r.get("hour")) for r in ranked if r.get("hour") is not None]
    except Exception:
        return list(range(9, 21))


def should_call_now(campaign_id: str, timezone: str, best_hours: list[int]) -> bool:
    """
    True when current local hour is in the top 50% of predicted hours.
    """
    _ = campaign_id
    if not best_hours:
        return True
    try:
        tz = pytz.timezone(timezone or "Asia/Kolkata")
    except Exception:
        tz = pytz.timezone("Asia/Kolkata")
    now_hour = datetime.now(tz).hour
    cutoff = max(1, len(best_hours) // 2)
    top_half = set(best_hours[:cutoff])
    return now_hour in top_half
