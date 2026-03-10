import logging
from datetime import datetime

import db

logger = logging.getLogger("holidays.engine")

INDIA_PUBLIC_HOLIDAYS_2026 = [
    ("Republic Day", "2026-01-26"),
    ("Holi", "2026-03-03"),
    ("Good Friday", "2026-04-03"),
    ("Ambedkar Jayanti", "2026-04-14"),
    ("Ram Navami", "2026-04-16"),
    ("Maharashtra Day", "2026-05-01"),
    ("Independence Day", "2026-08-15"),
    ("Janmashtami", "2026-08-22"),
    ("Gandhi Jayanti", "2026-10-02"),
    ("Dussehra", "2026-10-20"),
    ("Diwali", "2026-11-08"),
    ("Christmas", "2026-12-25"),
]


def seed_default_holidays(business_id: str, db_conn=None) -> int:
    _ = db_conn
    rows = [
        {
            "name": name,
            "date": date,
            "country_code": "IN",
            "is_recurring": True,
            "skip_calls": True,
        }
        for name, date in INDIA_PUBLIC_HOLIDAYS_2026
    ]
    inserted = db.seed_holidays(business_id, rows)
    logger.info("[HOLIDAY] seeded defaults business_id=%s inserted=%s", business_id, inserted)
    return inserted


def is_holiday_today(business_id: str, db_conn=None) -> tuple[bool, str]:
    _ = db_conn
    today = datetime.utcnow().date().isoformat()
    rows = db.get_holidays(business_id)
    for row in rows:
        if str(row.get("date")) == today and bool(row.get("skip_calls", True)):
            return True, row.get("name") or "Holiday"
    return False, ""
