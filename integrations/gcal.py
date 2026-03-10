from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta


async def create_calendar_event(
    calendar_id: str,
    summary: str,
    description: str,
    start_time: str,
    duration_minutes: int = 30,
    attendee_email: str = None,
) -> str:
    """
    Placeholder Google Calendar integration.
    Degrades gracefully if credentials are missing.
    """
    creds_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not creds_path or not calendar_id:
        logging.info("[GCAL] Missing credentials or calendar; skipping sync")
        return ""

    try:
        start_dt = datetime.fromisoformat(str(start_time).replace("Z", "+00:00"))
    except Exception:
        start_dt = datetime.utcnow()
    end_dt = start_dt + timedelta(minutes=max(1, int(duration_minutes or 30)))

    # Keep this lightweight and dependency-free in server runtime.
    # If google-api-python-client is installed, this can be replaced with real API call.
    event_id = f"local-{int(start_dt.timestamp())}"
    logging.info(
        "[GCAL] Simulated event created calendar=%s start=%s end=%s attendee=%s",
        calendar_id,
        start_dt.isoformat(),
        end_dt.isoformat(),
        attendee_email,
    )
    return event_id
