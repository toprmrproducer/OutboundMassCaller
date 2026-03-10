import asyncio
import logging
from datetime import datetime

import db

logger = logging.getLogger("reminders.scheduler")


def schedule_reminders_for_booking(booking: dict, business: dict, lead: dict, agent_id: str, db_conn=None) -> list[dict]:
    _ = db_conn
    hours_list = business.get("reminder_hours_before") or [24, 2]
    if isinstance(hours_list, str):
        hours_list = [int(x) for x in hours_list.split(",") if x.strip().isdigit()]
    created = db.create_reminder_calls_for_booking(
        booking_id=str(booking["id"]),
        business_id=str(business["id"]),
        lead_id=str(lead.get("id")) if lead else None,
        agent_id=agent_id,
        phone=lead.get("phone") or booking.get("caller_phone"),
        hours_list=hours_list,
    )
    return created


async def run_reminder_dispatch_loop():
    while True:
        try:
            due = db.get_due_reminders(buffer_minutes=5)
            if due:
                from ui_server import dispatch_outbound_call

                for reminder in due:
                    try:
                        lead_name = reminder.get("lead_name") or "there"
                        appointment_time = reminder.get("booking_start_time")
                        reminder_prompt = (
                            "You are calling {lead_name} to remind them about their appointment scheduled "
                            "for {appointment_time}. Confirm they will attend. If they want to cancel "
                            "or reschedule, collect their preference and say the team will be in touch."
                        ).format(lead_name=lead_name, appointment_time=appointment_time)

                        lead = db.get_lead(str(reminder.get("lead_id"))) if reminder.get("lead_id") else None
                        campaign = db.get_campaign(str(lead.get("campaign_id"))) if lead and lead.get("campaign_id") else None
                        if not campaign:
                            db.update_reminder_status(str(reminder["id"]), "failed")
                            continue

                        response = await dispatch_outbound_call(
                            phone=reminder["phone"],
                            agent_id=str(reminder.get("agent_id") or campaign.get("agent_id")),
                            sip_trunk_id=str(campaign.get("sip_trunk_id")),
                            business_id=str(reminder.get("business_id")),
                            campaign_id=str(campaign.get("id")),
                            lead_id=str(reminder.get("lead_id")) if reminder.get("lead_id") else None,
                            script_override=reminder_prompt,
                            call_attempt_number=1,
                        )
                        room_id = response.get("room_id")
                        call_row = db.create_call(
                            lead_id=str(reminder.get("lead_id")) if reminder.get("lead_id") else None,
                            campaign_id=str(campaign.get("id")),
                            agent_id=str(reminder.get("agent_id") or campaign.get("agent_id")),
                            sip_trunk_id=str(campaign.get("sip_trunk_id")),
                            business_id=str(reminder.get("business_id")),
                            phone=reminder.get("phone"),
                            room_id=room_id,
                            call_attempt_number=1,
                            call_direction="outbound",
                        )
                        call_id = str(call_row.get("id")) if call_row else None
                        db.update_reminder_status(str(reminder["id"]), "dispatched", call_id=call_id)
                        logger.info("[REMINDER] Dispatched reminder id=%s room=%s", reminder.get("id"), room_id)
                    except Exception as inner:
                        logger.warning("[REMINDER] Failed dispatch id=%s error=%s", reminder.get("id"), inner)
                        db.update_reminder_status(str(reminder.get("id")), "failed")
        except Exception as e:
            logger.error("[REMINDER] loop error: %s", e)

        await asyncio.sleep(60)
