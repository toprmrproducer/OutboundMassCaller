import asyncio
import logging
import os
from datetime import datetime, timedelta

import pytz

import db

_active_dialers: dict[str, asyncio.Task] = {}


async def start_campaign(campaign_id: str):
    if campaign_id in _active_dialers and not _active_dialers[campaign_id].done():
        return
    task = asyncio.create_task(_run_campaign(campaign_id))
    _active_dialers[campaign_id] = task
    logging.info(f"[DIALER] Campaign {campaign_id} started")


async def stop_campaign(campaign_id: str):
    if campaign_id in _active_dialers:
        _active_dialers[campaign_id].cancel()
        del _active_dialers[campaign_id]
    logging.info(f"[DIALER] Campaign {campaign_id} stopped")


async def _run_campaign(campaign_id: str):
    from ui_server import dispatch_outbound_call

    while True:
        try:
            campaign = db.get_campaign(campaign_id)
            if not campaign or campaign["status"] not in ("active",):
                break

            if not _in_call_window(campaign):
                next_open = _next_window_open(campaign)
                logging.info("[SCHEDULER] Campaign %s outside call window. Next window opens at %s.", campaign_id, next_open)
                await asyncio.sleep(60)
                continue

            trunk_id = str(campaign["sip_trunk_id"])
            trunk = db.get_sip_trunk(trunk_id)
            if not trunk:
                logging.error(f"[DIALER] Missing trunk for campaign {campaign_id}")
                await asyncio.sleep(10)
                continue

            active = db.get_active_calls(campaign.get("business_id"))
            active_on_trunk = [c for c in active if str(c.get("sip_trunk_id", "")) == trunk_id]
            max_concurrent = min(int(campaign["max_concurrent_calls"]), int(trunk["max_concurrent_calls"]))
            if len(active_on_trunk) >= max_concurrent:
                await asyncio.sleep(5)
                continue

            leads = db.get_next_pending_leads(campaign_id, 1)
            if not leads:
                requeued = False
                if campaign.get("retry_failed"):
                    retry_statuses = ["failed", "no_answer"]
                    for status in retry_statuses:
                        stale = db.get_leads(campaign_id, status=status, limit=500, offset=0)
                        for lead in stale:
                            if int(lead.get("call_attempts") or 0) < int(campaign.get("max_retries") or 2):
                                next_time = datetime.utcnow() + timedelta(minutes=int(campaign.get("retry_delay_minutes") or 60))
                                db.update_lead(
                                    str(lead["id"]),
                                    status="pending",
                                    next_call_at=next_time.isoformat(),
                                )
                                requeued = True
                if not requeued:
                    db.update_campaign(campaign_id, status="completed", completed_at=datetime.utcnow().isoformat())
                    logging.info(f"[DIALER] Campaign {campaign_id} completed")
                    break
                await asyncio.sleep(10)
                continue

            lead = leads[0]
            if lead.get("next_call_at"):
                try:
                    when = lead["next_call_at"]
                    if isinstance(when, str):
                        when_dt = datetime.fromisoformat(when.replace("Z", "+00:00"))
                    else:
                        when_dt = when
                    if when_dt and when_dt > datetime.utcnow():
                        continue
                except Exception:
                    pass

            if db.is_dnc(str(campaign["business_id"]), lead["phone"]):
                db.update_lead_status(str(lead["id"]), "dnc")
                logging.info("[DNC] Skipping %s - on DNC list", lead["phone"])
                continue

            selected_agent_id = db.pick_variant_agent(campaign_id) or str(campaign["agent_id"])
            logging.info("[AB] Campaign %s selected variant agent %s for lead %s", campaign_id, selected_agent_id, lead["phone"])

            asyncio.create_task(
                dispatch_outbound_call(
                    phone=lead["phone"],
                    agent_id=selected_agent_id,
                    sip_trunk_id=str(campaign["sip_trunk_id"]),
                    business_id=str(campaign["business_id"]),
                    campaign_id=campaign_id,
                    lead_id=str(lead["id"]),
                    script_override=None,
                    call_attempt_number=int(lead.get("call_attempts", 0)),
                )
            )

            interval = 60.0 / max(int(campaign["calls_per_minute"]), 1)
            await asyncio.sleep(interval)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"[DIALER] Error: {e}")
            await asyncio.sleep(10)


async def run_scheduled_callbacks():
    """Run continuously to fire scheduled callbacks."""
    from ui_server import dispatch_outbound_call

    while True:
        try:
            due = db.get_due_callbacks()
            for cb in due:
                campaign = db.get_campaign(str(cb["campaign_id"]))
                if not campaign:
                    db.update_callback(str(cb["id"]), "cancelled")
                    continue

                script_override = None
                if cb.get("script_id"):
                    script = db.get_script(str(cb["script_id"]))
                    if script:
                        script_override = script.get("system_prompt")

                db.update_callback(str(cb["id"]), "completed")
                db.update_lead_status(str(cb["lead_id"]), "calling")

                asyncio.create_task(
                    dispatch_outbound_call(
                        phone=cb["phone"],
                        agent_id=str(campaign["agent_id"]),
                        sip_trunk_id=str(campaign["sip_trunk_id"]),
                        business_id=str(campaign["business_id"]),
                        campaign_id=str(cb["campaign_id"]),
                        lead_id=str(cb["lead_id"]),
                        script_override=script_override,
                        call_attempt_number=1,
                    )
                )
        except Exception as e:
            logging.error(f"[CALLBACKS] Error: {e}")
        await asyncio.sleep(30)


def _in_call_window(campaign: dict) -> bool:
    tz_name = campaign.get("timezone", "Asia/Kolkata")
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    if campaign.get("skip_sundays", True) and now.weekday() == 6:
        return False
    start_str = campaign.get("call_window_start", "09:00")
    end_str = campaign.get("call_window_end", "20:00")
    sh, sm = map(int, start_str.split(":"))
    eh, em = map(int, end_str.split(":"))
    start = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end = now.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= now <= end


def _next_window_open(campaign: dict) -> str:
    tz_name = campaign.get("timezone", "Asia/Kolkata")
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    start_str = campaign.get("call_window_start", "09:00")
    sh, sm = map(int, start_str.split(":"))
    candidate = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
    if now >= candidate:
        candidate = candidate + timedelta(days=1)
    if campaign.get("skip_sundays", True):
        while candidate.weekday() == 6:
            candidate = candidate + timedelta(days=1)
    return candidate.isoformat()
