import asyncio
import logging
import os
from datetime import datetime, timedelta

import pytz

from campaign_runner import get_shutdown_event
import db
from holidays.holiday_engine import is_holiday_today
from ml.best_time import get_best_call_hours, should_call_now
from number_health.spam_checker import pick_best_number

_active_dialers: dict[str, asyncio.Task] = {}


async def start_campaign(campaign_id: str):
    if campaign_id in _active_dialers and not _active_dialers[campaign_id].done():
        return
    task = asyncio.create_task(_run_campaign(campaign_id))
    _active_dialers[campaign_id] = task
    logging.info(f"[DIALER] Campaign {campaign_id} started")
    try:
        campaign = db.get_campaign(campaign_id) or {}
        if campaign.get("business_id"):
            from integrations.webhook_dispatcher import dispatch_event

            asyncio.create_task(
                dispatch_event(
                    str(campaign.get("business_id")),
                    "campaign.started",
                    {"campaign_id": campaign_id, "name": campaign.get("name")},
                )
            )
    except Exception:
        pass


async def stop_campaign(campaign_id: str):
    if campaign_id in _active_dialers:
        _active_dialers[campaign_id].cancel()
        del _active_dialers[campaign_id]
    logging.info(f"[DIALER] Campaign {campaign_id} stopped")


async def _run_campaign(campaign_id: str):
    from ui_server import dispatch_outbound_call

    shutdown_event = get_shutdown_event()
    best_hours: list[int] = []
    best_hours_refreshed_at: datetime | None = None
    while True:
        try:
            if shutdown_event.is_set():
                logging.info("[RUNNER] Shutting down campaign %s", campaign_id)
                break
            campaign = db.get_campaign(campaign_id)
            if not campaign or campaign["status"] not in ("active",):
                break

            if campaign.get("requires_approval") and campaign.get("approval_status") != "approved":
                logging.info("[APPROVAL] Campaign %s waiting for approval; skipping cycle.", campaign_id)
                await asyncio.sleep(30)
                continue

            budget_cap = campaign.get("budget_cap_usd")
            if budget_cap is not None:
                spend = db.get_campaign_spend(campaign_id)
                try:
                    cap = float(budget_cap)
                except Exception:
                    cap = 0.0
                if cap > 0:
                    if spend >= cap:
                        calls_made = db.get_campaign_call_count(campaign_id)
                        db.update_campaign(
                            campaign_id,
                            status="paused",
                            paused_at=datetime.utcnow().isoformat(),
                            pause_reason="budget_cap_reached",
                            calls_made_before_pause=calls_made,
                        )
                        logging.info("[BUDGET] Campaign %s hit budget cap of $%s. Paused.", campaign_id, cap)
                        db.create_notification(
                            business_id=str(campaign.get("business_id")),
                            type="budget_alert",
                            title="Campaign Paused (Budget Cap)",
                            body=f"Campaign {campaign.get('name') or campaign_id} hit budget cap ${cap}",
                            resource_type="campaign",
                            resource_id=str(campaign_id),
                        )
                        break
                    if spend >= cap * 0.9:
                        logging.warning("[BUDGET] Campaign %s at 90%% of budget cap.", campaign_id)
                        db.create_notification(
                            business_id=str(campaign.get("business_id")),
                            type="budget_alert",
                            title="Campaign Near Budget Cap",
                            body=f"Campaign {campaign.get('name') or campaign_id} has reached 90% of budget.",
                            resource_type="campaign",
                            resource_id=str(campaign_id),
                        )

            now_utc = datetime.utcnow()
            if best_hours_refreshed_at is None or (now_utc - best_hours_refreshed_at) >= timedelta(minutes=30):
                best_hours = get_best_call_hours(campaign_id, db_conn=None)
                best_hours_refreshed_at = now_utc
            if not should_call_now(campaign_id, campaign.get("timezone", "Asia/Kolkata"), best_hours):
                logging.info("[PREDICTOR] Suboptimal hour for campaign %s. Skipping cycle.", campaign_id)
                await asyncio.sleep(30)
                continue

            is_holiday, holiday_name = is_holiday_today(str(campaign.get("business_id")))
            if is_holiday:
                logging.info("[HOLIDAY] %s - skipping dialing for campaign %s", holiday_name, campaign_id)
                await asyncio.sleep(60)
                continue

            if not _in_call_window(campaign):
                next_open = _next_window_open(campaign)
                logging.info("[SCHEDULER] Campaign %s outside call window. Next window opens at %s.", campaign_id, next_open)
                wait_seconds = 60
                try:
                    tz = pytz.timezone(campaign.get("timezone", "Asia/Kolkata"))
                    now_local = datetime.now(tz)
                    next_open_dt = datetime.fromisoformat(next_open)
                    wait_seconds = max(60, int((next_open_dt - now_local).total_seconds()))
                except Exception:
                    wait_seconds = 60
                await asyncio.sleep(min(wait_seconds, 3600))
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
                    db.create_notification(
                        business_id=str(campaign.get("business_id")),
                        type="campaign_complete",
                        title="Campaign Completed",
                        body=f"Campaign {campaign.get('name') or campaign_id} has completed.",
                        resource_type="campaign",
                        resource_id=str(campaign_id),
                    )
                    try:
                        from integrations.webhook_dispatcher import dispatch_event

                        asyncio.create_task(
                            dispatch_event(
                                str(campaign.get("business_id")),
                                "campaign.completed",
                                {"campaign_id": campaign_id, "name": campaign.get("name")},
                            )
                        )
                    except Exception:
                        pass
                    await _advance_sequence_if_needed(campaign_id)
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
                try:
                    from integrations.webhook_dispatcher import dispatch_event

                    asyncio.create_task(
                        dispatch_event(
                            str(campaign.get("business_id")),
                            "lead.updated",
                            {"lead_id": str(lead.get("id")), "status": "dnc", "phone": lead.get("phone")},
                        )
                    )
                except Exception:
                    pass
                continue

            selected_agent_id = db.pick_variant_agent(campaign_id) or str(campaign["agent_id"])
            logging.info("[AB] Campaign %s selected variant agent %s for lead %s", campaign_id, selected_agent_id, lead["phone"])

            number_pool = trunk.get("number_pool") or []
            if isinstance(number_pool, str):
                try:
                    import json

                    number_pool = json.loads(number_pool)
                except Exception:
                    number_pool = []
            health_rows = db.get_number_health(trunk_id)
            selected_from_number = pick_best_number(trunk, number_pool, health_rows)
            if selected_from_number and (trunk.get("rotation_strategy") or "round_robin") == "round_robin":
                ordered_pool = [str(n) for n in number_pool if str(n).strip()]
                if ordered_pool and selected_from_number in ordered_pool:
                    next_index = (ordered_pool.index(selected_from_number) + 1) % len(ordered_pool)
                    db.update_sip_trunk(trunk_id, last_used_number_index=next_index)

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
                    from_number=selected_from_number,
                )
            )
            if selected_from_number:
                db.increment_number_usage(trunk_id, selected_from_number)

            interval = 60.0 / max(int(campaign["calls_per_minute"]), 1)
            await asyncio.sleep(interval)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logging.error(f"[DIALER] Error: {e}")
            await asyncio.sleep(10)


async def _advance_sequence_if_needed(campaign_id: str):
    seq = db.get_campaign_sequence(campaign_id)
    if not seq:
        return
    steps = db.get_sequence_steps(str(seq.get("sequence_id")))
    if not steps:
        return

    current_order = int(seq.get("step_order") or 0)
    next_steps = [s for s in steps if int(s.get("step_order") or 0) > current_order]
    if not next_steps:
        return
    next_step = sorted(next_steps, key=lambda s: int(s.get("step_order") or 0))[0]
    next_campaign_id = str(next_step.get("campaign_id") or "")
    if not next_campaign_id:
        return

    src_campaign = db.get_campaign(campaign_id) or {}
    dst_campaign = db.get_campaign(next_campaign_id) or {}
    if not src_campaign or not dst_campaign:
        return

    all_leads = db.get_leads(campaign_id, status=None, limit=50000, offset=0)
    filter_disposition = (next_step.get("filter_disposition") or "").strip()
    import_rows = []
    for lead in all_leads:
        if filter_disposition:
            history = db.get_call_history_for_lead(str(lead.get("id")))
            latest = history[0] if history else {}
            if str((latest or {}).get("disposition") or "").lower() != filter_disposition.lower():
                continue
        import_rows.append(
            {
                "phone": lead.get("phone"),
                "name": lead.get("name"),
                "email": lead.get("email"),
                "language": lead.get("language") or "hi-IN",
                "custom_data": lead.get("custom_data") or {},
            }
        )
    moved = db.bulk_create_leads(next_campaign_id, str(dst_campaign.get("business_id")), import_rows)

    trigger = str(next_step.get("trigger") or "previous_complete")
    delay_days = int(next_step.get("delay_days") or 0)
    if trigger == "manual":
        logging.info("[SEQUENCE] Wave complete. Next wave %s is manual for %s leads.", next_campaign_id, moved)
        return

    scheduled_start = datetime.utcnow() + timedelta(days=max(delay_days, 0))
    if trigger == "delay_days" or delay_days > 0:
        db.update_campaign(next_campaign_id, status="scheduled", scheduled_start_at=scheduled_start.isoformat())
        logging.info(
            "[SEQUENCE] Wave %s complete. Scheduled wave %s for %s leads at %s.",
            campaign_id,
            next_campaign_id,
            moved,
            scheduled_start.isoformat(),
        )
        return

    db.update_campaign(next_campaign_id, status="active", started_at=datetime.utcnow().isoformat())
    await start_campaign(next_campaign_id)
    logging.info(
        "[SEQUENCE] Wave %s complete. Starting wave %s for %s leads.",
        campaign_id,
        next_campaign_id,
        moved,
    )


async def run_scheduled_callbacks():
    """Run continuously to fire scheduled callbacks."""
    from ui_server import dispatch_outbound_call

    shutdown_event = get_shutdown_event()
    while True:
        try:
            if shutdown_event.is_set():
                logging.info("[CALLBACKS] Shutdown requested")
                break
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
