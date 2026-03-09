import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone
from typing import Any

import pytz
from dotenv import load_dotenv
from livekit import api as lkapi

import db

load_dotenv()

logger = logging.getLogger("dialer")


class CampaignDialer:
    def __init__(self, campaign_id: str):
        self.campaign_id = campaign_id
        self.running = False
        self._task: asyncio.Task | None = None

    async def start(self):
        if self.running:
            logger.info("Dialer already running for campaign %s", self.campaign_id)
            return
        self.running = True
        self._task = asyncio.create_task(self._run())
        logger.info("Dialer started for campaign %s", self.campaign_id)

    async def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Dialer stopped for campaign %s", self.campaign_id)

    def _get_trunk_for_campaign(self, campaign: dict[str, Any]) -> dict[str, Any] | None:
        trunk_id = campaign.get("sip_trunk_id")
        if not trunk_id:
            return None
        for trunk in db.get_sip_trunks():
            if str(trunk.get("id")) == str(trunk_id):
                return trunk
        return None

    async def _run(self):
        try:
            while self.running:
                campaign = db.get_campaign(self.campaign_id)
                if not campaign:
                    logger.error("Campaign %s not found, stopping dialer", self.campaign_id)
                    self.running = False
                    return

                if campaign.get("status") != "active":
                    await asyncio.sleep(5)
                    continue

                trunk = self._get_trunk_for_campaign(campaign)
                if not trunk:
                    logger.error("Campaign %s has no valid SIP trunk", self.campaign_id)
                    await asyncio.sleep(10)
                    continue

                agent = db.get_agent(str(campaign.get("agent_id"))) if campaign.get("agent_id") else None
                if not agent:
                    logger.error("Campaign %s has no valid agent", self.campaign_id)
                    await asyncio.sleep(10)
                    continue

                calls_per_minute = max(1, int(campaign.get("calls_per_minute", 5) or 5))
                interval = 60.0 / calls_per_minute

                if not self._in_call_window(campaign):
                    await asyncio.sleep(60)
                    continue

                active = db.get_active_calls()
                active_for_trunk = [
                    call
                    for call in active
                    if str(call.get("sip_trunk_id")) == str(trunk.get("id"))
                    and call.get("status") in {"active", "ringing", "calling"}
                ]
                if len(active_for_trunk) >= int(trunk.get("max_concurrent_calls", 5) or 5):
                    await asyncio.sleep(5)
                    continue

                leads = db.get_next_pending_leads(self.campaign_id, limit=1)
                if not leads:
                    db.update_campaign(self.campaign_id, status="completed", completed_at="NOW()")
                    self.running = False
                    logger.info("Campaign %s completed: no pending leads", self.campaign_id)
                    return

                lead = leads[0]
                db.update_lead_status(str(lead["id"]), "calling")

                asyncio.create_task(self._dispatch_call(lead, campaign, trunk, agent))
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Campaign dialer task cancelled: %s", self.campaign_id)
            raise
        except Exception as exc:
            logger.exception("Campaign dialer run loop failed for %s: %s", self.campaign_id, exc)
            self.running = False

    async def _dispatch_call(
        self,
        lead: dict[str, Any],
        campaign: dict[str, Any],
        trunk: dict[str, Any],
        agent: dict[str, Any],
    ):
        livekit_url = os.getenv("LIVEKIT_URL", "")
        livekit_key = os.getenv("LIVEKIT_API_KEY", "")
        livekit_secret = os.getenv("LIVEKIT_API_SECRET", "")

        if not all([livekit_url, livekit_key, livekit_secret]):
            logger.error("LiveKit credentials missing; cannot dispatch call")
            db.update_lead_status(str(lead["id"]), "failed")
            return

        phone = str(lead.get("phone", "")).strip()
        if not phone:
            logger.error("Lead %s has no phone", lead.get("id"))
            db.update_lead_status(str(lead["id"]), "failed")
            return

        room_name = f"cmp-{self.campaign_id[:8]}-{phone.replace('+', '')}-{random.randint(1000, 9999)}"
        call_record = db.create_call(
            lead_id=str(lead.get("id")),
            campaign_id=str(campaign.get("id")),
            agent_id=str(agent.get("id")),
            sip_trunk_id=str(trunk.get("id")),
            phone=phone,
            room_id=room_name,
        )

        if not call_record:
            logger.error("Failed to create call record for lead %s", lead.get("id"))
            db.update_lead_status(str(lead["id"]), "failed")
            return

        lk = lkapi.LiveKitAPI(url=livekit_url, api_key=livekit_key, api_secret=livekit_secret)
        try:
            await lk.sip.create_sip_participant(
                lkapi.CreateSIPParticipantRequest(
                    room_name=room_name,
                    sip_trunk_id=trunk["trunk_id"],
                    sip_call_to=phone,
                    participant_identity=f"sip_{phone.replace('+', '')}",
                    participant_name=lead.get("name") or f"Lead {phone[-4:]}",
                    wait_until_answered=False,
                )
            )

            metadata = json.dumps(
                {
                    "phone_number": phone,
                    "lead_id": str(lead.get("id")),
                    "campaign_id": str(campaign.get("id")),
                    "agent_id": str(agent.get("id")),
                    "sip_trunk_id": str(trunk.get("id")),
                    "sip_trunk_livekit_id": trunk.get("trunk_id"),
                }
            )

            await lk.agent_dispatch.create_dispatch(
                lkapi.CreateAgentDispatchRequest(
                    agent_name="outbound-caller",
                    room=room_name,
                    metadata=metadata,
                )
            )

            db.upsert_active_call(
                room_id=room_name,
                phone=phone,
                campaign_id=str(campaign.get("id")),
                agent_id=str(agent.get("id")),
                status="ringing",
                sip_trunk_id=str(trunk.get("id")),
            )
            db.update_call(room_name, status="ringing")
            logger.info("Dispatched call for lead %s in room %s", lead.get("id"), room_name)
        except Exception as exc:
            logger.exception("Dispatch failed for lead %s: %s", lead.get("id"), exc)
            db.update_call(room_name, status="failed", summary=f"Dispatch failed: {exc}")
            db.update_lead_status(str(lead["id"]), "failed")
            db.remove_active_call(room_name)
        finally:
            await lk.aclose()

    def _in_call_window(self, campaign) -> bool:
        tz = pytz.timezone(campaign.get("timezone", "Asia/Kolkata"))
        now = datetime.now(tz)
        start = campaign.get("call_window_start", "09:00")
        end = campaign.get("call_window_end", "20:00")

        start_h, start_m = map(int, start.split(":"))
        end_h, end_m = map(int, end.split(":"))
        start_time = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
        end_time = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
        return start_time <= now <= end_time


_dialers: dict[str, CampaignDialer] = {}


async def start_campaign_dialer(campaign_id: str):
    try:
        if campaign_id in _dialers and _dialers[campaign_id].running:
            return
        dialer = CampaignDialer(campaign_id)
        _dialers[campaign_id] = dialer
        await dialer.start()
    except Exception as exc:
        logger.exception("start_campaign_dialer failed for %s: %s", campaign_id, exc)
        raise


async def stop_campaign_dialer(campaign_id: str):
    try:
        if campaign_id in _dialers:
            await _dialers[campaign_id].stop()
            del _dialers[campaign_id]
    except Exception as exc:
        logger.exception("stop_campaign_dialer failed for %s: %s", campaign_id, exc)
        raise
