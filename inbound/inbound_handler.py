import json
import logging
import os
import uuid

import db

logger = logging.getLogger("inbound.handler")


async def handle_inbound_call(from_phone: str, to_phone: str, trunk_id: str | None = None) -> dict:
    trunk = None
    if trunk_id:
        trunk = db.get_sip_trunks()
        trunk = next((t for t in trunk if str(t.get("trunk_id")) == str(trunk_id)), None)
    if not trunk:
        trunk = db.get_trunk_by_phone_number(to_phone)

    if not trunk:
        return {"action": "reject", "reason": "trunk_not_found"}

    if not bool(trunk.get("inbound_enabled")):
        return {
            "action": "reject",
            "reason": "inbound_disabled",
            "message": trunk.get("inbound_fallback_message") or "Thank you for calling. Our team will get back to you shortly.",
        }

    lead = db.get_lead_by_phone_and_business(from_phone, str(trunk.get("business_id")))
    agent_id = None
    lead_context = {}

    if lead:
        history = db.get_call_history_for_lead(str(lead["id"]))
        if history:
            agent_id = str(history[0].get("agent_id")) if history[0].get("agent_id") else None
        lead_context = {
            "lead_id": str(lead.get("id")),
            "name": lead.get("name"),
            "phone": lead.get("phone"),
            "language": lead.get("language"),
            "custom_data": lead.get("custom_data") or {},
        }

    if not agent_id and trunk.get("inbound_agent_id"):
        agent_id = str(trunk.get("inbound_agent_id"))

    if not agent_id:
        return {
            "action": "reject",
            "reason": "no_inbound_agent",
            "message": trunk.get("inbound_fallback_message") or "Thank you for calling. Our team will get back to you shortly.",
        }

    room_name = f"inbound_{from_phone}_{uuid.uuid4().hex[:8]}"

    first_line = (
        f"Hello {lead.get('name')}! I see you called us back. How can I help you today?"
        if lead and lead.get("name")
        else "Thank you for calling! How can I help you?"
    )

    metadata = {
        "phone_number": from_phone,
        "to_phone_number": to_phone,
        "agent_id": agent_id,
        "business_id": str(trunk.get("business_id")),
        "sip_trunk_id": str(trunk.get("id")),
        "lead_id": str(lead.get("id")) if lead else None,
        "call_direction": "inbound",
        "inbound_first_line": first_line,
        "lead_context": lead_context,
    }

    call_row = db.create_call(
        lead_id=str(lead.get("id")) if lead else None,
        campaign_id=str(lead.get("campaign_id")) if lead and lead.get("campaign_id") else None,
        agent_id=agent_id,
        sip_trunk_id=str(trunk.get("id")),
        business_id=str(trunk.get("business_id")),
        phone=from_phone,
        room_id=room_name,
        call_attempt_number=1,
        call_direction="inbound",
    )

    return {
        "action": "accept",
        "room_name": room_name,
        "agent_id": agent_id,
        "call_id": str(call_row.get("id")) if call_row else None,
        "lead_context": lead_context,
        "modified_first_line": first_line,
        "metadata": metadata,
    }


async def on_inbound_call_webhook(request_data: dict) -> dict:
    from livekit import api as lk_api

    from_phone = str(request_data.get("from") or request_data.get("from_number") or "")
    to_phone = str(request_data.get("to") or request_data.get("to_number") or "")
    trunk_id = request_data.get("sip_trunk_id")

    result = await handle_inbound_call(from_phone=from_phone, to_phone=to_phone, trunk_id=trunk_id)

    if result.get("action") != "accept":
        return {
            "data": {
                "action": "reject",
                "reason": result.get("reason"),
                "message": result.get("message", "Call cannot be accepted."),
            },
            "error": None,
        }

    try:
        lk = lk_api.LiveKitAPI(
            url=os.environ["LIVEKIT_URL"],
            api_key=os.environ["LIVEKIT_API_KEY"],
            api_secret=os.environ["LIVEKIT_API_SECRET"],
        )
        try:
            await lk.room.create_room(lk_api.CreateRoomRequest(name=result["room_name"]))
            await lk.agent_dispatch.create_dispatch(
                lk_api.CreateAgentDispatchRequest(
                    room=result["room_name"],
                    agent_name="outbound-caller",
                    metadata=json.dumps(result.get("metadata") or {}),
                )
            )
        finally:
            await lk.aclose()
    except Exception as e:
        logger.warning("[INBOUND] unable to auto-dispatch agent: %s", e)

    return {
        "data": {
            "action": "accept",
            "room_name": result["room_name"],
            "metadata": json.dumps(result.get("metadata") or {}),
            "agent_id": result.get("agent_id"),
            "call_id": result.get("call_id"),
            "first_line": result.get("modified_first_line"),
        },
        "error": None,
    }
