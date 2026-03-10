import asyncio
import logging
from datetime import datetime

import db
from whatsapp import send_whatsapp_message

logger = logging.getLogger("surveys.engine")


def parse_survey_response(survey: dict, raw_response: str) -> dict:
    value = (raw_response or "").strip().lower()
    response_type = (survey.get("response_type") or "numeric").lower()
    valid_responses = [str(x).lower() for x in (survey.get("valid_responses") or [])]

    if response_type == "yesno":
        yes_set = {"yes", "y", "haan", "ha", "haanji", "ok"}
        no_set = {"no", "n", "nahi", "nope"}
        if value in yes_set:
            return {"valid": True, "normalized_value": "yes"}
        if value in no_set:
            return {"valid": True, "normalized_value": "no"}
        return {"valid": False, "normalized_value": value}

    if response_type == "numeric":
        value = "".join(ch for ch in value if ch.isdigit())
        if value and (not valid_responses or value in valid_responses):
            return {"valid": True, "normalized_value": value}
        return {"valid": False, "normalized_value": value}

    # freetext
    if valid_responses and value not in valid_responses:
        return {"valid": False, "normalized_value": value}
    return {"valid": bool(value), "normalized_value": value}


async def _send_survey_message(survey: dict, business: dict, phone: str):
    await asyncio.sleep(max(int(survey.get("send_delay_minutes") or 2), 0) * 60)
    message = survey.get("question") or "How would you rate your experience? Reply 1-5"

    send_via = (survey.get("send_via") or "whatsapp").lower()
    sent = False

    if send_via in {"whatsapp", "both"}:
        instance = business.get("whatsapp_instance")
        token = business.get("whatsapp_token")
        if instance and token:
            sent = await send_whatsapp_message(instance, token, phone, message) or sent

    if send_via in {"sms", "both"}:
        try:
            from integrations.sms import send_sms

            sent = await send_sms(
                business.get("sms_provider"),
                business.get("sms_api_key"),
                business.get("sms_sender_id"),
                phone,
                message,
            ) or sent
        except Exception as e:
            logger.warning("[SURVEY] SMS send unavailable: %s", e)

    return sent


async def trigger_survey_for_call(call: dict, lead: dict, business: dict) -> bool:
    try:
        survey = db.get_active_survey_for_agent(str(call.get("agent_id")))
        if not survey:
            return False

        disp = (call.get("disposition") or "").lower()
        triggers = [str(x).lower() for x in (survey.get("trigger_dispositions") or [])]
        if triggers and disp not in triggers:
            return False

        response_row = db.create_survey_response(
            survey_id=str(survey["id"]),
            call_id=str(call.get("id")) if call.get("id") else None,
            lead_id=str(lead.get("id")) if lead and lead.get("id") else None,
            business_id=str(business.get("id")),
            phone=lead.get("phone") if lead else call.get("phone"),
        )
        if not response_row:
            return False

        asyncio.create_task(_send_survey_message(survey, business, lead.get("phone") if lead else call.get("phone")))
        logger.info("[SURVEY] Scheduled survey id=%s for call=%s", survey.get("id"), call.get("id"))
        return True
    except Exception as e:
        logger.warning("[SURVEY] trigger failed: %s", e)
        return False


async def handle_incoming_survey_response(phone: str, message: str) -> bool:
    latest = db.get_latest_sent_survey_for_phone(phone)
    if not latest:
        return False

    survey = db.get_survey(str(latest.get("survey_id")))
    if not survey:
        return False

    parsed = parse_survey_response(survey, message)
    if not parsed.get("valid"):
        return False

    ok = db.update_survey_response(phone, parsed.get("normalized_value"), datetime.utcnow().isoformat())
    if ok:
        logger.info("[SURVEY] response recorded phone=%s value=%s", phone, parsed.get("normalized_value"))
    return ok
