from __future__ import annotations

import logging

import aiohttp


async def send_slack_notification(webhook_url: str, text: str, blocks: list = None) -> bool:
    if not webhook_url:
        return False
    payload = {"text": text}
    if blocks:
        payload["blocks"] = blocks
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                return resp.status in (200, 201)
    except Exception as e:
        logging.warning("[SLACK] send failed: %s", e)
        return False


def build_booking_notification(lead_name, phone, campaign_name, booking_time) -> dict:
    text = f"New booking: {lead_name} ({phone}) in {campaign_name} at {booking_time}"
    return {
        "text": text,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*New Booking*\n{text}"}},
        ],
    }


def build_campaign_complete_notification(campaign_name, stats: dict) -> dict:
    text = f"Campaign complete: {campaign_name}. Stats: {stats}"
    return {
        "text": text,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Campaign Complete*\n{text}"}},
        ],
    }


def build_budget_alert_notification(campaign_name, spent, cap) -> dict:
    text = f"Budget alert: {campaign_name} spent ${spent} of ${cap}"
    return {
        "text": text,
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*Budget Alert*\n{text}"}},
        ],
    }
