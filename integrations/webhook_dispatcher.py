from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time

import aiohttp

import db


async def dispatch_event(business_id: str, event_type: str, payload: dict):
    """
    Dispatch webhook events to subscribed endpoints.
    Never raises.
    """
    try:
        hooks = db.get_webhooks(business_id)
        if not hooks:
            return

        body = json.dumps(payload or {}, default=str)
        ts = str(int(time.time()))

        async with aiohttp.ClientSession() as session:
            for hook in hooks:
                if not hook.get("is_active", True):
                    continue
                events = hook.get("events") or []
                if event_type not in events:
                    continue

                headers = {
                    "Content-Type": "application/json",
                    "X-RapidXAI-Event": event_type,
                    "X-RapidXAI-Timestamp": ts,
                }
                secret = hook.get("secret")
                if secret:
                    sig = hmac.new(str(secret).encode(), body.encode(), hashlib.sha256).hexdigest()
                    headers["X-RapidXAI-Signature"] = sig

                success = False
                status = None
                resp_body = ""
                for attempt in range(2):
                    try:
                        async with session.post(
                            hook.get("url"),
                            data=body,
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            status = resp.status
                            resp_body = await resp.text()
                            success = resp.status < 400
                            if success:
                                break
                    except Exception as e:
                        resp_body = str(e)
                        success = False
                    if attempt == 0 and not success:
                        await __import__("asyncio").sleep(2)

                db.log_webhook_delivery(
                    webhook_id=str(hook.get("id")),
                    event_type=event_type,
                    payload=payload,
                    response_status=status,
                    response_body=resp_body,
                    success=success,
                )
    except Exception as e:
        logging.warning("[WEBHOOK] dispatch_event failed: %s", e)
