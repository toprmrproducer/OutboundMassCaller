from __future__ import annotations

import logging

import aiohttp

BASE_URL = "https://api.hubapi.com"


async def _request(method: str, url: str, api_key: str, json_payload: dict | None = None) -> tuple[int, dict | str]:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, json=json_payload, headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                text = await resp.text()
                try:
                    data = await resp.json()
                except Exception:
                    data = text
                return resp.status, data
    except Exception as e:
        logging.warning("[HUBSPOT] request failed %s %s: %s", method, url, e)
        return 0, str(e)


async def sync_call_to_hubspot(api_key, call: dict, lead: dict) -> bool:
    if not api_key:
        logging.warning("[HUBSPOT] api key missing; skipping sync")
        return False

    phone = (lead or {}).get("phone")
    email = (lead or {}).get("email")
    name = (lead or {}).get("name")

    contact_payload = {
        "properties": {
            "phone": phone,
            "email": email,
            "firstname": name,
        }
    }
    status, _ = await _request("POST", f"{BASE_URL}/crm/v3/objects/contacts", api_key, contact_payload)
    if status and status >= 400:
        logging.warning("[HUBSPOT] contact sync failed status=%s", status)

    call_payload = {
        "properties": {
            "hs_call_title": f"Outbound call to {phone}",
            "hs_call_body": (call or {}).get("summary") or "",
            "hs_call_duration": int((call or {}).get("duration_seconds") or 0) * 1000,
            "hs_call_status": (call or {}).get("disposition") or (call or {}).get("status") or "completed",
        }
    }
    status, _ = await _request("POST", f"{BASE_URL}/crm/v3/objects/calls", api_key, call_payload)
    if not status or status >= 400:
        logging.warning("[HUBSPOT] call sync failed status=%s", status)
        return False
    return True


async def import_contacts_from_hubspot(api_key, list_id=None) -> list[dict]:
    if not api_key:
        return []

    url = f"{BASE_URL}/crm/v3/objects/contacts?limit=100"
    status, data = await _request("GET", url, api_key, None)
    if not status or status >= 400 or not isinstance(data, dict):
        return []

    results = []
    for row in (data.get("results") or []):
        props = row.get("properties") or {}
        results.append(
            {
                "phone": props.get("phone"),
                "name": (props.get("firstname") or "").strip() or None,
                "email": props.get("email"),
                "custom_data": {"hubspot_id": row.get("id"), "list_id": list_id},
            }
        )
    return [r for r in results if r.get("phone")]
