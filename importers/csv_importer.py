from __future__ import annotations

import csv
import io
import json
import re

PHONE_REGEX = re.compile(r"^\+?[1-9]\d{7,14}$")


def _pick_value(row: dict, column_map: dict, key: str) -> str:
    source = (column_map or {}).get(key, key)
    return str((row or {}).get(source, "") or "").strip()


def parse_and_validate_csv(
    file_bytes: bytes,
    campaign_id: str,
    business_id: str,
    column_map: dict,
    dnc_phones: set[str],
) -> dict:
    """
    Parse CSV and validate rows for lead import.
    """
    _ = campaign_id
    _ = business_id
    max_rows = 50000

    text = (file_bytes or b"").decode("utf-8-sig", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))

    stats = {
        "total_rows": 0,
        "valid": 0,
        "invalid_phone": 0,
        "duplicates_in_file": 0,
        "dnc_skipped": 0,
    }
    errors = []
    valid_leads = []

    seen = set()
    for idx, row in enumerate(reader, start=2):
        stats["total_rows"] += 1
        if stats["total_rows"] > max_rows:
            errors.append({"row": idx, "phone": "", "reason": "max_rows_exceeded"})
            break

        phone = _pick_value(row, column_map, "phone").replace(" ", "")
        if not phone or not PHONE_REGEX.match(phone):
            stats["invalid_phone"] += 1
            errors.append({"row": idx, "phone": phone, "reason": "invalid_phone"})
            continue

        if phone in seen:
            stats["duplicates_in_file"] += 1
            continue

        if phone in (dnc_phones or set()):
            stats["dnc_skipped"] += 1
            continue

        seen.add(phone)

        raw_custom = _pick_value(row, column_map, "custom_data")
        custom_data = {}
        if raw_custom:
            try:
                custom_data = json.loads(raw_custom)
                if not isinstance(custom_data, dict):
                    custom_data = {"raw": raw_custom}
            except Exception:
                custom_data = {"raw": raw_custom}

        valid_leads.append(
            {
                "phone": phone,
                "name": _pick_value(row, column_map, "name") or None,
                "email": _pick_value(row, column_map, "email") or None,
                "language": _pick_value(row, column_map, "language") or "hi-IN",
                "custom_data": custom_data,
            }
        )

    stats["valid"] = len(valid_leads)
    return {
        "valid_leads": valid_leads,
        "stats": stats,
        "errors": errors,
    }
