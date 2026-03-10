from __future__ import annotations

import re

import requests


_SHEET_ID_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)")
_GID_RE = re.compile(r"[?&]gid=(\d+)")


def fetch_sheet_as_csv(sheet_url: str) -> bytes:
    """
    Convert a Google Sheets URL to CSV export URL and fetch it.
    """
    url = str(sheet_url or "").strip()
    if "docs.google.com/spreadsheets" not in url:
        raise ValueError("invalid_google_sheets_url")

    m = _SHEET_ID_RE.search(url)
    if not m:
        raise ValueError("invalid_google_sheets_url")

    sheet_id = m.group(1)
    gid_match = _GID_RE.search(url)
    gid = gid_match.group(1) if gid_match else "0"

    export_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    resp = requests.get(export_url, timeout=20)
    if resp.status_code != 200:
        raise ValueError(f"sheet_fetch_failed_{resp.status_code}")
    return resp.content
