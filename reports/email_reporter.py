from __future__ import annotations

import logging
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import db


def build_daily_summary_html(business_id: str, stats: dict) -> str:
    return f"""
    <html>
      <body>
        <h2>RapidXAI Daily Summary</h2>
        <p>Business ID: {business_id}</p>
        <table border='1' cellpadding='8' cellspacing='0'>
          <tr><th>Metric</th><th>Value</th></tr>
          <tr><td>Total Calls Today</td><td>{stats.get('total_calls_today', 0)}</td></tr>
          <tr><td>Active Calls Now</td><td>{stats.get('active_calls_now', 0)}</td></tr>
          <tr><td>Total Leads</td><td>{stats.get('total_leads', 0)}</td></tr>
          <tr><td>Active Campaigns</td><td>{stats.get('active_campaigns', 0)}</td></tr>
          <tr><td>Bookings Today</td><td>{stats.get('bookings_today', 0)}</td></tr>
          <tr><td>Total Cost Today (USD)</td><td>{stats.get('total_cost_today_usd', 0)}</td></tr>
        </table>
      </body>
    </html>
    """


async def send_report_email(to_addresses: list[str], subject: str, html_body: str) -> bool:
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASS")

    if not host or not to_addresses:
        logging.info("[REPORT] SMTP not configured; skipping email send")
        logging.info("[REPORT] Subject=%s Recipients=%s", subject, to_addresses)
        return False

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = user or "noreply@rapidxai.local"
        msg["To"] = ", ".join(to_addresses)
        msg.attach(MIMEText(html_body, "html"))

        server = smtplib.SMTP(host, port, timeout=15)
        try:
            server.starttls()
            if user and password:
                server.login(user, password)
            server.sendmail(msg["From"], to_addresses, msg.as_string())
        finally:
            server.quit()
        return True
    except Exception as e:
        logging.error("[REPORT] send_report_email failed: %s", e)
        return False


async def run_scheduled_reports():
    while True:
        try:
            businesses = db.get_businesses()
            for business in businesses:
                business_id = str(business.get("id"))
                reports = db.get_scheduled_reports(business_id)
                now = datetime.utcnow()
                for report in reports:
                    if not report.get("is_active", True):
                        continue
                    frequency = str(report.get("frequency") or "daily").lower()
                    last_sent = report.get("last_sent_at")
                    due = False
                    if not last_sent:
                        due = True
                    else:
                        try:
                            last_dt = last_sent if isinstance(last_sent, datetime) else datetime.fromisoformat(str(last_sent).replace("Z", "+00:00"))
                        except Exception:
                            last_dt = now - timedelta(days=365)
                        if frequency == "weekly":
                            due = now - last_dt >= timedelta(days=7)
                        else:
                            due = now - last_dt >= timedelta(hours=24)

                    if not due:
                        continue

                    stats = db.get_platform_stats(business_id)
                    html = build_daily_summary_html(business_id, stats)
                    subject = f"RapidXAI Report: {report.get('report_type', 'daily_summary')}"
                    to_addresses = list(report.get("send_to") or [])
                    sent = await send_report_email(to_addresses, subject, html)
                    if sent:
                        db.update_report_last_sent(str(report.get("id")))
        except Exception as e:
            logging.warning("[REPORT] run_scheduled_reports loop error: %s", e)
        import asyncio

        await asyncio.sleep(3600)
