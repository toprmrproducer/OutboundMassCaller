from __future__ import annotations

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


async def send_followup_email(
    smtp_host,
    smtp_port,
    smtp_user,
    smtp_pass,
    to_email,
    lead_name,
    disposition,
    booking_time=None,
) -> bool:
    if not smtp_host or not to_email:
        return False

    disposition = str(disposition or "").lower()
    if disposition == "booked":
        body = f"Hi {lead_name or 'there'}, your appointment is confirmed for {booking_time or 'the scheduled time'}."
    elif disposition == "callback_requested":
        body = f"Hi {lead_name or 'there'}, we noted your callback request and will reach you shortly."
    else:
        body = f"Hi {lead_name or 'there'}, thank you for your interest. Our team will follow up shortly."

    html = f"<html><body><p>{body}</p></body></html>"

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = "RapidXAI Follow-up"
        msg["From"] = smtp_user or "noreply@rapidxai.local"
        msg["To"] = to_email
        msg.attach(MIMEText(html, "html"))

        server = smtplib.SMTP(smtp_host, int(smtp_port or 587), timeout=15)
        try:
            server.starttls()
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.sendmail(msg["From"], [to_email], msg.as_string())
        finally:
            server.quit()
        return True
    except Exception as e:
        logging.warning("[EMAIL] follow-up send failed: %s", e)
        return False
