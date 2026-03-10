from __future__ import annotations

import io
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle


def generate_campaign_report_pdf(campaign_id: str, stats: dict, calls: list, funnel: dict) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph(f"Campaign Report: {campaign_id}", styles["Title"]))
    story.append(Paragraph(f"Generated at: {datetime.utcnow().isoformat()}Z", styles["Normal"]))
    story.append(Spacer(1, 12))

    summary_data = [["Metric", "Value"]]
    for k, v in (stats or {}).items():
        summary_data.append([str(k), str(v)])
    summary_table = Table(summary_data, hAlign="LEFT")
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ]
        )
    )
    story.append(Paragraph("Summary", styles["Heading2"]))
    story.append(summary_table)
    story.append(Spacer(1, 12))

    funnel_data = [["Stage", "Value"]] + [[k, v] for k, v in (funnel or {}).items()]
    funnel_table = Table(funnel_data, hAlign="LEFT")
    funnel_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ]
        )
    )
    story.append(Paragraph("Conversion Funnel", styles["Heading2"]))
    story.append(funnel_table)
    story.append(Spacer(1, 12))

    top_calls = (calls or [])[:50]
    calls_data = [["Phone", "Duration", "Disposition", "Quality Score"]]
    for c in top_calls:
        calls_data.append(
            [
                str(c.get("phone") or ""),
                str(c.get("duration_seconds") or ""),
                str(c.get("disposition") or ""),
                str(c.get("quality_score") or ""),
            ]
        )
    calls_table = Table(calls_data, hAlign="LEFT")
    calls_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ]
        )
    )
    story.append(Paragraph("Top Calls", styles["Heading2"]))
    story.append(calls_table)

    doc.build(story)
    return buffer.getvalue()
