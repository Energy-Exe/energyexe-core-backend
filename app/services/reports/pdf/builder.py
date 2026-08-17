"""EnergyExe branded PDF builder — promoted from the brain-agent sandbox seed
(``REPORT_PDF_PY`` in ``brain_agent_skill_files.py``, EPR-68) into a first-class
module for the reports platform. Keep the two visually in sync until the seed
constant is refactored to read this module.
"""

from xml.sax.saxutils import escape

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Image,
    ListFlowable,
    ListItem,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

BRAND = colors.HexColor("#2563EB")  # electric blue
INK = colors.HexColor("#0F1B2D")  # near-black navy
SLATE = colors.HexColor("#475569")  # body text
LINE = colors.HexColor("#E2E8F0")  # light rule
ZEBRA = colors.HexColor("#F1F5F9")  # alternating row
MUTED = colors.HexColor("#94A3B8")  # de-emphasised text (Pass rows)

# Severity accents (match the client-ui findings table)
SEVERITY_COLORS = {
    "confirmed": colors.HexColor("#DC2626"),  # red
    "indicative": colors.HexColor("#D97706"),  # amber
    "watch": colors.HexColor("#2563EB"),  # blue
    "pass": colors.HexColor("#16A34A"),  # green
    "suppressed": colors.HexColor("#94A3B8"),  # grey
}


def _esc(text) -> str:
    """Paragraph() parses XML-ish markup — free text (windfarm names, LLM
    narratives) must be escaped or a stray '&'/'<' aborts the whole render."""
    return escape(str(text))


class PdfBuilder:
    """Flowing-document builder: heading/paragraph/bullets/table/image/save."""

    def __init__(self, title: str = "", subtitle: str | None = None):
        self._flow = []
        ss = getSampleStyleSheet()
        self.s_title = ParagraphStyle(
            "t", parent=ss["Title"], textColor=INK, fontSize=22, spaceAfter=4, alignment=TA_LEFT
        )
        self.s_sub = ParagraphStyle(
            "st", parent=ss["Normal"], textColor=BRAND, fontSize=11, spaceAfter=14
        )
        self.s_h2 = ParagraphStyle(
            "h2", parent=ss["Heading2"], textColor=BRAND, fontSize=14, spaceBefore=14, spaceAfter=6
        )
        self.s_h3 = ParagraphStyle(
            "h3", parent=ss["Heading3"], textColor=INK, fontSize=11, spaceBefore=10, spaceAfter=4
        )
        self.s_body = ParagraphStyle(
            "b", parent=ss["Normal"], textColor=SLATE, fontSize=10, leading=15, spaceAfter=8
        )
        self.s_small = ParagraphStyle(
            "sm", parent=ss["Normal"], textColor=MUTED, fontSize=8, leading=11, spaceAfter=6
        )
        if title:
            self._flow.append(Paragraph(_esc(title), self.s_title))
        if subtitle:
            self._flow.append(Paragraph(_esc(subtitle), self.s_sub))

    def heading(self, text, level=2):
        style = self.s_title if level <= 1 else self.s_h2 if level == 2 else self.s_h3
        self._flow.append(Paragraph(_esc(text), style))
        return self

    def paragraph(self, text, style=None):
        st = self.s_sub if str(style).lower() in ("subtitle", "sub", "lead") else self.s_body
        self._flow.append(Paragraph(_esc(text), st))
        return self

    def small(self, text):
        self._flow.append(Paragraph(_esc(text), self.s_small))
        return self

    def bullets(self, items):
        li = [ListItem(Paragraph(_esc(x), self.s_body), leftIndent=10) for x in items]
        self._flow.append(ListFlowable(li, bulletType="bullet", bulletColor=BRAND, start="square"))
        self._flow.append(Spacer(1, 6))
        return self

    def metric_cards(self, cards):
        """A single-row strip of metric cards: [{label, value, unit}].

        Cards may optionally carry ``delta_pct`` (change vs the prior period);
        when any card in the row has one, a third sub-line row is added.
        """
        if not cards:
            return self
        values = [
            f"{c.get('value', 'n/a')}{(' ' + c['unit']) if c.get('unit') else ''}" for c in cards
        ]
        labels = [c.get("label", "") for c in cards]
        grid = [values, labels]
        has_deltas = any(c.get("delta_pct") is not None for c in cards)
        if has_deltas:
            grid.append(
                [
                    (
                        f"{c['delta_pct']:+.1f}% vs prior period"
                        if c.get("delta_pct") is not None
                        else ""
                    )
                    for c in cards
                ]
            )
        t = Table(grid, hAlign="LEFT", colWidths=[1.65 * inch] * len(cards))
        style = [
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, 0), 14),
            ("TEXTCOLOR", (0, 0), (-1, 0), INK),
            ("FONTSIZE", (0, 1), (-1, 1), 8),
            ("TEXTCOLOR", (0, 1), (-1, 1), SLATE),
            ("BOX", (0, 0), (-1, -1), 0.5, LINE),
            ("INNERGRID", (0, 0), (-1, -1), 0.5, LINE),
            ("TOPPADDING", (0, 0), (-1, 0), 8),
            ("BOTTOMPADDING", (0, 1), (-1, 1), 8),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ]
        if has_deltas:
            style += [
                ("FONTSIZE", (0, 2), (-1, 2), 7),
                ("TEXTCOLOR", (0, 2), (-1, 2), SLATE),
                ("BOTTOMPADDING", (0, 2), (-1, 2), 8),
            ]
        t.setStyle(TableStyle(style))
        self._flow.append(t)
        self._flow.append(Spacer(1, 12))
        return self

    def table(self, data, rows=None, row_text_colors=None):
        """table(rows) with row 0 as header, or table(headers, rows).

        ``row_text_colors``: optional list of reportlab colors per body row
        (index 0 = first body row) — used to accent severity / mute Pass rows.
        """
        if rows is not None:
            grid = [list(data)] + [list(r) for r in rows]
        else:
            grid = [list(r) for r in data]
        data = [["" if c is None else str(c) for c in r] for r in grid]
        t = Table(data, repeatRows=1, hAlign="LEFT")
        style = [
            ("BACKGROUND", (0, 0), (-1, 0), BRAND),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8.5),
            ("TEXTCOLOR", (0, 1), (-1, -1), SLATE),
            ("LINEBELOW", (0, 0), (-1, -1), 0.4, LINE),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]
        for i in range(1, len(data)):
            if i % 2 == 0:
                style.append(("BACKGROUND", (0, i), (-1, i), ZEBRA))
        if row_text_colors:
            for i, color in enumerate(row_text_colors, start=1):
                if color is not None and i < len(data):
                    style.append(("TEXTCOLOR", (0, i), (-1, i), color))
        t.setStyle(TableStyle(style))
        self._flow.append(t)
        self._flow.append(Spacer(1, 10))
        return self

    def image(self, path, width_in=6.0):
        img = Image(str(path))
        w = width_in * inch
        img.drawHeight = img.drawHeight * (w / img.drawWidth)
        img.drawWidth = w
        img.hAlign = "LEFT"
        self._flow.append(img)
        self._flow.append(Spacer(1, 10))
        return self

    def save(self, filename):
        doc = SimpleDocTemplate(
            str(filename),
            pagesize=A4,
            leftMargin=0.8 * inch,
            rightMargin=0.8 * inch,
            topMargin=0.8 * inch,
            bottomMargin=0.8 * inch,
            title=str(filename),
        )
        doc.build(self._flow)
        return filename
