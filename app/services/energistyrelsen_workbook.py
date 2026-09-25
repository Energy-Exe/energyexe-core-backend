"""Read the published Vinddatasæt without guessing missing generation values."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from io import BytesIO
import re

from openpyxl import load_workbook


def _month(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m")
    # Try two-digit months first: with the shorter alternative first,
    # "2023-10" would match as "2023-01".
    match = re.search(r"(20\d{2})[-/](1[0-2]|0?[1-9])(?!\d)", str(value or ""))
    return f"{match.group(1)}-{int(match.group(2)):02d}" if match else None


def _number(value):
    if value is None or str(value).strip().lower() in {"", "-", "n/a", "nan"}:
        return None
    if isinstance(value, str):
        value = value.replace(" ", "").replace(" ", "")
        value = value.replace(".", "").replace(",", ".") if "," in value else value
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Invalid generation value: {value!r}") from exc


def _date(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    match = re.match(r"(\d{2})[-/.](\d{2})[-/.](\d{4})", text)
    if match:
        return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
    return None


# Stamdata headers are matched by substring so small wording changes in
# future publications do not break the import.
STAMDATA_KEYS = {
    "park": "parknummer",
    "connected": "nettilslutning",
    "decommissioned": "afmeldning",
    "capacity_kw": "kapacitet",
    "hub_height_m": "navhøjde",
    "make": "fabrikat",
    "model": "typebetegnelse",
}


def turbine_attributes(meta):
    """Typed stamdata for one GSRN from the raw header→text metadata."""
    out = {key: None for key in STAMDATA_KEYS}
    for header, value in (meta or {}).items():
        lowered = str(header).lower()
        for key, needle in STAMDATA_KEYS.items():
            if needle in lowered:
                out[key] = value
    park = out["park"]
    out["park"] = park.strip().upper() if isinstance(park, str) and park.strip() else None
    out["connected"] = _date(out["connected"])
    out["decommissioned"] = _date(out["decommissioned"])
    for key in ("capacity_kw", "hub_height_m"):
        try:
            out[key] = _number(out[key])
        except ValueError:
            out[key] = None
    for key in ("make", "model"):
        text = out[key]
        out[key] = text.strip() if isinstance(text, str) and text.strip() else None
    return out


def parse_vinddata(content: bytes, start_month: str, end_month: str):
    """Return {(GSRN, YYYY-MM): MWh}, metadata and blank counts.

    Duplicate GSRNs may fill each other's blanks. Differing nonblank values
    for the same month are rejected before any database write.
    """
    book = load_workbook(BytesIO(content), read_only=True, data_only=True)
    if "Vinddatasæt" not in book.sheetnames:
        raise ValueError("Expected worksheet 'Vinddatasæt'")
    rows = book["Vinddatasæt"].iter_rows(values_only=True)
    next(rows, None)
    next(rows, None)
    headers = next(rows, None)
    if not headers or "GSRN" not in str(headers[0]).upper():
        raise ValueError("Expected GSRN in column A of header row 3")
    months = {i: month for i, heading in enumerate(headers) if (month := _month(heading)) and start_month <= month <= end_month}
    if not months:
        raise ValueError("No monthly columns in requested range")
    values = {}
    original_values = {}
    metadata = {}
    gsrns = set()
    for row_number, row in enumerate(rows, 4):
        raw_gsrn = row[0] if row else None
        if raw_gsrn is None:
            continue
        gsrn = str(raw_gsrn).strip()
        if gsrn.endswith(".0") and gsrn[:-2].isdigit():
            gsrn = gsrn[:-2]
        if not gsrn.isdigit():
            continue
        gsrns.add(gsrn)
        metadata.setdefault(gsrn, {str(headers[i]): str(row[i]) for i in range(1, min(len(row), len(headers))) if row[i] is not None and not _month(headers[i])})
        for col, month in months.items():
            value = _number(row[col] if col < len(row) else None)
            if value is None:
                continue
            key = (gsrn, month)
            # generation_data_raw stores three decimal places of MWh.
            mwh = (value / 1000).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
            if key in original_values and original_values[key] != value:
                raise ValueError(f"Conflicting GSRN {gsrn} month {month} at row {row_number}")
            original_values[key] = value
            values[key] = mwh
    missing = {month: sum((gsrn, month) not in values for gsrn in gsrns)
               for month in months.values()}
    return values, metadata, missing
