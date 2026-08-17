"""Per-schema evidence formatting for reports (EPR-88 enrichment).

Each detection schema stores its computed values in ``Opportunity.data_slots``
with detector-specific keys. This module curates, orders, labels and formats
the display-worthy slots per schema so the web report and the PDF render the
exact same strings. Slots that are missing or None are skipped silently —
detectors degrade gracefully and so does the evidence panel.

Annotation slots (``baseline_caveat``, ``provisional``, ``reclassified_from``,
``overlap_downgraded_from``) become human-readable ``notes`` rather than grid
items. The raw ``period`` slot is excluded — the report row carries the
finding's detection window separately.
"""

from datetime import date, datetime
from typing import Any, Callable, Optional

from app.models.opportunity import SchemaCode

_MONTH_LIST_CAP = 6


# ── value formatters ────────────────────────────────────────────────────


def _num(value: float, decimals: int = 1) -> str:
    text = f"{value:,.{decimals}f}"
    if decimals > 0:
        text = text.rstrip("0").rstrip(".")
    return text


def _fmt_pct(v: Any) -> str:  # value already in percent
    return f"{_num(float(v))}%"


def _fmt_pct_ratio(v: Any) -> str:  # 0–1 ratio
    return f"{_num(float(v) * 100)}%"


def _fmt_pp(v: Any) -> str:
    return f"{_num(float(v))} pp"


def _fmt_signed_pct(v: Any) -> str:
    return f"{float(v):+.1f}%"


def _fmt_signed(v: Any) -> str:
    return f"{float(v):+.2f}"


def _fmt_num(v: Any) -> str:
    return _num(float(v), 2) if isinstance(v, float) and abs(v) < 100 else f"{float(v):,.0f}"


def _fmt_int(v: Any) -> str:
    return f"{int(v):,}"


def _fmt_year(v: Any) -> str:
    # Calendar years must not pick up a thousands separator ("2,025").
    return str(int(v))


def _fmt_gwh(v: Any) -> str:
    return f"{_num(float(v))} GWh"


def _fmt_hours(v: Any) -> str:
    return f"{float(v):,.0f} h"


def _fmt_index(v: Any) -> str:
    return f"{float(v):.2f}"


def _fmt_pvalue(v: Any) -> str:
    return f"{float(v):.3f}"


def _fmt_date(v: Any) -> str:
    if isinstance(v, (datetime, date)):
        return f"{v:%d %b %Y}"
    try:
        return f"{datetime.fromisoformat(str(v)):%d %b %Y}"
    except ValueError:
        return str(v)


def _fmt_str(v: Any) -> str:
    text = str(v).replace("_", " ").strip()
    return text[:1].upper() + text[1:]


def _fmt_bool(v: Any) -> str:
    return "Yes" if v else "No"


def _fmt_month_list(v: Any) -> str:
    months = [str(m) for m in v] if isinstance(v, (list, tuple)) else [str(v)]
    shown = months[:_MONTH_LIST_CAP]
    extra = len(months) - len(shown)
    text = ", ".join(shown)
    return f"{text} +{extra} more" if extra > 0 else text


def _fmt_by_year(v: Any) -> str:
    if not isinstance(v, dict):
        return str(v)
    return " · ".join(f"{year}: {float(val):.2f}" for year, val in sorted(v.items()))


# ── slot specs ──────────────────────────────────────────────────────────

Formatter = Callable[[Any], str]

# (slot_key, label, formatter) — or a callable(data_slots) -> Optional[(label, value)]
Slot = tuple[str, str, Formatter]
ComputedSlot = Callable[[dict], Optional[tuple[str, str]]]


def _range(lo_key: str, hi_key: str, label: str, suffix: str = "") -> ComputedSlot:
    def compute(slots: dict) -> Optional[tuple[str, str]]:
        lo, hi = slots.get(lo_key), slots.get(hi_key)
        if lo is None or hi is None:
            return None
        return label, f"{float(lo):+.1f} to {float(hi):+.1f}{suffix}"

    return compute


def _daterange(start_key: str, end_key: str, label: str) -> ComputedSlot:
    def compute(slots: dict) -> Optional[tuple[str, str]]:
        start, end = slots.get(start_key), slots.get(end_key)
        if start is None or end is None:
            return None
        return label, f"{_fmt_date(start)} – {_fmt_date(end)}"

    return compute


_FIN_OPEX_SLOTS: tuple = (
    ("opex_per_mwh", "Opex per MWh", lambda v: _num(float(v))),
    ("zone_opex_median", "Peer median opex/MWh", lambda v: _num(float(v))),
    ("pct_over_median", "Over peer median", _fmt_pct),
    ("location_type", "Location type", _fmt_str),
    ("full_years", "Fiscal years used", _fmt_int),
)

EVIDENCE_SLOTS: dict[SchemaCode, tuple] = {
    SchemaCode.OPS_01: (
        ("odi_pct", "Avg disruption (ODI)", _fmt_pct),
        ("odi_months_below_threshold", "Months below threshold", _fmt_int),
        ("odi_threshold", "Threshold", _fmt_pct),
        ("disruption_month_list", "Disrupted months", _fmt_month_list),
    ),
    SchemaCode.OPS_02: (
        ("hodi_pct", "High-wind ODI", _fmt_pct),
        ("ssr", "Seasonal shortfall ratio", _fmt_index),
        ("high_wind_months", "High-wind months", _fmt_month_list),
        ("months_observed", "Months observed", _fmt_int),
    ),
    SchemaCode.OPS_03: (
        ("odi_pct", "Avg disruption (ODI)", _fmt_pct),
        ("contract_type", "O&M contract type", _fmt_str),
        ("has_availability_penalties", "Availability penalties", _fmt_bool),
    ),
    SchemaCode.OPS_04: (
        ("slope_pct_per_year", "Degradation slope", lambda v: f"{float(v):+.2f}%/yr"),
        ("p_value", "p-value", _fmt_pvalue),
        ("r_squared", "R²", _fmt_index),
        _range("ci_lower_95_pct", "ci_upper_95_pct", "95% CI", "%/yr"),
        ("years_of_data", "Years of data", lambda v: _num(float(v))),
        ("n_constraint_hours_excluded", "Constraint hours excluded", _fmt_int),
    ),
    SchemaCode.OPS_05: (("curtailment_pct", "Curtailed share of output", _fmt_pct),),
    SchemaCode.OPS_06: (
        ("norm_index_p50", "Wind-normalised index", _fmt_index),
        ("consecutive_months_below_threshold", "Consecutive months below", _fmt_int),
        ("threshold", "Threshold", lambda v: _num(float(v))),
        ("months_observed", "Months observed", _fmt_int),
    ),
    SchemaCode.OPS_07: (
        ("turbine_count", "Turbines", _fmt_int),
        ("pct_in_final_5yr", "Fleet in final 5 years", _fmt_pct_ratio),
        ("any_past_design_life", "Past design life", _fmt_bool),
        ("design_life_years", "Design life", lambda v: f"{int(v)} yr"),
        ("as_of_year", "As of", _fmt_year),
    ),
    SchemaCode.OPS_08: (
        ("review_status", "Review status", _fmt_str),
        ("duration_hours", "Constraint duration", _fmt_hours),
        ("mean_q90_ratio", "Mean Q90 ratio", _fmt_index),
        ("mean_q50_ratio", "Mean Q50 ratio", _fmt_index),
        ("flag_trigger", "Trigger", _fmt_str),
        _daterange("period_start", "period_end", "Constraint window"),
    ),
    SchemaCode.MKT_01: (
        ("capture_rate", "Capture rate", _fmt_pct_ratio),
        ("zone_avg_capture", "Zone average", _fmt_pct_ratio),
        ("gap_pp", "Gap vs zone", _fmt_pp),
        ("price_zone", "Price zone", str),
        ("ppa_status", "PPA status", _fmt_str),
        ("cannibalisation_index", "Cannibalisation index", _fmt_index),
        ("ppa_expiry_date", "PPA expiry", _fmt_date),
    ),
    SchemaCode.MKT_02: (
        ("price_zone", "Price zone", str),
        ("mkt01_severity", "MKT-01 severity", _fmt_str),
        ("ppa_status", "PPA status", _fmt_str),
        ("storage_present", "BESS on site", _fmt_bool),
    ),
    SchemaCode.MKT_03: (
        ("cannibalisation_index", "Cannibalisation index", _fmt_index),
        ("ci_trend_yoy", "CI trend YoY", _fmt_signed),
        ("ci_values_by_year", "CI by year", _fmt_by_year),
        ("price_zone", "Price zone", str),
        ("ppa_status", "PPA status", _fmt_str),
    ),
    SchemaCode.MKT_04: (
        ("ppa_buyer", "PPA buyer", str),
        ("ppa_end_date", "PPA end date", _fmt_date),
        ("months_until_expiry", "Months until expiry", lambda v: _num(float(v))),
        ("contract_type", "Contract type", _fmt_str),
        ("ppa_status", "PPA status", _fmt_str),
    ),
    SchemaCode.MKT_06: (
        ("negative_price_hours", "Negative-price hours", _fmt_hours),
        ("negative_price_hours_per_year", "Hours per year", lambda v: _num(float(v))),
        ("window_days", "Window analysed", lambda v: f"{int(v):,} days"),
    ),
    SchemaCode.FIN_01: (
        ("attainment_pct", "P50 attainment", _fmt_pct),
        ("actual_gwh", "Actual generation", _fmt_gwh),
        ("p50_target_gwh", "P50 target", _fmt_gwh),
        ("prior_attainment_pct", "Prior-year attainment", _fmt_pct),
        ("attainment_year", "Assessment year", _fmt_year),
    ),
    SchemaCode.FIN_02: _FIN_OPEX_SLOTS,
    SchemaCode.FIN_03: _FIN_OPEX_SLOTS,
    SchemaCode.DQ_01: (
        ("max_gap_hours", "Largest gap", _fmt_hours),
        ("total_gap_hours", "Total gap hours", _fmt_hours),
        ("gap_count", "Gaps in period", _fmt_int),
        _daterange("largest_gap_start", "largest_gap_end", "Largest gap window"),
    ),
}


# ── annotation notes ────────────────────────────────────────────────────


def _annotation_notes(slots: dict) -> list[str]:
    notes: list[str] = []
    if slots.get("baseline_caveat"):
        notes.append("Severity capped at Indicative — the degradation baseline is a placeholder.")
    if slots.get("provisional"):
        notes.append("Provisional pending OPS-08 structural-constraint review.")
    reclassified = slots.get("reclassified_from")
    if reclassified:
        codes = ", ".join(str(c).replace("_", "-") for c in reclassified)
        notes.append(f"Absorbs suppressed {codes}.")
    downgraded = slots.get("overlap_downgraded_from")
    if downgraded:
        notes.append(
            f"Downgraded from {_fmt_str(downgraded)} — overlaps confirmed cannibalisation."
        )
    return notes


# ── public API ──────────────────────────────────────────────────────────


def format_evidence(schema_code: SchemaCode, data_slots: Optional[dict]) -> dict:
    """Curated, formatted evidence for one finding.

    Returns ``{"items": [{"label", "value"}], "notes": [str]}``. Unknown or
    missing slots are skipped; formatter errors skip the single slot rather
    than dropping the whole panel.
    """
    slots = data_slots or {}
    items: list[dict[str, str]] = []
    for spec in EVIDENCE_SLOTS.get(schema_code, ()):
        try:
            if callable(spec):  # ComputedSlot
                computed = spec(slots)
                if computed is not None:
                    label, value = computed
                    items.append({"label": label, "value": value})
                continue
            key, label, formatter = spec
            value = slots.get(key)
            if value is None:
                continue
            items.append({"label": label, "value": formatter(value)})
        except (TypeError, ValueError):
            continue
    return {"items": items, "notes": _annotation_notes(slots)}
