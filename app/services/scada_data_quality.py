"""Data Quality page (EPR-230): coverage cards and the Opportunity Schema triggers table.

Pure functions over numbers the SCADA service has already read, so they test without a
database. All coverage is over the farm's whole data period.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

TRIGGER_CONFIG = Path(__file__).resolve().parent.parent / "data" / "scada_trigger_sources.json"

# A source counts as available at this coverage, the pipeline's own minimum.
MIN_COVERAGE = 0.90


def load_trigger_config() -> Dict[str, Any]:
    return json.loads(TRIGGER_CONFIG.read_text())


def coverage_cards(
    expected: int,
    present: int,
    power_null_pct: Optional[float],
    wind_null_pct: Optional[float],
    price_hours: Optional[int],
    price_expected: Optional[int],
) -> Dict[str, Any]:
    """Counts behind the four cards. Power and wind are judged on their own, from the share
    of present rows where that one signal is null; price is None when no series is linked."""

    def usable(null_pct: Optional[float]) -> Optional[int]:
        return None if null_pct is None else round(present * (1 - null_pct / 100))

    return {
        "timestamps": {"present": present, "expected": expected},
        "power": {"usable": usable(power_null_pct), "expected": expected},
        "wind": {"usable": usable(wind_null_pct), "expected": expected},
        "price": (
            None
            if price_hours is None or not price_expected
            else {"priced": price_hours, "expected": price_expected}
        ),
    }


def trigger_table(config: Dict[str, Any], farm: str, price_ok: Optional[bool]) -> Dict[str, Any]:
    """One row per trigger: its sources, which are available, and met = all available.

    `price_ok` is the measured price coverage check; None when the portal cannot measure it
    (no SCADA period or no linked series), and then the config's farm list decides."""
    labels: Dict[str, str] = config["sources"]
    have = config["farms"].get(farm)
    rows: List[Dict[str, Any]] = []
    for trig in config["triggers"]:
        sources = [
            {
                "key": key,
                "label": labels[key],
                "available": (
                    price_ok
                    if key == "day_ahead_price" and price_ok is not None
                    else have is not None and key in have
                ),
            }
            for key in trig["sources"]
        ]
        rows.append(
            {
                "code": trig["code"],
                "name": trig["name"],
                "sources": sources,
                "met": all(s["available"] for s in sources),
            }
        )
    return {
        "provisional": bool(config.get("provisional")),
        "set_up": have is not None,
        "rows": rows,
    }


def completeness_grid(rows: List[Any], cod_month: Dict[str, str]) -> Dict[str, Any]:
    """Turbine x month grid from monthly sums (turbine, mo, rp, rv, ex, pre).

    Months run from the farm's first data month to its last. A cell is
    [valid_pct, pre_cod, present_pct]; a month before the turbine's COD with no rows is
    still marked pre-COD, so it shows hatched instead of missing.
    """
    turbines = sorted({r.turbine for r in rows})
    present = sorted({r.mo for r in rows})
    months: List[str] = []
    if present:
        y, m = int(present[0][:4]), int(present[0][5:])
        while f"{y:04d}-{m:02d}" <= present[-1]:
            months.append(f"{y:04d}-{m:02d}")
            y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    m_idx = {mo: i for i, mo in enumerate(months)}
    t_idx = {t: i for i, t in enumerate(turbines)}
    grid: List[List[Optional[List[float]]]] = [
        [[0.0, 1, 0.0] if mo < cod_month.get(t, "") else None for mo in months] for t in turbines
    ]
    for r in rows:
        ex = float(r.ex or 0)
        if ex <= 0:
            continue
        grid[t_idx[r.turbine]][m_idx[r.mo]] = [
            round(float(r.rv or 0) / ex * 100, 1),
            1 if r.pre else 0,
            round(float(r.rp or 0) / ex * 100, 1),
        ]
    return {"turbines": turbines, "months": months, "grid": grid}
