"""Export one windfarm's hourly data to the spec script's CSV format.

Used by:
  - scripts/capture_pipeline_baseline.py (P0.5)
  - one-off Layer-2 spec side-by-side comparisons (Milestone A merge gate)

The vendored spec script (`tests/reference/energyexe_pipeline_full.py`) expects a CSV
with the columns `time, wind_speed_mps, power_mw, Price[Currency/MWh]`. Currency is
expected to be EUR throughout (the spec hardcodes EUR in scenario thresholds and
log labels).

The SQL uses CTE pre-aggregation per the pattern memorialised in
`reference_pipeline_csv_export_cte_pattern.md` — multi-unit + multi-source windfarms
(e.g. East Anglia One has 2 ELEXON BMUs × ELEXON+ENTSOE prices) double-sum without it.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)


@dataclass
class ExportResult:
    """Metadata about a completed spec-format CSV export."""

    out_path: Path
    windfarm_id: int
    windfarm_code: str
    rated_mw: float
    row_count: int
    gen_start: date
    gen_end: date
    currency: str
    has_gbp_prices: bool
    nan_price_rows: int


_WINDFARM_META_SQL = text(
    """
    SELECT w.id, w.code, w.nameplate_capacity_mw,
           (SELECT STRING_AGG(DISTINCT currency, ',' ORDER BY currency)
              FROM price_data WHERE windfarm_id = w.id) AS currencies
    FROM   windfarms w
    WHERE  w.id = :wf_id
    """
)


def _build_export_sql(
    *,
    start_year: Optional[int],
    end_year: Optional[int],
    require_eur: bool,
) -> text:
    """Build the export query with optional filters baked in as SQL literals.

    We do NOT use bind params for the conditional filters because asyncpg's
    strict type inference chokes on patterns like `:param IS FALSE` (booleans)
    and `:param IS NULL OR ...` (untyped nulls). Baking these into the SQL is
    simpler and faster than dynamic-cast workarounds — the filters are pure
    integers / static strings, no SQL-injection surface.
    See MEMORY.md "asyncpg requires datetime objects for timestamptz parameters".
    """
    year_filter = ""
    if start_year is not None:
        year_filter += f"          AND EXTRACT(YEAR FROM g.hour) >= {int(start_year)}\n"
    if end_year is not None:
        year_filter += f"          AND EXTRACT(YEAR FROM g.hour) <= {int(end_year)}\n"

    currency_filter = "          AND currency = 'EUR'\n" if require_eur else ""

    return text(
        f"""
        WITH gen_hourly AS (
            SELECT g.hour,
                   SUM(g.generation_mwh)  AS power_mw,
                   BOOL_OR(g.is_ramp_up)  AS any_ramp_up
            FROM   generation_data g
            JOIN   generation_units gu ON g.generation_unit_id = gu.id
            WHERE  gu.windfarm_id = :wf_id
              AND  g.generation_mwh IS NOT NULL
{year_filter}            GROUP BY g.hour
        ),
        wx_hourly AS (
            SELECT hour, AVG(wind_speed_100m) AS wind_speed_mps
            FROM   weather_data
            WHERE  windfarm_id = :wf_id
            GROUP BY hour
        ),
        price_hourly AS (
            SELECT hour, AVG(day_ahead_price) AS price
            FROM   price_data
            WHERE  windfarm_id = :wf_id
{currency_filter}            GROUP BY hour
        )
        SELECT to_char(g.hour AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS time,
               w.wind_speed_mps,
               g.power_mw,
               p.price AS price_per_mwh
        FROM   gen_hourly g
        LEFT JOIN wx_hourly   w ON w.hour = g.hour
        LEFT JOIN price_hourly p ON p.hour = g.hour
        WHERE  NOT g.any_ramp_up
        ORDER BY g.hour
        """
    )


async def export_windfarm_to_spec_format(
    session: AsyncSession,
    wf_id: int,
    out_path: Path,
    *,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    allow_gbp: bool = False,
) -> ExportResult:
    """Write windfarm `wf_id`'s hourly data to `out_path` in spec-CSV format.

    The spec script expects EUR-denominated prices. If the windfarm only has GBP
    pricing and `allow_gbp` is False (the default), this raises ValueError —
    Module 3 / Module 6 EUR figures from the spec would be wrong otherwise.
    Pass `allow_gbp=True` to override (and accept that downstream EUR comparisons
    are unreliable for this run — useful for Module 1b / Module 5 validation where
    price doesn't matter).

    Args:
        session: an AsyncSession bound to the EnergyExe Postgres.
        wf_id: windfarm primary key.
        out_path: where to write the CSV. Created (with parents) if missing.
        start_year / end_year: optional inclusive year filter on `generation_data.hour`.
        allow_gbp: opt-in to writing GBP-priced data with the EUR-labeled spec column.

    Returns:
        ExportResult with metadata. The CSV is written even when row_count is
        small — caller decides whether that's enough.

    Raises:
        ValueError: if the windfarm doesn't exist, has no rated_mw, or has only
            GBP pricing and `allow_gbp` is False.
    """
    meta_row = (await session.execute(_WINDFARM_META_SQL, {"wf_id": wf_id})).one_or_none()
    if meta_row is None:
        raise ValueError(f"Windfarm id={wf_id} not found")

    rated_mw = float(meta_row.nameplate_capacity_mw) if meta_row.nameplate_capacity_mw else 0.0
    if rated_mw <= 0:
        raise ValueError(f"Windfarm id={wf_id} has no nameplate_capacity_mw")

    currencies = (meta_row.currencies or "").split(",") if meta_row.currencies else []
    currencies = [c for c in currencies if c]
    has_eur = "EUR" in currencies
    has_gbp = "GBP" in currencies
    primary_currency = (
        "EUR"
        if has_eur
        else (currencies[0] if currencies else "UNKNOWN")
    )
    if not has_eur and has_gbp and not allow_gbp:
        raise ValueError(
            f"Windfarm id={wf_id} ({meta_row.code}) has only GBP pricing. "
            "Spec script assumes EUR — Module 3/6 outputs would be wrong. "
            "Pass allow_gbp=True if you only need Module 1b / Module 5 results "
            "(price-independent)."
        )

    require_eur = has_eur and not allow_gbp
    if has_gbp and allow_gbp:
        logger.warning(
            "spec_csv_exporter_gbp_allowed",
            wf_id=wf_id,
            code=meta_row.code,
            note="Module 3/6 EUR figures from spec script will be GBP-valued; ignore them.",
        )

    query = _build_export_sql(
        start_year=start_year, end_year=end_year, require_eur=require_eur
    )
    rows = (await session.execute(query, {"wf_id": wf_id})).all()

    if not rows:
        raise ValueError(
            f"Windfarm id={wf_id} returned 0 rows from the export query "
            f"(start_year={start_year}, end_year={end_year}, require_eur={require_eur})"
        )

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    nan_price_rows = 0
    gen_start: Optional[date] = None
    gen_end: Optional[date] = None

    with out_path.open("w", newline="") as f:
        # lineterminator='\n' to match psql \COPY output exactly (byte-identical
        # CSVs simplify diffing against manually-extracted reference fixtures).
        writer = csv.writer(f, lineterminator="\n")
        writer.writerow(["time", "wind_speed_mps", "power_mw", "Price[Currency/MWh]"])
        for r in rows:
            if r.price_per_mwh is None:
                nan_price_rows += 1
            writer.writerow(
                [
                    r.time,
                    f"{float(r.wind_speed_mps):.4f}" if r.wind_speed_mps is not None else "",
                    f"{float(r.power_mw):.4f}" if r.power_mw is not None else "",
                    f"{float(r.price_per_mwh):.4f}" if r.price_per_mwh is not None else "",
                ]
            )
            # Track date range from the formatted timestamp string (YYYY-MM-DD HH:MM:SS).
            day = date.fromisoformat(r.time[:10])
            if gen_start is None or day < gen_start:
                gen_start = day
            if gen_end is None or day > gen_end:
                gen_end = day

    assert gen_start is not None and gen_end is not None  # rows non-empty checked above

    result = ExportResult(
        out_path=out_path,
        windfarm_id=wf_id,
        windfarm_code=meta_row.code,
        rated_mw=rated_mw,
        row_count=len(rows),
        gen_start=gen_start,
        gen_end=gen_end,
        currency=primary_currency,
        has_gbp_prices=has_gbp,
        nan_price_rows=nan_price_rows,
    )
    logger.info(
        "spec_csv_exported",
        wf_id=wf_id,
        code=meta_row.code,
        path=str(out_path),
        rows=result.row_count,
        currency=result.currency,
        nan_price_rows=nan_price_rows,
    )
    return result
