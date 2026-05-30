"""DQ-01 · Generation data gaps — M6 data-quality detector (issue #109).

Detects consecutive *missing-generation* hours per windfarm over the detection
window and (a) logs each significant gap into ``data_anomalies`` as a
``missing_generation_data`` anomaly carrying a typed ``gap_hours``, and (b)
produces a DQ-01 ``DetectorResult`` whose severity reflects the largest gap.

The gap math is split into a **pure, DB-free** core so the spec's behavioural
cases ("two non-consecutive 20h gaps", "complete month") are testable without a
database:

* :func:`find_generation_gaps` — given the hours that HAVE generation data, find
  the contiguous missing-hour runs *between* present data. Bracketed-by-data
  only: no present hours ⇒ no gaps (so a windfarm with no data at all is not
  reported as one giant gap).
* :func:`classify_gap_severity` — the spec severity ladder:
  ``>= 72h → CONFIRMED``, ``>= 48h → INDICATIVE``, ``>= 24h → WATCH``,
  else ``None`` (gaps < 24h are not logged).

Severity boundaries (locked by tests):
    24 → WATCH, 23 → None, 48 → INDICATIVE, 72 → CONFIRMED.

Idempotency
===========
:func:`detect_generation_gaps` reconciles the window's
``missing_generation_data`` anomalies to *exactly* the gaps found this run:
existing anomalies for the windfarm in the window are removed first, then one row
is inserted per current gap. So re-running after a backfill (the gap is gone)
removes the stale row — re-running with the same gaps re-creates the identical
set. No DB-level unique constraint is required (the ``data_anomalies`` table has
none on ``(windfarm_id, anomaly_type, period_start)``); reconcile-then-insert is
the idempotency mechanism.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional, Sequence, Tuple

from sqlalchemy import and_, delete

from app.models.data_anomaly import AnomalyStatus, AnomalyType, DataAnomaly
from app.models.opportunity import SchemaCode, Severity
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

# Spec severity ladder for a single gap's length, in hours (inclusive lower
# bounds). A gap shorter than WATCH (24h) is not logged at all.
GAP_HOURS_CONFIRMED = 72
GAP_HOURS_INDICATIVE = 48
GAP_HOURS_WATCH = 24

# The anomaly_type string written to data_anomalies for a DQ-01 gap.
MISSING_GENERATION_DATA = AnomalyType.MISSING_GENERATION_DATA

# data_anomalies.severity is a free-text string column with its own vocabulary
# (low/medium/high/critical); map the DQ-01 opportunity Severity onto it so the
# logged anomaly row carries a sensible severity independent of the detection
# Severity enum.
_ANOMALY_SEVERITY_BY_TIER = {
    Severity.CONFIRMED: "critical",
    Severity.INDICATIVE: "high",
    Severity.WATCH: "medium",
}


# ─── Pure, DB-free helpers ────────────────────────────────────────────────────


def find_generation_gaps(
    present_hours: Sequence[datetime],
    period_start: datetime,
    period_end: datetime,
) -> List[Tuple[datetime, datetime, int]]:
    """Find contiguous missing-hour runs *between* present generation hours.

    Args:
        present_hours: the hours (hour-aligned ``datetime``) that HAVE generation
            data in the window. Order does not matter; duplicates are collapsed.
        period_start: window start (inclusive). Used only to bound the search.
        period_end: window end (exclusive).

    Returns:
        A chronological list of ``(gap_start, gap_end, gap_hours)`` tuples, where
        ``gap_start`` is the first missing hour, ``gap_end`` is the first present
        hour *after* the gap (exclusive end), and ``gap_hours`` is the count of
        missing hours (``gap_end - gap_start`` in whole hours).

    Contract:
        * Gaps are only those **bracketed by present data** — i.e. the run of
          missing hours strictly between two consecutive present hours. Missing
          hours before the first present hour or after the last present hour are
          NOT reported (we can only assert "data is missing" where surrounding
          data proves the windfarm was reporting). Consequently, **empty
          ``present_hours`` ⇒ no gaps** (``[]``), never "the whole window is one
          gap" — this is the snapshot-safety contract relied on by the M1
          characterization harness.
        * Present hours are normalised to whole hours and de-duplicated; values
          outside ``[period_start, period_end)`` are ignored.
    """
    # Normalise to the top of the hour, de-dup, drop out-of-window hours, sort.
    cleaned = sorted(
        {_floor_hour(h) for h in present_hours if h is not None and period_start <= h < period_end}
    )
    if len(cleaned) < 2:
        # Need at least two present hours to bracket a gap between them.
        return []

    gaps: List[Tuple[datetime, datetime, int]] = []
    for prev, nxt in zip(cleaned, cleaned[1:]):
        delta_hours = int((nxt - prev).total_seconds() // 3600)
        missing = delta_hours - 1
        if missing > 0:
            gap_start = prev + timedelta(hours=1)
            gap_end = nxt  # exclusive end = first present hour after the gap
            gaps.append((gap_start, gap_end, missing))
    return gaps


def classify_gap_severity(gap_hours: Optional[int]) -> Optional[Severity]:
    """Classify a single gap's length into a DQ-01 severity tier.

    Spec ladder (inclusive lower bounds): ``>= 72 → CONFIRMED``,
    ``>= 48 → INDICATIVE``, ``>= 24 → WATCH``, else ``None`` (not logged).
    Boundaries (locked by tests): 24 → WATCH, 23 → None, 48 → INDICATIVE,
    72 → CONFIRMED. Returns ``None`` for a missing / non-positive gap.
    """
    if gap_hours is None:
        return None
    if gap_hours >= GAP_HOURS_CONFIRMED:
        return Severity.CONFIRMED
    if gap_hours >= GAP_HOURS_INDICATIVE:
        return Severity.INDICATIVE
    if gap_hours >= GAP_HOURS_WATCH:
        return Severity.WATCH
    return None


def _floor_hour(dt: datetime) -> datetime:
    """Truncate a datetime to the top of its hour (preserve tzinfo)."""
    return dt.replace(minute=0, second=0, microsecond=0)


def _loggable_gaps(
    gaps: Sequence[Tuple[datetime, datetime, int]]
) -> List[Tuple[datetime, datetime, int]]:
    """Gaps whose length meets the WATCH floor (>= 24h) — the ones we log/fire on."""
    return [g for g in gaps if classify_gap_severity(g[2]) is not None]


# ─── DB I/O: idempotent anomaly reconciliation ────────────────────────────────


async def detect_generation_gaps(
    db,
    windfarm_id: int,
    start: datetime,
    end: datetime,
) -> List[Tuple[datetime, datetime, int]]:
    """Find generation gaps in ``[start, end)`` and idempotently log anomalies.

    Reuses the pure :func:`find_generation_gaps` over the windfarm's present
    hours, keeps only the loggable gaps (>= 24h), and reconciles the window's
    ``missing_generation_data`` anomalies to *exactly* those gaps:

        1. Delete every existing ``missing_generation_data`` anomaly for this
           windfarm whose ``period_start`` falls in ``[start, end)``.
        2. Insert one ``DataAnomaly`` per current loggable gap, with
           ``anomaly_type='missing_generation_data'`` and the typed ``gap_hours``.

    This is idempotent: a re-run with the same gaps reproduces the identical row
    set; a re-run after a backfill (gap gone) deletes the stale row and inserts
    nothing. Returns the list of loggable ``(gap_start, gap_end, gap_hours)``
    gaps that were logged.

    The DB I/O is deliberately thin — all gap math lives in the pure helpers.
    """
    present_hours = await _load_present_hours(db, windfarm_id, start, end)
    gaps = _loggable_gaps(find_generation_gaps(present_hours, start, end))

    # 1. Clear prior DQ-01 anomalies for this windfarm in the window (idempotency).
    await db.execute(
        delete(DataAnomaly).where(
            and_(
                DataAnomaly.windfarm_id == windfarm_id,
                DataAnomaly.anomaly_type == MISSING_GENERATION_DATA,
                DataAnomaly.period_start >= start,
                DataAnomaly.period_start < end,
            )
        )
    )

    # 2. Insert one row per current gap.
    for gap_start, gap_end, gap_hours in gaps:
        severity = classify_gap_severity(gap_hours)
        db.add(
            DataAnomaly(
                anomaly_type=MISSING_GENERATION_DATA,
                severity=_ANOMALY_SEVERITY_BY_TIER.get(severity, "medium"),
                status=AnomalyStatus.PENDING,
                windfarm_id=windfarm_id,
                period_start=gap_start,
                period_end=gap_end,
                gap_hours=gap_hours,
                description=(
                    f"Missing generation data: {gap_hours} consecutive hours "
                    f"with no data for windfarm {windfarm_id}"
                ),
                anomaly_metadata={"gap_hours": gap_hours},
                detected_at=datetime.utcnow(),
            )
        )

    return gaps


async def _load_present_hours(
    db, windfarm_id: int, start: datetime, end: datetime
) -> List[datetime]:
    """Distinct present generation hours for the windfarm in ``[start, end)``."""
    from sqlalchemy import text

    query = text(
        """
        SELECT DISTINCT date_trunc('hour', hour) AS present_hour
        FROM generation_data
        WHERE windfarm_id = :wf_id
          AND hour >= :start AND hour < :end
        ORDER BY present_hour
    """
    )
    try:
        result = await db.execute(query, {"wf_id": windfarm_id, "start": start, "end": end})
        rows = result.fetchall()
    except Exception:
        return []
    return [r.present_hour for r in rows if r.present_hour is not None]


# ─── Detector entrypoint ──────────────────────────────────────────────────────


async def detect(ctx: DetectionContext) -> Optional[DetectorResult]:
    """DQ-01: generation data gaps.

    Reads the windfarm's gaps via ``ctx.load_generation_gaps()`` (None/empty-safe;
    a windfarm with no data at all yields no gaps), keeps only the loggable gaps
    (>= 24h), and fires at the severity of the **largest** gap. Returns ``None``
    when there is no loggable gap — so legacy scenarios (no generation injected)
    stay clean and the M1 characterization snapshot is byte-identical.

    The DetectorResult surfaces the largest gap plus an aggregate view in
    ``data_slots``: ``max_gap_hours``, ``total_gap_hours``, ``gap_count``.
    """
    raw_gaps = await ctx.load_generation_gaps()
    if not raw_gaps:
        return None

    gaps = _loggable_gaps(raw_gaps)
    if not gaps:
        return None

    max_gap_hours = max(g[2] for g in gaps)
    severity = classify_gap_severity(max_gap_hours)
    if severity is None:
        return None

    total_gap_hours = sum(g[2] for g in gaps)
    largest = max(gaps, key=lambda g: g[2])

    data_slots = {
        "max_gap_hours": max_gap_hours,
        "total_gap_hours": total_gap_hours,
        "gap_count": len(gaps),
        "largest_gap_start": largest[0].isoformat(),
        "largest_gap_end": largest[1].isoformat(),
        "period": f"{ctx.period_start.date()} to {ctx.period_end.date()}",
    }

    return DetectorResult(
        schema_code=SchemaCode.DQ_01,
        severity=severity,
        branch=None,
        data_slots=data_slots,
        missing_slots=[],
    )
