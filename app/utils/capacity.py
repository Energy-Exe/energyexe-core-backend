"""Installed capacity in force per hour, derived from ``generation_units`` dates.

Why this exists
---------------
Every performance-pipeline module used to divide each hour's generation by the
windfarm's single ``nameplate_capacity_mw``. Installed capacity is not constant:
phased build-outs, partial decommissioning and repowering change it (Smøla
150.4 → 145.8 MW on 2024-02-01; Midtfjellet 12.5 → 149.6 MW over 2012-2018).
Dividing by today's nameplate inflates or deflates ``p_pu`` for every hour
whose real capacity differed, which biases the power curve, the wind-normalised
index, ODI lost MWh, degradation and the commercial constraint proxy.

Why not ``generation_data.capacity_mw``
---------------------------------------
That column is the unit's registered capacity copied in at aggregation time. It
is never re-stamped when a unit is edited, it is NULL on some ingestion paths,
and it only exists for hours where the unit happened to report a row — so an
outage that shows up as missing rows would silently shrink the denominator and
hide lost energy. Unit metadata (dates + registered capacity) is the source of
truth analysts actually maintain, so capacity is derived from it here.

Definition
----------
``S(h)`` = sum of ``capacity_mw`` over units whose window covers hour ``h``:
``COALESCE(first_power_date, start_date) <= h < COALESCE(end_date, +inf)``,
start inclusive, end exclusive (same rule as ``app/utils/unit_resolver.py``),
dates taken as UTC midnight (same as the ramp-up flags).

The series is **anchored to nameplate**: ``capacity(h) = nameplate × S(h) / S(anchor)``
where the anchor is *today* (or the last date the farm had any unit in force).
Anchoring keeps every constant-capacity farm byte-identical to the old
behaviour and absorbs registry rounding (Clyde 522.0 vs 521.8). If ``S(anchor)``
disagrees with nameplate by more than ``ANCHOR_TOLERANCE`` the registry is
inconsistent and we fall back to constant nameplate (today's behaviour) rather
than invent a value — see ``capacity_basis_status``.

The anchor is deliberately frame-independent: a one-year load and a full-history
load must put the same hour on the same basis, otherwise the power curve and
the modules that compare against it would use different units.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Union

import numpy as np
import pandas as pd
import structlog
from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger(__name__)

# Registry rounding tolerance between today's summed unit capacity and nameplate.
ANCHOR_TOLERANCE = 0.03

# DataFrame.attrs marker set by attach_capacity(); capacity_col() refuses to
# fall back to a constant when a frame carrying this marker lost its column.
CAPACITY_BASIS_ATTR = "capacity_basis"
BASIS_IN_FORCE = "in_force"

STATUS_IN_FORCE = "in_force"  # anchored in-force series applied
STATUS_BLOCKED = "blocked"  # registry disagrees with nameplate → constant nameplate
STATUS_NO_UNITS = "no_units"  # no dated/sized units → constant nameplate
STATUS_IMPLAUSIBLE = "implausible"  # output contradicts the unit history → constant nameplate

# Farms whose unit history is contradicted by their own output: hours well
# above the capacity the registry says was in force. The anchor check cannot
# catch these (today's units agree with nameplate), so they stay on constant
# nameplate until the registry is corrected. Listed, with the evidence, in
# docs/reports/2026-10/capacity_audit/README.md.
#   7434 Chungwei (Zhongwei Da-an): 92 MW in 2020-21 and 132 MW in 2022 against
#        75.9 MW registered — Phase 1 is dated as ended 2013-01-31 and ChuangWei
#        as started 2023-10-26, both contradicted by the metered output.
REGISTRY_BLOCKED_WINDFARM_IDS: frozenset = frozenset({7434})

# A year in which more than this share of hours sits above the curve ceiling on
# the in-force basis means a unit's dates or capacity are wrong for that period
# (the loader then reverts the farm to constant nameplate and logs it). Smøla's
# genuine 150.4 MW years and Aikengall II's early-commissioning months (5 % in
# 2020) stay well under it; Chungwei's 2020 and 2022 (21-23 %) do not.
IMPLAUSIBLE_CEILING_SHARE = 0.10

DateLike = Union[date, datetime, pd.Timestamp, None]


@dataclass(frozen=True)
class UnitWindow:
    """One generation unit's registered capacity and the dates it was in force.

    ``start``/``end`` are UTC-midnight timestamps; ``None`` means unbounded.
    ``end`` is exclusive.
    """

    capacity_mw: float
    start: Optional[pd.Timestamp]
    end: Optional[pd.Timestamp]


# ─── Small helpers ───────────────────────────────────────────────


def utc_midnight(value: DateLike) -> Optional[pd.Timestamp]:
    """Date → tz-aware UTC midnight timestamp (None passes through)."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    ts = pd.Timestamp(value)
    if ts.tz is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.normalize()


def _today_utc(today: DateLike = None) -> pd.Timestamp:
    if today is None:
        return pd.Timestamp.now(tz="UTC").normalize()
    ts = utc_midnight(today)
    assert ts is not None
    return ts


def _hours_utc(hours: Union[pd.Series, pd.Index, Sequence]) -> pd.Series:
    h = pd.to_datetime(pd.Series(hours), utc=True)
    return h


# ─── Loading ────────────────────────────────────────────────────


_UNIT_WINDOWS_SQL = text(
    """
    SELECT windfarm_id,
           capacity_mw,
           COALESCE(first_power_date, start_date) AS start_date,
           end_date
    FROM generation_units
    WHERE windfarm_id IN :ids
      AND capacity_mw IS NOT NULL
      AND capacity_mw > 0
      AND (is_active OR end_date IS NOT NULL)
    ORDER BY windfarm_id, start_date NULLS FIRST, id
    """
).bindparams(bindparam("ids", expanding=True))


async def load_unit_windows(
    db: AsyncSession, windfarm_ids: Iterable[int]
) -> Dict[int, List[UnitWindow]]:
    """Load the unit windows for one or more windfarms.

    Units are included when they have a positive registered capacity and are
    either active or have an explicit end date. Inactive units without an end
    date are soft-deleted placeholders (ENERGISTYRELSEN seeds) and are ignored.
    """
    ids = sorted({int(i) for i in windfarm_ids})
    out: Dict[int, List[UnitWindow]] = {i: [] for i in ids}
    if not ids:
        return out
    rows = (await db.execute(_UNIT_WINDOWS_SQL, {"ids": ids})).fetchall()
    for wf_id, cap, start, end in rows:
        out[int(wf_id)].append(
            UnitWindow(
                capacity_mw=float(cap),
                start=utc_midnight(start),
                end=utc_midnight(end),
            )
        )
    return out


def windows_from_rows(rows: Iterable[Mapping]) -> List[UnitWindow]:
    """Build windows from dicts with ``capacity_mw``, ``start``/``start_date``/
    ``first_power_date`` and ``end``/``end_date`` keys (test + script helper)."""
    out: List[UnitWindow] = []
    for r in rows:
        cap = r.get("capacity_mw")
        if cap is None or float(cap) <= 0:
            continue
        start = r.get("start")
        if start is None:
            start = r.get("first_power_date") or r.get("start_date")
        end = r.get("end", r.get("end_date"))
        out.append(UnitWindow(float(cap), utc_midnight(start), utc_midnight(end)))
    return out


# ─── Pure computation ───────────────────────────────────────────


def installed_series(windows: Sequence[UnitWindow], hours) -> pd.Series:
    """Raw ``S(h)``: summed registered capacity of the units in force each hour."""
    h = _hours_utc(hours)
    total = pd.Series(0.0, index=h.index)
    for w in windows:
        mask = pd.Series(True, index=h.index)
        if w.start is not None:
            mask &= h >= w.start
        if w.end is not None:
            mask &= h < w.end
        total = total + w.capacity_mw * mask.astype(float)
    return total


def installed_at(windows: Sequence[UnitWindow], at: DateLike) -> float:
    """Raw ``S`` at one instant."""
    ts = utc_midnight(at)
    if ts is None:
        return 0.0
    total = 0.0
    for w in windows:
        if w.start is not None and ts < w.start:
            continue
        if w.end is not None and ts >= w.end:
            continue
        total += w.capacity_mw
    return total


def anchor_date(windows: Sequence[UnitWindow], today: DateLike = None) -> pd.Timestamp:
    """Frame-independent anchor: today, or the last day any unit was in force."""
    t = _today_utc(today)
    if installed_at(windows, t) > 0:
        return t
    # Fully decommissioned farm: the last in-force day is the day before the
    # latest end date that has already passed.
    ended = [w.end - pd.Timedelta(days=1) for w in windows if w.end is not None and w.end <= t]
    return max(ended) if ended else t


def anchor_scale(
    windows: Sequence[UnitWindow], nameplate: float, today: DateLike = None
) -> Optional[float]:
    """``nameplate / S(anchor)`` when the registry agrees with nameplate.

    Returns None when there is nothing to anchor to or the disagreement exceeds
    ``ANCHOR_TOLERANCE`` (blocked — caller falls back to constant nameplate).
    """
    if not windows or not nameplate or nameplate <= 0:
        return None
    s_anchor = installed_at(windows, anchor_date(windows, today))
    if s_anchor <= 0:
        return None
    if abs(s_anchor / float(nameplate) - 1.0) > ANCHOR_TOLERANCE:
        return None
    return float(nameplate) / s_anchor


def registry_blocked(windfarm_id: Optional[int]) -> bool:
    """True for farms kept on constant nameplate by ``REGISTRY_BLOCKED_WINDFARM_IDS``."""
    return windfarm_id is not None and int(windfarm_id) in REGISTRY_BLOCKED_WINDFARM_IDS


def capacity_basis_status(
    windows: Sequence[UnitWindow],
    nameplate: float,
    today: DateLike = None,
    *,
    windfarm_id: Optional[int] = None,
) -> str:
    """One of STATUS_IN_FORCE / STATUS_BLOCKED / STATUS_NO_UNITS."""
    if registry_blocked(windfarm_id):
        return STATUS_BLOCKED
    if not windows:
        return STATUS_NO_UNITS
    return (
        STATUS_IN_FORCE if anchor_scale(windows, nameplate, today) is not None else STATUS_BLOCKED
    )


def capacity_series(
    windows: Sequence[UnitWindow],
    hours,
    nameplate: float,
    *,
    anchor: bool = True,
    today: DateLike = None,
    windfarm_id: Optional[int] = None,
) -> pd.Series:
    """Capacity in force per hour (MW), aligned to ``hours``' index.

    Anchored to nameplate by default (see module docstring). ``anchor=False``
    returns the raw unit sum and is meant for tests and audits only. Hours with
    no unit in force (``S(h) = 0``) fall back to nameplate, as does a farm in
    ``REGISTRY_BLOCKED_WINDFARM_IDS`` (pass ``windfarm_id`` to apply that).
    """
    h = _hours_utc(hours)
    const = pd.Series(float(nameplate), index=h.index)
    if not windows or registry_blocked(windfarm_id):
        return const
    s = installed_series(windows, h)
    if anchor:
        scale = anchor_scale(windows, nameplate, today)
        if scale is None:
            return const
        cap = s * scale
    else:
        cap = s
    return cap.where(cap > 0, float(nameplate))


def capacity_at(
    windows: Sequence[UnitWindow],
    at: DateLike,
    nameplate: float,
    *,
    anchor: bool = True,
    today: DateLike = None,
    windfarm_id: Optional[int] = None,
) -> float:
    """Capacity in force at one instant (same rules as ``capacity_series``)."""
    s = installed_at(windows, at)
    if not windows or registry_blocked(windfarm_id):
        return float(nameplate)
    if anchor:
        scale = anchor_scale(windows, nameplate, today)
        if scale is None:
            return float(nameplate)
        s = s * scale
    return s if s > 0 else float(nameplate)


def attach_capacity(
    df: pd.DataFrame,
    windows: Sequence[UnitWindow],
    nameplate: float,
    *,
    today: DateLike = None,
    windfarm_id: Optional[int] = None,
) -> pd.DataFrame:
    """Add ``capacity_mw`` and recompute ``p_pu`` on the in-force basis.

    Marks the frame (``df.attrs``) so downstream ``capacity_col`` calls refuse
    to silently revert to nameplate if the column is projected away.
    """
    df["capacity_mw"] = capacity_series(
        windows, df["hour"], nameplate, today=today, windfarm_id=windfarm_id
    ).values
    df["p_pu"] = df["generation_mwh"].astype(float) / df["capacity_mw"]
    df.attrs[CAPACITY_BASIS_ATTR] = BASIS_IN_FORCE
    return df


def implausible_years(
    df: pd.DataFrame, ceiling: float, *, min_share: float = IMPLAUSIBLE_CEILING_SHARE
) -> Dict[int, float]:
    """Years whose share of hours with ``p_pu`` above ``ceiling`` exceeds ``min_share``.

    On the in-force basis a persistent excess means the registry under-states
    the capacity that was really running (wrong unit dates or capacity), and
    dividing by it would inflate every ratio for those years and drop a fifth
    of the hours at the curve ceiling. Empty when the basis is plausible.
    """
    if df.empty or "p_pu" not in df.columns or "year" not in df.columns:
        return {}
    share = (df["p_pu"] > ceiling).groupby(df["year"]).mean()
    return {int(y): round(float(v), 4) for y, v in share.items() if v > min_share}


def capacity_col(df: pd.DataFrame, rated_mw: float) -> pd.Series:
    """Per-hour capacity to convert per-unit values back to MW.

    Returns ``df["capacity_mw"]`` when present. Frames produced by the loader
    carry a basis marker; if such a frame lost the column this raises instead
    of quietly multiplying by nameplate. Plain fixtures (no marker, no column)
    get a constant ``rated_mw`` series — the pre-existing behaviour.
    """
    if "capacity_mw" in df.columns:
        return df["capacity_mw"].astype(float)
    if df.attrs.get(CAPACITY_BASIS_ATTR) == BASIS_IN_FORCE:
        raise ValueError(
            "DataFrame is on the in-force capacity basis but has no 'capacity_mw' "
            "column — a projection dropped it. Keep the column instead of falling "
            "back to nameplate."
        )
    return pd.Series(float(rated_mw), index=df.index)


# ─── Period helpers (capacity-factor views) ──────────────────────


def _as_utc(value: DateLike) -> pd.Timestamp:
    """Any date/datetime → tz-aware UTC timestamp (a bare date is UTC midnight)."""
    ts = pd.Timestamp(value)
    return ts.tz_localize("UTC") if ts.tz is None else ts.tz_convert("UTC")


def _overlap_hours(a_start, a_end, b_start: pd.Timestamp, b_end: pd.Timestamp) -> float:
    lo = b_start if a_start is None else max(a_start, b_start)
    hi = b_end if a_end is None else min(a_end, b_end)
    return max(0.0, (hi - lo).total_seconds() / 3600.0)


def in_force_hours(windows: Sequence[UnitWindow], start: pd.Timestamp, end: pd.Timestamp) -> float:
    """Hours inside [start, end) where at least one unit is in force (interval union)."""
    spans = []
    for w in windows:
        lo = start if w.start is None else max(w.start, start)
        hi = end if w.end is None else min(w.end, end)
        if hi > lo:
            spans.append((lo, hi))
    if not spans:
        return 0.0
    spans.sort()
    total = 0.0
    cur_lo, cur_hi = spans[0]
    for lo, hi in spans[1:]:
        if lo > cur_hi:
            total += (cur_hi - cur_lo).total_seconds() / 3600.0
            cur_lo, cur_hi = lo, hi
        else:
            cur_hi = max(cur_hi, hi)
    total += (cur_hi - cur_lo).total_seconds() / 3600.0
    return total


def capacity_hours(
    windows: Sequence[UnitWindow],
    start: DateLike,
    end: DateLike,
    nameplate: float,
    *,
    today: DateLike = None,
    windfarm_id: Optional[int] = None,
) -> float:
    """MW·h of capacity in force over [start, end) — the capacity-factor denominator.

    Exactly the integral of ``capacity_series`` over the window: anchored unit
    capacity where a unit is in force, nameplate where none is (``S(h) = 0``),
    constant nameplate when the registry is blocked or empty. Hours-weighted
    means are therefore consistent between the hourly and the period helpers.
    """
    s_ts = _as_utc(start)
    e_ts = _as_utc(end)
    window_h = max(0.0, (e_ts - s_ts).total_seconds() / 3600.0)
    if window_h == 0:
        return 0.0
    scale = None
    if windows and not registry_blocked(windfarm_id):
        scale = anchor_scale(windows, nameplate, today)
    if scale is None:
        return float(nameplate) * window_h
    raw = sum(w.capacity_mw * _overlap_hours(w.start, w.end, s_ts, e_ts) for w in windows)
    covered = in_force_hours(windows, s_ts, e_ts)
    return raw * scale + float(nameplate) * (window_h - covered)


def monthly_capacity(
    windows: Sequence[UnitWindow],
    months: Iterable[str],
    nameplate: float,
    *,
    today: DateLike = None,
    windfarm_id: Optional[int] = None,
) -> Dict[str, float]:
    """Hours-weighted average capacity in force per ``YYYY-MM`` month.

    A phase boundary inside a month is pro-rated by hours. Months are small
    enough that this never hits the calendar-window-average pitfall that a
    per-year average would (Midtfjellet 2012: 966 covered hours at 12.5 MW).
    """
    out: Dict[str, float] = {}
    for m in months:
        start = pd.Timestamp(f"{m}-01", tz="UTC")
        end = start + pd.offsets.MonthBegin(1)
        hours = (end - start).total_seconds() / 3600.0
        out[m] = (
            capacity_hours(windows, start, end, nameplate, today=today, windfarm_id=windfarm_id)
            / hours
        )
    return out


def weighted_capacity(caps: Mapping[str, float], hours: Mapping[str, float]) -> Optional[float]:
    """Σ cap×hours / Σ hours over the periods present in both mappings."""
    num = 0.0
    den = 0.0
    for k, h in hours.items():
        if k in caps and h:
            num += float(caps[k]) * float(h)
            den += float(h)
    return num / den if den > 0 else None
