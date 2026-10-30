"""SQL building blocks for the capacity-factor denominator in raw-SQL endpoints.

Same definition as ``app/utils/capacity.py`` (installed capacity in force from
``generation_units`` dates, anchored to nameplate with a 3 % tolerance, nameplate
where no unit is in force or the registry is blocked), expressed as CTEs so the
fleet-wide generation queries can divide by capacity-hours instead of
``nameplate × window hours``. Everything here is O(units); nothing joins
``generation_data`` per hour.

One documented divergence from the Python helper: the "hours with no unit in
force" term uses the span from the earliest unit start to the latest unit end
(not the exact interval union), so a farm with a *gap* between two phases is
treated as in force across the gap. No farm with performance data has such a gap.
"""

from __future__ import annotations

from app.utils.capacity import ANCHOR_TOLERANCE

TODAY_UTC = "(CURRENT_DATE::timestamp AT TIME ZONE 'UTC')"

# Unit windows as timestamptz (UTC midnight), the anchor date per farm and the
# summed capacity in force at that anchor. Append these to a WITH clause.
UNIT_CTES = f"""
unit_win AS (
    SELECT gu.windfarm_id,
           gu.capacity_mw::float8 AS capacity_mw,
           (COALESCE(gu.first_power_date, gu.start_date, DATE '1900-01-01')::timestamp
                AT TIME ZONE 'UTC') AS s,
           (COALESCE(gu.end_date, DATE '9999-12-31')::timestamp AT TIME ZONE 'UTC') AS e
    FROM generation_units gu
    WHERE gu.capacity_mw > 0
      AND (gu.is_active OR gu.end_date IS NOT NULL)
),
unit_anchor AS (
    SELECT uw.windfarm_id,
           CASE WHEN BOOL_OR(uw.s <= {TODAY_UTC} AND {TODAY_UTC} < uw.e) THEN {TODAY_UTC}
                ELSE MAX(uw.e) FILTER (WHERE uw.e <= {TODAY_UTC}) - interval '1 day'
           END AS anchor_ts
    FROM unit_win uw
    GROUP BY uw.windfarm_id
),
unit_today AS (
    SELECT uw.windfarm_id, SUM(uw.capacity_mw) AS s_anchor
    FROM unit_win uw
    JOIN unit_anchor ua ON ua.windfarm_id = uw.windfarm_id
    WHERE uw.s <= ua.anchor_ts AND ua.anchor_ts < uw.e
    GROUP BY uw.windfarm_id
)"""


def unit_overlap_lateral(
    start_expr: str, end_expr: str, windfarm_ref: str, alias: str = "uo"
) -> str:
    """LATERAL subquery giving raw capacity-hours and in-force hours of one farm
    over [start_expr, end_expr). Reference its columns as ``<alias>.raw_cap_hours``
    and ``<alias>.in_force_hours``."""
    return f"""
LEFT JOIN LATERAL (
    SELECT SUM(uw.capacity_mw * GREATEST(0, EXTRACT(EPOCH FROM
               (LEAST(uw.e, {end_expr}) - GREATEST(uw.s, {start_expr}))) / 3600.0)) AS raw_cap_hours,
           GREATEST(0, EXTRACT(EPOCH FROM
               (LEAST(MAX(uw.e), {end_expr}) - GREATEST(MIN(uw.s), {start_expr}))) / 3600.0)
               AS in_force_hours
    FROM unit_win uw
    WHERE uw.windfarm_id = {windfarm_ref}
) {alias} ON TRUE"""


def capacity_hours_expr(
    window_hours_expr: str,
    nameplate_expr: str = "wf.nameplate_capacity_mw",
    s_anchor_expr: str = "ut.s_anchor",
    overlap_alias: str = "uo",
) -> str:
    """Capacity-hours over one window for one farm row.

    ``window_hours_expr`` is the window length in hours (a bound ``:hours`` or a
    typed SQL expression — asyncpg cannot type ``:end - :start`` on its own).
    Needs ``unit_today`` joined as ``ut`` (or ``s_anchor_expr`` pointing at it)
    and ``unit_overlap_lateral`` joined as ``overlap_alias``.
    """
    window_h = f"({window_hours_expr})"
    return f"""CASE
    WHEN {s_anchor_expr} > 0
         AND ABS({s_anchor_expr} / {nameplate_expr} - 1) <= {ANCHOR_TOLERANCE}
    THEN COALESCE({overlap_alias}.raw_cap_hours, 0) * {nameplate_expr} / {s_anchor_expr}
         + {nameplate_expr} * GREATEST(0, {window_h} - COALESCE({overlap_alias}.in_force_hours, 0))
    ELSE {nameplate_expr} * {window_h}
END"""


def capacity_hours_window_sql(
    start_param: str = ":start_date", end_param: str = ":end_date", hours_param: str = ":hours"
) -> str:
    """Complete ``WITH ... SELECT windfarm_id, cap_hours FROM ...`` for every
    windfarm with a positive nameplate over one fixed window of ``hours_param`` hours."""
    return f"""WITH {UNIT_CTES}
SELECT wf.id AS windfarm_id,
       {capacity_hours_expr(hours_param)} AS cap_hours
FROM windfarms wf
LEFT JOIN unit_today ut ON ut.windfarm_id = wf.id
{unit_overlap_lateral(start_param, end_param, "wf.id")}
WHERE wf.nameplate_capacity_mw > 0"""


def capacity_hours_cte(
    start_param: str = ":start_date", end_param: str = ":end_date", hours_param: str = ":hours"
) -> str:
    """``unit_win, unit_anchor, unit_today, cap`` CTE bodies (no leading WITH) so a
    fleet query can ``LEFT JOIN cap ON cap.windfarm_id = wf.id`` and divide by
    ``cap.cap_hours`` instead of ``nameplate × :hours``."""
    return f"""{UNIT_CTES},
cap AS (
    SELECT wf.id AS windfarm_id,
           {capacity_hours_expr(hours_param)} AS cap_hours
    FROM windfarms wf
    LEFT JOIN unit_today ut ON ut.windfarm_id = wf.id
    {unit_overlap_lateral(start_param, end_param, "wf.id")}
    WHERE wf.nameplate_capacity_mw > 0
)"""
