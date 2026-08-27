"""Service for price analytics including capture rate calculations."""

import calendar
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional, Tuple

import structlog
from sqlalchemy import and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bidzone import Bidzone
from app.models.generation_data import GenerationData
from app.models.price_data import PriceData
from app.models.windfarm import Windfarm
from app.services.exchange_rate_service import ExchangeRateService

logger = structlog.get_logger()

AggregationType = Literal["hour", "day", "week", "month", "year"]

# price_data rows are stored in the source's native currency (EPR-93)
_SOURCE_CURRENCY = {"ELEXON": "GBP", "ENTSOE": "EUR"}

GB_BIDZONE_CODE = "10YGB----------A"


def _bucket_bounds(
    period_iso: str, aggregation: str, range_start: date, range_end: date
) -> Tuple[date, date]:
    """Date bounds of one DATE_TRUNC bucket, clamped to the query range.

    Only month/year buckets get their own FX window; finer aggregations
    fall back to the full range to cap rate lookups.
    """
    bucket_start = datetime.fromisoformat(period_iso).date()
    if aggregation == "month":
        last_day = calendar.monthrange(bucket_start.year, bucket_start.month)[1]
        bucket_end = bucket_start.replace(day=last_day)
    elif aggregation == "year":
        bucket_end = date(bucket_start.year, 12, 31)
    else:
        return range_start, range_end
    clamped_start = max(bucket_start, range_start)
    clamped_end = min(bucket_end, range_end)
    if clamped_start > clamped_end:
        # DATE_TRUNC buckets follow the DB session timezone and can spill just
        # outside the UTC query range at the edges; an inverted clamp would
        # make the FX AVG return no rows, so use the bucket's own bounds.
        return bucket_start, bucket_end
    return clamped_start, clamped_end


def apply_capture_rate_conversion(
    payload: Dict[str, Any], period_rates: List[Decimal], overall_rate: Decimal
) -> None:
    """Scale the monetary fields of a capture-rate payload in place.

    ``capture_rate`` ratios, MWh volumes and hour counts are currency-invariant
    and must not change. The overall revenue/achieved price are recomputed from
    the per-bucket-converted periods so the KPI total agrees with the chart.
    """
    total_revenue = 0.0
    for period, rate in zip(payload["periods"], period_rates):
        r = float(rate)
        period["revenue_eur"] = round(period["revenue_eur"] * r, 2)
        if period["achieved_price"] is not None:
            period["achieved_price"] = period["achieved_price"] * r
        if period["market_average_price"] is not None:
            period["market_average_price"] = period["market_average_price"] * r
        total_revenue += period["revenue_eur"]

    overall = payload["overall"]
    overall["total_revenue_eur"] = round(total_revenue, 2)
    if overall["total_generation_mwh"]:
        overall["achieved_price"] = total_revenue / overall["total_generation_mwh"]
    if overall["market_average_price"] is not None:
        overall["market_average_price"] = overall["market_average_price"] * float(overall_rate)


def apply_revenue_metrics_conversion(payload: Dict[str, Any], period_rates: List[Decimal]) -> None:
    """Scale the monetary fields of a revenue-metrics payload in place."""
    for period, rate in zip(payload["periods"], period_rates):
        r = float(rate)
        period["day_ahead_revenue_eur"] = round(period["day_ahead_revenue_eur"] * r, 2)
        period["total_revenue_eur"] = round(period["total_revenue_eur"] * r, 2)
        if period["avg_day_ahead_price"] is not None:
            period["avg_day_ahead_price"] = period["avg_day_ahead_price"] * r
        if period["avg_intraday_price"] is not None:
            period["avg_intraday_price"] = period["avg_intraday_price"] * r


def apply_compare_conversion(payload: Dict[str, Any], rate: Decimal) -> None:
    """Scale the monetary fields of a compare-capture-rates payload in place."""
    r = float(rate)
    for wf in payload["windfarms"]:
        if wf["achieved_price"] is not None:
            wf["achieved_price"] = wf["achieved_price"] * r
        if wf["market_average_price"] is not None:
            wf["market_average_price"] = wf["market_average_price"] * r
        wf["total_revenue_eur"] = round(wf["total_revenue_eur"] * r, 2)


# ── EPR-126: same-hours-both-sides capture rate ──────────────────────────────

MARKET_AVERAGE_BASIS = "observed_hours"


def observed_hours_sql(price_column: str, exclude_ramp_up: bool, farm_ref: str) -> str:
    """SQL for one farm's *observed* hours: every hour with a generation row
    (units summed, ramp-up excluded on request) joined to the farm's OWN price
    row for the chosen source and column.

    A capture rate divides a generation-weighted price by a time-weighted one,
    and both sides must be taken over the same hours. The old ``market_avg``
    CTE averaged every ``price_data`` row in the zone, which (a) leaked
    price-only hours into the denominator — months past a lagging generation
    feed (Norwegian NVE data runs ~8 months behind the price feed, at 4× the
    price), interior gaps — and (b) weighted each hour by the number of farms
    holding a price row (``price_data`` is per ``(hour, windfarm, source)``).

    ``farm_ref`` is ``':windfarm_id'`` for a single farm or ``'f.id'`` inside a
    ``LATERAL`` over ``windfarms f``. The hour range is repeated on ``p.hour`` so
    the planner takes one range scan on ``idx_price_windfarm_hour`` instead of
    per-row probes. Binds used: ``start_date``, ``end_date``, ``price_source``.
    """
    ramp = "AND g.is_ramp_up = false" if exclude_ramp_up else ""
    return f"""
        SELECT h.hour, h.net_mwh, p.{price_column} AS price
        FROM (
            SELECT g.hour,
                   SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) AS net_mwh
            FROM generation_data g
            WHERE g.windfarm_id = {farm_ref}
              AND g.hour >= :start_date
              AND g.hour < :end_date
              {ramp}
            GROUP BY g.hour
        ) h
        JOIN price_data p
          ON p.windfarm_id = {farm_ref}
         AND p.hour = h.hour
         AND p.source = :price_source
         AND p.hour >= :start_date
         AND p.hour < :end_date
        WHERE p.{price_column} IS NOT NULL
    """


# Aggregates over an observed-hours relation aliased ``o`` (hour, net_mwh, price).
# Zero/negative-output hours stay in the market average (that is what a capture
# rate measures — the farm was there and earned nothing); only generating hours
# carry revenue weight, exactly as the old numerator did.
CAPTURE_AGGREGATES_SQL = """
    COUNT(*)                                                           AS hours_observed,
    COUNT(*) FILTER (WHERE o.net_mwh > 0)                              AS hours_generating,
    AVG(o.price)                                                       AS market_average_price,
    COALESCE(SUM(o.net_mwh) FILTER (WHERE o.net_mwh > 0), 0)           AS total_generation_mwh,
    COALESCE(SUM(o.net_mwh * o.price) FILTER (WHERE o.net_mwh > 0), 0) AS revenue
"""


def _num(value: Any) -> Optional[float]:
    """``float(value)`` for a numeric DB value, ``None`` otherwise (NULL or a
    non-numeric stand-in from a test double)."""
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        return None
    return float(value)


def _count(value: Any) -> int:
    n = _num(value)
    return int(n) if n is not None else 0


def capture_from_totals(
    revenue: Any, generation: Any, market_average: Any
) -> Tuple[Optional[float], Optional[float]]:
    """``(achieved_price, capture_rate)`` with the guards the old SQL applied:
    no positive generation → no achieved price; no positive market average →
    no rate."""
    gen = _num(generation) or 0.0
    if gen <= 0:
        return None, None
    achieved = (_num(revenue) or 0.0) / gen
    market = _num(market_average)
    if market is None or market <= 0:
        return achieved, None
    return achieved, achieved / market


def rollup_capture_periods(periods: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Whole-window figures from per-period rows.

    Σ revenue / Σ generation and an hour-weighted mean of the period market
    averages reproduce exactly what aggregating the observed hours directly
    would give, so the old second round trip (``market_avg_query``) is gone.
    """
    total_gen = sum(p.get("total_generation_mwh") or 0.0 for p in periods)
    total_rev = sum(p.get("revenue_eur") or 0.0 for p in periods)
    hours = sum(int(p.get("hours_observed") or 0) for p in periods)
    gen_hours = sum(int(p.get("hours_generating") or 0) for p in periods)
    weighted = sum(
        (p["market_average_price"] or 0.0) * (p.get("hours_observed") or 0)
        for p in periods
        if p.get("market_average_price") is not None
    )
    market = weighted / hours if hours else None
    achieved, capture = capture_from_totals(total_rev, total_gen, market)
    return {
        "total_generation_mwh": total_gen,
        "total_revenue_eur": total_rev,
        "achieved_price": achieved,
        "market_average_price": market,
        "capture_rate": capture,
        "hours_observed": hours,
        "hours_generating": gen_hours,
    }


def coverage_pct(hours_observed: int, start: datetime, end: datetime) -> Optional[float]:
    """Observed hours as a % of the wall-clock window (``None`` for an empty window)."""
    total = (end - start).total_seconds() / 3600.0
    if total <= 0:
        return None
    return round(hours_observed / total * 100.0, 1)


def month_range(start: datetime, end: datetime) -> List[date]:
    """First-of-month dates for every calendar month touched by ``[start, end)``."""
    last = end - timedelta(microseconds=1)
    if last < start:
        return []
    months: List[date] = []
    y, m = start.year, start.month
    while (y, m) <= (last.year, last.month):
        months.append(date(y, m, 1))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


class PriceAnalyticsService:
    """Service for price analytics including capture rate calculations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def calculate_capture_rate(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        aggregation: AggregationType = "month",
        price_type: str = "day_ahead",
        exclude_ramp_up: bool = True,
        display_currency: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Calculate capture rate for a windfarm.

        Capture Rate = Achieved Price / Market Average Price
        - Achieved Price = Revenue / Total Generation (revenue-weighted average)
        - Market Average Price = simple time-weighted average of the farm's
          price over its OBSERVED hours — the hours it has a generation row
          for (EPR-126, ``observed_hours_sql``). Both sides cover the same
          hours, so price-only months past a lagging generation feed and
          interior gaps never inflate the denominator. ``overall.hours_observed``
          / ``coverage_pct`` say how much of the window the figures rest on and
          ``market_average_basis`` names the convention.

        Args:
            windfarm_id: Windfarm ID
            start_date: Start date for analysis
            end_date: End date for analysis
            aggregation: Time aggregation level (hour, day, week, month, year)
            price_type: Price type to use (day_ahead or intraday)
            display_currency: Convert monetary values to this currency (EPR-93).
                If a rate is unavailable the response stays in the native
                currency; the response-level ``currency`` field is authoritative.

        Returns:
            Dict with capture rate metrics by period. A period with observed
            but no generating hours is returned with ``capture_rate: None``.
        """
        price_column = "day_ahead_price" if price_type == "day_ahead" else "intraday_price"
        price_source = await self._get_preferred_price_source(windfarm_id)

        query = text(
            f"""
            WITH observed AS (
                {observed_hours_sql(price_column, exclude_ramp_up, ':windfarm_id')}
            )
            SELECT
                DATE_TRUNC(:aggregation, o.hour) AS period,
                {CAPTURE_AGGREGATES_SQL}
            FROM observed o
            GROUP BY DATE_TRUNC(:aggregation, o.hour)
            ORDER BY period
        """
        )

        result = await self.db.execute(
            query,
            {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date,
                "aggregation": aggregation,
                "price_source": price_source,
            },
        )
        rows = result.fetchall()

        # Get windfarm info
        windfarm = await self._get_windfarm(windfarm_id)

        periods = []
        for row in rows:
            achieved, capture = capture_from_totals(
                row.revenue, row.total_generation_mwh, row.market_average_price
            )
            hours_observed = _count(row.hours_observed)
            periods.append(
                {
                    "period": row.period.isoformat() if row.period else None,
                    "total_generation_mwh": _num(row.total_generation_mwh) or 0.0,
                    "revenue_eur": _num(row.revenue) or 0.0,
                    "achieved_price": achieved,
                    "market_average_price": _num(row.market_average_price),
                    # Historically the zone's price-row count for the bucket;
                    # now the farm's observed hours (nothing renders it).
                    "hours_in_period": hours_observed,
                    "hours_observed": hours_observed,
                    "hours_generating": _count(row.hours_generating),
                    "capture_rate": capture,
                }
            )

        overall = rollup_capture_periods(periods)
        overall["coverage_pct"] = coverage_pct(overall["hours_observed"], start_date, end_date)
        overall["market_average_basis"] = MARKET_AVERAGE_BASIS

        native_currency = _SOURCE_CURRENCY.get(price_source, "EUR")
        result_payload = {
            "windfarm_id": windfarm_id,
            "windfarm_name": windfarm.name if windfarm else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": aggregation,
            "price_type": price_type,
            # Historical field names say _eur; `currency` is authoritative (EPR-93)
            "native_currency": native_currency,
            "currency": native_currency,
            "display_currency": display_currency,
            "exchange_rate_used": None,
            "overall": overall,
            "periods": periods,
        }

        if display_currency and display_currency != native_currency:
            rates = await self._resolve_rates(
                native_currency,
                display_currency,
                periods,
                aggregation,
                start_date.date(),
                end_date.date(),
            )
            if rates is not None:
                period_rates, overall_rate = rates
                apply_capture_rate_conversion(result_payload, period_rates, overall_rate)
                result_payload["currency"] = display_currency
                result_payload["exchange_rate_used"] = float(overall_rate)

        return result_payload

    async def calculate_revenue_metrics(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        aggregation: AggregationType = "month",
        exclude_ramp_up: bool = True,
        display_currency: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Calculate revenue metrics for a windfarm.

        Args:
            windfarm_id: Windfarm ID
            start_date: Start date for analysis
            end_date: End date for analysis
            aggregation: Time aggregation level
            display_currency: Convert monetary values to this currency (EPR-93)

        Returns:
            Dict with revenue metrics by period
        """
        price_source = await self._get_preferred_price_source(windfarm_id)
        ramp_up_clause = "AND g.is_ramp_up = false" if exclude_ramp_up else ""

        query = text(
            f"""
            SELECT
                DATE_TRUNC(:aggregation, g.hour) as period,
                SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) as total_generation_mwh,
                SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * p.day_ahead_price) as day_ahead_revenue,
                SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * COALESCE(p.intraday_price, p.day_ahead_price)) as total_revenue,
                AVG(p.day_ahead_price) as avg_day_ahead_price,
                AVG(p.intraday_price) as avg_intraday_price,
                COUNT(DISTINCT g.hour) as hours_with_generation
            FROM generation_data g
            JOIN price_data p ON g.windfarm_id = p.windfarm_id AND g.hour = p.hour AND p.source = :price_source
            WHERE g.windfarm_id = :windfarm_id
              AND g.hour >= :start_date
              AND g.hour < :end_date
              AND (g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
              {ramp_up_clause}
            GROUP BY DATE_TRUNC(:aggregation, g.hour)
            ORDER BY period
        """
        )

        result = await self.db.execute(
            query,
            {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date,
                "aggregation": aggregation,
                "price_source": price_source,
            },
        )
        rows = result.fetchall()

        windfarm = await self._get_windfarm(windfarm_id)

        periods = []
        for row in rows:
            periods.append(
                {
                    "period": row.period.isoformat() if row.period else None,
                    "total_generation_mwh": float(row.total_generation_mwh)
                    if row.total_generation_mwh
                    else 0,
                    "day_ahead_revenue_eur": float(row.day_ahead_revenue)
                    if row.day_ahead_revenue
                    else 0,
                    "total_revenue_eur": float(row.total_revenue) if row.total_revenue else 0,
                    "avg_day_ahead_price": float(row.avg_day_ahead_price)
                    if row.avg_day_ahead_price
                    else None,
                    "avg_intraday_price": float(row.avg_intraday_price)
                    if row.avg_intraday_price
                    else None,
                    "hours_with_generation": row.hours_with_generation,
                }
            )

        native_currency = _SOURCE_CURRENCY.get(price_source, "EUR")
        result_payload = {
            "windfarm_id": windfarm_id,
            "windfarm_name": windfarm.name if windfarm else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": aggregation,
            "native_currency": native_currency,
            "currency": native_currency,
            "display_currency": display_currency,
            "exchange_rate_used": None,
            "periods": periods,
        }

        if display_currency and display_currency != native_currency:
            rates = await self._resolve_rates(
                native_currency,
                display_currency,
                periods,
                aggregation,
                start_date.date(),
                end_date.date(),
            )
            if rates is not None:
                period_rates, overall_rate = rates
                apply_revenue_metrics_conversion(result_payload, period_rates)
                result_payload["currency"] = display_currency
                result_payload["exchange_rate_used"] = float(overall_rate)

        return result_payload

    async def compare_capture_rates(
        self,
        windfarm_ids: List[int],
        start_date: datetime,
        end_date: datetime,
        aggregation: AggregationType = "month",
        exclude_ramp_up: bool = True,
    ) -> Dict[str, Any]:
        """
        Compare capture rates across multiple windfarms.

        Args:
            windfarm_ids: List of windfarm IDs to compare
            start_date: Start date for analysis
            end_date: End date for analysis
            aggregation: Time aggregation level

        Returns:
            Dict with capture rates for each windfarm
        """
        results = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": aggregation,
            "windfarms": [],
        }

        for windfarm_id in windfarm_ids:
            capture_data = await self.calculate_capture_rate(
                windfarm_id=windfarm_id,
                start_date=start_date,
                end_date=end_date,
                aggregation=aggregation,
                exclude_ramp_up=exclude_ramp_up,
            )

            results["windfarms"].append(
                {
                    "windfarm_id": windfarm_id,
                    "windfarm_name": capture_data.get("windfarm_name"),
                    "overall_capture_rate": capture_data["overall"]["capture_rate"],
                    "total_generation_mwh": capture_data["overall"]["total_generation_mwh"],
                    "total_revenue_eur": capture_data["overall"]["total_revenue_eur"],
                }
            )

        # Sort by capture rate descending
        results["windfarms"].sort(
            key=lambda x: x["overall_capture_rate"] if x["overall_capture_rate"] else 0,
            reverse=True,
        )

        return results

    async def get_price_profile(
        self,
        bidzone_id: int,
        start_date: datetime,
        end_date: datetime,
        aggregation: AggregationType = "hour",
    ) -> Dict[str, Any]:
        """
        Get price profile for a bidzone showing average prices by time of day.

        Args:
            bidzone_id: Bidzone ID
            start_date: Start date for analysis
            end_date: End date for analysis
            aggregation: Hour for hourly profile, day for daily

        Returns:
            Dict with price profile data
        """
        if aggregation == "hour":
            # Hourly profile - average by hour of day
            query = text(
                """
                SELECT
                    EXTRACT(HOUR FROM hour) as hour_of_day,
                    AVG(day_ahead_price) as avg_day_ahead,
                    MIN(day_ahead_price) as min_day_ahead,
                    MAX(day_ahead_price) as max_day_ahead,
                    STDDEV(day_ahead_price) as stddev_day_ahead,
                    COUNT(*) as sample_count
                FROM price_data
                WHERE bidzone_id = :bidzone_id
                  AND hour >= :start_date
                  AND hour < :end_date
                  AND day_ahead_price IS NOT NULL
                  AND source = CASE
                      WHEN :bidzone_id = (SELECT id FROM bidzones WHERE code = '10YGB----------A') THEN 'ELEXON'
                      ELSE 'ENTSOE'
                  END
                GROUP BY EXTRACT(HOUR FROM hour)
                ORDER BY hour_of_day
            """
            )
        else:
            # Daily profile
            query = text(
                """
                SELECT
                    EXTRACT(DOW FROM hour) as day_of_week,
                    AVG(day_ahead_price) as avg_day_ahead,
                    MIN(day_ahead_price) as min_day_ahead,
                    MAX(day_ahead_price) as max_day_ahead,
                    STDDEV(day_ahead_price) as stddev_day_ahead,
                    COUNT(*) as sample_count
                FROM price_data
                WHERE bidzone_id = :bidzone_id
                  AND hour >= :start_date
                  AND hour < :end_date
                  AND day_ahead_price IS NOT NULL
                  AND source = CASE
                      WHEN :bidzone_id = (SELECT id FROM bidzones WHERE code = '10YGB----------A') THEN 'ELEXON'
                      ELSE 'ENTSOE'
                  END
                GROUP BY EXTRACT(DOW FROM hour)
                ORDER BY day_of_week
            """
            )

        result = await self.db.execute(
            query,
            {
                "bidzone_id": bidzone_id,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        rows = result.fetchall()

        # Get bidzone info
        bidzone = await self._get_bidzone(bidzone_id)

        profile = []
        for row in rows:
            if aggregation == "hour":
                profile.append(
                    {
                        "hour_of_day": int(row.hour_of_day),
                        "avg_price": float(row.avg_day_ahead) if row.avg_day_ahead else None,
                        "min_price": float(row.min_day_ahead) if row.min_day_ahead else None,
                        "max_price": float(row.max_day_ahead) if row.max_day_ahead else None,
                        "stddev": float(row.stddev_day_ahead) if row.stddev_day_ahead else None,
                        "sample_count": row.sample_count,
                    }
                )
            else:
                day_names = [
                    "Sunday",
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                ]
                profile.append(
                    {
                        "day_of_week": int(row.day_of_week),
                        "day_name": day_names[int(row.day_of_week)],
                        "avg_price": float(row.avg_day_ahead) if row.avg_day_ahead else None,
                        "min_price": float(row.min_day_ahead) if row.min_day_ahead else None,
                        "max_price": float(row.max_day_ahead) if row.max_day_ahead else None,
                        "stddev": float(row.stddev_day_ahead) if row.stddev_day_ahead else None,
                        "sample_count": row.sample_count,
                    }
                )

        return {
            "bidzone_id": bidzone_id,
            "bidzone_code": bidzone.code if bidzone else None,
            "bidzone_name": bidzone.name if bidzone else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": aggregation,
            "profile": profile,
        }

    async def get_generation_price_correlation(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        exclude_ramp_up: bool = True,
    ) -> Dict[str, Any]:
        """
        Calculate correlation between generation and prices for a windfarm.

        This helps understand if the windfarm tends to generate more when
        prices are high (positive correlation) or low (negative correlation).
        """
        price_source = await self._get_preferred_price_source(windfarm_id)
        ramp_up_clause = "AND g.is_ramp_up = false" if exclude_ramp_up else ""

        query = text(
            f"""
            SELECT
                g.generation_mwh,
                p.day_ahead_price
            FROM generation_data g
            JOIN price_data p ON g.windfarm_id = p.windfarm_id AND g.hour = p.hour AND p.source = :price_source
            WHERE g.windfarm_id = :windfarm_id
              AND g.hour >= :start_date
              AND g.hour < :end_date
              AND g.generation_mwh IS NOT NULL
              AND p.day_ahead_price IS NOT NULL
              {ramp_up_clause}
        """
        )

        result = await self.db.execute(
            query,
            {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date,
                "price_source": price_source,
            },
        )
        rows = result.fetchall()

        if len(rows) < 2:
            windfarm = await self._get_windfarm(windfarm_id)
            return {
                "windfarm_id": windfarm_id,
                "windfarm_name": windfarm.name if windfarm else None,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "correlation": None,
                "sample_size": len(rows),
                "message": "Insufficient data for correlation calculation",
            }

        # Calculate Pearson correlation
        generations = [float(r.generation_mwh) for r in rows]
        prices = [float(r.day_ahead_price) for r in rows]

        n = len(generations)
        sum_gen = sum(generations)
        sum_price = sum(prices)
        sum_gen_sq = sum(g * g for g in generations)
        sum_price_sq = sum(p * p for p in prices)
        sum_gen_price = sum(g * p for g, p in zip(generations, prices))

        numerator = n * sum_gen_price - sum_gen * sum_price
        denominator = (n * sum_gen_sq - sum_gen**2) ** 0.5 * (
            n * sum_price_sq - sum_price**2
        ) ** 0.5

        correlation = numerator / denominator if denominator != 0 else 0

        windfarm = await self._get_windfarm(windfarm_id)

        return {
            "windfarm_id": windfarm_id,
            "windfarm_name": windfarm.name if windfarm else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "correlation": round(correlation, 4),
            "sample_size": n,
            "interpretation": self._interpret_correlation(correlation),
        }

    def _interpret_correlation(self, correlation: float) -> str:
        """Interpret correlation coefficient."""
        if correlation >= 0.7:
            return "Strong positive - generation tends to be high when prices are high"
        elif correlation >= 0.4:
            return "Moderate positive - some tendency for high generation during high prices"
        elif correlation >= 0.1:
            return "Weak positive - slight tendency for high generation during high prices"
        elif correlation >= -0.1:
            return "No correlation - generation and prices are independent"
        elif correlation >= -0.4:
            return "Weak negative - slight tendency for low generation during high prices"
        elif correlation >= -0.7:
            return "Moderate negative - generation tends to be low when prices are high"
        else:
            return "Strong negative - generation is typically low when prices are high"

    @staticmethod
    def compute_zone_average_capture_rate(
        windfarms: List[Dict[str, Any]],
    ) -> Optional[float]:
        """Generation-weighted mean capture rate across a bidzone's windfarms.

        Issue #94: the zone average that MKT-01 (low capture contracting) gates on.
        Each entry in ``windfarms`` must carry ``capture_rate`` and
        ``total_generation_mwh`` (the shape produced by
        :meth:`compare_capture_rates_by_bidzone`). The average is::

            sum(capture_rate_i * generation_i) / sum(generation_i)

        i.e. weighted by each farm's generation so a tiny farm with an extreme
        capture rate cannot skew the zone benchmark. Farms with a missing/None
        ``capture_rate`` are skipped (their generation is excluded from the
        denominator too, so they neither contribute to nor dilute the mean).

        Returns ``None`` when the (effective) total generation is 0 or the list is
        empty — there is no meaningful benchmark to compare against.
        """
        weighted_sum = 0.0
        total_generation = 0.0
        for wf in windfarms:
            capture_rate = wf.get("capture_rate")
            generation = wf.get("total_generation_mwh") or 0
            if capture_rate is None or generation <= 0:
                continue
            weighted_sum += capture_rate * generation
            total_generation += generation

        if total_generation <= 0:
            return None
        return weighted_sum / total_generation

    async def compare_capture_rates_by_bidzone(
        self,
        bidzone_id: int,
        start_date: datetime,
        end_date: datetime,
        exclude_ramp_up: bool = True,
        display_currency: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compare capture rates across all windfarms in a bidzone.

        EPR-126: one ``LATERAL`` pass per farm over its own generation × price
        rows. Each farm's market average is taken over ITS observed hours, so
        every peer is divided by a like-for-like denominator, and the
        zone-wide ``DISTINCT ON (hour)`` price scan (2.7M rows for GB) is gone.
        The fragment is the one ``calculate_capture_rate`` uses, so a farm's own
        rate and its entry in this list agree for the same window — what the
        MKT-01 ``gap_pp`` assumes. Farms with no generating hour in the window
        are omitted, as before.

        Args:
            bidzone_id: Bidzone ID
            start_date: Start date for analysis
            end_date: End date for analysis
            exclude_ramp_up: Whether to exclude ramp-up period records
            display_currency: Convert monetary values to this currency (EPR-93)

        Returns:
            Dict with bidzone info and per-windfarm capture rates
        """
        bidzone = await self._get_bidzone(bidzone_id)

        # Determine price source for this bidzone
        bidzone_code = bidzone.code if bidzone else None
        price_source = "ELEXON" if bidzone_code == GB_BIDZONE_CODE else "ENTSOE"

        query = text(
            f"""
            SELECT
                f.id AS windfarm_id,
                f.name AS windfarm_name,
                f.nameplate_capacity_mw AS capacity_mw,
                s.hours_observed,
                s.hours_generating,
                s.market_average_price,
                s.total_generation_mwh,
                s.revenue
            FROM windfarms f
            CROSS JOIN LATERAL (
                SELECT
                    {CAPTURE_AGGREGATES_SQL}
                FROM (
                    {observed_hours_sql('day_ahead_price', exclude_ramp_up, 'f.id')}
                ) o
            ) s
            WHERE f.bidzone_id = :bidzone_id
        """
        )

        result = await self.db.execute(
            query,
            {
                "bidzone_id": bidzone_id,
                "start_date": start_date,
                "end_date": end_date,
                "price_source": price_source,
            },
        )
        rows = result.fetchall()

        windfarms = []
        for row in rows:
            hours_observed = _count(row.hours_observed)
            hours_generating = _count(row.hours_generating)
            if hours_generating == 0:
                # No generating hour in the window — the old inner join dropped
                # these too, and the comparison chart draws a null as a 0 bar.
                continue
            achieved, capture = capture_from_totals(
                row.revenue, row.total_generation_mwh, row.market_average_price
            )
            windfarms.append(
                {
                    "windfarm_id": row.windfarm_id,
                    "windfarm_name": row.windfarm_name,
                    "capacity_mw": _num(row.capacity_mw),
                    "capture_rate": capture,
                    "achieved_price": achieved,
                    "market_average_price": _num(row.market_average_price),
                    "total_generation_mwh": _num(row.total_generation_mwh) or 0.0,
                    "total_revenue_eur": _num(row.revenue) or 0.0,
                    "hours_observed": hours_observed,
                    "hours_generating": hours_generating,
                    "coverage_pct": coverage_pct(hours_observed, start_date, end_date),
                }
            )
        # capture_rate DESC NULLS LAST
        windfarms.sort(key=lambda w: (w["capture_rate"] is None, -(w["capture_rate"] or 0.0)))

        native_currency = _SOURCE_CURRENCY.get(price_source, "EUR")
        result_payload = {
            "bidzone_id": bidzone_id,
            "bidzone_code": bidzone_code,
            "bidzone_name": bidzone.name if bidzone else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "native_currency": native_currency,
            "currency": native_currency,
            "display_currency": display_currency,
            "exchange_rate_used": None,
            "market_average_basis": MARKET_AVERAGE_BASIS,
            "windfarms": windfarms,
            # Issue #94 root-cause fix: this key was previously omitted, so
            # MKT-01's ``ctx.load_capture_rate()`` always read None and the
            # detector exited. Generation-weighted mean over the farms above.
            "zone_average_capture_rate": self.compute_zone_average_capture_rate(windfarms),
        }

        if display_currency and display_currency != native_currency:
            rate = await ExchangeRateService(self.db).get_rate_for_period(
                native_currency, display_currency, start_date.date(), end_date.date()
            )
            if rate is not None:
                apply_compare_conversion(result_payload, rate)
                result_payload["currency"] = display_currency
                result_payload["exchange_rate_used"] = float(rate)

        return result_payload

    async def zone_capture_rate_by_month(
        self,
        bidzone_id: int,
        start_date: datetime,
        end_date: datetime,
        exclude_ramp_up: bool = True,
    ) -> Dict[str, Any]:
        """
        Aggregate the bidzone-level capture rate one bucket per calendar month
        in [start_date, end_date]. Powers the radar/spider chart on the FE (#31).

        Returns a payload with one axis per calendar month in the window — each
        axis is the MWh-weighted capture rate across all windfarms in the zone,
        every farm's rate taken over its own observed hours (EPR-126, same
        fragment as ``compare_capture_rates_by_bidzone``). Months no farm was
        observed in keep their axis with null values so the spider keeps its
        shape.
        """
        bidzone = await self._get_bidzone(bidzone_id)
        bidzone_code = bidzone.code if bidzone else None
        price_source = "ELEXON" if bidzone_code == GB_BIDZONE_CODE else "ENTSOE"

        query = text(
            f"""
            SELECT
                f.id AS windfarm_id,
                s.month,
                s.hours_observed,
                s.hours_generating,
                s.market_average_price,
                s.total_generation_mwh,
                s.revenue
            FROM windfarms f
            CROSS JOIN LATERAL (
                SELECT
                    date_trunc('month', o.hour)::date AS month,
                    {CAPTURE_AGGREGATES_SQL}
                FROM (
                    {observed_hours_sql('day_ahead_price', exclude_ramp_up, 'f.id')}
                ) o
                GROUP BY date_trunc('month', o.hour)
            ) s
            WHERE f.bidzone_id = :bidzone_id
        """
        )

        result = await self.db.execute(
            query,
            {
                "bidzone_id": bidzone_id,
                "start_date": start_date,
                "end_date": end_date,
                "price_source": price_source,
            },
        )

        by_month: Dict[date, List[Any]] = {}
        for row in result.fetchall():
            month = row.month
            if isinstance(month, datetime):
                month = month.date()
            by_month.setdefault(month, []).append(row)

        month_labels = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]
        axes = []
        for month in month_range(start_date, end_date):
            farms = []
            hours = 0
            weighted_market = 0.0
            for row in by_month.get(month, []):
                achieved, capture = capture_from_totals(
                    row.revenue, row.total_generation_mwh, row.market_average_price
                )
                farm_hours = _count(row.hours_observed)
                market = _num(row.market_average_price)
                if market is not None:
                    hours += farm_hours
                    weighted_market += market * farm_hours
                farms.append(
                    {
                        "capture_rate": capture,
                        "total_generation_mwh": _num(row.total_generation_mwh) or 0.0,
                        "revenue": _num(row.revenue) or 0.0,
                    }
                )
            total_generation = sum(f["total_generation_mwh"] for f in farms)
            total_revenue = sum(f["revenue"] for f in farms)
            axes.append(
                {
                    "month": month.month,
                    "year": month.year,
                    "label": month_labels[month.month - 1],
                    "capture_rate": self.compute_zone_average_capture_rate(farms),
                    "achieved_price": (
                        total_revenue / total_generation if total_generation > 0 else None
                    ),
                    "market_average_price": weighted_market / hours if hours else None,
                    "total_generation_mwh": total_generation,
                    "total_revenue": total_revenue,
                    "windfarm_count": sum(1 for f in farms if f["total_generation_mwh"] > 0),
                }
            )

        return {
            "bidzone_id": bidzone_id,
            "bidzone_code": bidzone_code,
            "bidzone_name": bidzone.name if bidzone else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "market_average_basis": MARKET_AVERAGE_BASIS,
            "axes": axes,
        }

    async def negative_price_exposure(
        self,
        windfarm_id: int,
        start: datetime,
        end: datetime,
    ) -> Dict[str, int]:
        """``{"negative_hours", "observed_hours"}`` over ``[start, end)``, one pass.

        ``negative_hours`` is the MKT-06 count — hours the farm generates
        (``net_generation > 0``) at a negative day-ahead price. ``observed_hours``
        is every hour with both a generation row and a price row: the days the
        farm was actually observed, which is what a per-year rate must be
        annualised over (EPR-126) rather than the wall-clock window.

        Same join shape as the capture-rate queries — the farm's preferred price
        source per ``(windfarm_id, hour, source)``, ``COUNT(DISTINCT g.hour)`` so
        multi-unit farms count each clock-hour once. Counts are never ``None``.
        """
        price_source = await self._get_preferred_price_source(windfarm_id)

        query = text(
            """
            SELECT
                COUNT(DISTINCT g.hour) FILTER (
                    WHERE p.day_ahead_price < 0
                      AND (g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
                ) AS negative_hours,
                COUNT(DISTINCT g.hour) AS observed_hours
            FROM generation_data g
            JOIN price_data p
                ON g.windfarm_id = p.windfarm_id
               AND g.hour = p.hour
               AND p.source = :price_source
            WHERE g.windfarm_id = :windfarm_id
              AND g.hour >= :start
              AND g.hour < :end
              AND p.day_ahead_price IS NOT NULL
        """
        )

        result = await self.db.execute(
            query,
            {
                "windfarm_id": windfarm_id,
                "start": start,
                "end": end,
                "price_source": price_source,
            },
        )
        row = result.fetchone()
        if row is None:
            return {"negative_hours": 0, "observed_hours": 0}
        return {
            "negative_hours": _count(row.negative_hours),
            "observed_hours": _count(row.observed_hours),
        }

    async def count_negative_price_hours(
        self,
        windfarm_id: int,
        start: datetime,
        end: datetime,
    ) -> int:
        """Count hours in ``[start, end)`` where the farm generates at a negative price.

        Powers MKT-06 (negative-price-hours exposure, issue #105). A negative
        day-ahead price is only a *commercial* problem when the asset is actually
        producing into it — when the turbine is curtailed/idle there is no
        curtailment-avoided exposure, so non-generating hours are explicitly
        EXCLUDED. The count is therefore::

            COUNT(DISTINCT g.hour)
            WHERE net_generation > 0  AND  p.day_ahead_price < 0

        Thin wrapper over :meth:`negative_price_exposure`, which also returns
        the observed-hours denominator. Returns ``0`` (never ``None``).
        """
        exposure = await self.negative_price_exposure(windfarm_id, start, end)
        return exposure["negative_hours"]

    async def _resolve_rates(
        self,
        native_currency: str,
        display_currency: str,
        periods: List[Dict[str, Any]],
        aggregation: str,
        range_start: date,
        range_end: date,
    ) -> Optional[Tuple[List[Decimal], Decimal]]:
        """One FX rate per period bucket plus one full-range rate, memoised.

        Returns None if any lookup has no data — callers then leave the whole
        payload in the native currency (all-or-nothing, unlike the financials
        per-period fallback; ECB daily coverage makes this path theoretical).
        """
        svc = ExchangeRateService(self.db)
        cache: Dict[Tuple[date, date], Optional[Decimal]] = {}

        async def rate_for(start: date, end: date) -> Optional[Decimal]:
            key = (start, end)
            if key not in cache:
                cache[key] = await svc.get_rate_for_period(
                    native_currency, display_currency, start, end
                )
            return cache[key]

        overall_rate = await rate_for(range_start, range_end)
        if overall_rate is None:
            return None

        period_rates: List[Decimal] = []
        for period in periods:
            if period.get("period"):
                start, end = _bucket_bounds(period["period"], aggregation, range_start, range_end)
            else:
                start, end = range_start, range_end
            rate = await rate_for(start, end)
            if rate is None:
                return None
            period_rates.append(rate)

        return period_rates, overall_rate

    async def _get_preferred_price_source(self, windfarm_id: int) -> str:
        """Resolve preferred price source: ELEXON for GB windfarms, ENTSOE for all others."""
        query = text(
            """
            SELECT CASE WHEN b.code = '10YGB----------A' THEN 'ELEXON' ELSE 'ENTSOE' END as source
            FROM windfarms w
            JOIN bidzones b ON w.bidzone_id = b.id
            WHERE w.id = :windfarm_id
        """
        )
        result = await self.db.execute(query, {"windfarm_id": windfarm_id})
        row = result.fetchone()
        return row.source if row else "ENTSOE"

    async def _get_windfarm(self, windfarm_id: int) -> Optional[Windfarm]:
        """Get windfarm by ID."""
        stmt = select(Windfarm).where(Windfarm.id == windfarm_id)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()

    async def _get_bidzone(self, bidzone_id: int) -> Optional[Bidzone]:
        """Get bidzone by ID."""
        stmt = select(Bidzone).where(Bidzone.id == bidzone_id)
        result = await self.db.execute(stmt)
        return result.scalar_one_or_none()
