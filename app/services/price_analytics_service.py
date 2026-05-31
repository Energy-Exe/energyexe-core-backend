"""Service for price analytics including capture rate calculations."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional

import structlog
from sqlalchemy import and_, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.bidzone import Bidzone
from app.models.generation_data import GenerationData
from app.models.price_data import PriceData
from app.models.windfarm import Windfarm

logger = structlog.get_logger()

AggregationType = Literal["hour", "day", "week", "month", "year"]


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
    ) -> Dict[str, Any]:
        """
        Calculate capture rate for a windfarm.

        Capture Rate = Achieved Price / Market Average Price
        - Achieved Price = Revenue / Total Generation (revenue-weighted average)
        - Market Average Price = Simple time-weighted average of market prices

        Args:
            windfarm_id: Windfarm ID
            start_date: Start date for analysis
            end_date: End date for analysis
            aggregation: Time aggregation level (hour, day, week, month, year)
            price_type: Price type to use (day_ahead or intraday)

        Returns:
            Dict with capture rate metrics by period
        """
        price_column = "day_ahead_price" if price_type == "day_ahead" else "intraday_price"
        price_source = await self._get_preferred_price_source(windfarm_id)
        ramp_up_clause = "AND g.is_ramp_up = false" if exclude_ramp_up else ""

        # SQL query for capture rate calculation
        query = text(
            f"""
            WITH windfarm_metrics AS (
                SELECT
                    DATE_TRUNC(:aggregation, g.hour) as period,
                    SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) as total_generation_mwh,
                    SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * p.{price_column}) as revenue_eur,
                    CASE
                        WHEN SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
                        THEN SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * p.{price_column}) / SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0))
                        ELSE NULL
                    END as achieved_price
                FROM generation_data g
                JOIN price_data p ON g.windfarm_id = p.windfarm_id AND g.hour = p.hour AND p.source = :price_source
                WHERE g.windfarm_id = :windfarm_id
                  AND g.hour >= :start_date
                  AND g.hour < :end_date
                  AND p.{price_column} IS NOT NULL
                  AND (g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
                  {ramp_up_clause}
                GROUP BY DATE_TRUNC(:aggregation, g.hour)
            ),
            market_metrics AS (
                SELECT
                    DATE_TRUNC(:aggregation, p.hour) as period,
                    AVG(p.{price_column}) as market_average_price,
                    COUNT(*) as hours_in_period
                FROM price_data p
                WHERE p.bidzone_id = (SELECT bidzone_id FROM windfarms WHERE id = :windfarm_id)
                  AND p.hour >= :start_date
                  AND p.hour < :end_date
                  AND p.{price_column} IS NOT NULL
                  AND p.source = :price_source
                GROUP BY DATE_TRUNC(:aggregation, p.hour)
            )
            SELECT
                w.period,
                w.total_generation_mwh,
                w.revenue_eur,
                w.achieved_price,
                m.market_average_price,
                m.hours_in_period,
                CASE
                    WHEN m.market_average_price > 0 AND w.achieved_price IS NOT NULL
                    THEN w.achieved_price / m.market_average_price
                    ELSE NULL
                END as capture_rate
            FROM windfarm_metrics w
            JOIN market_metrics m ON w.period = m.period
            ORDER BY w.period
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
        total_generation = Decimal("0")
        total_revenue = Decimal("0")

        for row in rows:
            period_data = {
                "period": row.period.isoformat() if row.period else None,
                "total_generation_mwh": float(row.total_generation_mwh)
                if row.total_generation_mwh
                else 0,
                "revenue_eur": float(row.revenue_eur) if row.revenue_eur else 0,
                "achieved_price": float(row.achieved_price) if row.achieved_price else None,
                "market_average_price": float(row.market_average_price)
                if row.market_average_price
                else None,
                "hours_in_period": row.hours_in_period,
                "capture_rate": float(row.capture_rate) if row.capture_rate else None,
            }
            periods.append(period_data)

            if row.total_generation_mwh:
                total_generation += Decimal(str(row.total_generation_mwh))
            if row.revenue_eur:
                total_revenue += Decimal(str(row.revenue_eur))

        # Calculate overall metrics
        overall_achieved_price = (
            float(total_revenue / total_generation) if total_generation > 0 else None
        )

        # Get overall market average
        market_avg_query = text(
            f"""
            SELECT AVG({price_column}) as market_average
            FROM price_data
            WHERE bidzone_id = (SELECT bidzone_id FROM windfarms WHERE id = :windfarm_id)
              AND hour >= :start_date
              AND hour < :end_date
              AND {price_column} IS NOT NULL
              AND source = :price_source
        """
        )
        market_avg_result = await self.db.execute(
            market_avg_query,
            {
                "windfarm_id": windfarm_id,
                "start_date": start_date,
                "end_date": end_date,
                "price_source": price_source,
            },
        )
        market_avg_row = market_avg_result.fetchone()
        overall_market_average = (
            float(market_avg_row.market_average)
            if market_avg_row and market_avg_row.market_average
            else None
        )

        overall_capture_rate = (
            overall_achieved_price / overall_market_average
            if overall_achieved_price and overall_market_average
            else None
        )

        return {
            "windfarm_id": windfarm_id,
            "windfarm_name": windfarm.name if windfarm else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": aggregation,
            "price_type": price_type,
            "overall": {
                "total_generation_mwh": float(total_generation),
                "total_revenue_eur": float(total_revenue),
                "achieved_price": overall_achieved_price,
                "market_average_price": overall_market_average,
                "capture_rate": overall_capture_rate,
            },
            "periods": periods,
        }

    async def calculate_revenue_metrics(
        self,
        windfarm_id: int,
        start_date: datetime,
        end_date: datetime,
        aggregation: AggregationType = "month",
        exclude_ramp_up: bool = True,
    ) -> Dict[str, Any]:
        """
        Calculate revenue metrics for a windfarm.

        Args:
            windfarm_id: Windfarm ID
            start_date: Start date for analysis
            end_date: End date for analysis
            aggregation: Time aggregation level

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

        return {
            "windfarm_id": windfarm_id,
            "windfarm_name": windfarm.name if windfarm else None,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "aggregation": aggregation,
            "periods": periods,
        }

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
    ) -> Dict[str, Any]:
        """
        Compare capture rates across all windfarms in a bidzone.

        Args:
            bidzone_id: Bidzone ID
            start_date: Start date for analysis
            end_date: End date for analysis
            exclude_ramp_up: Whether to exclude ramp-up period records

        Returns:
            Dict with bidzone info and per-windfarm capture rates
        """
        bidzone = await self._get_bidzone(bidzone_id)

        # Determine price source for this bidzone
        bidzone_code = bidzone.code if bidzone else None
        price_source = "ELEXON" if bidzone_code == "10YGB----------A" else "ENTSOE"

        ramp_up_clause = "AND g.is_ramp_up = false" if exclude_ramp_up else ""

        # ELEXON stores one price row per (hour, windfarm) — averaging
        # without DISTINCT ON would weight each hour by the number of
        # windfarms in the zone (155x for GB), making the AVG slow on
        # large zones.  ENTSOE prices already have a single row per hour
        # so DISTINCT ON is a no-op there.
        query = text(
            f"""
            WITH market_avg AS (
                SELECT AVG(day_ahead_price) as market_average_price
                FROM (
                    SELECT DISTINCT ON (hour) day_ahead_price
                    FROM price_data
                    WHERE bidzone_id = :bidzone_id
                      AND hour >= :start_date
                      AND hour < :end_date
                      AND day_ahead_price IS NOT NULL
                      AND source = :price_source
                    ORDER BY hour, day_ahead_price
                ) x
            )
            SELECT
                g.windfarm_id,
                w.name as windfarm_name,
                w.nameplate_capacity_mw as capacity_mw,
                SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) as total_generation_mwh,
                SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * p.day_ahead_price) as total_revenue_eur,
                CASE
                    WHEN SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
                    THEN SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * p.day_ahead_price)
                         / SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0))
                    ELSE NULL
                END as achieved_price,
                ma.market_average_price,
                CASE
                    WHEN SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0 AND ma.market_average_price > 0
                    THEN (SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * p.day_ahead_price)
                         / SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)))
                         / ma.market_average_price
                    ELSE NULL
                END as capture_rate
            FROM generation_data g
            JOIN price_data p ON g.windfarm_id = p.windfarm_id AND g.hour = p.hour AND p.source = :price_source
            JOIN windfarms w ON g.windfarm_id = w.id
            CROSS JOIN market_avg ma
            WHERE w.bidzone_id = :bidzone_id
              AND g.hour >= :start_date
              AND g.hour < :end_date
              AND (g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
              AND p.day_ahead_price IS NOT NULL
              {ramp_up_clause}
            GROUP BY g.windfarm_id, w.name, w.nameplate_capacity_mw, ma.market_average_price
            ORDER BY capture_rate DESC NULLS LAST
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
            windfarms.append(
                {
                    "windfarm_id": row.windfarm_id,
                    "windfarm_name": row.windfarm_name,
                    "capacity_mw": float(row.capacity_mw) if row.capacity_mw else None,
                    "capture_rate": float(row.capture_rate) if row.capture_rate else None,
                    "achieved_price": float(row.achieved_price) if row.achieved_price else None,
                    "market_average_price": float(row.market_average_price)
                    if row.market_average_price
                    else None,
                    "total_generation_mwh": float(row.total_generation_mwh)
                    if row.total_generation_mwh
                    else 0,
                    "total_revenue_eur": float(row.total_revenue_eur)
                    if row.total_revenue_eur
                    else 0,
                }
            )

        return {
            "bidzone_id": bidzone_id,
            "bidzone_code": bidzone_code,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "windfarms": windfarms,
            # Issue #94 root-cause fix: this key was previously omitted, so
            # MKT-01's ``ctx.load_capture_rate()`` always read None and the
            # detector exited. Generation-weighted mean over the farms above.
            "zone_average_capture_rate": self.compute_zone_average_capture_rate(windfarms),
        }

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

        Returns a payload with 12 axes (one per month in window) — each axis is
        the MWh-weighted capture rate across all windfarms in the zone.
        """
        bidzone = await self._get_bidzone(bidzone_id)
        bidzone_code = bidzone.code if bidzone else None
        price_source = "ELEXON" if bidzone_code == "10YGB----------A" else "ENTSOE"

        ramp_up_clause = "AND g.is_ramp_up = false" if exclude_ramp_up else ""

        # Per-month market average (DISTINCT ON hour to avoid 155x overcounting on
        # ELEXON where price_data has one row per (hour, windfarm)).
        query = text(
            f"""
            WITH market_avg AS (
                SELECT
                    date_trunc('month', hour)::date AS month,
                    AVG(day_ahead_price) AS market_average_price
                FROM (
                    SELECT DISTINCT ON (hour) hour, day_ahead_price
                    FROM price_data
                    WHERE bidzone_id = :bidzone_id
                      AND hour >= :start_date
                      AND hour < :end_date
                      AND day_ahead_price IS NOT NULL
                      AND source = :price_source
                    ORDER BY hour, day_ahead_price
                ) x
                GROUP BY date_trunc('month', hour)
            ),
            zone_revenue AS (
                SELECT
                    date_trunc('month', g.hour)::date AS month,
                    SUM(g.generation_mwh - COALESCE(g.consumption_mwh, 0)) AS total_generation_mwh,
                    SUM((g.generation_mwh - COALESCE(g.consumption_mwh, 0)) * p.day_ahead_price)
                        AS total_revenue,
                    COUNT(DISTINCT g.windfarm_id) AS windfarm_count
                FROM generation_data g
                JOIN price_data p
                    ON g.windfarm_id = p.windfarm_id
                   AND g.hour = p.hour
                   AND p.source = :price_source
                JOIN windfarms w ON g.windfarm_id = w.id
                WHERE w.bidzone_id = :bidzone_id
                  AND g.hour >= :start_date
                  AND g.hour < :end_date
                  AND (g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
                  AND p.day_ahead_price IS NOT NULL
                  {ramp_up_clause}
                GROUP BY date_trunc('month', g.hour)
            )
            SELECT
                ma.month,
                ma.market_average_price,
                COALESCE(zr.total_generation_mwh, 0) AS total_generation_mwh,
                COALESCE(zr.total_revenue, 0) AS total_revenue,
                COALESCE(zr.windfarm_count, 0) AS windfarm_count,
                CASE
                    WHEN COALESCE(zr.total_generation_mwh, 0) > 0
                     AND ma.market_average_price > 0
                    THEN (zr.total_revenue / zr.total_generation_mwh) / ma.market_average_price
                    ELSE NULL
                END AS capture_rate,
                CASE
                    WHEN COALESCE(zr.total_generation_mwh, 0) > 0
                    THEN zr.total_revenue / zr.total_generation_mwh
                    ELSE NULL
                END AS achieved_price
            FROM market_avg ma
            LEFT JOIN zone_revenue zr ON zr.month = ma.month
            ORDER BY ma.month
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
        for row in result.fetchall():
            month_dt = row.month
            axes.append(
                {
                    "month": month_dt.month,
                    "year": month_dt.year,
                    "label": month_labels[month_dt.month - 1],
                    "capture_rate": float(row.capture_rate)
                    if row.capture_rate is not None
                    else None,
                    "achieved_price": float(row.achieved_price)
                    if row.achieved_price is not None
                    else None,
                    "market_average_price": float(row.market_average_price)
                    if row.market_average_price is not None
                    else None,
                    "total_generation_mwh": float(row.total_generation_mwh or 0),
                    "total_revenue": float(row.total_revenue or 0),
                    "windfarm_count": int(row.windfarm_count or 0),
                }
            )

        return {
            "bidzone_id": bidzone_id,
            "bidzone_code": bidzone_code,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "axes": axes,
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

        where ``net_generation = generation_mwh - COALESCE(consumption_mwh, 0)``
        (matching the net-of-consumption convention used by the capture-rate
        queries, so French units with both directions are not double-counted).

        The price is joined per ``(windfarm_id, hour, source)`` using the farm's
        preferred price source (ELEXON for GB, ENTSOE otherwise) — the same join
        shape as ``calculate_capture_rate`` — and ``COUNT(DISTINCT g.hour)`` so a
        windfarm with multiple generation-unit rows per hour still counts each
        clock-hour once.

        Returns ``0`` (never ``None``) when no qualifying hours exist or no data
        is reachable, so callers can treat the result as a plain count.
        """
        price_source = await self._get_preferred_price_source(windfarm_id)

        query = text(
            """
            SELECT COUNT(DISTINCT g.hour) AS negative_hours
            FROM generation_data g
            JOIN price_data p
                ON g.windfarm_id = p.windfarm_id
               AND g.hour = p.hour
               AND p.source = :price_source
            WHERE g.windfarm_id = :windfarm_id
              AND g.hour >= :start
              AND g.hour < :end
              AND p.day_ahead_price IS NOT NULL
              AND p.day_ahead_price < 0
              AND (g.generation_mwh - COALESCE(g.consumption_mwh, 0)) > 0
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
        if row is None or row.negative_hours is None:
            return 0
        return int(row.negative_hours)

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
