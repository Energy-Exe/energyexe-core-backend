"""EPR-126 Part B — same-hours-both-sides capture rate.

The market-average denominator is taken over the farm's OBSERVED hours (hours
with a generation row joined to the farm's own price row), never over every
price row in the zone. Pure helpers are tested directly; the service methods
are tested for SQL shape and payload arithmetic against a canned session
(no Postgres in the test environment).
"""

from datetime import date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services.price_analytics_service import (
    CAPTURE_AGGREGATES_SQL,
    MARKET_AVERAGE_BASIS,
    PriceAnalyticsService,
    capture_from_totals,
    coverage_pct,
    month_range,
    observed_hours_sql,
    rollup_capture_periods,
)


def _norm(sql) -> str:
    return " ".join(str(sql).split())


def _row(**fields):
    row = MagicMock()
    for key, value in fields.items():
        setattr(row, key, value)
    return row


def _result(rows=None, one=None):
    res = MagicMock()
    res.fetchall.return_value = rows or []
    res.fetchone.return_value = one
    return res


def _lookup(obj):
    """A ``select(Model)`` result — ``_get_windfarm`` / ``_get_bidzone``."""
    res = MagicMock()
    res.scalar_one_or_none.return_value = obj
    return res


def _service(results):
    db = MagicMock()
    db.execute = AsyncMock(side_effect=results)
    return PriceAnalyticsService(db), db


# ── pure helpers ───────────────────────────────────────────────────────────


class TestPureHelpers:
    def test_capture_from_totals_guards(self):
        assert capture_from_totals(0, 0, 20) == (None, None)
        assert capture_from_totals(Decimal("100"), Decimal("10"), None) == (10.0, None)
        assert capture_from_totals(100, 10, 0) == (10.0, None)
        achieved, capture = capture_from_totals(Decimal("155.6"), Decimal("10"), Decimal("19.61"))
        assert achieved == pytest.approx(15.56)
        assert capture == pytest.approx(0.7935, abs=1e-4)  # Smøla, aligned hours

    def test_rollup_is_hour_weighted_not_period_averaged(self):
        periods = [
            {
                "total_generation_mwh": 100.0,
                "revenue_eur": 1000.0,
                "hours_observed": 100,
                "hours_generating": 80,
                "market_average_price": 10.0,
            },
            {
                "total_generation_mwh": 300.0,
                "revenue_eur": 6000.0,
                "hours_observed": 300,
                "hours_generating": 250,
                "market_average_price": 20.0,
            },
        ]
        overall = rollup_capture_periods(periods)
        assert overall["market_average_price"] == pytest.approx(17.5)  # not 15
        assert overall["achieved_price"] == pytest.approx(7000 / 400)
        assert overall["capture_rate"] == pytest.approx(17.5 / 17.5)
        assert overall["hours_observed"] == 400 and overall["hours_generating"] == 330
        assert overall["total_generation_mwh"] == 400.0
        assert overall["total_revenue_eur"] == 7000.0

        empty = rollup_capture_periods([])
        assert empty["capture_rate"] is None
        assert empty["market_average_price"] is None
        assert empty["hours_observed"] == 0

    def test_coverage_pct(self):
        # 2024-09-05 → 2026-01-01 = 483 days = 11,592 h
        assert coverage_pct(11592, datetime(2024, 9, 5), datetime(2026, 1, 1)) == 100.0
        assert coverage_pct(5796, datetime(2024, 9, 5), datetime(2026, 1, 1)) == 50.0
        assert coverage_pct(0, datetime(2025, 1, 1), datetime(2025, 1, 1)) is None

    def test_month_range(self):
        assert month_range(datetime(2025, 1, 1), datetime(2026, 1, 1)) == [
            date(2025, m, 1) for m in range(1, 13)
        ]
        assert month_range(datetime(2025, 11, 15), datetime(2026, 2, 1)) == [
            date(2025, 11, 1),
            date(2025, 12, 1),
            date(2026, 1, 1),
        ]
        assert month_range(datetime(2025, 1, 1), datetime(2025, 1, 1)) == []

    def test_observed_hours_sql_shape(self):
        sql = _norm(observed_hours_sql("day_ahead_price", True, ":windfarm_id"))
        assert "GROUP BY g.hour" in sql
        assert "p.source = :price_source" in sql
        assert "AND g.is_ramp_up = false" in sql
        assert "g.windfarm_id = :windfarm_id" in sql and "p.windfarm_id = :windfarm_id" in sql
        # Range repeated on the price side → index range scan, not per-row probes.
        assert "p.hour >= :start_date" in sql and "p.hour < :end_date" in sql
        assert "bidzone" not in sql

        lateral = _norm(observed_hours_sql("day_ahead_price", False, "f.id"))
        assert "is_ramp_up" not in lateral
        assert "g.windfarm_id = f.id" in lateral and "p.windfarm_id = f.id" in lateral

        agg = _norm(CAPTURE_AGGREGATES_SQL)
        assert "COUNT(*) AS hours_observed" in agg
        assert "AVG(o.price) AS market_average_price" in agg
        assert "FILTER (WHERE o.net_mwh > 0)" in agg


# ── calculate_capture_rate ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_calculate_capture_rate_single_statement_with_aligned_denominator():
    rows = [
        _row(
            period=datetime(2024, 1, 1),
            hours_observed=8784,
            hours_generating=8000,
            market_average_price=Decimal("40"),
            total_generation_mwh=Decimal("100000"),
            revenue=Decimal("3200000"),
        ),
        _row(
            period=datetime(2025, 1, 1),
            hours_observed=8760,
            hours_generating=8500,
            market_average_price=Decimal("20"),
            total_generation_mwh=Decimal("120000"),
            revenue=Decimal("1920000"),
        ),
    ]
    service, db = _service([_result(one=_row(source="ENTSOE")), _result(rows=rows), _lookup(None)])

    payload = await service.calculate_capture_rate(
        7213, datetime(2024, 1, 1), datetime(2026, 1, 1), aggregation="year"
    )

    # source lookup, ONE analytics statement, windfarm lookup — no market_avg_query.
    assert db.execute.await_count == 3
    stmt, params = db.execute.call_args_list[1].args
    sql = _norm(stmt)
    assert "WITH observed AS" in sql and "AVG(o.price)" in sql
    assert "DATE_TRUNC(:aggregation, o.hour)" in sql
    assert "bidzone_id" not in sql and "DISTINCT ON" not in sql
    assert params == {
        "windfarm_id": 7213,
        "start_date": datetime(2024, 1, 1),
        "end_date": datetime(2026, 1, 1),
        "aggregation": "year",
        "price_source": "ENTSOE",
    }

    p24, p25 = payload["periods"]
    assert p24["capture_rate"] == pytest.approx(32 / 40)
    assert p25["capture_rate"] == pytest.approx(16 / 20)
    assert p24["hours_in_period"] == 8784 and p24["hours_observed"] == 8784
    assert p24["hours_generating"] == 8000
    assert p24["revenue_eur"] == 3200000.0 and p24["total_generation_mwh"] == 100000.0

    overall = payload["overall"]
    assert overall["hours_observed"] == 17544 and overall["hours_generating"] == 16500
    assert overall["market_average_price"] == pytest.approx((40 * 8784 + 20 * 8760) / 17544)
    assert overall["achieved_price"] == pytest.approx(5120000 / 220000)
    assert overall["capture_rate"] == pytest.approx(
        overall["achieved_price"] / overall["market_average_price"]
    )
    assert overall["total_revenue_eur"] == 5120000.0
    assert overall["market_average_basis"] == MARKET_AVERAGE_BASIS == "observed_hours"
    assert overall["coverage_pct"] == 100.0  # 731 days = 17,544 h


@pytest.mark.asyncio
async def test_calculate_capture_rate_period_without_generation_keeps_null_rate():
    rows = [
        _row(
            period=datetime(2026, 1, 1),
            hours_observed=500,
            hours_generating=0,
            market_average_price=Decimal("80"),
            total_generation_mwh=0,
            revenue=0,
        )
    ]
    service, _ = _service([_result(one=_row(source="ENTSOE")), _result(rows=rows), _lookup(None)])
    payload = await service.calculate_capture_rate(1, datetime(2026, 1, 1), datetime(2026, 2, 1))
    (period,) = payload["periods"]
    assert period["capture_rate"] is None and period["achieved_price"] is None
    assert period["market_average_price"] == 80.0
    assert payload["overall"]["capture_rate"] is None
    assert payload["overall"]["hours_observed"] == 500


# ── compare_capture_rates_by_bidzone ───────────────────────────────────────


@pytest.mark.asyncio
async def test_compare_by_bidzone_uses_lateral_per_farm_denominators():
    rows = [
        _row(
            windfarm_id=1,
            windfarm_name="Smøla",
            capacity_mw=Decimal("150"),
            hours_observed=11591,
            hours_generating=10412,
            market_average_price=Decimal("19.61"),
            total_generation_mwh=Decimal("100"),
            revenue=Decimal("1556"),
        ),
        _row(
            windfarm_id=2,
            windfarm_name="Peer",
            capacity_mw=Decimal("30"),
            hours_observed=8000,
            hours_generating=7000,
            market_average_price=Decimal("25"),
            total_generation_mwh=Decimal("300"),
            revenue=Decimal("6000"),
        ),
        _row(  # observed but never generating → omitted (chart draws null as a 0 bar)
            windfarm_id=3,
            windfarm_name="NoGen",
            capacity_mw=None,
            hours_observed=500,
            hours_generating=0,
            market_average_price=Decimal("30"),
            total_generation_mwh=0,
            revenue=0,
        ),
    ]
    bidzone = MagicMock()
    bidzone.code, bidzone.name = "10YNO-3--------J", "NO3"
    service, db = _service([_lookup(bidzone), _result(rows=rows)])

    payload = await service.compare_capture_rates_by_bidzone(
        69, datetime(2024, 9, 5), datetime(2026, 1, 1)
    )

    stmt, params = db.execute.call_args_list[1].args
    sql = _norm(stmt)
    assert "CROSS JOIN LATERAL" in sql
    assert "DISTINCT ON" not in sql and "p.bidzone_id" not in sql
    assert "f.bidzone_id = :bidzone_id" in sql and "g.windfarm_id = f.id" in sql
    assert params["price_source"] == "ENTSOE" and params["bidzone_id"] == 69

    assert [w["windfarm_id"] for w in payload["windfarms"]] == [2, 1]  # capture DESC
    smola = payload["windfarms"][1]
    assert smola["capture_rate"] == pytest.approx(15.56 / 19.61, abs=1e-4)
    assert smola["market_average_price"] == pytest.approx(19.61)
    assert smola["hours_observed"] == 11591 and smola["hours_generating"] == 10412
    assert smola["coverage_pct"] == pytest.approx(11591 / (483 * 24) * 100, abs=0.1)
    peer = payload["windfarms"][0]
    assert peer["capture_rate"] == pytest.approx(20 / 25)
    assert peer["market_average_price"] == 25.0  # its OWN observed-hours average

    expected_zone = (smola["capture_rate"] * 100 + peer["capture_rate"] * 300) / 400
    assert payload["zone_average_capture_rate"] == pytest.approx(expected_zone)
    assert payload["market_average_basis"] == "observed_hours"
    assert payload["bidzone_code"] == "10YNO-3--------J"


# ── zone_capture_rate_by_month ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_zone_by_month_keeps_every_axis_and_weights_farms():
    rows = [
        _row(
            windfarm_id=1,
            month=date(2025, 1, 1),
            hours_observed=744,
            hours_generating=700,
            market_average_price=Decimal("50"),
            total_generation_mwh=Decimal("100"),
            revenue=Decimal("4000"),
        ),
        _row(
            windfarm_id=2,
            month=date(2025, 1, 1),
            hours_observed=372,
            hours_generating=300,
            market_average_price=Decimal("40"),
            total_generation_mwh=Decimal("100"),
            revenue=Decimal("3600"),
        ),
    ]
    bidzone = MagicMock()
    bidzone.code, bidzone.name = "10YNO-2--------T", "NO2"
    service, db = _service([_lookup(bidzone), _result(rows=rows)])

    payload = await service.zone_capture_rate_by_month(
        68, datetime(2025, 1, 1), datetime(2025, 4, 1)
    )

    assert [(a["year"], a["month"], a["label"]) for a in payload["axes"]] == [
        (2025, 1, "Jan"),
        (2025, 2, "Feb"),
        (2025, 3, "Mar"),
    ]
    jan = payload["axes"][0]
    assert jan["windfarm_count"] == 2 and jan["total_generation_mwh"] == 200.0
    assert jan["capture_rate"] == pytest.approx(((40 / 50) * 100 + (36 / 40) * 100) / 200)
    assert jan["market_average_price"] == pytest.approx((50 * 744 + 40 * 372) / 1116)
    assert jan["achieved_price"] == pytest.approx(7600 / 200)
    feb = payload["axes"][1]
    assert feb["capture_rate"] is None and feb["windfarm_count"] == 0
    assert feb["total_generation_mwh"] == 0.0

    sql = _norm(db.execute.call_args_list[1].args[0])
    assert "CROSS JOIN LATERAL" in sql and "DISTINCT ON" not in sql
    assert "GROUP BY date_trunc('month', o.hour)" in sql


# ── negative_price_exposure ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_negative_price_exposure_returns_both_counts_in_one_query():
    service, db = _service(
        [
            _result(one=_row(source="ELEXON")),
            _result(one=_row(negative_hours=400, observed_hours=4380)),
        ]
    )
    exposure = await service.negative_price_exposure(5, datetime(2025, 1, 1), datetime(2026, 1, 1))
    assert exposure == {"negative_hours": 400, "observed_hours": 4380}
    assert db.execute.await_count == 2
    stmt, params = db.execute.call_args_list[1].args
    sql = _norm(stmt)
    assert "COUNT(DISTINCT g.hour) FILTER" in sql
    assert "COUNT(DISTINCT g.hour) AS observed_hours" in sql
    assert "p.day_ahead_price < 0" in sql
    assert params["price_source"] == "ELEXON"
