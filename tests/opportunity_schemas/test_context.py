"""Tests for DetectionContext + DetectorResult.

All tests are DB-free: ``db`` is an AsyncMock and any query result is faked via
``execute(...)`` return values. These tests also pin the test-injection contract
(``prefetched=...``) that every downstream detector test (#92–#112) relies on.
"""

from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.models.opportunity import SchemaCode, Severity
from app.services import financial_opex_metrics as fom
from app.services.opportunity_schemas.context import DetectionContext, DetectorResult

START = datetime(2024, 1, 1)
END = datetime(2026, 1, 1)


def _make_db():
    """An AsyncMock session whose ``execute`` is awaitable."""
    db = MagicMock()
    db.execute = AsyncMock()
    return db


def test_detector_result_defaults():
    """DetectorResult exposes the agreed field surface with safe defaults."""
    r = DetectorResult(schema_code=SchemaCode.OPS_01, severity=Severity.WATCH)
    assert r.schema_code is SchemaCode.OPS_01
    assert r.severity is Severity.WATCH
    assert r.branch is None
    assert r.data_slots == {}
    assert r.missing_slots == []
    assert r.suppression_reason is None


def test_windfarm_id_accepts_object_or_int():
    """windfarm_id normalizes either a bare int or an ORM-like object."""
    ctx_int = DetectionContext(db=_make_db(), windfarm=42, period_start=START, period_end=END)
    assert ctx_int.windfarm_id == 42

    ctx_obj = DetectionContext(
        db=_make_db(), windfarm=SimpleNamespace(id=7), period_start=START, period_end=END
    )
    assert ctx_obj.windfarm_id == 7


@pytest.mark.asyncio
async def test_prefetched_values_are_returned_without_db():
    """prefetched short-circuits the accessor — proves the injection contract."""
    db = _make_db()
    sentinel = {"capture_rate": 0.62, "zone_avg": 0.69, "gap_pp": 7.0, "bidzone_code": "NO2"}
    ctx = DetectionContext(
        db=db,
        windfarm=1,
        period_start=START,
        period_end=END,
        prefetched={"capture_rate": sentinel},
    )

    result = await ctx.load_capture_rate()

    assert result is sentinel
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_curtailment_pct_prefetched_short_circuits():
    """#94: a prefetched curtailment_pct short-circuits the DB query."""
    db = _make_db()
    ctx = DetectionContext(
        db=db,
        windfarm=1,
        period_start=START,
        period_end=END,
        prefetched={"curtailment_pct": 18.0},
    )
    assert await ctx.load_curtailment_pct() == 18.0
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_curtailment_pct_formula_from_db_row():
    """#94: curtailment_pct = curtailed / (curtailed + generation) * 100.

    curtailed=100, generation=900 → 100/1000*100 = 10.0 (memoized: one query).
    """
    db = _make_db()
    row = SimpleNamespace(curtailed=100, generation=900)
    result_obj = MagicMock()
    result_obj.fetchone.return_value = row
    db.execute.return_value = result_obj

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_curtailment_pct() == pytest.approx(10.0)
    assert await ctx.load_curtailment_pct() == pytest.approx(10.0)  # memoized
    assert db.execute.await_count == 1


@pytest.mark.asyncio
async def test_load_curtailment_pct_none_when_zero_total():
    """#94: zero curtailed + generation → None (suppression won't trigger)."""
    db = _make_db()
    row = SimpleNamespace(curtailed=0, generation=0)
    result_obj = MagicMock()
    result_obj.fetchone.return_value = row
    db.execute.return_value = result_obj

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_curtailment_pct() is None


@pytest.mark.asyncio
async def test_load_curtailment_pct_none_on_db_error():
    """#94: an unreachable/failing query degrades to None, not an exception."""
    db = _make_db()
    db.execute.side_effect = RuntimeError("no such table: generation_data")

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_curtailment_pct() is None


@pytest.mark.asyncio
async def test_load_capture_rate_is_memoized():
    """Two load_capture_rate() calls hit the DB exactly once."""
    db = _make_db()

    # Fake the PriceAnalyticsService so no real query is built.
    fake_pa = MagicMock()
    fake_pa.calculate_capture_rate = AsyncMock(
        return_value={"overall": {"capture_rate": 0.60}, "periods": []}
    )
    fake_pa.compare_capture_rates_by_bidzone = AsyncMock(
        return_value={"zone_average_capture_rate": 0.70}
    )

    # db.execute returns: bidzone_id lookup, then bidzone code lookup.
    bidzone_row = MagicMock()
    bidzone_row.scalar_one_or_none.return_value = 99
    code_row = MagicMock()
    code_row.scalar_one_or_none.return_value = "NO2"
    db.execute.side_effect = [bidzone_row, code_row]

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    ctx._price_analytics_svc = fake_pa  # inject fake analytics

    first = await ctx.load_capture_rate()
    second = await ctx.load_capture_rate()

    assert first == second
    assert first == {
        "capture_rate": 0.6,
        "zone_avg": 0.7,
        "gap_pp": 10.0,
        "bidzone_code": "NO2",
    }
    # Underlying DB call ran exactly once across the two accessor calls.
    assert db.execute.await_count == 2  # one bidzone lookup + one code lookup, total
    assert fake_pa.calculate_capture_rate.await_count == 1
    assert fake_pa.compare_capture_rates_by_bidzone.await_count == 1


@pytest.mark.asyncio
async def test_load_monthly_performance_matches_legacy_calc():
    """Accessor returns the normalized monthly rows from canned proxy rows.

    The legacy ``_calc_monthly_availability`` first tries the
    ``performance_summaries`` ORM query, then falls back to the raw-SQL
    availability proxy. Faking the ORM path against a mock session is brittle, so
    we drive the documented fallback path: the ORM query returns no rows and the
    proxy SQL returns canned rows. We assert the accessor produces the same
    shape/keys the legacy method produces for those same rows.
    """
    db = _make_db()

    # 1st execute() = performance_summaries ORM query -> no summaries (fallback).
    empty_summaries = MagicMock()
    empty_summaries.scalars.return_value.all.return_value = []

    # 2nd execute() = proxy SQL -> canned rows.
    proxy_result = MagicMock()
    proxy_result.fetchall.return_value = [
        SimpleNamespace(month="2024-01", gen_hours=700, total_hours=744, availability_pct=94.09),
        SimpleNamespace(month="2024-02", gen_hours=690, total_hours=696, availability_pct=99.14),
    ]
    db.execute.side_effect = [empty_summaries, proxy_result]

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    rows = await ctx.load_monthly_performance()

    assert rows == [
        {"month": "2024-01", "gen_hours": 700, "total_hours": 744, "availability_pct": 94.09},
        {"month": "2024-02", "gen_hours": 690, "total_hours": 696, "availability_pct": 99.14},
    ]
    # Each row carries exactly the legacy keys.
    for row in rows:
        assert set(row.keys()) == {"month", "gen_hours", "total_hours", "availability_pct"}

    # Memoized: a second call does not re-query.
    db.execute.reset_mock()
    rows_again = await ctx.load_monthly_performance()
    assert rows_again == rows
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_degradation_result_prefetched_short_circuits():
    """#99: a prefetched degradation_result short-circuits the DB query."""
    db = _make_db()
    sentinel = {"slope_pct_per_year": -4.0, "p_value": 0.04, "reference_curve": "q50"}
    ctx = DetectionContext(
        db=db,
        windfarm=1,
        period_start=START,
        period_end=END,
        prefetched={"degradation_result": sentinel},
    )
    assert await ctx.load_degradation_result() is sentinel
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_degradation_result_none_when_no_row():
    """#99: no degradation row → None, and the result is memoized."""
    db = _make_db()
    empty = MagicMock()
    empty.scalars.return_value.first.return_value = None
    db.execute.return_value = empty

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_degradation_result() is None

    db.execute.reset_mock()
    assert await ctx.load_degradation_result() is None  # memoized
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_degradation_result_none_on_db_error():
    """#99: a failing query degrades to None, not an exception."""
    db = _make_db()
    db.execute.side_effect = RuntimeError("no such table: degradation_results")
    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_degradation_result() is None


@pytest.mark.asyncio
async def test_load_ppa_info_empty_when_no_ppa():
    """load_ppa_info returns {} when no PPA row exists, and memoizes."""
    db = _make_db()
    ppa_result = MagicMock()
    ppa_result.scalars.return_value.first.return_value = None
    db.execute.return_value = ppa_result

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_ppa_info() == {}

    db.execute.reset_mock()
    assert await ctx.load_ppa_info() == {}
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_compute_zone_opex_median_prefetched_short_circuits():
    """#108: a prefetched per-location-type median short-circuits the DB query."""
    db = _make_db()
    ctx = DetectionContext(
        db=db,
        windfarm=SimpleNamespace(id=1, bidzone_id=5),
        period_start=START,
        period_end=END,
        prefetched={"zone_opex_median:onshore": 31.8},
    )
    assert await ctx.compute_zone_opex_median("onshore") == 31.8
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_compute_zone_opex_median_none_when_location_type_missing():
    """#108: unknown location_type → None (no defined cohort), no DB access."""
    db = _make_db()
    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.compute_zone_opex_median(None) is None
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_load_own_opex_financials_prefetched_short_circuits():
    """#108: a prefetched own_opex_financials dict short-circuits the DB query."""
    db = _make_db()
    sentinel = {
        "total_opex_eur": 60e6,
        "generation_gwh": 1000.0,
        "full_years": 2,
        "relationship_type": "primary_asset",
    }
    ctx = DetectionContext(
        db=db,
        windfarm=1,
        period_start=START,
        period_end=END,
        prefetched={"own_opex_financials": sentinel},
    )
    assert await ctx.load_own_opex_financials() is sentinel
    db.execute.assert_not_called()


# ── EPR-117: loaders respect the window they claim ──────────────────────


def test_last_complete_calendar_year():
    from app.services.opportunity_schemas.context import last_complete_calendar_year

    assert last_complete_calendar_year(datetime(2026, 1, 1)) == 2025
    assert last_complete_calendar_year(datetime(2025, 12, 31, 23, 59, 59)) == 2025
    assert last_complete_calendar_year(datetime(2025, 6, 30)) == 2024


@pytest.mark.asyncio
async def test_monthly_performance_orm_query_is_month_bounded():
    """A Sep-2024 → Aug-2026 window must not pull Jan-2024 months (year-only filter)."""
    db = _make_db()
    empty_summaries = MagicMock()
    empty_summaries.scalars.return_value.all.return_value = []
    proxy = MagicMock()
    proxy.fetchall.return_value = []
    db.execute.side_effect = [empty_summaries, proxy]

    ctx = DetectionContext(
        db=db, windfarm=1, period_start=datetime(2024, 9, 4), period_end=datetime(2026, 8, 25)
    )
    await ctx.load_monthly_performance()

    stmt = str(db.execute.await_args_list[0].args[0])
    assert "performance_summaries.month" in stmt
    assert "BETWEEN" in stmt


@pytest.mark.asyncio
async def test_norm_index_series_clipped_at_window_end():
    db = _make_db()
    res = MagicMock()
    res.scalars.return_value.all.return_value = []
    db.execute.return_value = res

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_norm_index_series() is None
    stmt = str(db.execute.await_args.args[0])
    assert "performance_summaries.month" in stmt
    assert "<=" in stmt


@pytest.mark.asyncio
async def test_annual_generation_bound_to_last_complete_year():
    db = _make_db()
    res = MagicMock()
    res.fetchall.return_value = []
    db.execute.return_value = res

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=datetime(2025, 6, 30))
    assert await ctx.load_annual_generation_gwh() is None
    stmt = str(db.execute.await_args.args[0])
    assert "hour >= :since" in stmt and "hour < :until" in stmt
    params = db.execute.await_args.args[1]
    # Window ends mid-2025 → last complete year 2024; scan 2021-01-01 .. 2025-01-01.
    assert params["since"] == datetime(2021, 1, 1, tzinfo=timezone.utc)
    assert params["until"] == datetime(2025, 1, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_structural_constraint_flags_must_overlap_window():
    db = _make_db()
    res = MagicMock()
    res.scalars.return_value.all.return_value = []
    db.execute.return_value = res

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_structural_constraint_flags() is None
    stmt = str(db.execute.await_args.args[0])
    assert "period_start <" in stmt
    assert "period_end >" in stmt


@pytest.mark.asyncio
async def test_degradation_prefers_rows_ending_inside_window():
    db = _make_db()
    empty = MagicMock()
    empty.scalars.return_value.first.return_value = None
    db.execute.return_value = empty

    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_degradation_result() is None
    assert "analysis_end <=" in str(db.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_own_opex_financials_bound_to_window_end():
    """Filings ending after the window end are not this window's costs (EPR-117).

    The accessor now delegates to ``financial_opex_metrics``; the filings query
    is still the FIRST statement executed, bound positionally with an ``as_of``
    date, and consumed via ``.fetchall()`` — an empty result yields ``None``.
    """
    db = _make_db()
    res = MagicMock()
    res.fetchall.return_value = []
    db.execute.return_value = res

    ctx = DetectionContext(
        db=db, windfarm=1, period_start=START, period_end=datetime(2025, 12, 31, 23, 59, 59)
    )
    assert await ctx.load_own_opex_financials() is None
    assert db.execute.await_args.args[1]["as_of"] == date(2025, 12, 31)
    assert "fd.period_end <= :as_of" in str(db.execute.await_args.args[0])


def test_zone_capture_cache_expires_and_is_bounded(monkeypatch):
    from app.services.opportunity_schemas import context as c

    c._ZONE_CAPTURE_CACHE.clear()
    clock = {"t": 1000.0}
    monkeypatch.setattr(c._time, "monotonic", lambda: clock["t"])

    c._zone_cache_put(("a",), {"x": 1})
    assert c._zone_cache_get(("a",)) == {"x": 1}
    clock["t"] += c._ZONE_CAPTURE_TTL_SECONDS + 1
    assert c._zone_cache_get(("a",)) is None  # expired

    for i in range(c._ZONE_CAPTURE_MAX_ENTRIES + 5):
        clock["t"] += 1
        c._zone_cache_put((i,), i)
    assert len(c._ZONE_CAPTURE_CACHE) == c._ZONE_CAPTURE_MAX_ENTRIES
    assert c._zone_cache_get((0,)) is None  # oldest evicted
    c._ZONE_CAPTURE_CACHE.clear()


# ── windfarm metadata fallback for bare-int windfarms (FIN-02/03 in the nightly) ──


def _meta_result(row):
    res = MagicMock()
    res.first.return_value = row
    return res


@pytest.mark.asyncio
async def test_load_windfarm_meta_bare_int_falls_back_to_db_once():
    db = _make_db()
    db.execute.return_value = _meta_result(("onshore", date(2022, 1, 17), 69))
    ctx = DetectionContext(db=db, windfarm=7197, period_start=START, period_end=END)

    assert await ctx.load_windfarm_meta() == {
        "location_type": "onshore",
        "commercial_operational_date": date(2022, 1, 17),
        "bidzone_id": 69,
    }
    assert await ctx.windfarm_attr("location_type") == "onshore"
    assert await ctx._resolve_bidzone_id() == 69
    db.execute.assert_awaited_once()  # memoized under "windfarm_meta"


@pytest.mark.asyncio
async def test_load_windfarm_meta_none_safe_on_empty_or_failing_lookup():
    db = _make_db()
    db.execute.return_value = _meta_result(None)
    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_windfarm_meta() is None
    assert await ctx.windfarm_attr("location_type") is None

    db2 = _make_db()
    db2.execute.side_effect = RuntimeError("boom")
    ctx2 = DetectionContext(db=db2, windfarm=1, period_start=START, period_end=END)
    assert await ctx2.load_windfarm_meta() is None


@pytest.mark.asyncio
async def test_load_windfarm_meta_magicmock_row_resolves_none():
    """A MagicMock row (or any non str/date/int values) must not leak through the gate."""
    db = _make_db()
    db.execute.return_value = _meta_result(MagicMock())
    ctx = DetectionContext(db=db, windfarm=1, period_start=START, period_end=END)
    assert await ctx.load_windfarm_meta() is None


@pytest.mark.asyncio
async def test_windfarm_attr_prefers_object_attr_without_db():
    db = _make_db()
    ctx = DetectionContext(
        db=db,
        windfarm=SimpleNamespace(id=1, location_type="offshore", bidzone_id=81),
        period_start=START,
        period_end=END,
    )
    assert await ctx.windfarm_attr("location_type") == "offshore"
    assert await ctx.windfarm_attr("bidzone_id") == 81
    db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_windfarm_attr_object_missing_attr_falls_back_to_db():
    db = _make_db()
    db.execute.return_value = _meta_result(("onshore", None, 69))
    ctx = DetectionContext(
        db=db, windfarm=SimpleNamespace(id=1), period_start=START, period_end=END
    )
    assert await ctx.windfarm_attr("location_type") == "onshore"


# ── FIN-02/03 accessors delegate to financial_opex_metrics ──────────────


def _metrics(wf, ent, value, currency="EUR"):
    return fom.OpexMetrics(
        windfarm_id=wf,
        financial_entity_id=ent,
        currency=currency,
        total_opex=value * 1000.0,
        total_revenue=None,
        ebitda=None,
        generation_mwh=1000.0,
        opex_per_mwh=value,
        ebitda_margin_pct=None,
        rows_used=3,
        years_used=[2023, 2024, 2025],
        period_start=date(2023, 1, 1),
        period_end=date(2025, 12, 31),
        native_currency="NOK",
        native_opex_per_mwh=value * 11.3,
        generation_source="metered",
        min_coverage_pct=99.0,
    )


@pytest.mark.asyncio
async def test_own_opex_financials_maps_helper_output_to_legacy_shape(monkeypatch):
    monkeypatch.setattr(
        fom, "opex_metrics_for_windfarms", AsyncMock(return_value={7197: _metrics(7197, 38, 16.0)})
    )
    ctx = DetectionContext(
        db=_make_db(), windfarm=7197, period_start=START, period_end=datetime(2025, 12, 31)
    )
    out = await ctx.load_own_opex_financials()
    assert out["total_opex_eur"] == pytest.approx(16_000.0)
    assert out["generation_gwh"] == pytest.approx(1.0)
    assert out["full_years"] == 3
    assert out["relationship_type"] == "primary_asset"
    assert out["currency"] == "EUR"
    assert out["years_used"] == [2023, 2024, 2025]
    assert out["native_currency"] == "NOK"
    assert out["generation_source"] == "metered"
    call = fom.opex_metrics_for_windfarms.await_args
    assert call.kwargs["windfarm_ids"] == [7197]
    assert call.kwargs["as_of"] == date(2025, 12, 31)
    assert call.kwargs["display_currency"] == "EUR"
    assert call.kwargs["include_synthetic"] is False


@pytest.mark.asyncio
async def test_own_opex_financials_helper_failure_returns_none(monkeypatch):
    monkeypatch.setattr(
        fom, "opex_metrics_for_windfarms", AsyncMock(side_effect=RuntimeError("db"))
    )
    ctx = DetectionContext(db=_make_db(), windfarm=7197, period_start=START, period_end=END)
    assert await ctx.load_own_opex_financials() is None


@pytest.mark.asyncio
async def test_zone_opex_median_excludes_subject_and_needs_three_peers(monkeypatch):
    from app.services.opportunity_schemas import context as c

    c._ZONE_CAPTURE_CACHE.clear()
    cohort = {
        7197: _metrics(7197, 38, 16.0),  # the subject — must not vote
        1: _metrics(1, 10, 10.0),
        2: _metrics(2, 20, 12.0),
        3: _metrics(3, 30, 20.0),
    }
    fake = AsyncMock(return_value=cohort)
    monkeypatch.setattr(fom, "opex_metrics_for_cohort", fake)

    ctx = DetectionContext(
        db=_make_db(),
        windfarm=SimpleNamespace(id=7197, bidzone_id=69, location_type="onshore"),
        period_start=START,
        period_end=datetime(2025, 12, 31),
    )
    assert await ctx.compute_zone_opex_median("onshore") == 12.0
    assert ctx.peek("zone_opex_peer_count:onshore") == 3
    assert fake.await_args.kwargs["bidzone_id"] == 69
    assert fake.await_args.kwargs["location_type"] == "onshore"
    assert fake.await_args.kwargs["as_of"] == date(2025, 12, 31)

    # Only two true peers → no benchmark.
    c._ZONE_CAPTURE_CACHE.clear()
    monkeypatch.setattr(
        fom, "opex_metrics_for_cohort", AsyncMock(return_value={k: cohort[k] for k in (7197, 1, 2)})
    )
    ctx2 = DetectionContext(
        db=_make_db(),
        windfarm=SimpleNamespace(id=7197, bidzone_id=69, location_type="onshore"),
        period_start=START,
        period_end=datetime(2025, 12, 31),
    )
    assert await ctx2.compute_zone_opex_median("onshore") is None
    assert ctx2.peek("zone_opex_peer_count:onshore") is None
    c._ZONE_CAPTURE_CACHE.clear()


@pytest.mark.asyncio
async def test_zone_opex_median_cohort_cached_per_bidzone_and_as_of(monkeypatch):
    from app.services.opportunity_schemas import context as c

    c._ZONE_CAPTURE_CACHE.clear()
    cohort = {i: _metrics(i, i * 10, float(i)) for i in range(1, 6)}
    fake = AsyncMock(return_value=cohort)
    monkeypatch.setattr(fom, "opex_metrics_for_cohort", fake)

    def _ctx(wf_id):
        return DetectionContext(
            db=_make_db(),
            windfarm=SimpleNamespace(id=wf_id, bidzone_id=69, location_type="onshore"),
            period_start=START,
            period_end=datetime(2025, 12, 31),
        )

    assert await _ctx(1).compute_zone_opex_median("onshore") == 3.5  # median of 2,3,4,5
    assert await _ctx(5).compute_zone_opex_median("onshore") == 2.5  # median of 1,2,3,4
    fake.assert_awaited_once()  # one cohort scan serves every farm in it
    c._ZONE_CAPTURE_CACHE.clear()


@pytest.mark.asyncio
async def test_zone_opex_median_helper_failure_returns_none(monkeypatch):
    from app.services.opportunity_schemas import context as c

    c._ZONE_CAPTURE_CACHE.clear()
    monkeypatch.setattr(fom, "opex_metrics_for_cohort", AsyncMock(side_effect=RuntimeError("db")))
    ctx = DetectionContext(
        db=_make_db(),
        windfarm=SimpleNamespace(id=1, bidzone_id=69),
        period_start=START,
        period_end=END,
    )
    assert await ctx.compute_zone_opex_median("onshore") is None


# ── EPR-126: as_of_date for point-in-time schemas ─────────────────────────


def test_as_of_date_defaults_to_period_end_and_honours_the_nightly_override():
    from datetime import date as _date
    from datetime import datetime as _datetime

    from app.services.opportunity_schemas.context import DetectionContext as _Ctx

    clipped_end = _datetime(2025, 12, 31, 23, 59, 59, 999999)
    report_ctx = _Ctx(
        db=None, windfarm=1, period_start=_datetime(2025, 1, 1), period_end=clipped_end
    )
    # A period-scoped report assesses as of its window end.
    assert report_ctx.as_of_date == _date(2025, 12, 31)

    # The nightly clips period_end to the farm's last metered day but must
    # keep assessing fleet age / PPA expiry as of the RUN date.
    nightly_ctx = _Ctx(
        db=None,
        windfarm=1,
        period_start=_datetime(2024, 9, 5),
        period_end=clipped_end,
        as_of=_date(2026, 8, 27),
    )
    assert nightly_ctx.as_of_date == _date(2026, 8, 27)
    assert nightly_ctx.period_end == clipped_end  # the window itself is untouched


# ── EPR-126 Part B: shared capture payload, observed hours ────────────────


@pytest.mark.asyncio
async def test_capture_rate_payload_is_fetched_once_for_mkt01_and_mkt03():
    db = MagicMock()
    lookup = MagicMock()
    lookup.scalar_one_or_none.return_value = None  # no bidzone → MKT-01 stops early
    db.execute = AsyncMock(return_value=lookup)
    ctx = DetectionContext(
        db=db, windfarm=7213, period_start=datetime(2024, 9, 5), period_end=datetime(2026, 1, 1)
    )
    fake_pa = MagicMock()
    fake_pa.calculate_capture_rate = AsyncMock(
        return_value={
            "overall": {"capture_rate": 0.79},
            "periods": [
                {"period": "2024-01-01T00:00:00", "capture_rate": 0.80},
                {"period": "2025-01-01T00:00:00", "capture_rate": 0.84},
            ],
        }
    )
    ctx._price_analytics_svc = fake_pa

    ci = await ctx.load_cannibalisation_index()
    assert ci["ci_by_year"] == {"2024": round(1 / 0.80, 4), "2025": round(1 / 0.84, 4)}
    assert await ctx.load_capture_rate() is None  # bidzone lookup returned None
    assert fake_pa.calculate_capture_rate.await_count == 1  # shared payload


@pytest.mark.asyncio
async def test_observed_hours_and_negative_hours_share_one_exposure_query():
    ctx = DetectionContext(
        db=MagicMock(),
        windfarm=1,
        period_start=datetime(2025, 1, 1),
        period_end=datetime(2026, 1, 1),
    )
    fake_pa = MagicMock()
    fake_pa.negative_price_exposure = AsyncMock(
        return_value={"negative_hours": 400, "observed_hours": 4380}
    )
    ctx._price_analytics_svc = fake_pa

    assert await ctx.load_negative_price_hours() == 400
    assert await ctx.load_observed_hours() == 4380
    assert fake_pa.negative_price_exposure.await_count == 1

    prefetched = DetectionContext(
        db=None,
        windfarm=1,
        period_start=datetime(2025, 1, 1),
        period_end=datetime(2026, 1, 1),
        prefetched={"observed_hours": 100},
    )
    assert await prefetched.load_observed_hours() == 100
