"""Tests for ``app.services.financial_opex_metrics`` — the shared OPEX-per-MWh rule.

DB-free. The pure functions get hand-built rows; the async entry points get an
``AsyncMock`` session whose ``execute`` returns ``.fetchall()``-shaped results.
The golden case reproduces prod report 41 (Lutelandet, wf 7197) row-for-row.
"""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.services import financial_opex_metrics as fom
from app.services.financial_opex_metrics import (
    DROP_CURRENCY_MISMATCH,
    DROP_FX_UNAVAILABLE,
    DROP_NO_DENOMINATOR,
    DROP_OVERLAP,
    FilingRow,
    GenerationStat,
    OpexMetrics,
    aggregate_opex_metrics,
    cohort_median,
    opex_metrics_for_cohort,
    opex_metrics_for_windfarms,
    pick_best_generation,
)

WF = 7197
ENT = 38

# Lutelandet (prod, NOK). COD 2022-01-17 → FY2021 pre-COD and FY2022 inside
# COD+365d are excluded by the SQL ramp-up rule; FY2023–2025 are eligible.
LUT_OPEX = {2021: 2_704_180, 2022: 19_864_033, 2023: 25_416_682, 2024: 22_346_161, 2025: 21_113_793}
LUT_METERED = {2022: 121_878.6, 2023: 112_438.1, 2024: 134_406.6, 2025: 133_916.5}


def _filing(
    fd_id,
    year,
    opex,
    *,
    currency="NOK",
    reported=None,
    revenue=None,
    ebitda=None,
    wf=WF,
    ent=ENT,
    start=None,
    end=None,
):
    return FilingRow(
        windfarm_id=wf,
        financial_entity_id=ent,
        fd_id=fd_id,
        period_start=start or date(year, 1, 1),
        period_end=end or date(year, 12, 31),
        currency=currency,
        total_opex=Decimal(str(opex)),
        total_revenue=Decimal(str(revenue)) if revenue is not None else None,
        ebitda=Decimal(str(ebitda)) if ebitda is not None else None,
        reported_generation_gwh=Decimal(str(reported)) if reported is not None else None,
    )


def _gen(mwh, coverage=100.0, source="NVE"):
    return GenerationStat(gen_mwh=Decimal(str(mwh)), coverage_pct=coverage, source=source)


def _lutelandet_eligible():
    # newest first, as the SQL returns them
    return [
        _filing(5, 2025, LUT_OPEX[2025], reported=134.0),
        _filing(4, 2024, LUT_OPEX[2024]),
        _filing(3, 2023, LUT_OPEX[2023]),
    ]


@pytest.fixture(autouse=True)
def _clear_fx_cache():
    fom._FX_CACHE.clear()
    yield
    fom._FX_CACHE.clear()


# ─── golden prod case ─────────────────────────────────────────────────────────


def test_lutelandet_pooled_native_never_682():
    filings = _lutelandet_eligible()
    generation = {
        5: _gen(LUT_METERED[2025]),
        4: _gen(LUT_METERED[2024]),
        3: _gen(LUT_METERED[2023]),
    }

    out = aggregate_opex_metrics(filings, generation, None, display_currency=None)
    m = out[WF]

    expected = sum(LUT_OPEX[y] for y in (2023, 2024, 2025)) / sum(
        LUT_METERED[y] for y in (2023, 2024, 2025)
    )
    assert m.opex_per_mwh == pytest.approx(expected, abs=0.01)  # ≈ 180.9 NOK/MWh
    assert m.opex_per_mwh != pytest.approx(682.4, abs=1)
    assert m.years_used == [2023, 2024, 2025]
    assert m.rows_used == 3
    assert m.currency == "NOK"
    assert m.native_currency == "NOK"
    assert m.native_opex_per_mwh == pytest.approx(expected, abs=0.01)
    assert m.generation_source == "metered"
    assert m.min_coverage_pct == 100.0
    assert m.dropped == []


def test_old_bug_shape_only_the_filing_with_a_denominator_is_used():
    """Five years of OPEX, one year of reported GWh, no metered data → ONE usable
    filing (157.6), never five years of cost over one year of output (682)."""
    filings = [
        _filing(5, 2025, LUT_OPEX[2025], reported=134.0),
        _filing(4, 2024, LUT_OPEX[2024]),
        _filing(3, 2023, LUT_OPEX[2023]),
        _filing(2, 2022, LUT_OPEX[2022]),
        _filing(1, 2021, LUT_OPEX[2021]),
    ]
    out = aggregate_opex_metrics(filings, {}, None, display_currency=None, max_rows=5)
    m = out[WF]
    assert m.opex_per_mwh == pytest.approx(LUT_OPEX[2025] / 134_000, abs=0.01)  # 157.57
    assert m.rows_used == 1
    assert m.generation_source == "reported"
    assert sorted(d["reason"] for d in m.dropped) == [DROP_NO_DENOMINATOR] * 4


def test_single_fy_metered_matches_reported_within_rounding():
    filings = [_filing(5, 2025, LUT_OPEX[2025], reported=134.0)]
    out = aggregate_opex_metrics(filings, {5: _gen(LUT_METERED[2025])}, None, display_currency=None)
    assert out[WF].opex_per_mwh == pytest.approx(157.66, abs=0.01)
    assert out[WF].generation_source == "metered"


# ─── denominator rules ────────────────────────────────────────────────────────


def test_low_coverage_falls_back_to_reported_then_drops():
    with_reported = [_filing(1, 2025, 1_000_000, reported=50.0)]
    out = aggregate_opex_metrics(
        with_reported, {1: _gen(10_000, coverage=30.0)}, None, display_currency=None
    )
    assert out[WF].generation_source == "reported"
    assert out[WF].opex_per_mwh == pytest.approx(20.0)

    without = [_filing(1, 2025, 1_000_000)]
    out = aggregate_opex_metrics(
        without, {1: _gen(10_000, coverage=30.0)}, None, display_currency=None
    )
    assert WF not in out


def test_zero_or_negative_metered_generation_is_not_a_denominator():
    filings = [_filing(1, 2025, 1_000_000)]
    out = aggregate_opex_metrics(filings, {1: _gen(-5.0)}, None, display_currency=None)
    assert WF not in out


def test_non_positive_opex_rejected_even_if_injected():
    filings = [_filing(1, 2025, 0), _filing(2, 2024, -10)]
    out = aggregate_opex_metrics(
        filings, {1: _gen(1000), 2: _gen(1000)}, None, display_currency=None
    )
    assert WF not in out


def test_overlapping_periods_keep_newest_only():
    filings = [
        _filing(2, 2025, 1_200_000),
        # 18-month filing Jul-2024 → Dec-2025 overlaps the FY2025 row
        _filing(1, 2024, 1_500_000, start=date(2024, 7, 1), end=date(2025, 12, 31)),
    ]
    out = aggregate_opex_metrics(
        filings, {2: _gen(100_000), 1: _gen(150_000)}, None, display_currency=None
    )
    m = out[WF]
    assert m.rows_used == 1
    assert [d["reason"] for d in m.dropped] == [DROP_OVERLAP]


def test_max_rows_one_uses_latest_only():
    filings = _lutelandet_eligible()
    generation = {
        5: _gen(LUT_METERED[2025]),
        4: _gen(LUT_METERED[2024]),
        3: _gen(LUT_METERED[2023]),
    }
    out = aggregate_opex_metrics(filings, generation, None, display_currency=None, max_rows=1)
    assert out[WF].years_used == [2025]
    assert out[WF].opex_per_mwh == pytest.approx(157.66, abs=0.01)


def test_generation_mismatch_is_logged_not_dropped():
    filings = [_filing(1, 2025, 1_000_000, reported=100.0)]  # reported 100 GWh vs metered 50 GWh
    out = aggregate_opex_metrics(filings, {1: _gen(50_000)}, None, display_currency=None)
    assert out[WF].generation_source == "metered"
    assert out[WF].opex_per_mwh == pytest.approx(20.0)


# ─── currency rules ───────────────────────────────────────────────────────────


def test_display_mode_converts_and_drops_unconvertible():
    filings = [_filing(2, 2025, 1_000_000), _filing(1, 2024, 1_000_000)]
    rates = {("NOK", date(2025, 1, 1), date(2025, 12, 31)): Decimal("0.0887")}  # 2024 missing
    out = aggregate_opex_metrics(
        filings, {2: _gen(10_000), 1: _gen(10_000)}, rates, display_currency="EUR"
    )
    m = out[WF]
    assert m.currency == "EUR"
    assert m.opex_per_mwh == pytest.approx(8.87)
    assert m.native_currency == "NOK"
    assert m.native_opex_per_mwh == pytest.approx(100.0)
    assert [d["reason"] for d in m.dropped] == [DROP_FX_UNAVAILABLE]


def test_display_mode_same_currency_needs_no_rate():
    filings = [_filing(1, 2025, 500_000, currency="EUR")]
    out = aggregate_opex_metrics(filings, {1: _gen(10_000)}, {}, display_currency="EUR")
    assert out[WF].opex_per_mwh == pytest.approx(50.0)


def test_native_mode_never_mixes_currencies():
    filings = [
        _filing(3, 2025, 500_000, currency="EUR"),
        _filing(2, 2024, 5_000_000, currency="NOK"),
        _filing(1, 2023, 5_000_000, currency="NOK"),
    ]
    gen = {3: _gen(10_000), 2: _gen(10_000), 1: _gen(10_000)}
    out = aggregate_opex_metrics(filings, gen, None, display_currency=None)
    m = out[WF]
    assert m.currency == "EUR"
    assert m.rows_used == 1
    assert [d["reason"] for d in m.dropped] == [DROP_CURRENCY_MISMATCH, DROP_CURRENCY_MISMATCH]


def test_implausible_per_mwh_dropped_in_display_mode():
    # 5,000 EUR/MWh — a unit slip, not a real cost base (the band tops out at 1,000).
    filings = [_filing(1, 2025, 50_000_000, currency="EUR")]
    out = aggregate_opex_metrics(filings, {1: _gen(10_000)}, {}, display_currency="EUR")
    assert WF not in out

    # Native mode has no EUR band — the value is shown as filed.
    out = aggregate_opex_metrics(filings, {1: _gen(10_000)}, None, display_currency=None)
    assert out[WF].opex_per_mwh == pytest.approx(5000.0)


def test_revenue_and_ebitda_pooled_only_when_every_row_has_them():
    filings = [
        _filing(2, 2025, 1_000_000, revenue=2_000_000, ebitda=1_000_000),
        _filing(1, 2024, 1_000_000),
    ]
    out = aggregate_opex_metrics(
        filings, {2: _gen(10_000), 1: _gen(10_000)}, None, display_currency=None
    )
    assert out[WF].total_revenue is None
    assert out[WF].ebitda_margin_pct is None

    out = aggregate_opex_metrics(filings[:1], {2: _gen(10_000)}, None, display_currency=None)
    assert out[WF].ebitda_margin_pct == pytest.approx(50.0)


# ─── generation source selection ──────────────────────────────────────────────


def _gen_row(fd_id, source, gen, hours, months):
    return SimpleNamespace(
        fd_id=fd_id,
        source=source,
        gen_mwh=Decimal(str(gen)),
        hours_with_data=hours,
        months_with_data=months,
    )


def test_pick_best_generation_never_sums_sources():
    filings = {1: _filing(1, 2025, 1)}
    rows = [_gen_row(1, "NVE", 1000, 8760, 12), _gen_row(1, "OTHER", 900, 8000, 12)]
    best = pick_best_generation(rows, filings)
    assert best[1].source == "NVE"
    assert best[1].gen_mwh == Decimal("1000")
    assert best[1].coverage_pct == 100.0


def test_pick_best_generation_monthly_sources_use_month_coverage():
    filings = {1: _filing(1, 2025, 1)}
    rows = [
        _gen_row(1, "ENERGISTYRELSEN", 5000, 12, 12),  # one row per month
        _gen_row(1, "ENTSOE", 40, 100, 2),  # sparse hourly
    ]
    best = pick_best_generation(rows, filings)
    assert best[1].source == "ENERGISTYRELSEN"
    assert best[1].coverage_pct == 100.0

    only_hourly = pick_best_generation([_gen_row(1, "ENTSOE", 40, 100, 2)], filings)
    assert only_hourly[1].coverage_pct == pytest.approx(100 / 8760 * 100, abs=0.1)


def test_pick_best_generation_ignores_unknown_filings_and_null_gen():
    filings = {1: _filing(1, 2025, 1)}
    rows = [
        _gen_row(99, "NVE", 1000, 8760, 12),
        SimpleNamespace(fd_id=1, source="NVE", gen_mwh=None, hours_with_data=0, months_with_data=0),
    ]
    assert pick_best_generation(rows, filings) == {}


# ─── cohort median ────────────────────────────────────────────────────────────


def _m(wf, ent, value, currency="EUR"):
    return OpexMetrics(
        windfarm_id=wf,
        financial_entity_id=ent,
        currency=currency,
        total_opex=1.0,
        total_revenue=None,
        ebitda=None,
        generation_mwh=1.0,
        opex_per_mwh=value,
        ebitda_margin_pct=None,
        rows_used=1,
        years_used=[2025],
        period_start=date(2025, 1, 1),
        period_end=date(2025, 12, 31),
        native_currency=currency,
        native_opex_per_mwh=value,
        generation_source="metered",
        min_coverage_pct=100.0,
    )


def test_cohort_median_needs_three_entities():
    assert cohort_median({1: _m(1, 10, 10.0), 2: _m(2, 20, 20.0)}) is None
    assert cohort_median({1: _m(1, 10, 10.0), 2: _m(2, 20, 20.0), 3: _m(3, 30, 30.0)}) == (20.0, 3)


def test_cohort_median_one_vote_per_entity_and_display_currency_only():
    cohort = {
        1: _m(1, 10, 10.0),
        2: _m(2, 10, 10.0),  # same entity as farm 1 → one vote
        3: _m(3, 30, 30.0),
        4: _m(4, 40, 50.0),
        5: _m(5, 50, 999.0, currency="NOK"),  # unconverted → ignored
    }
    assert cohort_median(cohort) == (30.0, 3)


# ─── async entry points (mocked session) ──────────────────────────────────────


def _result(rows):
    res = MagicMock()
    res.fetchall.return_value = rows
    return res


def _filing_sql_row(fd_id, year, opex, *, wf=WF, ent=ENT, currency="NOK", reported=None):
    return SimpleNamespace(
        windfarm_id=wf,
        financial_entity_id=ent,
        cod=date(2022, 1, 17),
        fd_id=fd_id,
        period_start=date(year, 1, 1),
        period_end=date(year, 12, 31),
        currency=currency,
        is_synthetic=False,
        total_operating_expenses=Decimal(str(opex)),
        total_revenue=None,
        ebitda=None,
        reported_generation_gwh=Decimal(str(reported)) if reported is not None else None,
    )


@pytest.mark.asyncio
async def test_windfarms_entry_point_sql_contract_and_sequencing():
    db = MagicMock()
    db.execute = AsyncMock(
        side_effect=[
            _result([_filing_sql_row(5, 2025, LUT_OPEX[2025], reported=134.0)]),
            _result([_gen_row(5, "NVE", LUT_METERED[2025], 8760, 12)]),
        ]
    )
    out = await opex_metrics_for_windfarms(
        db, windfarm_ids=[WF], as_of=date(2025, 12, 31), display_currency=None, max_rows=1
    )
    assert out[WF].opex_per_mwh == pytest.approx(157.66, abs=0.01)

    first_call = db.execute.await_args_list[0]
    sql = str(first_call.args[0])
    params = first_call.args[1]
    assert "relationship_type = 'primary_asset'" in sql
    assert "fd.period_end <= :as_of" in sql
    assert "INTERVAL '365 days'" in sql
    assert "NOT fd.is_synthetic" in sql
    assert "wfe.windfarm_id = ANY(:wf_ids)" in sql
    assert params["as_of"] == date(2025, 12, 31)
    assert params["wf_ids"] == [WF]

    second_call = db.execute.await_args_list[1]
    assert "mv_generation_monthly_by_windfarm" in str(second_call.args[0])
    assert "generation_data" not in str(second_call.args[0])
    assert second_call.args[1] == {"fd_ids": [5]}


@pytest.mark.asyncio
async def test_windfarms_entry_point_no_filings_short_circuits():
    db = MagicMock()
    db.execute = AsyncMock(return_value=_result([]))
    out = await opex_metrics_for_windfarms(db, windfarm_ids=[WF], as_of=date(2025, 12, 31))
    assert out == {}
    db.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_windfarms_entry_point_include_synthetic_toggles_sql():
    db = MagicMock()
    db.execute = AsyncMock(return_value=_result([]))
    await opex_metrics_for_windfarms(
        db, windfarm_ids=[WF], as_of=date(2025, 12, 31), include_synthetic=True
    )
    assert "NOT fd.is_synthetic" not in str(db.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_cohort_entry_point_scopes_by_bidzone_and_location(monkeypatch):
    db = MagicMock()
    db.execute = AsyncMock(
        side_effect=[
            _result(
                [
                    _filing_sql_row(1, 2025, 1_000_000, wf=1, ent=10),
                    _filing_sql_row(2, 2025, 2_000_000, wf=2, ent=20),
                ]
            ),
            _result([_gen_row(1, "NVE", 10_000, 8760, 12), _gen_row(2, "NVE", 10_000, 8760, 12)]),
        ]
    )
    rate = Decimal("0.1")
    monkeypatch.setattr(
        fom,
        "_fetch_rates",
        AsyncMock(return_value={("NOK", date(2025, 1, 1), date(2025, 12, 31)): rate}),
    )
    out = await opex_metrics_for_cohort(
        db,
        bidzone_id=69,
        location_type="onshore",
        as_of=date(2025, 12, 31),
        exclude_windfarm_id=7197,
    )
    assert out[1].opex_per_mwh == pytest.approx(10.0)
    assert out[2].opex_per_mwh == pytest.approx(20.0)
    sql = str(db.execute.await_args_list[0].args[0])
    params = db.execute.await_args_list[0].args[1]
    assert "w.bidzone_id = :bidzone_id" in sql and "w.location_type = :location_type" in sql
    assert params["bidzone_id"] == 69 and params["location_type"] == "onshore"
    assert params["exclude_wf_id"] == 7197


@pytest.mark.asyncio
async def test_fx_rates_cached_and_unsupported_currency_skipped(monkeypatch):
    calls = []

    async def fake_rate(self, from_ccy, to_ccy, start, end):
        calls.append((from_ccy, to_ccy, start, end))
        return Decimal("0.0887")

    monkeypatch.setattr(fom.ExchangeRateService, "get_rate_for_period", fake_rate)
    filings = [
        _filing(1, 2025, 1),
        _filing(2, 2025, 1, wf=2),
        _filing(3, 2025, 1, currency="SEK", wf=3),
    ]
    rates = await fom._fetch_rates(MagicMock(), filings, "EUR")
    assert rates[("NOK", date(2025, 1, 1), date(2025, 12, 31))] == Decimal("0.0887")
    assert rates[("SEK", date(2025, 1, 1), date(2025, 12, 31))] is None
    assert calls == [("NOK", "EUR", date(2025, 1, 1), date(2025, 12, 31))]

    # Second lookup for the same (ccy, period) is served from the module cache.
    await fom._fetch_rates(MagicMock(), filings[:1], "EUR")
    assert len(calls) == 1
