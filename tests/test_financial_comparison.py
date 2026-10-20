"""Financial page of Performance Analytics (EPR-189): filing rules, values,
the batched FX helper, the payload shape and the endpoint."""

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.api.v1.endpoints import performance_analytics as ep
from app.core.deps import get_current_active_user
from app.models.user import User
from app.services import financial_comparison_service as fcs
from app.services.exchange_rate_service import ExchangeRateService
from app.services.financial_comparison_service import (
    Filing,
    FinancialComparisonService,
    eligible_filings,
    filing_values,
    financial_year,
    generation_mwh,
    is_full_year,
)
from app.services.financial_opex_metrics import GenerationStat, pick_best_generation

D = Decimal


def filing(fd_id=1, entity=10, start=date(2023, 1, 1), end=date(2023, 12, 31), **kw):
    base = dict(
        currency="EUR",
        revenue=D("1000000"),
        opex=D("400000"),
        ebitda=D("600000"),
        reported_gwh=None,
    )
    base.update(kw)
    return Filing(fd_id=fd_id, entity_id=entity, period_start=start, period_end=end, **base)


ONE = {"EUR": D("1"), "NOK": D("11"), "GBP": D("0.85")}


# ─── Filing rules ─────────────────────────────────────────────────────────────


def test_financial_year_is_where_most_months_fall():
    assert financial_year(date(2023, 1, 1)) == 2023  # Jan–Dec
    assert financial_year(date(2023, 4, 1)) == 2023  # Apr–Mar: 9 of 12 months in 2023
    assert financial_year(date(2023, 7, 1)) == 2023  # Jul–Jun ties to its start
    assert financial_year(date(2023, 10, 1)) == 2024  # Oct–Sep: 9 of 12 in 2024


def test_only_full_twelve_month_periods():
    assert is_full_year(date(2023, 4, 1), date(2024, 3, 31))
    assert not is_full_year(date(2023, 1, 1), date(2023, 9, 30))  # 9 months
    assert not is_full_year(date(2023, 1, 1), date(2024, 3, 31))  # 15 months
    assert not is_full_year(date(2023, 1, 1), date(2024, 1, 5))  # 53-week year
    assert not is_full_year(date(2023, 1, 20), date(2023, 12, 31))  # starts mid-January


def test_duplicate_year_keeps_the_later_ending_filing_and_counts_drops():
    a = filing(1, start=date(2022, 4, 1), end=date(2023, 3, 31))  # FY 2022
    b = filing(2, start=date(2022, 1, 1), end=date(2022, 12, 31))  # FY 2022, ends earlier
    short = filing(3, start=date(2023, 4, 1), end=date(2023, 12, 31))
    kept, dropped = eligible_filings([a, b, short], {})
    assert {k: f.fd_id for k, f in kept.items()} == {(10, 2022): 1}
    assert dropped == {"duplicate_year": 1, "not_12_months": 1}


def test_ramp_up_uses_the_latest_cod_of_the_entity():
    f = filing(start=date(2023, 1, 1))
    kept, dropped = eligible_filings([f], {10: date(2022, 6, 1)})
    assert not kept and dropped == {"ramp_up": 1}
    kept, _ = eligible_filings([f], {10: date(2021, 12, 31)})
    assert kept


# ─── Values ───────────────────────────────────────────────────────────────────


def test_generation_metered_then_reported_then_none():
    f = filing(reported_gwh=D("8"))
    assert generation_mwh(f, GenerationStat(D("10000"), 90.0, "NVE")) == (D("10000"), "metered")
    assert generation_mwh(f, GenerationStat(D("4000"), 40.0, "NVE")) == (D("8000"), "reported")
    assert generation_mwh(filing(), None) == (None, None)


def test_combined_coverage_counts_every_linked_farm():
    """Two linked farms, data for one: half the expected hours ⇒ 50 %, not 100 %."""
    f = filing(start=date(2023, 1, 1), end=date(2023, 12, 31))
    row = SimpleNamespace(
        fd_id=1, source="NVE", gen_mwh=1000, hours_with_data=8760, months_with_data=12
    )
    assert pick_best_generation([row], {1: f})[1].coverage_pct == 100.0
    assert pick_best_generation([row], {1: f}, {1: 2})[1].coverage_pct == 50.0
    assert pick_best_generation([row], {1: f}, {1: 3})[1].coverage_pct == 33.3


def test_values_per_currency_and_margin():
    rev, opex, margin, implausible = filing_values(filing(), D("10000"), ONE)
    assert rev == {"EUR": 100.0, "NOK": 1100.0, "GBP": 85.0}
    assert opex == {"EUR": 40.0, "NOK": 440.0, "GBP": 34.0}
    assert margin == 60.0 and not implausible


def test_missing_opex_or_rate_keeps_the_rest():
    rev, opex, _, _ = filing_values(
        filing(opex=None), D("10000"), {"EUR": D("1"), "NOK": None, "GBP": D("0.85")}
    )
    assert rev == {"EUR": 100.0, "NOK": None, "GBP": 85.0}
    assert opex == {"EUR": None, "NOK": None, "GBP": None}


def test_no_generation_keeps_only_the_margin():
    rev, opex, margin, _ = filing_values(filing(), None, ONE)
    assert set(rev.values()) == {None} and set(opex.values()) == {None}
    assert margin == 60.0


def test_implausible_value_drops_both_per_mwh_values():
    # OPEX at 0.4 EUR/MWh (a unit slip) also drops the revenue figure.
    rev, opex, margin, implausible = filing_values(filing(opex=D("4000")), D("10000"), ONE)
    assert implausible
    assert set(rev.values()) == {None} and set(opex.values()) == {None}
    assert margin == 60.0


# ─── Batched FX ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_rates_for_periods_matches_the_single_period_rules():
    rows = [
        ("NOK", date(2023, 1, 2), D("11.0"), D("0.090")),
        ("NOK", date(2023, 1, 3), D("12.0"), D("0.080")),
        ("GBP", date(2023, 1, 2), D("0.80"), D("1.25")),
        ("NOK", date(2024, 1, 2), D("99"), D("0.01")),  # outside the period
    ]
    db = MagicMock()
    db.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=rows)))
    p = ("NOK", date(2023, 1, 1), date(2023, 1, 31))
    e = ("EUR", date(2023, 1, 1), date(2023, 1, 31))
    out = await ExchangeRateService(db).rates_for_periods({p, e}, ["EUR", "NOK", "GBP"])
    assert out[(*p, "EUR")] == D("0.085000")  # mean inverse rate
    assert out[(*p, "NOK")] == D("1")
    assert out[(*p, "GBP")] == D("0.072000")  # cross on common days only: 0.090 × 0.80
    assert out[(*e, "NOK")] == D("11.500000")  # mean rate
    db.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_rates_for_periods_without_rates_is_none():
    db = MagicMock()
    db.execute = AsyncMock(return_value=MagicMock(all=MagicMock(return_value=[])))
    p = ("GBP", date(2008, 1, 1), date(2008, 12, 31))
    out = await ExchangeRateService(db).rates_for_periods({p}, ["EUR"])
    assert out == {(*p, "EUR"): None}


# ─── Payload ──────────────────────────────────────────────────────────────────


def farm_row(
    entity, farm_id, name, mw, kind="onshore", zone="NO3", entity_name="Entity AS", cod=None
):
    return SimpleNamespace(
        entity_id=entity,
        entity_name=entity_name,
        id=farm_id,
        name=name,
        nameplate_capacity_mw=mw,
        location_type=kind,
        cod=cod,
        country_code="NOR",
        bidzone_name=zone,
    )


@pytest.mark.asyncio
async def test_payload_one_entry_per_entity_aligned_to_years():
    farms = [
        farm_row(1, 11, "Fosen A", 100.0, entity_name="Fosen Vind DA"),
        farm_row(1, 12, "Fosen B", 50.0, kind="offshore", entity_name="Fosen Vind DA"),
        farm_row(2, 21, "Solo", 30.0, zone="NO2"),
    ]
    filings = [
        filing(1, entity=1, start=date(2022, 1, 1), end=date(2022, 12, 31)),
        filing(2, entity=2, start=date(2023, 4, 1), end=date(2024, 3, 31), currency="NOK"),
    ]
    gen = {1: GenerationStat(D("10000"), 100.0, "NVE"), 2: GenerationStat(D("1000"), 100.0, "NVE")}

    async def rates(_self, periods, targets):
        return {
            (c, s, e, t): D("1") if c == t else (D("0.1") if t == "EUR" else D("0.08"))
            for c, s, e in periods
            for t in targets
        }

    svc = FinancialComparisonService(MagicMock(execute=AsyncMock()))
    gen_mock = AsyncMock(return_value=gen)
    with patch.object(svc, "_farms", AsyncMock(return_value=farms)), patch.object(
        svc, "_filings", AsyncMock(return_value=filings)
    ), patch.object(svc, "_linked_farm_counts", AsyncMock(return_value={1: 2, 2: 1})), patch.object(
        fcs, "generation_for_filings", gen_mock
    ), patch.object(
        ExchangeRateService, "rates_for_periods", rates
    ), patch.object(
        fcs.ComparisonAnalyticsService,
        "owners",
        AsyncMock(return_value={11: [{"name": "Statkraft", "pct": 50}]}),
    ):
        data = await svc.get_financial()

    assert gen_mock.await_args.args[2] == {1: 2, 2: 1}  # coverage over every linked farm
    assert data["years"] == [2022, 2023]
    fosen, solo = data["entries"]
    assert fosen["name"] == "Fosen Vind DA" and fosen["combined"] is True
    assert fosen["capacity_mw"] == 150.0 and fosen["type"] == "mixed"
    assert [f["id"] for f in fosen["farms"]] == [11, 12]
    assert fosen["owners"] == [{"name": "Statkraft"}]
    assert fosen["rev"]["EUR"] == [100.0, None] and fosen["periods"] == ["2022-01/2022-12", None]
    assert solo["name"] == "Solo" and solo["combined"] is False and solo["zone"] == "NO2"
    # NOK filing: 1,000,000 NOK × 0.1 / 1,000 MWh in EUR; native NOK at rate 1.
    assert solo["rev"]["EUR"] == [None, 100.0] and solo["rev"]["NOK"] == [None, 1000.0]
    assert solo["margin"] == [None, 60.0] and solo["currency"] == [None, "NOK"]
    assert solo["gen_basis"] == [None, "metered"]


@pytest.mark.asyncio
async def test_payload_empty_without_entities():
    svc = FinancialComparisonService(MagicMock(execute=AsyncMock()))
    with patch.object(svc, "_farms", AsyncMock(return_value=[])):
        assert await svc.get_financial() == {"years": [], "entries": []}


# ─── Endpoint ─────────────────────────────────────────────────────────────────


@pytest.fixture
def api(client):
    async def _user():
        return User(id=1, email="a@b.c", username="a", hashed_password="x", is_active=True)

    client.app.dependency_overrides[get_current_active_user] = _user
    with patch.object(ep, "get_cached_report", new=AsyncMock(return_value=None)), patch.object(
        ep, "cache_report", new=AsyncMock()
    ):
        yield client
    client.app.dependency_overrides.pop(get_current_active_user, None)


def test_financial_requires_auth(client):
    assert client.get("/api/v1/performance-analytics/financial").status_code in (401, 403)


def test_financial_caches_the_payload(api):
    with patch.object(
        ep.FinancialComparisonService, "get_financial", new=AsyncMock(return_value={"years": []})
    ) as svc:
        assert api.get("/api/v1/performance-analytics/financial").json() == {"years": []}
    svc.assert_awaited_once()
    assert ep.cache_report.await_args.args[0] == "perf-analytics:fin:v2"

    ep.get_cached_report.return_value = {"cached": True}
    with patch.object(ep.FinancialComparisonService, "get_financial", new=AsyncMock()) as svc:
        assert api.get("/api/v1/performance-analytics/financial").json() == {"cached": True}
    svc.assert_not_called()
