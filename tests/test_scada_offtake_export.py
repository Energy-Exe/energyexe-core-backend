"""EPR-138 private snapshot boundaries and reporting currency regressions."""

import copy
import json
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.models.exchange_rate import ExchangeRate
from app.models.scada_ppa import ScadaPpa
from app.services.scada_offtake_export import (
    ExportRequest,
    ScadaOfftakeExporter,
    empty_snapshot,
    native,
    seal_snapshot,
    select_reporting_currency,
    term_periods,
)
from app.services.scada_private_files import private_output_root, write_private_json
from scripts.export_scada_offtake import explicit_source, export_snapshot, main


def request(**kwargs):
    return ExportRequest(
        **{
            "farm": "test-farm",
            "owner_user_id": 7,
            "period_start": date(2024, 1, 1),
            "period_end": date(2025, 12, 31),
            **kwargs,
        }
    )


def term(**kwargs):
    return {
        "id": 3,
        "created_by_id": 7,
        "windfarm_id": 42,
        "ppa_status": "Active",
        "currency": "NOK",
        "power_share_pct": "50.00",
        "strike_price": "123.45",
        "effective_date": "2024-07-01",
        "expiration_date": "2025-03-31",
        **kwargs,
    }


def filing(id=1, **kwargs):
    return {
        "id": id,
        "financial_entity_id": id,
        "period_end": "2025-12-31",
        "period_start": "2025-01-01",
        "currency": "NOK",
        **kwargs,
    }


def test_currency_latest_across_all_entities_uses_end_then_start():
    rows = [
        filing(1, currency="GBP", period_start="2025-07-01", period_end="2025-09-30"),
        filing(2, currency="NOK"),
        filing(3, currency="DKK", period_start="2025-02-01"),
    ]
    result = select_reporting_currency(None, rows, [{"currency": "USD"}], [])
    assert result["currency"] == "DKK"
    assert result["provenance"]["latest_financial_filing_ids"] == [3]


@pytest.mark.parametrize("currencies", [("GBP", "NOK"), ("CHF", "SEK"), (None, "EUR")])
def test_tied_conflicting_reported_currency_requires_override(currencies):
    filings = [filing(1, currency=currencies[0]), filing(2, currency=currencies[1])]
    blocked = select_reporting_currency(None, filings, [{"currency": "EUR"}], [])
    assert blocked["currency"] is None
    assert blocked["blocked_reasons"] == [
        "conflicting_latest_financial_currencies_override_required"
    ]
    assert select_reporting_currency("USD", filings, [], [term()])["currency"] == "USD"


def test_unsupported_reported_currency_uses_eur_but_absent_currency_blocks():
    result = select_reporting_currency(None, [filing(currency="CHF")], [], [])
    assert result["currency"] == "EUR"
    assert result["provenance"]["fallback_reason"] == "unsupported_reported_currency_uses_eur"
    assert select_reporting_currency(None, [filing(currency=None)], [], [])["blocked_reasons"]


def test_non_gbp_cannot_target_gbp_explicitly_or_automatically():
    explicit = select_reporting_currency("GBP", [], [], [term()])
    assert explicit["currency"] is None
    assert explicit["blocked_reasons"] == ["non_gbp_contract_cannot_target_gbp"]
    automatic = select_reporting_currency(None, [filing(currency="GBP")], [], [term()])
    assert automatic["currency"] == "EUR"
    assert (
        automatic["provenance"]["fallback_reason"] == "non_gbp_contract_cannot_target_gbp_uses_eur"
    )


def test_market_source_currency_uses_observed_rows_and_blocks_mixed_sources():
    observed = [{"source": "ENTSOE", "currency": "EUR"}]
    assert select_reporting_currency(None, [], observed, [])["currency"] == "EUR"
    assert (
        select_reporting_currency(
            None, [], observed + [{"source": "ELEXON", "currency": "GBP"}], []
        )["currency"]
        is None
    )
    assert select_reporting_currency(None, [], [], [])["blocked_reasons"] == [
        "market_price_currency_unavailable"
    ]


def test_periods_clip_exact_inclusive_dates_without_year_fallback():
    assert term_periods(term(), date(2024, 8, 12), date(2025, 7, 1)) == [
        ("calendar_year", date(2024, 8, 12), date(2024, 12, 31)),
        ("calendar_year", date(2025, 1, 1), date(2025, 3, 31)),
        ("full_period", date(2024, 8, 12), date(2025, 3, 31)),
    ]
    assert (
        term_periods(
            term(effective_date="2026-01-01", expiration_date="2026-12-31"),
            date(2024, 1, 1),
            date(2025, 12, 31),
        )
        == []
    )
    assert term_periods(term(effective_date=None), date(2024, 1, 1), date(2025, 12, 31)) == []


async def test_complete_pagination_preserves_decimal_and_owner_isolation():
    service = ScadaOfftakeExporter(AsyncMock())
    rows = [
        ScadaPpa(
            id=i,
            created_by_id=7,
            windfarm_id=42,
            ppa_status="Active",
            currency="NOK",
            strike_price=Decimal("123.45"),
        )
        for i in range(201)
    ]

    async def scoped_page(**kwargs):
        assert kwargs["ppa_status"] == "Active"
        assert kwargs["windfarm_id"] == 42
        source = rows if kwargs["user_id"] == 7 else []
        start = kwargs["offset"]
        return source[start : start + kwargs["limit"]], len(source)

    service.ppas.list_ppas = AsyncMock(side_effect=scoped_page)
    result = await service.all_active_terms(7, 42)
    assert len(result) == 201
    assert result[0]["strike_price"] == "123.45"
    assert result[0]["power_share_pct"] is None
    assert result[0]["created_by_id"] == 7
    assert await service.all_active_terms(8, 42) == []
    assert [call.kwargs["offset"] for call in service.ppas.list_ppas.call_args_list] == [0, 200, 0]


@pytest.mark.parametrize(
    "scenario", ["foreign_owner", "wrong_farm", "wrong_status", "short_page", "duplicate"]
)
async def test_invalid_service_page_cannot_become_no_terms(scenario):
    service = ScadaOfftakeExporter(AsyncMock())
    row = ScadaPpa(id=1, created_by_id=7, windfarm_id=42, ppa_status="Active")
    if scenario == "foreign_owner":
        row.created_by_id = 8
    elif scenario == "wrong_farm":
        row.windfarm_id = 99
    elif scenario == "wrong_status":
        row.ppa_status = "Expired"
    page = [] if scenario == "short_page" else [row, row] if scenario == "duplicate" else [row]
    service.ppas.list_ppas = AsyncMock(return_value=(page, 2 if scenario == "duplicate" else 1))
    with pytest.raises(ValueError):
        await service.all_active_terms(7, 42)


async def test_access_failure_propagates_from_terms_reader():
    service = ScadaOfftakeExporter(AsyncMock())
    service.ppas.list_ppas = AsyncMock(side_effect=ConnectionError("cannot read"))
    with pytest.raises(ConnectionError):
        await service.all_active_terms(7, 42)


@pytest.mark.parametrize(
    "farm,reason",
    [
        (None, "farm_mapping_missing"),
        (
            {
                "farm": "test-farm",
                "windfarm_id": None,
                "timezone": "Europe/London",
                "pipeline_version": "1",
                "computed_at": None,
            },
            "farm_mapping_missing",
        ),
        (
            {
                "farm": "test-farm",
                "windfarm_id": 42,
                "timezone": None,
                "pipeline_version": "1",
                "computed_at": None,
            },
            "farm_timezone_missing_or_invalid",
        ),
    ],
)
async def test_missing_mapping_or_timezone_is_blocked(farm, reason):
    db = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = farm
    db.execute.return_value = result
    service = ScadaOfftakeExporter(db)
    service.ppas.list_ppas = AsyncMock()
    snapshot = await service.build_snapshot(request(), {})
    assert snapshot["readiness"]["status"] == "blocked"
    assert snapshot["readiness"]["blocked_reasons"] == [reason]
    service.ppas.list_ppas.assert_not_called()


async def test_no_terms_is_successful_scoped_read_not_access_failure():
    db = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.one_or_none.return_value = {
        "farm": "test-farm",
        "windfarm_id": 42,
        "timezone": "Europe/London",
        "pipeline_version": "1",
        "computed_at": None,
    }
    db.execute.return_value = result
    service = ScadaOfftakeExporter(db)
    service.all_active_terms = AsyncMock(return_value=[])
    service.financials.get_by_windfarm = AsyncMock(return_value=[])
    service.observed_market_sources = AsyncMock(
        return_value=[{"source": "actual", "currency": "EUR"}]
    )
    snapshot = await service.build_snapshot(request(), {})
    assert snapshot["readiness"]["status"] == "no_terms"
    assert snapshot["readiness"]["ppa_monetary_activation"] is False


async def test_market_window_uses_local_timezone_and_exclusive_next_day():
    db = AsyncMock()
    result = MagicMock()
    result.mappings.return_value.all.return_value = []
    db.execute.return_value = result
    service = ScadaOfftakeExporter(db)
    await service.observed_market_sources(
        42, request(period_start=date(2024, 7, 1), period_end=date(2024, 7, 1)), "Europe/London"
    )
    params = db.execute.call_args.args[1]
    assert params["first"] == datetime(2024, 6, 30, 23, tzinfo=timezone.utc)
    assert params["last"] == datetime(2024, 7, 1, 23, tzinfo=timezone.utc)


async def test_fx_reuses_service_keeps_native_amounts_and_reference_label():
    service = ScadaOfftakeExporter(AsyncMock())
    service.rates.get_rate_for_period = AsyncMock(return_value=Decimal("0.087654"))
    service.fx_observations = AsyncMock(return_value=[{"id": 9, "source": "ECB"}])
    original = term()
    schedule, reasons = await service.fx_schedule([original], "EUR", request())
    assert not reasons
    assert len(schedule) == 3
    assert original["strike_price"] == "123.45"
    assert schedule[0]["rate"] == "0.087654"
    reference = schedule[0]["nominal_price_references"][0]
    assert reference["original_amount"] == "123.45"
    assert reference["reporting_amount"] == str(Decimal("123.45") * Decimal("0.087654"))
    assert reference["label"] == "reporting reference; not received PPA revenue"
    assert service.rates.get_rate_for_period.call_args_list[0].args == (
        "NOK",
        "EUR",
        date(2024, 7, 1),
        date(2024, 12, 31),
    )


async def test_missing_fx_remains_unavailable_and_missing_fields_are_preserved():
    service = ScadaOfftakeExporter(AsyncMock())
    service.rates.get_rate_for_period = AsyncMock(return_value=None)
    service.fx_observations = AsyncMock(return_value=[])
    schedule, reasons = await service.fx_schedule([term(power_share_pct=None)], "EUR", request())
    assert schedule[0]["status"] == "unavailable"
    assert schedule[0]["nominal_price_references"][0]["reporting_amount"] is None
    assert "term:3:power_share_missing" in reasons
    assert any("fx_unavailable" in reason for reason in reasons)
    schedule, reasons = await service.fx_schedule(
        [term(currency=None, expiration_date=None)], "EUR", request()
    )
    assert schedule == []
    assert reasons == ["term:3:currency_missing", "term:3:contract_dates_missing"]


async def test_cross_currency_observations_only_include_matching_days():
    db = AsyncMock()
    result = MagicMock()
    result.scalars.return_value.all.return_value = [
        ExchangeRate(
            id=1,
            base_currency="EUR",
            quote_currency="NOK",
            rate_date=date(2024, 1, 1),
            rate=Decimal("10"),
            inverse_rate=Decimal("0.1"),
        ),
        ExchangeRate(
            id=2,
            base_currency="EUR",
            quote_currency="USD",
            rate_date=date(2024, 1, 1),
            rate=Decimal("1.2"),
            inverse_rate=Decimal("0.833333"),
        ),
        ExchangeRate(
            id=3,
            base_currency="EUR",
            quote_currency="NOK",
            rate_date=date(2024, 1, 2),
            rate=Decimal("12"),
            inverse_rate=Decimal("0.083333"),
        ),
    ]
    db.execute.return_value = result
    rows = await ScadaOfftakeExporter(db).fx_observations(
        "NOK", "USD", date(2024, 1, 1), date(2024, 1, 2)
    )
    assert [row["id"] for row in rows] == [1, 2]


def test_identity_changes_for_material_inputs_but_not_extraction_time():
    snapshot = empty_snapshot(
        request(),
        {"backend_revision": "abc", "transaction": {"snapshot": "one"}},
        blocked_reason="test",
    )
    snapshot["original_terms"] = [term()]
    original = seal_snapshot(snapshot)
    volatile = copy.deepcopy(original)
    volatile["exported_at"] = "different"
    volatile["source"]["transaction"] = {"snapshot": "two"}
    assert seal_snapshot(volatile)["snapshot_id"] == original["snapshot_id"]
    for key, changed_value in [
        ("owner_user_id", 8),
        ("suite", {"version": "next"}),
        ("fx_schedule", [{"rate": "0.1"}]),
        ("original_terms", [term(strike_price="123.46")]),
    ]:
        changed = copy.deepcopy(original)
        changed[key] = changed_value
        assert seal_snapshot(changed)["snapshot_id"] != original["snapshot_id"]
    assert json.loads(json.dumps(native({"value": Decimal("1.0100")})))["value"] == "1.0100"
    with pytest.raises(ValueError):
        native(Decimal("NaN"))


def test_explicit_source_never_defaults_to_application_database(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://app-secret@example.invalid/production")
    with pytest.raises(ValueError):
        explicit_source(None)
    with pytest.raises(ValueError):
        explicit_source("sqlite:///anything")
    url, provenance = explicit_source("postgresql://user:private-secret@localhost:9999/database")
    assert url.drivername == "postgresql+asyncpg"
    assert "private-secret" not in json.dumps(provenance)
    with pytest.raises(ValueError, match="Invalid explicit"):
        explicit_source("not-a-url private-secret")
    url, _ = explicit_source("postgresql://host/db?sslmode=require")
    assert url.query == {"ssl": "require"}


def test_private_paths_symlinks_sync_roots_permissions_and_no_overwrite(tmp_path, monkeypatch):
    shared = tmp_path / "shared"
    shared.mkdir()
    link = tmp_path / "alias"
    link.symlink_to(shared, target_is_directory=True)
    monkeypatch.setenv("ENERGYEXE_OPP_ROOT", str(shared))
    with pytest.raises(ValueError):
        private_output_root(link / "private")
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / ".git").write_text("gitdir: elsewhere")
    with pytest.raises(ValueError):
        private_output_root(repo / "ignored")
    public = tmp_path / "public"
    public.mkdir(mode=0o755)
    with pytest.raises(ValueError):
        private_output_root(public)
    assert public.stat().st_mode & 0o777 == 0o755
    root = private_output_root(tmp_path / "private" / "nested")
    assert root.stat().st_mode & 0o777 == 0o700
    assert root.parent.stat().st_mode & 0o777 == 0o700
    path = write_private_json(root, "snapshot.json", {"private": True})
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        write_private_json(root, "snapshot.json", {})
    destination = root / "linked.json"
    destination.symlink_to(path)
    with pytest.raises(FileExistsError):
        write_private_json(root, "linked.json", {})


async def test_source_session_readonly_repeatable_and_failure_is_redacted(monkeypatch):
    import scripts.export_scada_offtake as command

    engine = MagicMock()
    engine.dispose = AsyncMock()
    create = MagicMock(return_value=engine)
    monkeypatch.setattr(command, "create_async_engine", create)
    db = MagicMock()
    db.execute = AsyncMock(side_effect=ConnectionError("password=private-secret"))
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=db)
    session.__aexit__ = AsyncMock(return_value=False)
    db.begin.return_value.__aenter__ = AsyncMock()
    db.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(command, "AsyncSession", MagicMock(return_value=session))
    snapshot = await export_snapshot(request(), "postgresql+asyncpg://example.invalid/db", {})
    assert create.call_args.kwargs["isolation_level"] == "REPEATABLE READ"
    assert (
        create.call_args.kwargs["connect_args"]["server_settings"]["default_transaction_read_only"]
        == "on"
    )
    assert (
        str(db.execute.call_args.args[0])
        == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    )
    assert snapshot["readiness"]["blocked_reasons"] == ["source_access_or_read_failure"]
    assert "private-secret" not in json.dumps(snapshot)
    engine.dispose.assert_awaited_once()


async def test_successful_source_transaction_checks_isolation_before_scoped_read(monkeypatch):
    import scripts.export_scada_offtake as command

    engine = MagicMock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(command, "create_async_engine", MagicMock(return_value=engine))
    db = MagicMock()
    metadata = MagicMock()
    metadata.mappings.return_value.one.return_value = {
        "read_only": "on",
        "isolation": "repeatable read",
        "snapshot": "1:2:",
        "started_at": "2025-01-01T00:00:00Z",
    }
    db.execute = AsyncMock(side_effect=[None, None, metadata])
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=db)
    session.__aexit__ = AsyncMock(return_value=False)
    db.begin.return_value.__aenter__ = AsyncMock()
    db.begin.return_value.__aexit__ = AsyncMock(return_value=False)
    monkeypatch.setattr(command, "AsyncSession", MagicMock(return_value=session))
    collector = MagicMock()
    collector.build_snapshot = AsyncMock(return_value={"success": True})
    monkeypatch.setattr(command, "ScadaOfftakeExporter", MagicMock(return_value=collector))
    assert await export_snapshot(request(), "postgresql+asyncpg://example.invalid/db", {}) == {
        "success": True
    }
    assert (
        str(db.execute.call_args_list[0].args[0])
        == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    )
    assert str(db.execute.call_args_list[1].args[0]) == "SET LOCAL search_path TO public"
    assert "current_setting('transaction_read_only')" in str(db.execute.call_args_list[2].args[0])
    assert collector.build_snapshot.call_args.args[1]["transaction"]["read_only"] == "on"
    engine.dispose.assert_awaited_once()


async def test_ecb_actual_queries_use_daily_matching_six_decimals_and_no_filling():
    """Exercise the unchanged platform service SQL against actual local rows."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(ExchangeRate.__table__.create)
    async with AsyncSession(engine) as db:
        db.add_all(
            [
                ExchangeRate(
                    base_currency="EUR",
                    quote_currency="NOK",
                    rate_date=date(2024, 1, 1),
                    rate=Decimal("10"),
                    inverse_rate=Decimal("0.1"),
                    source="ECB",
                ),
                ExchangeRate(
                    base_currency="EUR",
                    quote_currency="USD",
                    rate_date=date(2024, 1, 1),
                    rate=Decimal("1.2"),
                    inverse_rate=Decimal("0.833333"),
                    source="ECB",
                ),
                # This unmatched source observation must not influence NOK->USD.
                ExchangeRate(
                    base_currency="EUR",
                    quote_currency="NOK",
                    rate_date=date(2024, 1, 2),
                    rate=Decimal("20"),
                    inverse_rate=Decimal("0.05"),
                    source="ECB",
                ),
            ]
        )
        await db.flush()
        exporter = ScadaOfftakeExporter(db)
        rates = exporter.rates
        assert await rates.get_rate_for_period(
            "NOK", "USD", date(2024, 1, 1), date(2024, 1, 2)
        ) == Decimal("0.120000")
        assert await rates.get_rate_for_period(
            "NOK", "EUR", date(2024, 1, 1), date(2024, 1, 2)
        ) == Decimal("0.075000")
        assert await rates.get_rate_for_period(
            "EUR", "NOK", date(2024, 1, 1), date(2024, 1, 2)
        ) == Decimal("15.000000")
        assert await rates.get_rate_for_period(
            "NOK", "NOK", date(2024, 1, 1), date(2024, 1, 2)
        ) == Decimal("1")
        assert (
            await rates.get_rate_for_period("NOK", "USD", date(2024, 1, 2), date(2024, 1, 2))
            is None
        )
        assert (
            await rates.get_rate_for_period("NOK", "EUR", date(2024, 1, 6), date(2024, 1, 7))
            is None
        )
        observations = await exporter.fx_observations(
            "NOK", "USD", date(2024, 1, 1), date(2024, 1, 2)
        )
        assert len(observations) == 2
        assert {row["rate_date"] for row in observations} == {"2024-01-01"}
    await engine.dispose()


def test_missing_source_cli_is_blocked_without_creating_artifact(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("SCADA_OFFTAKE_SOURCE_DATABASE_URL", raising=False)
    destination = tmp_path / "private"
    assert (
        main(
            [
                "--farm",
                "farm",
                "--owner-user-id",
                "7",
                "--from",
                "2025-01-01",
                "--to",
                "2025-12-31",
                "--output-root",
                str(destination),
            ]
        )
        == 2
    )
    assert not destination.exists()
    assert "Export blocked" in capsys.readouterr().err
