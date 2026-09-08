"""Pure helpers behind GET /scada/opportunities/by-year (EPR-131): the per-(series, year) class
totals and the comma-list split. No DB — the SQLite test suite cannot run scada.* SQL, and these
totals are computed in Python precisely so chart, table and CSV reconcile by construction."""

from decimal import Decimal

from app.services.scada_opportunity_service import split_csv, summarise_by_year


def _row(year, trigger, cls, real, cp, partial=False):
    return {
        "year": year,
        "trigger": trigger,
        "cls": cls,
        "gbp_real": real,
        "gbp_constant_price": cp,
        "partial_year": partial,
    }


def test_summarise_by_year_totals_decimals_and_nulls():
    rows = [
        _row(2016, "OPS_01", "REALIZED", Decimal("29883"), Decimal("55995"), partial=True),
        _row(2016, "MKT_03", "CURTAILMENT", Decimal("45060"), None, partial=True),
        _row(2016, "OPS_14", "RECOVERABLE", Decimal("43097"), Decimal("43097"), partial=True),
        _row(2017, "OPS_01", "REALIZED", Decimal("192318"), Decimal("344040")),
        _row(2017, "MKT_03", "CURTAILMENT", Decimal("90421"), None),
        _row(2017, "OPS_02", "REALIZED", Decimal("-4332"), Decimal("0")),
    ]
    out = summarise_by_year(rows)
    # real first, then constant-price; years ascending within a series
    assert [(s["series"], s["year"]) for s in out] == [
        ("real", 2016),
        ("real", 2017),
        ("constant_price", 2016),
        ("constant_price", 2017),
    ]
    by = {(s["series"], s["year"]): s for s in out}
    r16 = by[("real", 2016)]
    assert (
        r16["realized"] == 29883.0
        and r16["curtailment"] == 45060.0
        and r16["recoverable"] == 43097.0
    )
    assert r16["total"] == 29883.0 + 45060.0 + 43097.0 and r16["partial_year"] is True
    assert isinstance(r16["total"], float)  # Decimal coerced, JSON-safe
    r17 = by[("real", 2017)]
    assert r17["realized"] == 192318.0 - 4332.0 and r17["partial_year"] is False
    # null constant-price lines (market signals) contribute 0, never poison the total
    c17 = by[("constant_price", 2017)]
    assert c17["curtailment"] == 0.0 and c17["realized"] == 344040.0 and c17["total"] == 344040.0


def test_summarise_by_year_ignores_unknown_classes_and_handles_empty():
    assert summarise_by_year([]) == []
    out = summarise_by_year(
        [_row(2020, "X", "CONTEXT", 5, 5), _row(2020, "OPS_01", "REALIZED", 1, 2)]
    )
    by = {(s["series"], s["year"]): s for s in out}
    assert by[("real", 2020)]["total"] == 1.0 and by[("constant_price", 2020)]["total"] == 2.0


def test_split_csv():
    assert split_csv("OPS_08,OPS_09, OPS_12 ,,OPS_13,OPS_14") == [
        "OPS_08",
        "OPS_09",
        "OPS_12",
        "OPS_13",
        "OPS_14",
    ]
    assert split_csv("") == [] and split_csv(None) == []
