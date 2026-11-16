"""The value-ledger document builder: each pipeline sample's two tables give its document (within 1e-6), the
sums hold (format rules R2-R5) and the document fits the response model."""

import csv
import json
from pathlib import Path

import pytest

from app.schemas.scada_ledger import LedgerDocument
from app.services.scada_ledger_service import build_document, status_keys

SAMPLES = Path(__file__).resolve().parent / "fixtures" / "value_ledger"
BUILDS = ("hot-b", "rg2-b", "hot-c")


def sample(build: str):
    """(cause rows, run row, document) of one sample, the rows typed as the database returns them."""
    d = SAMPLES / build
    with (d / "cause_rows.csv").open(newline="") as f:
        rows = [
            {
                **r,
                "year": int(r["year"]),
                "value": float(r["value"]),
                "claimed_mwh": float(r["claimed_mwh"]) if r["claimed_mwh"] else None,
                "claims": int(r["claims"]) if r["claims"] else None,
                "summed": r["summed"] == "true",
            }
            for r in csv.DictReader(f)
        ]
    return (
        rows,
        json.loads((d / "run_row.json").read_text()),
        json.loads((d / "document.json").read_text()),
    )


def close(a, b) -> bool:
    """Equal, numbers within 1e-6."""
    if isinstance(a, dict):
        return isinstance(b, dict) and a.keys() == b.keys() and all(close(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return isinstance(b, list) and len(a) == len(b) and all(map(close, a, b))
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and abs(a - b) <= 1e-6
    return a == b


@pytest.mark.parametrize("build", BUILDS)
def test_the_builder_gives_the_sample_document(build):
    rows, run, doc = sample(build)
    assert close(build_document(rows, run), doc)


@pytest.mark.parametrize("build", BUILDS)
def test_the_document_adds_up(build):
    rows, run, _ = sample(build)
    doc = build_document(rows, run)
    years, head = run["years"], run["headline_year"]
    summed = {c["code"]: c["summed"] for c in run["catalog"]["causes"]}
    assert sum(r["value"] for r in rows if r["summed"]) / years == pytest.approx(
        head, abs=1.0
    )  # R2
    assert sum(r["value_year"] for r in doc["by_cause"] if summed[r["cause"]]) == pytest.approx(
        head, abs=1.0
    )  # R3
    assert sum(r["value"] for r in doc["by_year"]) == pytest.approx(
        sum(r["value"] for r in rows), abs=0.01
    )  # R4
    turbines = sum(r["value"] for r in rows if r["turbine"] != "FLEET")
    assert sum(r["value_year"] for r in doc["by_turbine"]) * years == pytest.approx(
        turbines, abs=0.01
    )  # R5
    assert "catalog" not in doc["run"] and doc["causes"] == run["catalog"]["causes"]


@pytest.mark.parametrize("build", BUILDS)
def test_the_document_fits_the_response_model(build):
    _, _, doc = sample(build)
    assert close(LedgerDocument.model_validate(doc).model_dump(), doc)


def test_status_keys_are_the_catalogs():
    _, run, _ = sample("rg2-b")
    keys = status_keys(run["catalog"])
    assert (
        "MKT_02",
        "FLEET",
        "RECOVERABLE",
    ) in keys  # a company-wide line keeps the register's key
    assert all(scope == "FLEET" or scope.startswith("VL:") for _, scope, _ in keys)
