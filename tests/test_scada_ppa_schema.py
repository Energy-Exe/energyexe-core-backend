"""Pure, DB-free tests for the SCADA PPA enums and validators (EPR-97).

The enums are plain ``str, Enum`` types stored in plain String columns (house convention — no
Postgres enum, no CHECK constraint), so the only thing standing between a typo and the database is
the Pydantic layer. These pin the exact values EPR-97 specifies, and the cross-field rules.
"""

import pytest
from pydantic import ValidationError

from app.models.scada_ppa import (
    CounterpartyRole,
    IndexationType,
    PpaStatus,
    PricingModel,
    ScadaPpa,
    SettlementMechanism,
    VolumeShape,
)
from app.schemas.scada_ppa import ScadaPpaCreate, ScadaPpaUpdate

_MINIMAL = dict(ppa_code="PPA-2026-001", ppa_buyer="Statkraft", windfarm_ids=[7309])


# ─── enums: the ticket's values, verbatim ────────────────────────────────────


@pytest.mark.parametrize(
    "enum_cls,expected",
    [
        (
            CounterpartyRole,
            {"Ultimate_Offtaker", "RTM", "Trader", "Utility_Supplier", "Unknown"},
        ),
        (SettlementMechanism, {"Physical", "Financial_CfD", "Route_to_Market"}),
        (VolumeShape, {"Pay_as_Produced", "Pay_as_Nominated", "Baseload"}),
        (PpaStatus, {"Draft", "Active", "Superseded", "Terminated", "Expired"}),
        (PricingModel, {"Fixed", "Indexed", "Collar"}),
        (IndexationType, {"None", "CPI", "RPI", "Fixed_Pct"}),
    ],
)
def test_enum_values_match_the_ticket(enum_cls, expected):
    assert {m.value for m in enum_cls} == expected


def test_superseded_status_exists():
    """EPR-97 calls this out specifically: without it, PPAs replaced mid-life cannot be told apart
    from those that ran to term, which distorts historical price-capture analysis."""
    assert PpaStatus.SUPERSEDED.value == "Superseded"


# ─── table shape ─────────────────────────────────────────────────────────────


def test_table_is_public_scada_prefixed_and_separate_from_perform():
    from app.models.ppa import PPA

    assert ScadaPpa.__tablename__ == "scada_ppa"
    assert ScadaPpa.__table__.schema is None, "app-authored SCADA state lives in public"
    assert PPA.__tablename__ == "ppas", "the Perform table must be untouched"


def test_natural_key_is_code_plus_windfarm():
    uniques = [
        c for c in ScadaPpa.__table__.constraints if c.__class__.__name__ == "UniqueConstraint"
    ]
    assert len(uniques) == 1
    assert {c.name for c in uniques[0].columns} == {"ppa_code", "windfarm_id"}


def test_windfarm_model_has_no_backref_to_scada_ppa():
    """Isolation guard: adding a Windfarm.scada_ppas relationship would edit the core model."""
    from app.models.windfarm import Windfarm

    assert "scada_ppas" not in Windfarm.__mapper__.relationships


# ─── validators ──────────────────────────────────────────────────────────────


def test_currency_is_normalised_to_upper():
    assert ScadaPpaCreate(**_MINIMAL, currency="gbp").currency == "GBP"
    assert ScadaPpaUpdate(currency="nok").currency == "NOK"


@pytest.mark.parametrize("bad", ["GB", "GBPP", "12A", "G8P"])
def test_currency_must_be_three_letters(bad):
    with pytest.raises(ValidationError):
        ScadaPpaCreate(**_MINIMAL, currency=bad)


def test_expiration_must_follow_effective():
    with pytest.raises(ValidationError):
        ScadaPpaCreate(**_MINIMAL, effective_date="2026-01-01", expiration_date="2025-01-01")
    with pytest.raises(ValidationError):
        ScadaPpaCreate(**_MINIMAL, effective_date="2026-01-01", expiration_date="2026-01-01")
    ok = ScadaPpaCreate(**_MINIMAL, effective_date="2026-01-01", expiration_date="2032-12-31")
    assert ok.expiration_date.year == 2032


def test_floor_must_not_exceed_cap():
    with pytest.raises(ValidationError):
        ScadaPpaCreate(**_MINIMAL, floor_price="65.00", cap_price="40.00")
    assert ScadaPpaCreate(**_MINIMAL, floor_price="40.00", cap_price="65.00").cap_price == 65


@pytest.mark.parametrize("pct", [0, -1, 100.01, 150])
def test_power_share_pct_bounds(pct):
    with pytest.raises(ValidationError):
        ScadaPpaCreate(**_MINIMAL, power_share_pct=pct)


def test_power_share_pct_accepts_full_offtake():
    assert ScadaPpaCreate(**_MINIMAL, power_share_pct=100).power_share_pct == 100


@pytest.mark.parametrize("pct", [-1, 100.01])
def test_availability_guarantee_bounds(pct):
    with pytest.raises(ValidationError):
        ScadaPpaCreate(**_MINIMAL, availability_guarantee_pct=pct)


def test_index_spread_may_be_negative():
    """Signed spread over/under the index — negative means a discount."""
    assert ScadaPpaCreate(**_MINIMAL, index_spread="-3.50").index_spread == -3.5


def test_windfarm_ids_must_be_unique_and_non_empty():
    with pytest.raises(ValidationError):
        ScadaPpaCreate(ppa_code="X", ppa_buyer="B", windfarm_ids=[7309, 7309])
    with pytest.raises(ValidationError):
        ScadaPpaCreate(ppa_code="X", ppa_buyer="B", windfarm_ids=[])


def test_status_defaults_to_draft_so_partial_records_are_saveable():
    assert ScadaPpaCreate(**_MINIMAL).ppa_status == "Draft"


def test_incomplete_contract_is_accepted():
    """ppa_notes is load-bearing precisely because structure varies; a Fixed PPA with no strike yet
    must still save. The UI hints at the missing field, the API does not reject it."""
    row = ScadaPpaCreate(**_MINIMAL, pricing_model="Fixed")
    assert row.strike_price is None


def test_update_is_fully_optional():
    """exclude_unset is what keeps a per-row edit from clobbering the sibling farm legs' terms."""
    patch = ScadaPpaUpdate(power_share_pct="30.00")
    assert patch.model_dump(exclude_unset=True) == {"power_share_pct": 30}
