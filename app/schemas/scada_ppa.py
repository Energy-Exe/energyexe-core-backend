"""Pydantic schemas for the SCADA PPA structure (EPR-97).

Separate from ``app/schemas/ppa.py`` (the Perform PPA) by design — the two never share types.

Enum values are validated here rather than in the DB (house convention: plain varchar columns +
Python-side validation). Cross-field rules are deliberately narrow: they catch data that is provably
wrong (a contract ending before it starts, a floor above its cap, a percentage out of range) but do
NOT require a complete contract. ``Draft`` is a real status and ``ppa_notes`` exists precisely because
structure varies, so partial rows must be saveable — the pricing-model-conditional requirements
(Fixed=>strike, Indexed=>index_name, Collar=>floor+cap) are surfaced as hints in the UI, not enforced.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

CounterpartyRoleLiteral = Literal[
    "Ultimate_Offtaker", "RTM", "Trader", "Utility_Supplier", "Unknown"
]
SettlementMechanismLiteral = Literal["Physical", "Financial_CfD", "Route_to_Market"]
VolumeShapeLiteral = Literal["Pay_as_Produced", "Pay_as_Nominated", "Baseload"]
PpaStatusLiteral = Literal["Draft", "Active", "Superseded", "Terminated", "Expired"]
PricingModelLiteral = Literal["Fixed", "Indexed", "Collar"]
IndexationTypeLiteral = Literal["None", "CPI", "RPI", "Fixed_Pct"]


class ScadaPpaTerms(BaseModel):
    """Every contract field except the asset link. Shared by create (fanned out over N windfarms)
    and the row schemas."""

    ppa_code: str = Field(..., min_length=1, max_length=50)
    ppa_buyer: str = Field(..., min_length=1, max_length=255)

    counterparty_role: Optional[CounterpartyRoleLiteral] = None
    settlement_mechanism: Optional[SettlementMechanismLiteral] = None
    volume_shape: Optional[VolumeShapeLiteral] = None
    ppa_status: PpaStatusLiteral = "Draft"

    execution_date: Optional[date] = None
    effective_date: Optional[date] = None
    expiration_date: Optional[date] = None

    currency: Optional[str] = Field(None, min_length=3, max_length=3)

    power_share_pct: Optional[Decimal] = Field(None, gt=0, le=100)

    pricing_model: Optional[PricingModelLiteral] = None
    strike_price: Optional[Decimal] = None
    floor_price: Optional[Decimal] = None
    cap_price: Optional[Decimal] = None

    index_name: Optional[str] = Field(None, max_length=50)
    index_spread: Optional[Decimal] = None  # signed: negative = discount

    indexation_type: Optional[IndexationTypeLiteral] = None
    indexation_rate_pct: Optional[Decimal] = None

    has_availability_penalties: Optional[bool] = None
    availability_guarantee_pct: Optional[Decimal] = Field(None, ge=0, le=100)

    ppa_notes: Optional[str] = None

    @field_validator("currency")
    @classmethod
    def _currency_is_iso4217(cls, v: Optional[str]) -> Optional[str]:
        """Stored as originally contracted; we only check it looks like an ISO 4217 alpha code."""
        if v is None:
            return v
        v = v.strip().upper()
        if not (len(v) == 3 and v.isalpha()):
            raise ValueError("currency must be a 3-letter ISO 4217 code, e.g. GBP")
        return v

    @model_validator(mode="after")
    def _check_cross_field_rules(self) -> "ScadaPpaTerms":
        if (
            self.effective_date is not None
            and self.expiration_date is not None
            and self.expiration_date <= self.effective_date
        ):
            raise ValueError("expiration_date must be after effective_date")
        if (
            self.floor_price is not None
            and self.cap_price is not None
            and self.floor_price > self.cap_price
        ):
            raise ValueError("floor_price must not exceed cap_price")
        return self


class ScadaPpaCreate(ScadaPpaTerms):
    """Create one PPA across one or more windfarms.

    A multi-farm PPA is N rows sharing ``ppa_code`` — the API fans these terms out over
    ``windfarm_ids`` rather than making the user re-key the contract per farm.
    """

    windfarm_ids: List[int] = Field(..., min_length=1)

    @field_validator("windfarm_ids")
    @classmethod
    def _unique_windfarm_ids(cls, v: List[int]) -> List[int]:
        if len(set(v)) != len(v):
            raise ValueError("windfarm_ids must not contain duplicates")
        return v


class ScadaPpaUpdate(BaseModel):
    """Edit ONE row. All fields optional; terms may legitimately diverge per farm within one PPA
    (``power_share_pct`` especially), so this never fans out across the ppa_code group."""

    windfarm_id: Optional[int] = None
    ppa_code: Optional[str] = Field(None, min_length=1, max_length=50)
    ppa_buyer: Optional[str] = Field(None, min_length=1, max_length=255)

    counterparty_role: Optional[CounterpartyRoleLiteral] = None
    settlement_mechanism: Optional[SettlementMechanismLiteral] = None
    volume_shape: Optional[VolumeShapeLiteral] = None
    ppa_status: Optional[PpaStatusLiteral] = None

    execution_date: Optional[date] = None
    effective_date: Optional[date] = None
    expiration_date: Optional[date] = None

    currency: Optional[str] = Field(None, min_length=3, max_length=3)
    power_share_pct: Optional[Decimal] = Field(None, gt=0, le=100)

    pricing_model: Optional[PricingModelLiteral] = None
    strike_price: Optional[Decimal] = None
    floor_price: Optional[Decimal] = None
    cap_price: Optional[Decimal] = None

    index_name: Optional[str] = Field(None, max_length=50)
    index_spread: Optional[Decimal] = None

    indexation_type: Optional[IndexationTypeLiteral] = None
    indexation_rate_pct: Optional[Decimal] = None

    has_availability_penalties: Optional[bool] = None
    availability_guarantee_pct: Optional[Decimal] = Field(None, ge=0, le=100)

    ppa_notes: Optional[str] = None

    _currency_is_iso4217 = field_validator("currency")(
        ScadaPpaTerms._currency_is_iso4217.__func__
    )

    @model_validator(mode="after")
    def _check_partial_cross_field_rules(self) -> "ScadaPpaUpdate":
        """Only checks pairs where BOTH sides are present in this patch — the endpoint re-validates
        the merged row so a half-supplied pair is caught against the stored values."""
        if (
            self.effective_date is not None
            and self.expiration_date is not None
            and self.expiration_date <= self.effective_date
        ):
            raise ValueError("expiration_date must be after effective_date")
        if (
            self.floor_price is not None
            and self.cap_price is not None
            and self.floor_price > self.cap_price
        ):
            raise ValueError("floor_price must not exceed cap_price")
        return self


class WindfarmBasic(BaseModel):
    """Declared locally to avoid circular imports — same trick as app/schemas/ppa.py."""

    id: int
    code: str
    name: str

    model_config = ConfigDict(from_attributes=True)


class ScadaPpa(ScadaPpaTerms):
    """One persisted row."""

    id: int
    windfarm_id: int
    created_at: datetime
    updated_at: datetime
    windfarm: Optional[WindfarmBasic] = None

    model_config = ConfigDict(from_attributes=True)


class ScadaPpaListResponse(BaseModel):
    """The SCADA list envelope ({items, total}), matching ScadaOpportunityListResponse rather than
    the core {items,total,limit,offset,has_more} shape."""

    items: List[ScadaPpa]
    total: int


class ScadaPpaDeleteResult(BaseModel):
    """Returned by the group delete so the caller knows how many farm rows went with it."""

    ppa_code: str
    deleted: int
