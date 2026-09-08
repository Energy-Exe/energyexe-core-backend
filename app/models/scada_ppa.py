"""SCADA PPA model (EPR-97) — the detailed offtake contract structure behind SCADA revenue attribution.

Deliberately SEPARATE from the Perform ``ppas`` table (``app/models/ppa.py``). That one is a thin
8-field record and stays exactly as it is: ``app/services/opportunity_detection_service.py`` reads it
for MKT_04/MKT_05, and nothing here may disturb that. This table is the richer, SCADA-only structure
EPR-97 specifies — collars, indexation, counterparty role, settlement mechanism, partial offtake —
and no Perform code path reads or writes it.

Like ``scada_finding_action``, it is app-authored state written only by the API, so it lives in the
ordinary public schema as a normal core model with a ``scada_`` prefix — not in the ``scada`` schema,
which is owned and truncate+reloaded by energyexe-scada-pipeline.

Multi-farm PPAs are N rows sharing one ``ppa_code``, one row per windfarm; ``(ppa_code, windfarm_id)``
is the natural key. Mid-life price steps, blended indexation and any structure the columns cannot hold
live in ``ppa_notes``, which is load-bearing infrastructure parsed by the agent at query time, not
commentary.

Downstream consumer (not yet wired — EPR-97 leaves it to Aje): the opportunity suite's MKT_01 price
capture reads contract terms from a hand-edited config block today; ``pricing_model`` + strike/floor/
cap/dates here are the shape its ``_contract_from_config()`` seam will eventually be fed from.
"""

from datetime import date, datetime, timezone
from enum import Enum
from typing import Optional

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


def _utcnow() -> datetime:
    """Naive-UTC, the house idiom for new timestamp columns (see scada_finding_action)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


class CounterpartyRole(str, Enum):
    """Nature of the counterparty on the contract.

    ``Ultimate_Offtaker`` and ``Utility_Supplier`` are kept apart because supply utilities re-hedge
    into their retail book and corporates do not. If that distinction proves noisy in practice they
    can collapse to one.
    """

    ULTIMATE_OFFTAKER = "Ultimate_Offtaker"  # entity actually consuming the power
    RTM = "RTM"  # route-to-market provider selling on (Statkraft, Axpo, Centrica in this role)
    TRADER = "Trader"  # financial counterparty, no physical consumption
    UTILITY_SUPPLIER = "Utility_Supplier"  # utility buying to serve retail supply obligations
    UNKNOWN = "Unknown"  # legacy records where the notes do not clarify the role


class SettlementMechanism(str, Enum):
    PHYSICAL = "Physical"
    FINANCIAL_CFD = "Financial_CfD"
    ROUTE_TO_MARKET = "Route_to_Market"


class VolumeShape(str, Enum):
    PAY_AS_PRODUCED = "Pay_as_Produced"
    PAY_AS_NOMINATED = "Pay_as_Nominated"
    BASELOAD = "Baseload"


class PpaStatus(str, Enum):
    """``Superseded`` distinguishes PPAs replaced mid-life (typically after equity changes hands)
    from those that ran to term — without it, historical price-capture analysis is distorted."""

    DRAFT = "Draft"
    ACTIVE = "Active"
    SUPERSEDED = "Superseded"
    TERMINATED = "Terminated"
    EXPIRED = "Expired"


class PricingModel(str, Enum):
    """A floor-only or cap-only PPA is recorded as ``Collar`` with the other side left null.

    MKT_01's ``apply_contract()`` carries four mechanisms (fixed/collar/floor/cap); EPR-97 specifies
    three. This is the one place the schema does not map 1:1 onto that consumer — see the ticket's
    open items.
    """

    FIXED = "Fixed"
    INDEXED = "Indexed"
    COLLAR = "Collar"


class IndexationType(str, Enum):
    NONE = "None"
    CPI = "CPI"
    RPI = "RPI"
    FIXED_PCT = "Fixed_Pct"


class ScadaPpa(Base):
    """One windfarm's leg of one SCADA PPA contract."""

    __tablename__ = "scada_ppa"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # The asset. A hard FK to the core windfarm, per EPR-97's stated schema. Note this means a SCADA
    # farm with no platform windfarm row (kelmarsh, penmanshiel) cannot hold a PPA — accepted, they
    # are open-data reference farms. No back_populates: Windfarm must stay untouched (isolation).
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id"), nullable=False, index=True
    )

    # Human-readable contract identifier, e.g. PPA-2026-001. Also what groups a multi-farm PPA.
    ppa_code: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # The named counterparty. Free text by decision on EPR-97 — there is no buyer/counterparty table
    # in the platform, and the ticket left the FK target blank.
    ppa_buyer: Mapped[str] = mapped_column(String(255), nullable=False)

    # Enum-ish columns are plain varchar validated in Pydantic — house convention, no DB enum/CHECK.
    counterparty_role: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    settlement_mechanism: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    volume_shape: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    ppa_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default=PpaStatus.DRAFT.value, server_default=PpaStatus.DRAFT.value
    )

    execution_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)  # signed
    effective_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)  # commercial start
    expiration_date: Mapped[Optional[date]] = mapped_column(Date, nullable=True)

    # ISO 4217, stored as originally contracted — never reconverted on redenomination. FX to EUR or
    # anything else is a reporting-layer concern.
    currency: Mapped[Optional[str]] = mapped_column(String(3), nullable=True)

    # % of METERED GENERATION covered, not of installed capacity: a 50% PPA on a 100 MW farm covers
    # 50% of MWh output, not 50 MW nameplate.
    power_share_pct: Mapped[Optional[float]] = mapped_column(Numeric(5, 2), nullable=True)

    pricing_model: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    strike_price: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    floor_price: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)
    cap_price: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)

    index_name: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    # Signed spread over/under the index, native currency per MWh. Negative = discount.
    index_spread: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), nullable=True)

    indexation_type: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    indexation_rate_pct: Mapped[Optional[float]] = mapped_column(Numeric(5, 2), nullable=True)

    has_availability_penalties: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    availability_guarantee_pct: Mapped[Optional[float]] = mapped_column(Numeric(5, 2), nullable=True)

    # Load-bearing, not commentary: step prices, buyer changes, partial offtake and any structure the
    # columns above cannot represent. The agent parses it at query time.
    ppa_notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=_utcnow, onupdate=_utcnow, nullable=False
    )

    # One-way on purpose — adding a Windfarm.scada_ppas backref would edit the core model.
    windfarm = relationship("Windfarm", lazy="selectin")

    __table_args__ = (
        UniqueConstraint("ppa_code", "windfarm_id", name="uq_scada_ppa_code_windfarm"),
    )

    def __repr__(self) -> str:
        return (
            f"<ScadaPpa(ppa_code='{self.ppa_code}', windfarm_id={self.windfarm_id}, "
            f"buyer='{self.ppa_buyer}', status='{self.ppa_status}')>"
        )
