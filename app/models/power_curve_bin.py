"""Power curve bin statistics — empirical power curves stored per windfarm."""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class PowerCurveBin(Base):
    """One row per wind speed bin per windfarm/year/curve_type."""

    __tablename__ = "power_curve_bins"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("windfarms.id", ondelete="CASCADE"), nullable=False, index=True
    )
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # NULL = overall_clean
    curve_type: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # raw, capability, overall_clean
    wind_bin: Mapped[float] = mapped_column(Numeric(4, 1), nullable=False)  # 2.0, 3.0, ... 25.0

    # Bin statistics (power output as fraction of rated capacity)
    q50_pu: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)  # Median (P50)
    q90_pu: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)  # 90th pct (P10)
    mean_pu: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)
    mad_pu: Mapped[Optional[float]] = mapped_column(Numeric(6, 5), nullable=True)  # Median abs dev
    sample_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc).replace(tzinfo=None), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    windfarm = relationship("Windfarm")

    __table_args__ = (
        UniqueConstraint("windfarm_id", "year", "curve_type", "wind_bin", name="uq_pcb_wf_year_type_bin"),
        Index("ix_pcb_windfarm_year", "windfarm_id", "year"),
    )

    def __repr__(self) -> str:
        return f"<PowerCurveBin(wf={self.windfarm_id}, year={self.year}, type={self.curve_type}, bin={self.wind_bin})>"
