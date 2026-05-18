"""Generation Concentration — distribution of generation by price decile.

Spec item 3 (Prioritisation 2026-03-30): "Distribution of power generation by
price. Does the wind farm generate in high (or low) price periods? Compare vs
price zone averages. AI agent descriptions and prompts; no client facing."

Per windfarm-period: rank all generation hours by hourly price, partition into
deciles, compute the share of total generation that fell into each decile.

Key derived metric: capture_ratio = volume-weighted-avg-price / time-weighted
-avg-price. Values >1 mean the windfarm generates more in higher-price hours
(positive price correlation, commercially good); <1 means inverse correlation
(commercially bad).
"""

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
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class GenerationConcentrationSummary(Base):
    """One row per (windfarm, period) — generation-by-price decile breakdown."""

    __tablename__ = "generation_concentration_summaries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    windfarm_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("windfarms.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Period semantics match performance_summaries (year + nullable month)
    period_type: Mapped[str] = mapped_column(String(10), nullable=False)  # 'year' | 'month'
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Volume + price baselines
    total_mwh: Mapped[Optional[float]] = mapped_column(Numeric(14, 3), nullable=True)
    total_hours: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    weighted_avg_capture_price_eur: Mapped[Optional[float]] = mapped_column(
        Numeric(12, 4), nullable=True
    )
    time_weighted_avg_price_eur: Mapped[Optional[float]] = mapped_column(
        Numeric(12, 4), nullable=True
    )
    capture_ratio: Mapped[Optional[float]] = mapped_column(
        Numeric(8, 4), nullable=True
    )  # weighted / time

    # Decile / quartile shares — top decile = D10 (highest 10% of prices)
    top_decile_share_pct: Mapped[Optional[float]] = mapped_column(
        Numeric(7, 3), nullable=True
    )
    top_quartile_share_pct: Mapped[Optional[float]] = mapped_column(
        Numeric(7, 3), nullable=True
    )
    bottom_decile_share_pct: Mapped[Optional[float]] = mapped_column(
        Numeric(7, 3), nullable=True
    )
    bottom_quartile_share_pct: Mapped[Optional[float]] = mapped_column(
        Numeric(7, 3), nullable=True
    )

    # Full breakdown — {"d1": 8.5, "d2": 9.1, ..., "d10": 12.3}
    decile_shares: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)

    # Comparison vs bidzone average — populated when peer aggregate is available
    vs_zone_capture_ratio_diff: Mapped[Optional[float]] = mapped_column(
        Numeric(8, 4), nullable=True
    )
    vs_zone_top_decile_diff: Mapped[Optional[float]] = mapped_column(
        Numeric(7, 3), nullable=True
    )

    # Pipeline tracking
    pipeline_run_id: Mapped[Optional[int]] = mapped_column(
        Integer,
        ForeignKey("import_job_executions.id", ondelete="SET NULL"),
        nullable=True,
    )
    computed_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        onupdate=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    windfarm = relationship("Windfarm")
    pipeline_run = relationship("ImportJobExecution")

    __table_args__ = (
        UniqueConstraint(
            "windfarm_id", "period_type", "year", "month",
            name="uq_generation_concentration_wf_period",
        ),
        Index("ix_genconc_wf_year", "windfarm_id", "year"),
    )

    def __repr__(self) -> str:
        period = f"{self.year}-{self.month:02d}" if self.month else str(self.year)
        return (
            f"<GenerationConcentrationSummary(wf={self.windfarm_id}, "
            f"period={period}, capture_ratio={self.capture_ratio})>"
        )
