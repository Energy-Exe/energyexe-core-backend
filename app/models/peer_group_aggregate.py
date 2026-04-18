"""Peer-group aggregate metrics — cached results for vs-zone-average comparisons.

Spec items 4 (degradation), 5 (disruption), 6 (wind normalisation), and the new
item 3 (generation concentration) all need to compare a single windfarm's metric
against the average for its bidzone / country / owner / turbine-model peer group.

Computing those aggregates on every API request would mean re-scanning all peer
windfarms' performance_summaries / degradation_results rows. We cache the
results here, refreshed by the daily pipeline cron after each module run.
"""

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class PeerGroupAggregate(Base):
    """Aggregate (avg, p10, p50, p90, n) of a metric over a peer group + period."""

    __tablename__ = "peer_group_aggregates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # Peer group identification
    group_type: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # 'bidzone' | 'country' | 'owner' | 'turbine_model'
    group_id: Mapped[int] = mapped_column(Integer, nullable=False)

    # Metric being aggregated. See `peer_aggregate_service.METRIC_KEYS`.
    metric_key: Mapped[str] = mapped_column(String(60), nullable=False)

    # Period (matches PerformanceSummary period_type semantics)
    period_type: Mapped[str] = mapped_column(String(10), nullable=False)  # 'year' | 'month'
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)  # NULL for yearly

    # Aggregate stats over the peer group
    windfarm_count: Mapped[int] = mapped_column(Integer, nullable=False)
    avg_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4), nullable=True)
    p10_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4), nullable=True)
    p50_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4), nullable=True)
    p90_value: Mapped[Optional[float]] = mapped_column(Numeric(14, 4), nullable=True)

    # Refresh tracking
    computed_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=lambda: datetime.now(timezone.utc).replace(tzinfo=None),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint(
            "group_type", "group_id", "metric_key", "period_type", "year", "month",
            name="uq_peer_group_aggregate",
        ),
        Index("ix_peer_aggregate_lookup", "group_type", "group_id", "metric_key", "year"),
    )

    def __repr__(self) -> str:
        period = f"{self.year}-{self.month:02d}" if self.month else str(self.year)
        return (
            f"<PeerGroupAggregate({self.group_type}={self.group_id}, "
            f"metric={self.metric_key}, period={period}, n={self.windfarm_count})>"
        )
