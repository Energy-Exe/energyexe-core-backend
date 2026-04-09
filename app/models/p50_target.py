from sqlalchemy import (
    DECIMAL,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.core.database import Base


class P50Target(Base):
    """P50 annual energy production target for a windfarm.

    Stores externally-provided P50 targets from wind resource assessments.
    Monthly P50 = Annual P50 / 12. Multiple targets per windfarm are allowed
    with non-overlapping date ranges.
    """

    __tablename__ = "p50_targets"
    __table_args__ = (
        UniqueConstraint(
            "windfarm_id",
            "p50_target_start_date",
            name="uq_p50_target_windfarm_start_date",
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    windfarm_id = Column(Integer, ForeignKey("windfarms.id"), nullable=False, index=True)

    # P50 target details
    p50_target_start_date = Column(Date, nullable=False)
    p50_target_end_date = Column(Date, nullable=True)  # None = ongoing
    p50_target_volume_gwh = Column(DECIMAL(12, 3), nullable=False)

    # Source and notes
    source = Column(String(500), nullable=True)  # URL to source document
    comment = Column(Text, nullable=True)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    windfarm = relationship("Windfarm", back_populates="p50_targets")
