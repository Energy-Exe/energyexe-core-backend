"""ReportContext — the inputs handed to every section data builder."""

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.portfolio import Portfolio
from app.models.windfarm import Windfarm


@dataclass
class ReportContext:
    """Everything a section data builder may need for one generation run.

    Builders must treat this as read-only and must not commit the session —
    the orchestrator owns transaction boundaries.
    """

    db: AsyncSession
    report_id: int
    scope_type: str  # 'windfarm' | 'portfolio'
    period_start: date
    period_end: date
    windfarm: Optional[Windfarm] = None
    portfolio: Optional[Portfolio] = None
    params: dict = field(default_factory=dict)

    @property
    def windfarm_id(self) -> Optional[int]:
        return self.windfarm.id if self.windfarm is not None else None

    @property
    def portfolio_id(self) -> Optional[int]:
        return self.portfolio.id if self.portfolio is not None else None
