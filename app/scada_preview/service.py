"""Read-only ingestion service; sessions are injected by the calling application."""

import json

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.scada_preview.schemas import IngestionSummary, farm_metadata

MAX_SUMMARY_BYTES = 2_000_000


class IngestionSummaryService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def available(self) -> bool:
        result = await self.db.execute(text("SELECT to_regclass('scada.ingestion_run')"))
        return result.scalar_one_or_none() is not None

    async def get(self, farm: str, run_id: str | None = None) -> IngestionSummary:
        if not await self.available():
            raise HTTPException(503, "Measured ingestion data not available")
        selector = "run_id = :run_id" if run_id is not None else "is_current = true"
        result = await self.db.execute(
            text(
                "SELECT run_id, CASE WHEN octet_length(summary::text) <= :max_bytes "
                "THEN summary ELSE NULL END AS summary "
                f"FROM scada.ingestion_run WHERE farm = :farm AND {selector}"
            ),
            {"farm": farm, "run_id": run_id, "max_bytes": MAX_SUMMARY_BYTES},
        )
        row = result.mappings().one_or_none()
        if row is None:
            raise HTTPException(404, "No published ingestion run for this farm and run")
        try:
            raw = json.loads(row["summary"]) if isinstance(row["summary"], str) else row["summary"]
            summary = IngestionSummary.model_validate(raw)
            if summary.farm.slug != farm or summary.run_id != row["run_id"]:
                raise ValueError("Published run identity mismatch")
        except (ValidationError, ValueError, TypeError):
            raise HTTPException(503, "Published ingestion summary failed validation") from None
        return summary

    async def farms(self) -> dict:
        if not await self.available():
            return {"farms": []}
        result = await self.db.execute(
            text("SELECT farm FROM scada.ingestion_run WHERE is_current = true ORDER BY farm")
        )
        farms = []
        for farm in result.scalars().all():
            summary = await self.get(farm)
            farms.append(
                {
                    "farm": farm,
                    "name": summary.farm.name,
                    "windfarm_id": summary.farm.windfarm_id,
                    "n_turbines": len(summary.farm.supplied_turbines),
                    "data_through": summary.source.last_label_utc[:10],
                    "days": len(summary.charts.daily_trends),
                    "ingestion": farm_metadata(summary),
                }
            )
        return {"farms": farms}
