"""Read-only ingestion service; sessions are injected by the calling application."""

import json

import structlog
from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.scada_preview.schemas import IngestionSummary, farm_metadata

MAX_SUMMARY_BYTES = 2_000_000
logger = structlog.get_logger(__name__)
UNAVAILABLE = "Measured ingestion data not available"

# Both stores are internal constants, never schema names supplied by a request.
# The normal application cannot silently fall back to the legacy/local store.
PROTECTED_SQL = {
    "present": text("SELECT to_regclass('scada_ingestion.ingestion_run')"),
    "farms": text(
        "SELECT farm FROM scada_ingestion.ingestion_run WHERE is_current = true ORDER BY farm"
    ),
    "current": text(
        "SELECT run_id, CASE WHEN octet_length(summary::text) <= :max_bytes "
        "THEN summary ELSE NULL END AS summary FROM scada_ingestion.ingestion_run "
        "WHERE farm = :farm AND is_current = true"
    ),
    "historical": text(
        "SELECT run_id, CASE WHEN octet_length(summary::text) <= :max_bytes "
        "THEN summary ELSE NULL END AS summary FROM scada_ingestion.ingestion_run "
        "WHERE farm = :farm AND run_id = :run_id"
    ),
}
LOCAL_SQL = {
    key: text(str(query).replace("scada_ingestion.ingestion_run", "scada.ingestion_run"))
    for key, query in PROTECTED_SQL.items()
}


class IngestionSummaryService:
    def __init__(self, db: AsyncSession, *, local_preview: bool = False):
        self.db = db
        self.sql = LOCAL_SQL if local_preview else PROTECTED_SQL

    async def available(self) -> bool:
        result = await self.db.execute(self.sql["present"])
        return result.scalar_one_or_none() is not None

    async def get(self, farm: str, run_id: str | None = None) -> IngestionSummary:
        try:
            return await self._read(farm, run_id)
        except SQLAlchemyError:
            logger.warning("ingestion_store_unavailable", farm=farm, run_id=run_id)
            raise HTTPException(503, UNAVAILABLE) from None

    async def _read(self, farm: str, run_id: str | None = None) -> IngestionSummary:
        if not await self.available():
            raise HTTPException(503, UNAVAILABLE)
        result = await self.db.execute(
            self.sql["historical" if run_id is not None else "current"],
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
            # Never log validation errors: they can contain the stored payload.
            logger.warning("ingestion_summary_invalid", farm=farm, run_id=row["run_id"])
            raise HTTPException(503, UNAVAILABLE) from None
        return summary

    async def farms(self) -> dict:
        # Isolate optional metadata failures from the legacy farm response and
        # its transaction (including missing permissions or a concurrent drop).
        try:
            async with self.db.begin_nested():
                if not await self.available():
                    return {"farms": []}
                result = await self.db.execute(self.sql["farms"])
                farms = []
                for farm in result.scalars().all():
                    try:
                        summary = await self._read(farm)
                    except HTTPException:
                        continue
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
        except SQLAlchemyError:
            logger.warning("ingestion_farm_metadata_unavailable")
            return {"farms": []}
