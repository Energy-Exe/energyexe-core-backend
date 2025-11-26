"""Service for PPA (Power Purchase Agreement) CRUD operations and Excel import."""

import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

import pandas as pd
import structlog
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.ppa import PPA
from app.models.windfarm import Windfarm
from app.schemas.ppa import (
    PPACreate,
    PPAImportError,
    PPAImportResult,
    PPAUpdate,
)

logger = structlog.get_logger()


class PPAService:
    """Service for PPA CRUD operations and Excel import."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_ppas(
        self,
        skip: int = 0,
        limit: int = 100,
    ) -> tuple[List[PPA], int]:
        """Get all PPAs with pagination."""
        # Get total count
        count_result = await self.db.execute(select(func.count(PPA.id)))
        total = count_result.scalar() or 0

        # Get paginated results
        result = await self.db.execute(
            select(PPA)
            .options(selectinload(PPA.windfarm))
            .offset(skip)
            .limit(limit)
            .order_by(PPA.created_at.desc())
        )
        ppas = list(result.scalars().all())
        return ppas, total

    async def get_ppa(self, ppa_id: int) -> Optional[PPA]:
        """Get a single PPA by ID."""
        result = await self.db.execute(
            select(PPA).options(selectinload(PPA.windfarm)).where(PPA.id == ppa_id)
        )
        return result.scalar_one_or_none()

    async def get_ppas_by_windfarm(self, windfarm_id: int) -> List[PPA]:
        """Get all PPAs for a specific windfarm."""
        result = await self.db.execute(
            select(PPA)
            .where(PPA.windfarm_id == windfarm_id)
            .order_by(PPA.ppa_start_date.desc())
        )
        return list(result.scalars().all())

    async def create_ppa(self, ppa_data: PPACreate) -> PPA:
        """Create a new PPA."""
        db_ppa = PPA(**ppa_data.model_dump())
        self.db.add(db_ppa)
        await self.db.commit()
        await self.db.refresh(db_ppa)
        return db_ppa

    async def update_ppa(self, ppa_id: int, ppa_update: PPAUpdate) -> Optional[PPA]:
        """Update an existing PPA."""
        result = await self.db.execute(select(PPA).where(PPA.id == ppa_id))
        db_ppa = result.scalar_one_or_none()

        if not db_ppa:
            return None

        update_data = ppa_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_ppa, field, value)

        await self.db.commit()
        await self.db.refresh(db_ppa)
        return db_ppa

    async def delete_ppa(self, ppa_id: int) -> Optional[PPA]:
        """Delete a PPA by ID."""
        result = await self.db.execute(select(PPA).where(PPA.id == ppa_id))
        db_ppa = result.scalar_one_or_none()

        if not db_ppa:
            return None

        await self.db.delete(db_ppa)
        await self.db.commit()
        return db_ppa

    async def import_from_excel(
        self,
        file_content: bytes,
        filename: str,
    ) -> PPAImportResult:
        """
        Import PPAs from Excel file.

        Expected columns:
        - windfarm_name: Name of the windfarm (exact match required)
        - ppa_buyer: Buyer company name
        - ppa_size_mw: PPA size in MW
        - ppa_duration_years: Duration in years (optional)
        - ppa_start_date: Start date (optional)
        - ppa_end_date: End date (optional)
        - ppa_notes: Notes (optional, max 200 chars)

        Upsert logic: Updates if (windfarm_id, ppa_buyer, ppa_start_date, ppa_end_date)
        match, otherwise creates a new record.
        """
        errors: List[PPAImportError] = []
        unmatched_windfarms: List[str] = []
        created = 0
        updated = 0
        skipped = 0

        # Save to temp file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file.write(file_content)
            tmp_path = Path(tmp_file.name)

        try:
            # Read Excel file
            df = pd.read_excel(tmp_path)
            total_rows = len(df)

            logger.info(f"Importing PPAs from {filename}", total_rows=total_rows)

            # Validate required columns
            required_columns = ["windfarm_name", "ppa_buyer", "ppa_size_mw"]
            missing_cols = [c for c in required_columns if c not in df.columns]
            if missing_cols:
                return PPAImportResult(
                    success=False,
                    total_rows=total_rows,
                    created=0,
                    updated=0,
                    skipped=0,
                    errors=[
                        PPAImportError(
                            row=0,
                            message=f"Missing required columns: {missing_cols}",
                        )
                    ],
                )

            # Build windfarm name -> id mapping
            windfarm_result = await self.db.execute(select(Windfarm))
            windfarm_lookup = {wf.name: wf.id for wf in windfarm_result.scalars().all()}

            logger.info(f"Loaded {len(windfarm_lookup)} windfarms for matching")

            # Process each row
            for idx, row in df.iterrows():
                row_num = idx + 2  # Excel rows are 1-indexed, plus header

                try:
                    # Get windfarm_name and match
                    windfarm_name = str(row["windfarm_name"]).strip()
                    if not windfarm_name or windfarm_name == "nan":
                        errors.append(
                            PPAImportError(
                                row=row_num,
                                field="windfarm_name",
                                message="Windfarm name is required",
                            )
                        )
                        skipped += 1
                        continue

                    windfarm_id = windfarm_lookup.get(windfarm_name)
                    if not windfarm_id:
                        if windfarm_name not in unmatched_windfarms:
                            unmatched_windfarms.append(windfarm_name)
                        errors.append(
                            PPAImportError(
                                row=row_num,
                                field="windfarm_name",
                                value=windfarm_name,
                                message=f"Windfarm not found: {windfarm_name}",
                            )
                        )
                        skipped += 1
                        continue

                    # Validate ppa_buyer
                    ppa_buyer = str(row["ppa_buyer"]).strip()
                    if not ppa_buyer or ppa_buyer == "nan":
                        errors.append(
                            PPAImportError(
                                row=row_num,
                                field="ppa_buyer",
                                message="PPA buyer is required",
                            )
                        )
                        skipped += 1
                        continue

                    # Validate ppa_size_mw
                    try:
                        ppa_size_mw = Decimal(str(row["ppa_size_mw"]))
                        if ppa_size_mw <= 0:
                            raise ValueError("Must be positive")
                    except (ValueError, TypeError) as e:
                        errors.append(
                            PPAImportError(
                                row=row_num,
                                field="ppa_size_mw",
                                value=str(row.get("ppa_size_mw")),
                                message=f"Invalid PPA size: {e}",
                            )
                        )
                        skipped += 1
                        continue

                    # Parse optional fields
                    ppa_duration_years = None
                    if "ppa_duration_years" in row and pd.notna(row["ppa_duration_years"]):
                        try:
                            ppa_duration_years = int(row["ppa_duration_years"])
                        except (ValueError, TypeError):
                            pass

                    ppa_start_date = None
                    if "ppa_start_date" in row and pd.notna(row["ppa_start_date"]):
                        try:
                            ppa_start_date = pd.to_datetime(row["ppa_start_date"]).date()
                        except (ValueError, TypeError):
                            pass

                    ppa_end_date = None
                    if "ppa_end_date" in row and pd.notna(row["ppa_end_date"]):
                        try:
                            ppa_end_date = pd.to_datetime(row["ppa_end_date"]).date()
                        except (ValueError, TypeError):
                            pass

                    ppa_notes = None
                    if "ppa_notes" in row and pd.notna(row["ppa_notes"]):
                        ppa_notes = str(row["ppa_notes"])[:200]

                    # Check for existing PPA (upsert logic)
                    existing_result = await self.db.execute(
                        select(PPA).where(
                            and_(
                                PPA.windfarm_id == windfarm_id,
                                PPA.ppa_buyer == ppa_buyer,
                                PPA.ppa_start_date == ppa_start_date,
                                PPA.ppa_end_date == ppa_end_date,
                            )
                        )
                    )
                    existing_ppa = existing_result.scalar_one_or_none()

                    if existing_ppa:
                        # Update existing
                        existing_ppa.ppa_size_mw = ppa_size_mw
                        existing_ppa.ppa_duration_years = ppa_duration_years
                        existing_ppa.ppa_notes = ppa_notes
                        updated += 1
                    else:
                        # Create new
                        new_ppa = PPA(
                            windfarm_id=windfarm_id,
                            ppa_buyer=ppa_buyer,
                            ppa_size_mw=ppa_size_mw,
                            ppa_duration_years=ppa_duration_years,
                            ppa_start_date=ppa_start_date,
                            ppa_end_date=ppa_end_date,
                            ppa_notes=ppa_notes,
                        )
                        self.db.add(new_ppa)
                        created += 1

                except Exception as e:
                    logger.error(f"Error processing row {row_num}", error=str(e))
                    errors.append(
                        PPAImportError(
                            row=row_num,
                            message=f"Unexpected error: {str(e)}",
                        )
                    )
                    skipped += 1
                    continue

            # Commit all changes
            await self.db.commit()

            logger.info(
                f"PPA import completed",
                created=created,
                updated=updated,
                skipped=skipped,
                errors=len(errors),
            )

            return PPAImportResult(
                success=len(errors) == 0,
                total_rows=total_rows,
                created=created,
                updated=updated,
                skipped=skipped,
                errors=errors,
                unmatched_windfarms=unmatched_windfarms,
            )

        except Exception as e:
            logger.error(f"Failed to import PPAs", error=str(e))
            await self.db.rollback()
            return PPAImportResult(
                success=False,
                total_rows=0,
                created=0,
                updated=0,
                skipped=0,
                errors=[
                    PPAImportError(
                        row=0,
                        message=f"Import failed: {str(e)}",
                    )
                ],
            )

        finally:
            # Clean up temp file
            tmp_path.unlink(missing_ok=True)
