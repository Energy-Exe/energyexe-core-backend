"""Service for FinancialData CRUD operations, computed fields, Excel import, and analytics."""

import tempfile
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import structlog
from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.financial_data import FinancialData
from app.models.financial_entity import FinancialEntity
from app.models.generation_data import GenerationData
from app.models.windfarm import Windfarm
from app.models.windfarm_financial_entity import WindfarmFinancialEntity
from app.schemas.financial_data import (
    FinancialDataCreate,
    FinancialDataImportError,
    FinancialDataImportResult,
    FinancialDataSummary,
    FinancialDataUpdate,
    FinancialRatioPeriod,
    FinancialRatiosResponse,
)

logger = structlog.get_logger()


class FinancialDataService:
    def __init__(self, db: AsyncSession):
        self.db = db

    # --- Computed fields ---

    @staticmethod
    def _compute_totals(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Auto-calculate subtotals only when not explicitly provided.
        Source data may have manually entered totals that don't match component sums.
        """

        def _decimal(val: Any) -> Optional[Decimal]:
            if val is None:
                return None
            if isinstance(val, Decimal):
                return val
            try:
                return Decimal(str(val))
            except (InvalidOperation, ValueError):
                return None

        def _sum_if_any(*vals: Any) -> Optional[Decimal]:
            """Sum values, treating None as 0, but return None if ALL are None."""
            decimals = [_decimal(v) for v in vals]
            if all(d is None for d in decimals):
                return None
            return sum((d or Decimal("0")) for d in decimals)

        # Total Revenue
        if data.get("total_revenue") is None and data.get("revenue") is not None:
            data["total_revenue"] = _sum_if_any(data.get("revenue"), data.get("other_revenue"))

        # Total Operating Expenses
        opex_fields = [
            "cost_of_goods", "grid_cost", "land_cost", "payroll_expenses",
            "service_agreements", "insurance", "other_operating_expenses",
        ]
        if data.get("total_operating_expenses") is None:
            opex_vals = [data.get(f) for f in opex_fields]
            if any(v is not None for v in opex_vals):
                data["total_operating_expenses"] = _sum_if_any(*opex_vals)

        # EBITDA
        if (
            data.get("ebitda") is None
            and data.get("total_revenue") is not None
            and data.get("total_operating_expenses") is not None
        ):
            tr = _decimal(data["total_revenue"]) or Decimal("0")
            toe = _decimal(data["total_operating_expenses"]) or Decimal("0")
            data["ebitda"] = tr - toe

        # EBIT
        if data.get("ebit") is None and data.get("ebitda") is not None:
            ebitda = _decimal(data["ebitda"]) or Decimal("0")
            dep = _decimal(data.get("depreciation")) or Decimal("0")
            data["ebit"] = ebitda - dep

        # Earnings Before Tax
        if data.get("earnings_before_tax") is None and data.get("ebit") is not None:
            ebit = _decimal(data["ebit"]) or Decimal("0")
            ni = _decimal(data.get("net_interest")) or Decimal("0")
            nof = _decimal(data.get("net_other_financial")) or Decimal("0")
            data["earnings_before_tax"] = ebit + ni + nof

        # Net Income
        if data.get("net_income") is None and data.get("earnings_before_tax") is not None:
            ebt = _decimal(data["earnings_before_tax"]) or Decimal("0")
            tax = _decimal(data.get("tax")) or Decimal("0")
            data["net_income"] = ebt - tax

        return data

    # --- CRUD ---

    async def get_list(
        self,
        skip: int = 0,
        limit: int = 100,
        entity_id: Optional[int] = None,
        year: Optional[int] = None,
        currency: Optional[str] = None,
    ) -> tuple[List[FinancialData], int]:
        """Get financial data records with pagination and filters."""
        query = select(FinancialData).options(
            selectinload(FinancialData.financial_entity)
        )
        count_query = select(func.count(FinancialData.id))

        if entity_id:
            query = query.where(FinancialData.financial_entity_id == entity_id)
            count_query = count_query.where(FinancialData.financial_entity_id == entity_id)

        if year:
            start = date(year, 1, 1)
            end = date(year, 12, 31)
            year_filter = and_(
                FinancialData.period_start >= start,
                FinancialData.period_start <= end,
            )
            query = query.where(year_filter)
            count_query = count_query.where(year_filter)

        if currency:
            query = query.where(FinancialData.currency == currency.upper())
            count_query = count_query.where(FinancialData.currency == currency.upper())

        count_result = await self.db.execute(count_query)
        total = count_result.scalar() or 0

        result = await self.db.execute(
            query.offset(skip).limit(limit).order_by(
                FinancialData.period_start.desc()
            )
        )
        items = list(result.scalars().all())
        return items, total

    async def get_financial_data(self, data_id: int) -> Optional[FinancialData]:
        """Get a single financial data record by ID."""
        result = await self.db.execute(
            select(FinancialData)
            .options(selectinload(FinancialData.financial_entity))
            .where(FinancialData.id == data_id)
        )
        return result.scalar_one_or_none()

    async def get_by_entity_and_period(
        self, entity_id: int, period_start: date
    ) -> Optional[FinancialData]:
        """Get financial data by entity and period start."""
        result = await self.db.execute(
            select(FinancialData).where(
                and_(
                    FinancialData.financial_entity_id == entity_id,
                    FinancialData.period_start == period_start,
                )
            )
        )
        return result.scalar_one_or_none()

    async def create(self, data: FinancialDataCreate) -> FinancialData:
        """Create a new financial data record with computed totals."""
        data_dict = data.model_dump()
        data_dict = self._compute_totals(data_dict)

        db_record = FinancialData(**data_dict)
        self.db.add(db_record)
        await self.db.commit()
        await self.db.refresh(db_record)
        return db_record

    async def update(
        self, data_id: int, data: FinancialDataUpdate
    ) -> Optional[FinancialData]:
        """Update an existing financial data record."""
        result = await self.db.execute(
            select(FinancialData).where(FinancialData.id == data_id)
        )
        db_record = result.scalar_one_or_none()
        if not db_record:
            return None

        update_data = data.model_dump(exclude_unset=True)
        update_data = self._compute_totals(update_data)

        for field, value in update_data.items():
            setattr(db_record, field, value)

        await self.db.commit()
        await self.db.refresh(db_record)
        return db_record

    async def delete(self, data_id: int) -> Optional[FinancialData]:
        """Delete a financial data record."""
        result = await self.db.execute(
            select(FinancialData).where(FinancialData.id == data_id)
        )
        db_record = result.scalar_one_or_none()
        if not db_record:
            return None

        await self.db.delete(db_record)
        await self.db.commit()
        return db_record

    async def get_by_windfarm(self, windfarm_id: int) -> List[FinancialData]:
        """Get all financial data for a windfarm through entity links."""
        result = await self.db.execute(
            select(FinancialData)
            .options(selectinload(FinancialData.financial_entity))
            .join(FinancialEntity)
            .join(WindfarmFinancialEntity,
                  WindfarmFinancialEntity.financial_entity_id == FinancialEntity.id)
            .where(WindfarmFinancialEntity.windfarm_id == windfarm_id)
            .order_by(FinancialData.period_start.desc())
        )
        return list(result.scalars().all())

    # --- Analytics ---

    async def get_windfarm_financial_summary(
        self, windfarm_id: int
    ) -> List[FinancialDataSummary]:
        """Get financial summary for a windfarm - most recent period per entity."""
        # Get entity IDs linked to this windfarm
        entity_result = await self.db.execute(
            select(WindfarmFinancialEntity.financial_entity_id).where(
                WindfarmFinancialEntity.windfarm_id == windfarm_id
            )
        )
        entity_ids = [row[0] for row in entity_result.all()]
        if not entity_ids:
            return []

        summaries = []
        for entity_id in entity_ids:
            # Get most recent financial data for this entity
            result = await self.db.execute(
                select(FinancialData)
                .options(selectinload(FinancialData.financial_entity))
                .where(FinancialData.financial_entity_id == entity_id)
                .order_by(FinancialData.period_start.desc())
                .limit(1)
            )
            fd = result.scalar_one_or_none()
            if fd and fd.financial_entity:
                summaries.append(
                    FinancialDataSummary(
                        financial_entity_id=fd.financial_entity.id,
                        financial_entity_name=fd.financial_entity.name,
                        financial_entity_code=fd.financial_entity.code,
                        entity_type=fd.financial_entity.entity_type,
                        currency=fd.currency,
                        period_start=fd.period_start,
                        period_end=fd.period_end,
                        revenue=fd.revenue,
                        total_revenue=fd.total_revenue,
                        total_operating_expenses=fd.total_operating_expenses,
                        ebitda=fd.ebitda,
                        net_income=fd.net_income,
                        reported_generation_gwh=fd.reported_generation_gwh,
                    )
                )
        return summaries

    # --- Financial Ratios ---

    @staticmethod
    def _compute_ratios(
        total_revenue: Optional[Decimal],
        total_opex: Optional[Decimal],
        ebitda: Optional[Decimal],
        generation_mwh: Optional[Decimal],
    ) -> Dict[str, Optional[Decimal]]:
        """Compute per-MWh ratios and EBITDA margin. Pure function, no DB access."""
        revenue_per_mwh: Optional[Decimal] = None
        opex_per_mwh: Optional[Decimal] = None
        ebitda_margin_pct: Optional[Decimal] = None

        has_generation = generation_mwh is not None and generation_mwh > 0

        if total_revenue is not None and has_generation:
            revenue_per_mwh = round(total_revenue / generation_mwh, 2)
        if total_opex is not None and has_generation:
            opex_per_mwh = round(total_opex / generation_mwh, 2)
        if ebitda is not None and total_revenue is not None and total_revenue > 0:
            ebitda_margin_pct = round((ebitda / total_revenue) * 100, 2)

        return {
            "revenue_per_mwh": revenue_per_mwh,
            "opex_per_mwh": opex_per_mwh,
            "ebitda_margin_pct": ebitda_margin_pct,
        }

    async def calculate_financial_ratios(
        self, windfarm_id: int
    ) -> List[FinancialRatiosResponse]:
        """Calculate financial ratios for a windfarm by combining financial and generation data."""
        # 1. Get financial entity IDs linked to this windfarm
        link_result = await self.db.execute(
            select(WindfarmFinancialEntity).where(
                WindfarmFinancialEntity.windfarm_id == windfarm_id
            )
        )
        links = list(link_result.scalars().all())
        if not links:
            return []

        # Get the requested windfarm name
        wf_result = await self.db.execute(
            select(Windfarm).where(Windfarm.id == windfarm_id)
        )
        windfarm = wf_result.scalar_one_or_none()
        if not windfarm:
            return []

        responses = []
        for link in links:
            entity_id = link.financial_entity_id

            # 2a. Get the financial entity
            entity_result = await self.db.execute(
                select(FinancialEntity).where(FinancialEntity.id == entity_id)
            )
            entity = entity_result.scalar_one_or_none()
            if not entity:
                continue

            # 2b. Get ALL windfarm_ids linked to this entity (handles holdcos)
            all_links_result = await self.db.execute(
                select(WindfarmFinancialEntity.windfarm_id).where(
                    WindfarmFinancialEntity.financial_entity_id == entity_id
                )
            )
            linked_wf_ids = [row[0] for row in all_links_result.all()]

            # 2c. Get COD from each linked windfarm → effective COD = max(all CODs)
            cod_result = await self.db.execute(
                select(Windfarm.commercial_operational_date).where(
                    Windfarm.id.in_(linked_wf_ids)
                )
            )
            cod_dates = [row[0] for row in cod_result.all() if row[0] is not None]
            effective_cod = max(cod_dates) if cod_dates else None

            # Ramp-up cutoff
            ramp_up_cutoff = None
            if effective_cod is not None:
                ramp_up_cutoff = effective_cod + timedelta(days=365)

            # 2d. Get all FinancialData for this entity
            fd_result = await self.db.execute(
                select(FinancialData)
                .where(FinancialData.financial_entity_id == entity_id)
                .order_by(FinancialData.period_start)
            )
            financial_records = list(fd_result.scalars().all())

            periods = []
            for fd in financial_records:
                # Check ramp-up exclusion
                is_excluded = False
                exclusion_reason = None
                if ramp_up_cutoff is not None and fd.period_start < ramp_up_cutoff:
                    is_excluded = True
                    exclusion_reason = (
                        f"Period starts before COD + 365 days "
                        f"(COD: {effective_cod}, cutoff: {ramp_up_cutoff})"
                    )

                # Query generation data for this period
                # period_end is inclusive, so go up to period_end + 1 day
                period_start_dt = datetime(
                    fd.period_start.year, fd.period_start.month, fd.period_start.day
                )
                period_end_dt = datetime(
                    fd.period_end.year, fd.period_end.month, fd.period_end.day
                ) + timedelta(days=1)

                gen_result = await self.db.execute(
                    select(
                        func.sum(
                            GenerationData.generation_mwh
                            - func.coalesce(GenerationData.consumption_mwh, 0)
                        ),
                        func.count(GenerationData.id),
                    ).where(
                        GenerationData.windfarm_id.in_(linked_wf_ids),
                        GenerationData.hour >= period_start_dt,
                        GenerationData.hour < period_end_dt,
                    )
                )
                row = gen_result.one()
                total_gen_mwh = row[0]  # can be None if no data
                if total_gen_mwh is not None:
                    total_gen_mwh = round(total_gen_mwh, 1)
                hours_count = row[1] or 0

                # Compute expected hours for coverage
                total_days = (fd.period_end - fd.period_start).days + 1
                expected_hours = total_days * 24 * len(linked_wf_ids)
                coverage_pct = None
                if expected_hours > 0 and hours_count > 0:
                    coverage_pct = round(
                        Decimal(str(hours_count)) / Decimal(str(expected_hours)) * 100, 1
                    )

                gen_available = total_gen_mwh is not None and hours_count > 0

                # Compute ratios (skip if ramp-up excluded)
                ratios = {"revenue_per_mwh": None, "opex_per_mwh": None, "ebitda_margin_pct": None}
                if not is_excluded and gen_available:
                    ratios = self._compute_ratios(
                        total_revenue=fd.total_revenue,
                        total_opex=fd.total_operating_expenses,
                        ebitda=fd.ebitda,
                        generation_mwh=total_gen_mwh,
                    )

                periods.append(
                    FinancialRatioPeriod(
                        financial_data_id=fd.id,
                        period_start=fd.period_start,
                        period_end=fd.period_end,
                        currency=fd.currency,
                        total_revenue=round(fd.total_revenue, 0) if fd.total_revenue is not None else None,
                        total_operating_expenses=round(fd.total_operating_expenses, 0) if fd.total_operating_expenses is not None else None,
                        ebitda=round(fd.ebitda, 0) if fd.ebitda is not None else None,
                        generation_mwh=total_gen_mwh,
                        generation_hours_count=hours_count,
                        revenue_per_mwh=ratios["revenue_per_mwh"],
                        opex_per_mwh=ratios["opex_per_mwh"],
                        ebitda_margin_pct=ratios["ebitda_margin_pct"],
                        is_ramp_up_excluded=is_excluded,
                        ramp_up_exclusion_reason=exclusion_reason,
                        generation_data_available=gen_available,
                        period_coverage_pct=coverage_pct,
                    )
                )

            responses.append(
                FinancialRatiosResponse(
                    windfarm_id=windfarm_id,
                    windfarm_name=windfarm.name,
                    financial_entity_id=entity.id,
                    financial_entity_name=entity.name,
                    entity_type=entity.entity_type,
                    cod=effective_cod,
                    linked_windfarm_ids=linked_wf_ids,
                    periods=periods,
                )
            )

        return responses

    # --- Excel Import ---

    async def import_from_excel(
        self,
        file_content: bytes,
        filename: str,
    ) -> FinancialDataImportResult:
        """Import financial data from an Excel file (row-per-record format)."""
        errors: List[FinancialDataImportError] = []
        unmatched_entities: List[str] = []
        created = 0
        updated = 0
        skipped = 0

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp_file:
            tmp_file.write(file_content)
            tmp_path = Path(tmp_file.name)

        try:
            df = pd.read_excel(tmp_path)
            total_rows = len(df)

            logger.info(f"Importing financial data from {filename}", total_rows=total_rows)

            required_columns = ["entity_code", "period_start", "period_end", "currency"]
            missing_cols = [c for c in required_columns if c not in df.columns]
            if missing_cols:
                return FinancialDataImportResult(
                    success=False,
                    total_rows=total_rows,
                    created=0, updated=0, skipped=0,
                    errors=[
                        FinancialDataImportError(
                            row=0,
                            message=f"Missing required columns: {missing_cols}",
                        )
                    ],
                )

            # Build entity code -> id lookup
            entity_result = await self.db.execute(select(FinancialEntity))
            entity_lookup = {e.code: e.id for e in entity_result.scalars().all()}

            financial_fields = [
                "revenue", "other_revenue", "total_revenue",
                "cost_of_goods", "grid_cost", "land_cost", "payroll_expenses",
                "service_agreements", "insurance", "other_operating_expenses",
                "total_operating_expenses", "ebitda", "depreciation", "ebit",
                "net_interest", "net_other_financial", "earnings_before_tax",
                "tax", "net_income", "reported_generation_gwh",
            ]

            for idx, row in df.iterrows():
                row_num = idx + 2

                try:
                    entity_code = str(row["entity_code"]).strip()
                    if not entity_code or entity_code == "nan":
                        errors.append(FinancialDataImportError(
                            row=row_num, field="entity_code",
                            message="Entity code is required",
                        ))
                        skipped += 1
                        continue

                    entity_id = entity_lookup.get(entity_code)
                    if not entity_id:
                        if entity_code not in unmatched_entities:
                            unmatched_entities.append(entity_code)
                        errors.append(FinancialDataImportError(
                            row=row_num, field="entity_code",
                            value=entity_code,
                            message=f"Entity not found: {entity_code}",
                        ))
                        skipped += 1
                        continue

                    period_start = pd.to_datetime(row["period_start"]).date()
                    period_end = pd.to_datetime(row["period_end"]).date()
                    currency = str(row["currency"]).strip().upper()

                    record_data = {
                        "financial_entity_id": entity_id,
                        "period_start": period_start,
                        "period_end": period_end,
                        "currency": currency,
                        "is_synthetic": bool(row.get("is_synthetic", False)) if pd.notna(row.get("is_synthetic", False)) else False,
                        "source": "excel_import",
                    }

                    # Parse period_length_months
                    if "period_length_months" in row and pd.notna(row["period_length_months"]):
                        try:
                            record_data["period_length_months"] = Decimal(str(row["period_length_months"]))
                        except (InvalidOperation, ValueError):
                            pass

                    # Parse financial fields
                    for field in financial_fields:
                        if field in row and pd.notna(row[field]):
                            try:
                                record_data[field] = Decimal(str(row[field]))
                            except (InvalidOperation, ValueError):
                                errors.append(FinancialDataImportError(
                                    row=row_num, field=field,
                                    value=str(row[field]),
                                    message=f"Invalid number: {row[field]}",
                                ))

                    # Parse comment
                    if "comment" in row and pd.notna(row["comment"]):
                        record_data["comment"] = str(row["comment"])

                    # Compute totals
                    record_data = self._compute_totals(record_data)

                    # Upsert
                    existing = await self.get_by_entity_and_period(entity_id, period_start)
                    if existing:
                        for field, value in record_data.items():
                            if field != "financial_entity_id":
                                setattr(existing, field, value)
                        updated += 1
                    else:
                        new_record = FinancialData(**record_data)
                        self.db.add(new_record)
                        created += 1

                except Exception as e:
                    logger.error(f"Error processing row {row_num}", error=str(e))
                    errors.append(FinancialDataImportError(
                        row=row_num, message=f"Unexpected error: {str(e)}",
                    ))
                    skipped += 1

            await self.db.commit()

            logger.info(
                "Financial data import completed",
                created=created, updated=updated, skipped=skipped, errors=len(errors),
            )

            return FinancialDataImportResult(
                success=len(errors) == 0,
                total_rows=total_rows,
                created=created, updated=updated, skipped=skipped,
                errors=errors,
                unmatched_entities=unmatched_entities,
            )

        except Exception as e:
            logger.error("Failed to import financial data", error=str(e))
            await self.db.rollback()
            return FinancialDataImportResult(
                success=False,
                total_rows=0,
                created=0, updated=0, skipped=0,
                errors=[FinancialDataImportError(
                    row=0, message=f"Import failed: {str(e)}",
                )],
            )
        finally:
            tmp_path.unlink(missing_ok=True)
