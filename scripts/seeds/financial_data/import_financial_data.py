"""
Seed import script for financial data from wide-format CSV.

Usage:
    poetry run python scripts/seeds/financial_data/import_financial_data.py

The CSV has columns as (windfarm, entity, period) combinations and rows as financial metrics.
"""

import asyncio
import csv
import os
import re
import sys
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models.financial_data import FinancialData
from app.models.financial_entity import FinancialEntity
from app.models.windfarm import Windfarm
from app.models.windfarm_financial_entity import WindfarmFinancialEntity

# Multi-windfarm entity mapping
MULTI_WINDFARM_MAP = {
    "Midgard Vind AS": ["Bessakerfjellet", "Skomakerfjellet"],
    "Renantis Norway AS": ["Hennøy", "Okla"],
    "Fosen Vind DA": ["Hitra 2", "Geitfjellet", "Harbaksfjellet", "Kvenndalsfjellet", "Storheia"],
    "Varanger Kraftvind AS": ["Raggovidda", "Raggovidda 2"],
}

# Row indices in the CSV (0-based)
ROW_WINDFARM = 1
ROW_PERIOD_START = 2
ROW_PERIOD_END = 3
ROW_PERIOD_LENGTH = 4
ROW_OWNER = 5
ROW_CURRENCY = 6
ROW_REPORTED_GEN = 10
ROW_REVENUE = 12
ROW_OTHER_REVENUE = 13
ROW_TOTAL_REVENUE = 15
ROW_COST_OF_GOODS = 17
ROW_GRID_COST = 18
ROW_LAND_COST = 19
ROW_PAYROLL = 20
ROW_SERVICE_AGREEMENTS = 21
ROW_INSURANCE = 22
ROW_OTHER_OPEX = 23
ROW_TOTAL_OPEX = 25
ROW_EBITDA = 27
ROW_DEPRECIATION = 29
ROW_EBIT = 31
ROW_NET_INTEREST = 33
ROW_NET_OTHER_FINANCIAL = 34
ROW_EBT = 36
ROW_TAX = 38
ROW_NET_INCOME = 40
ROW_NOTES = 45
ROW_SYNTHETIC_FLAG = 0

# First data column (0-based)
FIRST_DATA_COL = 4


def parse_accounting_number(value: str) -> Optional[Decimal]:
    """Parse accounting format numbers.

    "7,147,000"  → Decimal("7147000")
    "(466,360)"  → Decimal("-466360")
    "-"          → None
    ""           → None
    """
    if not value:
        return None
    value = value.strip()
    if value == "-" or value == "":
        return None

    negative = False
    if value.startswith("(") and value.endswith(")"):
        negative = True
        value = value[1:-1]

    # Remove thousands separators
    value = value.replace(",", "")

    try:
        result = Decimal(value)
        return -result if negative else result
    except (InvalidOperation, ValueError):
        return None


def parse_date(value: str) -> Optional[date]:
    """Parse date strings like '1-Jan-20' or '31-Dec-24'."""
    if not value or not value.strip():
        return None
    value = value.strip()
    try:
        dt = datetime.strptime(value, "%d-%b-%y")
        return dt.date()
    except ValueError:
        try:
            dt = datetime.strptime(value, "%d-%b-%Y")
            return dt.date()
        except ValueError:
            print(f"  WARNING: Could not parse date: {value}")
            return None


def slugify_code(name: str) -> str:
    """Convert entity name to a URL-safe code.

    "VARDAFJELLET VINDKRAFT AS" → "vardafjellet-vindkraft-as"
    """
    code = name.lower().strip()
    code = re.sub(r"[^a-z0-9\s-]", "", code)
    code = re.sub(r"\s+", "-", code)
    code = re.sub(r"-+", "-", code)
    return code.strip("-")


def read_wide_csv(filepath: str) -> List[Dict]:
    """Read the wide-format CSV and transpose into a list of record dicts."""
    records = []

    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    total_cols = len(rows[0]) if rows else 0
    print(f"CSV loaded: {len(rows)} rows x {total_cols} columns")

    for j in range(FIRST_DATA_COL, total_cols):
        windfarm_name = rows[ROW_WINDFARM][j].strip() if j < len(rows[ROW_WINDFARM]) else ""
        if not windfarm_name:
            continue

        owner_name = rows[ROW_OWNER][j].strip() if j < len(rows[ROW_OWNER]) else ""
        if not owner_name:
            continue

        period_start = parse_date(rows[ROW_PERIOD_START][j] if j < len(rows[ROW_PERIOD_START]) else "")
        period_end = parse_date(rows[ROW_PERIOD_END][j] if j < len(rows[ROW_PERIOD_END]) else "")
        if not period_start or not period_end:
            continue

        def _get(row_idx: int) -> str:
            if row_idx < len(rows) and j < len(rows[row_idx]):
                return rows[row_idx][j].strip()
            return ""

        period_length = _get(ROW_PERIOD_LENGTH)
        currency = _get(ROW_CURRENCY).upper() or "EUR"
        is_synthetic = _get(ROW_SYNTHETIC_FLAG).lower() == "synthetic"

        record = {
            "windfarm_name": windfarm_name,
            "owner_name": owner_name,
            "period_start": period_start,
            "period_end": period_end,
            "period_length_months": Decimal(period_length) if period_length else None,
            "currency": currency,
            "is_synthetic": is_synthetic,
            "reported_generation_gwh": parse_accounting_number(_get(ROW_REPORTED_GEN)),
            "revenue": parse_accounting_number(_get(ROW_REVENUE)),
            "other_revenue": parse_accounting_number(_get(ROW_OTHER_REVENUE)),
            "total_revenue": parse_accounting_number(_get(ROW_TOTAL_REVENUE)),
            "cost_of_goods": parse_accounting_number(_get(ROW_COST_OF_GOODS)),
            "grid_cost": parse_accounting_number(_get(ROW_GRID_COST)),
            "land_cost": parse_accounting_number(_get(ROW_LAND_COST)),
            "payroll_expenses": parse_accounting_number(_get(ROW_PAYROLL)),
            "service_agreements": parse_accounting_number(_get(ROW_SERVICE_AGREEMENTS)),
            "insurance": parse_accounting_number(_get(ROW_INSURANCE)),
            "other_operating_expenses": parse_accounting_number(_get(ROW_OTHER_OPEX)),
            "total_operating_expenses": parse_accounting_number(_get(ROW_TOTAL_OPEX)),
            "ebitda": parse_accounting_number(_get(ROW_EBITDA)),
            "depreciation": parse_accounting_number(_get(ROW_DEPRECIATION)),
            "ebit": parse_accounting_number(_get(ROW_EBIT)),
            "net_interest": parse_accounting_number(_get(ROW_NET_INTEREST)),
            "net_other_financial": parse_accounting_number(_get(ROW_NET_OTHER_FINANCIAL)),
            "earnings_before_tax": parse_accounting_number(_get(ROW_EBT)),
            "tax": parse_accounting_number(_get(ROW_TAX)),
            "net_income": parse_accounting_number(_get(ROW_NET_INCOME)),
            "comment": _get(ROW_NOTES) if ROW_NOTES < len(rows) else None,
            "source": "seed_import",
        }

        # Clean empty comment
        if record["comment"] == "" or record["comment"] == "-":
            record["comment"] = None

        records.append(record)

    return records


async def find_or_create_entity(
    db: AsyncSession, owner_name: str, entity_cache: Dict[str, FinancialEntity]
) -> FinancialEntity:
    """Find an existing entity or create a new one. Handles race conditions via savepoints."""
    if owner_name in entity_cache:
        return entity_cache[owner_name]

    code = slugify_code(owner_name)
    result = await db.execute(
        select(FinancialEntity).where(FinancialEntity.code == code)
    )
    entity = result.scalar_one_or_none()

    if entity:
        entity_cache[owner_name] = entity
        return entity

    is_holdco = owner_name in MULTI_WINDFARM_MAP
    entity = FinancialEntity(
        code=code,
        name=owner_name,
        entity_type="holdco" if is_holdco else "spv",
    )
    db.add(entity)
    await db.flush()
    entity_cache[owner_name] = entity
    return entity


async def ensure_windfarm_link(
    db: AsyncSession,
    entity_id: int,
    wf_id: int,
    relationship_type: str,
    linked_pairs: Set[Tuple[int, int]],
) -> None:
    """Create windfarm link if it doesn't exist. Check DB if not in local cache."""
    if (entity_id, wf_id) in linked_pairs:
        return

    # Check if link exists in DB already (from a previous run)
    existing = await db.execute(
        select(WindfarmFinancialEntity).where(
            WindfarmFinancialEntity.financial_entity_id == entity_id,
            WindfarmFinancialEntity.windfarm_id == wf_id,
        )
    )
    if existing.scalar_one_or_none():
        linked_pairs.add((entity_id, wf_id))
        return

    link = WindfarmFinancialEntity(
        financial_entity_id=entity_id,
        windfarm_id=wf_id,
        relationship_type=relationship_type,
    )
    db.add(link)
    linked_pairs.add((entity_id, wf_id))


async def run_import(db_url: str):
    """Main import function."""
    engine = create_async_engine(db_url, echo=False)
    async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    csv_path = os.path.join(
        os.path.dirname(__file__),
        "data",
        "Financial data_upload_2026 02 25(Financial data).csv",
    )

    print(f"Reading CSV: {csv_path}")
    records = read_wide_csv(csv_path)
    print(f"Parsed {len(records)} records from CSV")

    async with async_session() as db:
        # Build windfarm name -> id lookup
        wf_result = await db.execute(select(Windfarm))
        windfarm_lookup: Dict[str, int] = {wf.name: wf.id for wf in wf_result.scalars().all()}
        print(f"Loaded {len(windfarm_lookup)} windfarms from DB")

        # Track entities and links
        entity_cache: Dict[str, FinancialEntity] = {}  # owner_name -> entity
        linked_pairs: Set[Tuple[int, int]] = set()  # (entity_id, windfarm_id)
        unmatched_windfarms: Set[str] = set()

        created = 0
        updated = 0
        skipped = 0
        errors = 0

        for i, record in enumerate(records):
            try:
                owner_name = record["owner_name"]
                windfarm_name = record["windfarm_name"]

                # Find or create FinancialEntity
                entity = await find_or_create_entity(db, owner_name, entity_cache)

                # Link to windfarm(s)
                if windfarm_name.lower() == "multiple":
                    target_wfs = MULTI_WINDFARM_MAP.get(owner_name, [])
                    for wf_name in target_wfs:
                        wf_id = windfarm_lookup.get(wf_name)
                        if wf_id:
                            await ensure_windfarm_link(
                                db, entity.id, wf_id, "consolidated", linked_pairs
                            )
                        else:
                            unmatched_windfarms.add(wf_name)
                else:
                    wf_id = windfarm_lookup.get(windfarm_name)
                    if wf_id:
                        await ensure_windfarm_link(
                            db, entity.id, wf_id, "primary_asset", linked_pairs
                        )
                    else:
                        unmatched_windfarms.add(windfarm_name)

                # Upsert FinancialData
                existing_data_result = await db.execute(
                    select(FinancialData).where(
                        FinancialData.financial_entity_id == entity.id,
                        FinancialData.period_start == record["period_start"],
                    )
                )
                existing_data = existing_data_result.scalar_one_or_none()

                financial_fields = {
                    k: v for k, v in record.items()
                    if k not in ("windfarm_name", "owner_name")
                }

                if existing_data:
                    for field, value in financial_fields.items():
                        setattr(existing_data, field, value)
                    updated += 1
                else:
                    fd = FinancialData(
                        financial_entity_id=entity.id,
                        **financial_fields,
                    )
                    db.add(fd)
                    created += 1

                # Flush periodically
                if (i + 1) % 200 == 0:
                    await db.flush()
                    print(f"  Processed {i + 1}/{len(records)}...")

            except Exception as e:
                await db.rollback()
                errors += 1
                print(f"  ERROR on record {i}: {e}")

        await db.commit()

    await engine.dispose()

    print("\n=== Import Summary ===")
    print(f"Total records parsed:   {len(records)}")
    print(f"Created:                {created}")
    print(f"Updated:                {updated}")
    print(f"Skipped:                {skipped}")
    print(f"Errors:                 {errors}")
    print(f"Entities created:       {len(entity_cache)}")
    print(f"Windfarm links created: {len(linked_pairs)}")
    if unmatched_windfarms:
        print(f"Unmatched windfarms:    {sorted(unmatched_windfarms)}")


if __name__ == "__main__":
    from app.core.config import get_settings

    settings = get_settings()
    db_url = settings.database_url_async

    print(f"Connecting to database...")
    asyncio.run(run_import(db_url))
