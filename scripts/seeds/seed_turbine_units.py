#!/usr/bin/env python3
"""
Seed script for turbine_units table
"""

from datetime import datetime
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from app.models.turbine_model import TurbineModel
from app.models.turbine_unit import TurbineUnit
from app.models.windfarm import Windfarm

# Raw turbine unit data
TURBINE_UNITS_DATA = [
    {
        "windfarm_name": "Aberdeen",
        "turbine_model": "V164-8.0",
        "turbine_status": "operational",
        "units": 11,
    },
    {
        "windfarm_name": "Albatros / Hohe See",
        "turbine_model": "SWT-7.0-154",
        "turbine_status": "operational",
        "units": 87,
    },
    {
        "windfarm_name": "Alpha Ventus",
        "turbine_model": "AD5-116",
        "turbine_status": "operational",
        "units": 6,
    },
    {
        "windfarm_name": "Alpha Ventus",
        "turbine_model": "REpower 5M",
        "turbine_status": "operational",
        "units": 6,
    },
    {
        "windfarm_name": "Amrumbank West",
        "turbine_model": "SWT-3.6-120",
        "turbine_status": "operational",
        "units": 80,
    },
    {
        "windfarm_name": "Anholt",
        "turbine_model": "SWT-3.6-120",
        "turbine_status": "operational",
        "units": 111,
    },
    {
        "windfarm_name": "Arcadis Ost 1",
        "turbine_model": "V174-9.5",
        "turbine_status": "operational",
        "units": 27,
    },
    {
        "windfarm_name": "Arkona",
        "turbine_model": "SWT-6.0-154",
        "turbine_status": "operational",
        "units": 60,
    },
    {
        "windfarm_name": "Avedøre",
        "turbine_model": "SWT-3.6-120",
        "turbine_status": "operational",
        "units": 3,
    },
    {
        "windfarm_name": "Baltic 1",
        "turbine_model": "SWT-2.3-93",
        "turbine_status": "operational",
        "units": 21,
    },
    {
        "windfarm_name": "Baltic 2",
        "turbine_model": "SWT-3.6-120",
        "turbine_status": "operational",
        "units": 80,
    },
    {
        "windfarm_name": "Baltic Eagle",
        "turbine_model": "V174-9.5",
        "turbine_status": "operational",
        "units": 50,
    },
    {
        "windfarm_name": "Baltic Power",
        "turbine_model": "V236-15.0",
        "turbine_status": "under_installation",
        "units": 76,
    },
    {
        "windfarm_name": "BARD Offshore 1",
        "turbine_model": "BARD 5.0",
        "turbine_status": "operational",
        "units": 80,
    },
    {
        "windfarm_name": "Barrow",
        "turbine_model": "V90-3.0",
        "turbine_status": "operational",
        "units": 30,
    },
    {
        "windfarm_name": "Beatrice",
        "turbine_model": "SWT-7.0-154",
        "turbine_status": "operational",
        "units": 84,
    },
    {
        "windfarm_name": "Belwind 1",
        "turbine_model": "V90-3.0",
        "turbine_status": "operational",
        "units": 55,
    },
    {
        "windfarm_name": "Belwind 1",
        "turbine_model": "Haliade 150-6",
        "turbine_status": "operational",
        "units": 1,
    },
    {
        "windfarm_name": "Block Island",
        "turbine_model": "Haliade 150-6",
        "turbine_status": "operational",
        "units": 5,
    },
    {
        "windfarm_name": "Borkum Riffgat",
        "turbine_model": "SWT-3.6-120",
        "turbine_status": "operational",
        "units": 30,
    },
    {
        "windfarm_name": "Borkum Riffgrund 1",
        "turbine_model": "SWT-4.0-120",
        "turbine_status": "operational",
        "units": 78,
    },
    {
        "windfarm_name": "Borkum Riffgrund 2",
        "turbine_model": "V164-8.0",
        "turbine_status": "operational",
        "units": 56,
    },
]


def generate_turbine_unit_code(
    windfarm_code: str, turbine_model_code: str, unit_number: int
) -> str:
    """Generate a code following format: [TU]_[windfarm_code]_[turbine_model_code]_[3 digit unit number]

    Args:
        windfarm_code: The windfarm code (e.g., WF_OF_0011_000001)
        turbine_model_code: The turbine model code (e.g., V164-8.0)
        unit_number: The unit number within the windfarm

    Returns:
        Formatted code like: TU_WF_OF_0011_000001_V164-8.0_001
    """
    # Format unit number as 3 digits
    unit_str = str(unit_number).zfill(3)

    # Clean up turbine model code to make it filesystem-safe
    safe_model_code = turbine_model_code.replace(" ", "_").replace("/", "-")

    return f"TU_{windfarm_code}_{safe_model_code}_{unit_str}"


def seed_turbine_units(db: Session):
    """Seed turbine_units table with initial data"""
    print(f"  Checking for existing turbine units...")

    # Get existing turbine unit codes to avoid duplicates
    existing_codes = {tu.code for tu in db.query(TurbineUnit.code).all()}

    # Create lookups for windfarms and turbine models
    windfarms = {wf.name: wf for wf in db.query(Windfarm).all()}
    turbine_models = {tm.model: tm for tm in db.query(TurbineModel).all()}

    success_count = 0
    failure_count = 0
    failures = []

    for unit_data in TURBINE_UNITS_DATA:
        windfarm_name = unit_data["windfarm_name"]
        turbine_model_name = unit_data["turbine_model"]
        status = unit_data["turbine_status"]
        unit_count = unit_data["units"]

        # Look up windfarm
        windfarm = windfarms.get(windfarm_name)
        if not windfarm:
            error_msg = f"Windfarm '{windfarm_name}' not found"
            failures.append(
                {
                    "windfarm_name": windfarm_name,
                    "turbine_model": turbine_model_name,
                    "error": error_msg,
                }
            )
            failure_count += unit_count
            print(f"    ❌ {error_msg}")
            continue

        # Look up turbine model
        turbine_model = turbine_models.get(turbine_model_name)
        if not turbine_model:
            error_msg = f"Turbine model '{turbine_model_name}' not found"
            failures.append(
                {
                    "windfarm_name": windfarm_name,
                    "turbine_model": turbine_model_name,
                    "error": error_msg,
                }
            )
            failure_count += unit_count
            print(f"    ❌ {error_msg}")
            continue

        # Create individual turbine units
        units_created = 0
        for unit_num in range(1, unit_count + 1):
            try:
                # Generate unique code
                code = generate_turbine_unit_code(windfarm.code, turbine_model.model, unit_num)

                # Skip if already exists
                if code in existing_codes:
                    continue

                # Create turbine unit
                # Use windfarm coordinates as a placeholder since individual turbine coordinates are not provided
                turbine_unit = TurbineUnit(
                    code=code,
                    windfarm_id=windfarm.id,
                    turbine_model_id=turbine_model.id,
                    status=status,
                    lat=windfarm.lat or 0.0,  # Use windfarm lat or default to 0
                    lng=windfarm.lng or 0.0,  # Use windfarm lng or default to 0
                )

                db.add(turbine_unit)
                existing_codes.add(code)
                units_created += 1
                success_count += 1

            except Exception as e:
                failure_count += 1
                error_msg = str(e)
                failures.append(
                    {
                        "windfarm_name": windfarm_name,
                        "turbine_model": turbine_model_name,
                        "unit_number": unit_num,
                        "error": error_msg,
                    }
                )
                print(
                    f"    ❌ Failed to add unit {unit_num} for {windfarm_name}/{turbine_model_name}: {error_msg}"
                )
                db.rollback()

        if units_created > 0:
            print(
                f"    ✅ Added {units_created} units for {windfarm_name} with {turbine_model_name}"
            )

    # Commit successful changes
    if success_count > 0:
        db.commit()
        print(f"  Successfully added {success_count} turbine units")

    # Generate failure report if any
    if failures:
        import json
        from pathlib import Path

        report_path = Path(__file__).parent / "turbine_units_seed_failures.json"
        with open(report_path, "w") as f:
            json.dump(
                {
                    "summary": {
                        "total_attempted": sum(d["units"] for d in TURBINE_UNITS_DATA),
                        "successful": success_count,
                        "failed": failure_count,
                        "timestamp": datetime.now().isoformat(),
                    },
                    "failures": failures,
                },
                f,
                indent=2,
                default=str,
            )
        print(f"  Generated failure report: {report_path}")
        print(f"  Failed to add {failure_count} turbine units - see report for details")

    print(f"  Turbine unit seeding completed: {success_count} successful, {failure_count} failed")
