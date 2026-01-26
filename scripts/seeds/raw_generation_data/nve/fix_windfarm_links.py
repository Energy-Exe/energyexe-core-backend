#!/usr/bin/env python3
"""
Fix NVE generation units that are missing windfarm_id links.

Mappings based on analysis:
- Kjøllefjord (code 2) → Kjøllefjord windfarm (id: 7193)
- METCentre Karmøy (code 11) → METCentre Karmøy windfarm (id: 8767)
- Vikna (code 22) → Ytre Vikna windfarm (id: 7226)
- Valsneset testpark (code 40) → Valsneset windfarm (id: 7224)
- Raggovidda 2 (code 1090) → Raggovidda 2 windfarm (id: 8772)

Remaining unlinked (no matching windfarm found):
- Fjeldskår (code 1) - decommissioned 2018, not in windfarms table
- Kvalnes (code 23) - decommissioned 2018, not in windfarms table
- Hovden Vesterålen (code 24) - decommissioned 2015, not in windfarms table
- Sandøy (code 4) - "Nye Sandøy" exists but may be different facility
- Rye Vind (code 49) - no matching windfarm found
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parents[4]))

from sqlalchemy import text
from app.core.database import get_session_factory


# Mapping from generation_unit code to windfarm_id
MAPPINGS = {
    "2": 7193,      # Kjøllefjord → Kjøllefjord
    "11": 8767,     # METCentre Karmøy → METCentre Karmøy
    "22": 7226,     # Vikna → Ytre Vikna
    "40": 7224,     # Valsneset testpark → Valsneset
    "1090": 8772,   # Raggovidda 2 → Raggovidda 2
}


async def fix_windfarm_links(dry_run: bool = True):
    """Link NVE generation units to their windfarms."""

    async with get_session_factory()() as db:
        print("=" * 70)
        print("FIXING NVE GENERATION UNIT WINDFARM LINKS")
        print("=" * 70)
        print(f"\nMode: {'DRY RUN' if dry_run else 'APPLYING CHANGES'}")

        # Show current state
        print("\n--- Current Unlinked Generation Units ---")
        result = await db.execute(text("""
            SELECT id, name, code, source
            FROM generation_units
            WHERE source = 'NVE' AND windfarm_id IS NULL
            ORDER BY code
        """))
        unlinked = result.fetchall()
        for row in unlinked:
            print(f"  Code {row[2]}: {row[1]} (id: {row[0]})")

        # Apply mappings
        print("\n--- Applying Mappings ---")
        total_updated = 0

        for code, windfarm_id in MAPPINGS.items():
            # Verify windfarm exists
            result = await db.execute(text("""
                SELECT name FROM windfarms WHERE id = :wf_id
            """), {"wf_id": windfarm_id})
            wf_row = result.fetchone()
            if not wf_row:
                print(f"⚠️  Windfarm {windfarm_id} not found, skipping code {code}")
                continue

            windfarm_name = wf_row[0]

            # Find generation units with this code
            result = await db.execute(text("""
                SELECT id, name FROM generation_units
                WHERE source = 'NVE' AND code = :code AND windfarm_id IS NULL
            """), {"code": code})
            units = result.fetchall()

            for unit_id, unit_name in units:
                print(f"  {unit_name} (code {code}) → {windfarm_name}")

                if not dry_run:
                    await db.execute(text("""
                        UPDATE generation_units
                        SET windfarm_id = :wf_id
                        WHERE id = :unit_id
                    """), {"wf_id": windfarm_id, "unit_id": unit_id})
                    total_updated += 1

        if not dry_run:
            await db.commit()
            print(f"\n✓ Updated {total_updated} generation units")
        else:
            print(f"\n[DRY RUN] Would update {len([u for code in MAPPINGS for u in [1] if code in [row[2] for row in unlinked]])} generation units")

        # Show remaining unlinked
        print("\n--- Remaining Unlinked (no matching windfarm) ---")
        remaining_unlinked = [row for row in unlinked if row[2] not in MAPPINGS]
        if remaining_unlinked:
            for row in remaining_unlinked:
                print(f"  Code {row[2]}: {row[1]}")
            print("\nThese facilities need windfarm entries created or manual mapping:")
            print("  - Fjeldskår (code 1): Decommissioned 2018")
            print("  - Kvalnes (code 23): Decommissioned 2018")
            print("  - Hovden Vesterålen (code 24): Decommissioned 2015")
            print("  - Sandøy (code 4): 'Nye Sandøy' exists but may be different")
            print("  - Rye Vind (code 49): No matching windfarm found")
        else:
            print("  None - all units are linked!")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fix NVE generation unit windfarm links")
    parser.add_argument("--apply", action="store_true", help="Actually apply changes (default: dry run)")
    args = parser.parse_args()

    asyncio.run(fix_windfarm_links(dry_run=not args.apply))
