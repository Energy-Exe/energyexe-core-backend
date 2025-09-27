# Fix ENTSOE & TAIPOWER Windfarm Mappings

## Overview
This document explains how to fix the windfarm mapping issues for ENTSOE and TAIPOWER sources, which will increase the number of windfarms with data from 212 to approximately 250+.

## Current Issues

### ENTSOE
- **38 units** without windfarm mappings
- Results in **83,573 orphaned records**
- These are legitimate UK offshore wind farms that need mapping

### TAIPOWER
- All units have windfarm_id ✅
- But **578,446 records** have NULL windfarm_id due to:
  - Generation units not being linked during aggregation
  - The data was already processed without proper linking

## Step-by-Step Fix

### 1. Apply the Mapping Fixes

First, run the fix script in dry-run mode to preview:
```bash
poetry run python scripts/fix_entsoe_taipower_mappings.py
```

Review the output, then apply the fixes:
```bash
poetry run python scripts/fix_entsoe_taipower_mappings.py --apply
```

This will:
- Map 38 ENTSOE units to their correct windfarms
- Update 578,446 existing TAIPOWER records with windfarm_id

### 2. Re-run Aggregation for ENTSOE

Re-process ENTSOE data to fix the orphaned records:
```bash
poetry run python scripts/seeds/aggregate_generation_data/process_generation_data_robust.py \
  --start 2015-01-01 --end 2024-12-31 --source ENTSOE
```

### 3. Verify the Fix

Check if the fixes worked:
```bash
poetry run python -c "
import asyncio
from sqlalchemy import text
from app.core.database import get_session_factory

async def verify():
    factory = get_session_factory()
    async with factory() as db:
        result = await db.execute(text('''
            SELECT
                source,
                COUNT(*) as total_records,
                COUNT(CASE WHEN windfarm_id IS NULL THEN 1 END) as orphaned,
                COUNT(DISTINCT windfarm_id) as unique_windfarms
            FROM generation_data
            WHERE source IN ('ENTSOE', 'TAIPOWER')
            GROUP BY source
        '''))

        for row in result:
            print(f'{row.source}: {row.total_records:,} records, {row.orphaned:,} orphaned, {row.unique_windfarms} windfarms')

asyncio.run(verify())
"
```

### 4. Check API

After the fixes, the API should show more windfarms:
```bash
curl http://127.0.0.1:8000/api/v1/comparison/windfarms | jq length
```

Expected: ~250+ windfarms (up from 212)

## Expected Results

### Before Fix
- 212 windfarms with data
- ENTSOE: 39 windfarms with data
- TAIPOWER: 0 windfarms with data

### After Fix
- ~250+ windfarms with data
- ENTSOE: ~50+ windfarms with data
- TAIPOWER: 20 windfarms with data

## Mappings Applied

### ENTSOE Mappings (38 units → 13 windfarms)
- Beatrice (4 units)
- Dogger Bank A&B (10 units)
- East Anglia One (1 unit)
- Hornsea 1 (2 units)
- Hornsea 2 (3 units)
- Humber Gateway (1 unit)
- Moray East (3 units)
- Moray West (4 units)
- Neart Na Gaoithe (2 units)
- Seagreen (6 units)
- Thanet (2 units)

### TAIPOWER Fix
- 578,446 records updated with windfarm_id
- Links to 20 Taiwan windfarms

## Notes

- ELEXON and NVE are already 100% mapped
- EIA (USA) and ENERGISTYRELSEN (Denmark) data are not processed by daily aggregation
- To get all 1,578 windfarms with data, additional data sources need to be integrated

## Cleanup

After successful fixes, remove the fix script:
```bash
rm scripts/fix_entsoe_taipower_mappings.py
```