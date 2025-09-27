# Generation Unit Lifespan Implementation

## Overview
We've implemented date-based filtering for generation units to ensure capacity is only assigned when units are operational.

## Changes Made

### 1. Aggregation Script Updates (`process_generation_data_daily.py`)

#### Added Operational Date Check Method
```python
def is_unit_operational(self, unit_info: Dict, check_date: datetime) -> bool:
    """Check if a generation unit is operational on a given date."""
```

This method:
- Checks if a unit's start_date and end_date allow operation on the given date
- Returns False if the date is before start_date or after end_date
- Handles both date and datetime objects

#### Updated Generation Units Cache
The cache now includes start_date and end_date fields:
```python
self.generation_units_cache[key] = {
    'id': unit.id,
    'windfarm_id': unit.windfarm_id,
    'capacity_mw': float(unit.capacity_mw) if unit.capacity_mw else None,
    'name': unit.name,
    'start_date': unit.start_date,  # Added
    'end_date': unit.end_date      # Added
}
```

#### Updated Transform Methods
All source-specific transform methods now check operational dates:

- **ENTSOE**: Checks if unit is operational before assigning capacity
- **ELEXON**: Checks if unit is operational before assigning capacity
- **TAIPOWER**: Checks if unit is operational before assigning capacity
- **NVE**: Checks if unit is operational before assigning capacity

Example implementation:
```python
# Check if unit is operational on this date
if self.is_unit_operational(unit_info, hour):
    capacity_mw = unit_info.get('capacity_mw')
else:
    capacity_mw = None  # Unit not operational on this date
```

## Impact

### Correct Behavior
- Units with start_date: No capacity assigned before start_date
- Units with end_date: No capacity assigned after end_date
- Units without dates: Always considered operational (backward compatible)

### Capacity Factor Calculation
- When capacity_mw is None (unit not operational), capacity_factor also becomes None
- Prevents incorrect capacity factors from being calculated

### Windfarm Capacity
- Windfarm total capacity for a date now correctly sums only operational units
- Historical accuracy for windfarm capacity over time

## Testing

Test script created: `scripts/test_unit_date_filtering.py`

Test results show:
- ✅ Date filtering logic correctly implemented
- ⚠️ Existing data has capacity assigned outside operational periods (needs reprocessing)
- ✅ New data will be correctly filtered

## Next Steps

1. **Reprocess Historical Data**: Run aggregation for dates where units have start/end dates
2. **Update Reports**: Ensure reporting considers unit operational periods
3. **Validation**: Add checks to prevent capacity assignment outside operational periods

## Example Units with Date Restrictions

From our database:
- **NVE:21** (Andøya): Start Date: 2008-08-29
- **NVE:72** (Bjerkreim): Start Date: 2020-06-12
- **NVE:1095** (Dønnesfjord): Start Date: 2023-01-28

These units previously had capacity incorrectly assigned before their start dates, which is now fixed in the aggregation process.