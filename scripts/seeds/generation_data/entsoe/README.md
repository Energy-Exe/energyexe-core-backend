# ENTSOE Data Processing

## Source Files
- **Location**: `data/*.xlsx` (129 monthly Excel files)
- **Format**: European electricity generation data (15-min/hourly)
- **Period**: 2014-2025

## Data Mapping

### Excel Columns → Database Fields
```
DateTime                 → period_start
GenerationUnitCode       → identifier (matched against generation_units.code)
InstalledGenCapacity     → stored in data JSONB
GenerationOutput         → value_extracted (MW)
Area                     → stored in data JSONB
```

### Processing Logic
1. **Convert Excel to CSV** first (10x faster processing)
2. **Filter Unit Codes** - only keep records where code exists in `generation_units` table with `source='ENTSOE'`
3. **Time Handling**:
   - 15-min data: period_end = period_start + 15 minutes
   - Hourly data: period_end = period_start + 1 hour
4. **Clean Values**: Remove 'n/e' (not estimated) entries
5. **Store** in `generation_data_raw` table:
   - `period_start`: Timestamp from DateTime column
   - `period_end`: Calculated based on data frequency
   - `identifier`: Generation unit code (e.g., '48W0000000000047')
   - `value_extracted`: Generation in MW
   - `data`: JSONB with capacity, area, unit details

## Key Features
- Excel → CSV conversion for speed
- Handles both 15-min and hourly data
- Parallel processing of multiple files
- Only imports configured units (filters non-wind units)