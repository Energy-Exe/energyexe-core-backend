# ELEXON Data Processing

## Source Files
- **Location**: `data/*.csv` (4 CSV files)
- **Format**: Half-hourly UK electricity generation data
- **Size**: ~100M+ records total

## Data Mapping

### CSV Columns → Database Fields
```
Settlement Date  → period_start (converted to datetime)
Settlement Period → used to calculate exact half-hour slot
BMU ID           → identifier (matched against generation_units.code)
FPN Level        → value_extracted (MW output)
```

### Processing Logic
1. **Read CSV** with Polars (5-10x faster than pandas)
2. **Filter BMU IDs** - only keep records where BMU ID exists in `generation_units` table with `source='ELEXON'`
3. **Time Calculation** - combine date + period number to get exact 30-minute timestamp:
   - Period 1 = 00:00-00:30
   - Period 2 = 00:30-01:00
   - Period 48 = 23:30-00:00
4. **Store** in `generation_data_raw` table:
   - `period_start`: Start of 30-min period
   - `period_end`: End of 30-min period (start + 30 min)
   - `identifier`: BMU ID (e.g., 'T_WBURB-1')
   - `value_extracted`: Generation in MW
   - `data`: JSONB with additional metadata

## Key Features
- Strips whitespace from BMU IDs (important for matching)
- Parallel processing with configurable workers
- PostgreSQL COPY for bulk inserts (10-50x faster)
- Only imports configured units (filters ~70% of data)