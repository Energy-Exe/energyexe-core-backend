# Energistyrelsen Data Processing

## Source Files
- **Location**: `data/energistyrelsen_monthly_data_until_2025-01.xlsx`
- **Format**: Danish turbine monthly generation data (pivoted)
- **Period**: 2002-2025 (monthly aggregation)

## Data Structure - Pivoted Format
```
Columns 0-16: Turbine metadata (GSRN, location, specs)
Columns 17+:  Monthly generation values

            | GSRN    | Location | ... | 2002-02 | 2002-03 | 2002-04 | ...
Turbine 1   | 571234  | Jutland  | ... | 125000  | 134000  | 118000  | ... (kWh)
Turbine 2   | 571235  | Zealand  | ... | 98000   | 102000  | 95000   | ...
```

## Data Mapping

### Processing Logic
1. **Extract GSRN** (Grid System Registration Number) from column 1
2. **Match GSRN** against `generation_units` table with `source='ENERGISTYRELSEN'`
3. **Unpivot monthly columns** - each becomes a separate record:
   - Parse month from column header (e.g., '2002-02-01')
   - Convert kWh → MWh (divide by 1000)
4. **Calculate period boundaries**:
   - period_start: First day of month
   - period_end: Last second of month
5. **Store** in `generation_data_raw` table:
   - `period_type`: 'month' (not 'hour' like others)
   - `identifier`: GSRN code
   - `value_extracted`: Monthly generation in MWh
   - `data`: JSONB with original kWh, GSRN, month

## Key Features
- **Monthly data** (not hourly) - unique among all sources
- **kWh → MWh conversion** built-in
- Handles 10K+ turbines × 276 months = 2.7M+ data points
- Filters to only configured turbines (312 of 10K+)