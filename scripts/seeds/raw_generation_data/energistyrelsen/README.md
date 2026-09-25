# Energistyrelsen Data Processing

Current publications (2026+) come as two workbooks from
https://ens.dk (Data om vindmøller): **Vinddatasæt** (per-turbine stamdata +
monthly kWh; turbines metered collectively are blank) and
**Parkproduktion** (monthly kWh per park GUID). Park totals are split
equally over the park's turbines that are active in the month — the same
representation the historical import used.

```bash
# 1. Preview (no writes)
poetry run python scripts/jobs/import_vinddata.py Vinddata.xlsx --start 2023-01 --end 2026-08 \
  --park-file Parkproduktion.xlsx --update-decommissions --add-turbines \
  --park-map 9FF2FEBA-CEFC-4570-9052-E430239F5A03=8801 --model-map "SG DD-222=727" \
  --report-out preview.json
# 2. Review preview.json (months, parks.unmapped_with_production, turbines.create, revisions)
# 3. Apply: same command + --apply
```

Notes
- Values are reconciled per source month: unchanged rows stay, revised rows
  are rewritten, GSRNs absent from the workbook keep their existing rows.
- `--park-map` is only needed for a park that has no tracked turbine yet
  (e.g. Thor before its turbines were created). Afterwards the park →
  windfarm relation is inferred from existing turbine_units.
- A farm that already has placeholder units (e.g. `THOR-001`) never gets
  extra units: `--adopt-existing` gives the placeholders the GSRNs (earliest
  connection first); without it the GSRNs are listed as unresolved.
- Farms with ENTSOE hourly data for a month keep ENTSOE only in
  `generation_data` (raw rows are still stored).
- The old parallel importer below is disabled because it deletes the
  whole source.

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
