# Financial Ratios Feature

This document covers the financial ratios computation system that combines P&L financial data with actual generation data to produce per-MWh efficiency metrics and margins.

## Overview

The system computes three key ratios for each financial reporting period:

| Ratio | Formula | Unit |
|-------|---------|------|
| **Revenue/MWh** | `total_revenue / generated_mwh` | Currency per MWh |
| **Opex/MWh** | `total_operating_expenses / generated_mwh` | Currency per MWh |
| **EBITDA Margin** | `(ebitda / total_revenue) * 100` | Percentage |

Ratios are computed **on-demand** (no stored/cached values) so they always reflect the latest generation data ingested from ENTSOE, Elexon, NVE, etc.

## Architecture

### Data Flow

```
GET /financial-data/ratios/{windfarm_id}
  → Resolve linked FinancialEntity(s) via WindfarmFinancialEntity
  → For each entity, get ALL linked windfarm_ids (handles holdcos)
  → Determine effective COD = max(COD of all linked windfarms)
  → For each FinancialData period:
      → Skip if period_start < COD + 365 days (ramp-up)
      → SUM net generation from generation_data for linked windfarms
      → Compute 3 ratios
  → Return results with generation MWh for transparency
```

### Components

| Layer | File | What |
|-------|------|------|
| Schema | `app/schemas/financial_data.py` | `FinancialRatioPeriod`, `FinancialRatiosResponse` |
| Service | `app/services/financial_data_service.py` | `_compute_ratios()` (static), `calculate_financial_ratios()` |
| Endpoint | `app/api/v1/endpoints/financial_data.py` | `GET /ratios/{windfarm_id}` |
| Types | `admin-ui/src/types/financial.ts` | TypeScript interfaces |
| Hook | `admin-ui/src/lib/financial-data-api.ts` | `useFinancialRatios(windfarmId)` |
| UI | `admin-ui/src/components/windfarms/financial-data-section.tsx` | Ratio cards + trend table |

## API Endpoint

```
GET /api/v1/financial-data/ratios/{windfarm_id}
```

**Response:** `List[FinancialRatiosResponse]` — one entry per financial entity linked to the windfarm.

### Example Response

```json
[
  {
    "windfarm_id": 7201,
    "windfarm_name": "Midtfjellet",
    "financial_entity_id": 42,
    "financial_entity_name": "Midtfjellet Vindkraft AS",
    "entity_type": "spv",
    "cod": "2012-09-22",
    "linked_windfarm_ids": [7201],
    "periods": [
      {
        "financial_data_id": 101,
        "period_start": "2015-01-01",
        "period_end": "2015-12-31",
        "currency": "NOK",
        "total_revenue": 85000000,
        "total_operating_expenses": 52000000,
        "ebitda": 33000000,
        "generation_mwh": 220500.3,
        "generation_hours_count": 8760,
        "revenue_per_mwh": 385.49,
        "opex_per_mwh": 235.83,
        "ebitda_margin_pct": 38.82,
        "is_ramp_up_excluded": false,
        "ramp_up_exclusion_reason": null,
        "generation_data_available": true,
        "period_coverage_pct": 100.0
      }
    ]
  }
]
```

## Computation Details

### Net Generation

Generation MWh is sourced from the `generation_data` table (NOT the `reported_generation_gwh` field on financial records):

```python
SUM(generation_mwh - COALESCE(consumption_mwh, 0))
WHERE windfarm_id IN (linked_wf_ids)
  AND hour >= period_start
  AND hour < period_end + 1 day
```

This uses the existing `idx_gen_windfarm_hour` index for efficient querying.

### Ramp-Up Exclusion

Periods where `period_start < COD + 365 days` are excluded from ratio computation because early operational data is distorted by commissioning/ramp-up activity.

Rules:
- **Effective COD** = `max(COD)` across ALL windfarms linked to the entity
- If COD is `null` for all linked windfarms, no exclusion is applied
- Excluded periods are returned with `is_ramp_up_excluded=true` and `ramp_up_exclusion_reason` populated
- Financial source data (revenue, opex, ebitda) is still included for reference

### Holdco / Multi-Windfarm Entities

When a financial entity (e.g., a holdco) is linked to multiple windfarms:
1. Generation is summed across ALL linked windfarms
2. Effective COD is the latest COD across all linked windfarms
3. Period coverage accounts for expected hours across all windfarms: `hours_count / (total_days * 24 * num_windfarms) * 100`

### Rounding

| Value | Precision |
|-------|-----------|
| Revenue/MWh, Opex/MWh | 2 decimal places |
| EBITDA Margin (%) | 2 decimal places |
| Generation MWh | 1 decimal place |
| Coverage (%) | 1 decimal place |
| Financial totals (revenue, opex, ebitda) | 0 decimal places |

### Division Safety

- `generation_mwh = 0` or `null` → per-MWh ratios return `null`
- `total_revenue = 0` or `null` → EBITDA margin returns `null`
- `total_opex = null` → opex/MWh returns `null`, other ratios still computed

## Frontend Display

### Ratio Summary Cards

Three cards below the existing P&L summary cards:

| Card | Shows | YoY Change |
|------|-------|------------|
| Revenue/MWh | Latest non-excluded period value | vs. previous period |
| Opex/MWh | Latest non-excluded period value | vs. previous period |
| EBITDA Margin | Latest non-excluded period value | pp change vs. previous |

Cards show "N/A" when no generation data is available or all periods are ramp-up excluded.

### Efficiency Ratios Table

A trend table aligned with the P&L table's year columns:

| Metric | 2022 | 2023 | 2024 |
|--------|------|------|------|
| Generation (GWh) | 125.3 | 118.7 | 132.1 |
| Revenue/MWh | 76.20 | 82.50 | 85.20 |
| Opex/MWh | 48.30 | 54.90 | 52.10 |
| EBITDA Margin | 36.6% | 33.5% | 38.8% |

- Ramp-up excluded periods show a "Ramp-up" badge
- Periods with no generation data show "N/A"
- Low coverage (< 50%) shows a warning icon with tooltip

### Number Formatting

Large numbers are displayed in human-readable format:
- Values >= 1,000,000 shown as `XXX.XM` (e.g., `85.0M`)
- Values >= 1,000 shown as `XXXK` (e.g., `520K`)
- This formatting applies to both the financial-data-section (windfarm detail) and financial-data-page (list view)

## Edge Cases

| Case | Handling |
|------|----------|
| No financial entities linked to windfarm | Returns empty list `[]` |
| No generation data for period | `generation_data_available=false`, all ratios `null` |
| `generation_mwh = 0` | Per-MWh ratios `null` (avoids division by zero) |
| `total_revenue = null` or `0` | `revenue_per_mwh=null`, `ebitda_margin_pct=null` |
| COD is null for all windfarms | No ramp-up exclusion applied |
| Holdco linked to multiple windfarms | Generation summed across all linked windfarms |
| Negative EBITDA | Displayed correctly; margin can be negative |
| Nonexistent windfarm_id | Returns empty list `[]` |

## Tests

### Unit Tests

**File:** `tests/test_financial_ratios.py` (14 tests)

- `TestComputeRatios` — 8 tests covering normal values, zero/none generation, zero/none revenue, negative EBITDA, none opex, all none
- `TestRampUpExclusion` — 6 tests covering before/after cutoff, exactly at cutoff boundary, null COD, multi-windfarm max COD

```bash
poetry run pytest tests/test_financial_ratios.py -v
```

### Integration Tests

**File:** `tests/test_financial_ratios_api.py` (6 tests, requires running server)

- SPV windfarm ratios (Midtfjellet, id=7201)
- Ramp-up exclusion (Vardafjellet, id=7225, COD=2020-08-17)
- Holdco with multiple windfarms
- Nonexistent windfarm returns `[]`
- Generation coverage populated
- Response structure validation

```bash
# Start server first
poetry run uvicorn app.main:app --reload --port 8001

# Run tests
poetry run pytest tests/test_financial_ratios_api.py -v
```
