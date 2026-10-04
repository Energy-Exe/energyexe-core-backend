# Prices, FX and market data

Day-ahead / market-index power prices per bidzone, their processing into windfarm-level hourly
prices, the capture-rate and revenue analytics built on them, and ECB exchange rates for
currency conversion. Code: [`app/api/v1/endpoints/price_data.py`](../../app/api/v1/endpoints/price_data.py)
(router prefix `/api/v1/prices`), [`exchange_rates.py`](../../app/api/v1/endpoints/exchange_rates.py),
and the services listed below.

## Tables

| Table | Grain | Written by |
|---|---|---|
| `price_data_raw` | (source, bidzone, period) raw prices as fetched | `PriceDataStorageService` (ENTSOE day-ahead), `ElexonPriceStorageService` (GB market index `APXMIDP`) |
| `price_data` | hourly price per **windfarm** — unique `(hour, windfarm_id, source)` | `PriceProcessingService.process_raw_to_hourly` (maps each windfarm to its bidzone) |
| `exchange_rates` | daily ECB reference rates | `scripts/seeds/exchange_rates/import_ecb_rates.py` via `ECBClient` |

## Ingestion

| Source | Client | Storage | Scheduled job ([`scheduled-jobs.md`](../operations/scheduled-jobs.md)) |
|---|---|---|---|
| ENTSOE day-ahead (11 bidzones) | `app/services/entsoe_price_client.py` (`entsoe-py`, `ENTSOE_API_KEY`) | `price_data_storage_service.py` | `entsoe-prices-daily`, 22:30 UTC, day − 2: `scripts/seeds/power_prices/import_prices_from_api.py` then `process_to_hourly.py` |
| Elexon market index (GB, `10YGB----------A`; only the `APXMIDP` provider carries real prices, `N2EXMIDP` is zeros) | `app/services/elexon_client.py` | `elexon_price_storage_service.py` | `elexon-prices-daily`, 22:40 UTC, day − 1: `scripts/seeds/power_prices/elexon/import_elexon_prices.py` then `process_to_hourly.py --source ELEXON` |
| ECB exchange rates | `app/services/ecb_client.py` (published ~14:15 CET on business days) | `exchange_rate_service.py` reads them | `ecb-rates-daily`, 22:50 UTC Mon–Fri |

Bulk / historical loads: `scripts/seeds/power_prices/import_csv_prices.py`, `process_bulk_prices.py`.
Both nightly price jobs are the same two-step shape — fetch raw, then re-process to hourly for
the window — and the trigger endpoint's `records_imported` reflects the **fetch** step only, so
when checking a run confirm `price_data` actually grew.

## API (`/api/v1/prices`, all `get_current_user`)

| Group | Routes |
|---|---|
| Fetch / process (admin tooling) | `POST /fetch`, `POST /fetch-day`, `POST /process`, `POST /elexon/fetch`, `POST /elexon/process`, `POST /elexon/sync` |
| Raw and processed data | `GET /raw`, `GET /bidzones`, `GET /availability`, `GET /processed`, `GET /windfarms/{id}/statistics`, `GET /windfarms/{id}/coverage` |
| Analytics (`PriceAnalyticsService`) | `POST/GET /analytics/capture-rate[/{windfarm_id}]`, `POST /analytics/capture-rate/compare`, `GET /analytics/compare-capture-rates`, `GET /analytics/zone-capture-rate`, `POST/GET /analytics/revenue[/{windfarm_id}]`, `POST/GET /analytics/price-profile[/{bidzone_id}]`, `GET /analytics/correlation/{windfarm_id}`, portfolio roll-ups `GET /analytics/portfolio/{availability,revenue,capture-rates}` |

`PriceAnalyticsService` methods behind these: `calculate_capture_rate`, `calculate_revenue_metrics`,
`compare_capture_rates`, `compare_capture_rates_by_bidzone`, `zone_capture_rate_by_month`,
`get_price_profile`, `get_generation_price_correlation`, `negative_price_exposure`,
`count_negative_price_hours`. `_get_preferred_price_source` picks ELEXON for GB farms and
ENTSOE elsewhere. Capture rate = achieved price (revenue ÷ generation) ÷ the simple time-weighted market average price for the period.
These feed the MKT-01/02/03/06 opportunity detectors
([`opportunity-detection.md`](opportunity-detection.md)) and the report charts.

## Currency conversion

`ExchangeRateService` (`app/services/exchange_rate_service.py`): `get_rate_for_period(from, to,
start, end)` (average rate over the period, with inverse and cross-rate fallbacks through EUR) and
`convert_amount`. Used by financial ratios / OPEX-per-MWh (`display_currency`,
[`financial-ratios.md`](financial-ratios.md)), the peer summary and the client-side currency
toggle (EPR-93). `GET /api/v1/exchange-rates` lists stored rates.

## Gotchas

- `price_data` is **windfarm**-keyed, so a farm with no bidzone gets no prices; check
  `windfarms.bidzone_id` before suspecting the import.
- Elexon MID prices are the GB reference used throughout (also by the SCADA pipeline's
  `prices_30m`, D-015); ENTSOE GB day-ahead is not used for GB farms.
- Backfilling through the trigger endpoint is limited by the 150 s ALB health budget; run the
  seed scripts directly for anything longer than a few days.

Tests: `tests/test_market_api.py` (needs a live server), `tests/test_ecb_client.py`,
`tests/test_exchange_rate_service.py`, `tests/test_import_job_price_commands.py`.
