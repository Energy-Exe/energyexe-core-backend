# Ledger preview data (sandbox, not for clients)

These files feed the internal-only **Ledger preview** page in the client portal (`/scada/ledger`). The page shows how the SCADA opportunities would look if they were built as a value ledger: one cause per kWh, with signed values and a bridge from today's figure.

- **Where they come from:** the value-ledger sandbox in `energyexe-scada-pipeline`, branch `sandbox/value-ledger`, step `preview_export`. They are its quotable results only:
  - Hill of Towie, 28 September 2016 to 31 December 2024, in pounds;
  - Raggovidda 2, 2025, in Norwegian kroner.
- **What they are not:** they are not the register and not a client figure. The approved figures stay on the Value in Play page, which is unchanged.
- **Who can read them:** internal staff only (`get_current_internal_user`). The endpoint is on by default only outside production; see `SCADA_LEDGER_PREVIEW_ENABLED` in `app/core/config.py`.
- **To update:** re-run `preview_export` in the sandbox, then copy the two files here under the names `<farm>__<asset>.json`.
- **Per-year fields (added 2026-11-16, EPR-222):** each line per calendar year (`leaves_by_year`), the revenue the turbines actually earned by year (`realised_by_year`, actual energy at that period's price), and its average a year (`realised_year`). The client's Value in Play page uses them for its year selector and value-flow chart. The schema name stays `ledger-preview/v1`, because the fields are only added.
