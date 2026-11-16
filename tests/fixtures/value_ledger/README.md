# Value ledger samples (test fixtures)

The three samples of the value-ledger format (version 2), copied unchanged from `energyexe-scada-pipeline`, commit `d8c7ce0`, folder `docs/reference/value-ledger-format/samples/`. That repository's `docs/reference/value-ledger-format.md` is the format's spec.

| Folder | Asset | Answer key |
|---|---|---|
| `hot-b` | Hill of Towie (`HOT`) | B, published settings with the Stage 1 fixes |
| `rg2-b` | Raggovidda 2 (`RG2`, a staff-only farm) | B |
| `hot-c` | Hill of Towie (`HOT`) | C, the Stage 6 change |

Each folder holds the two stored tables' rows (`cause_rows.csv`, `run_row.json`) and the document the API must return from them (`document.json`). The tests check that the backend's document builder turns the first two into the third. Copy them again from the pipeline when the format changes.
