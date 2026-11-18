# Value ledger samples (test fixtures)

The five samples of the value-ledger format (version 2), copied unchanged from `energyexe-scada-pipeline` (B and C at commit `d8c7ce0`, D at tag `value-ledger-stage6`), folder `docs/reference/value-ledger-format/samples/`. That repository's `docs/reference/value-ledger-format.md` is the format's spec.

| Folder | Asset | Answer key |
|---|---|---|
| `hot-b` | Hill of Towie (`HOT`) | B, published settings with the Stage 1 fixes |
| `rg2-b` | Raggovidda 2 (`RG2`, a staff-only farm) | B |
| `hot-c` | Hill of Towie (`HOT`) | C, the step 11 defaults (anemometer correction and cut-in) |
| `hot-d` | Hill of Towie (`HOT`) | D, the default: C, with degradation shown but not added, and the T16 cap rule |
| `rg2-d` | Raggovidda 2 (`RG2`) | D, the default: B, with degradation shown but not added |

Each folder holds the two stored tables' rows (`cause_rows.csv`, `run_row.json`) and the document the API must return from them (`document.json`). The tests check that the backend's document builder turns the first two into the third. Copy them again from the pipeline when the format changes.
