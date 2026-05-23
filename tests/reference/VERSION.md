# Reference pipeline — vendored version

The `energyexe_pipeline_full.py` in this directory is a verbatim copy of the May 2026 reference Python pipeline produced by Aje Singh Rihel (`aje.rihel@energyexe.com`). It is the canonical reference for the Module 1-6 maths — our service-based implementation under `app/services/` must agree with this script's outputs within the tolerances documented in `docs/pipeline/spec-vs-implementation.md`.

## Why it's vendored

- **Reproducibility.** Spec equivalence comparisons need a stable target; an "evolving" reference defeats the purpose.
- **CI.** `tests/test_spec_equivalence.py` (when added in Layer 3) runs this script against a synthetic fixture CSV and asserts numerical equivalence with our service output.
- **Audit trail.** When stakeholders ask "why did the published %/yr change after the Module 5 fix?", we can point to this exact file as the source of the corrected maths.

## Provenance

| Field | Value |
|---|---|
| Original filename | `energyexe_pipeline_full_1.py` (downloaded copy of the version shared via SharePoint Development folder, May 2026) |
| Source author | Aje Singh Rihel (`aje.rihel@energyexe.com`) |
| Version label inside the file | "Structural-constraint-aware (post-Niord update)" |
| Source path used for vendoring | `/Users/mdfaisal/Downloads/energyexe_pipeline_full_1.py` |
| Vendored on | 2026-05-23 |
| SHA-256 of vendored copy | `e8209d9cbc6efc461d31634767205553f5e89eea3fe6de01e3469e0385d37386` |

## Updating

Do NOT edit `energyexe_pipeline_full.py` directly. If the reference is revised by Aje, treat it as a new version:

1. Drop the new file in place.
2. Update the SHA-256 + date above.
3. Re-run Layer-2 side-by-side comparison on the 5 reference windfarms; update tolerances if maths changed.
4. Re-run `tests/test_spec_equivalence.py` (Layer 3). Update committed fixture CSV output expectations if the new reference legitimately produces different numbers.

## Running it standalone

The script expects a CSV at `cfg.csv_path` (windows-style path by default — see top of the file). To run against a windfarm exported from our DB:

```bash
# After P0.2 (SpecCSVExporter) lands:
poetry run python scripts/export_windfarm_to_spec_csv.py --wf-id <ID> --year-from 2021 --year-to 2024 --out /tmp/wf_<code>.csv

# Run the reference (need statsmodels, sklearn, scipy, matplotlib, pandas, numpy):
poetry run python tests/reference/energyexe_pipeline_full.py
# (edit the cfg.csv_path at top, or override via CFG.csv_path before load_and_clean())
```

Outputs land in `os.path.dirname(cfg.csv_path)` — so the same dir as the input CSV. Roughly 25 CSV files + a few PNGs.
