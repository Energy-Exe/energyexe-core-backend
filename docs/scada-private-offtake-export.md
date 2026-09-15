# Private SCADA offtake snapshot (EPR-138 stage 1)

This internal operator command exports one owner's SCADA PPA inputs and platform
reporting FX into private local files. PPA monetary activation remains disabled.
It adds no endpoint, user permission, database table, lifecycle action or shared
result. It can ship independently if the pipeline release is held.

Run from the backend checkout using its Python environment:

```sh
export SCADA_OFFTAKE_SOURCE_DATABASE_URL='postgresql+asyncpg://...'
.venv/bin/python scripts/export_scada_offtake.py \
  --farm hill_of_towie --owner-user-id 123 \
  --from 2016-01-01 --to 2025-12-31 \
  --output-root /private/tmp/operator-private-offtake
```

Supply the source explicitly through that environment variable or
`--source-database-url`. Application `.env` and `DATABASE_URL` are never used.
The environment form avoids a credential-bearing process argument. The command
uses an independent PostgreSQL connection and one read-only, repeatable-read
transaction. It verifies those settings and uses `search_path=public` so the
existing PPA service reads `public.scada_ppa`. The explicit owner, mapped platform
windfarm and exact `Active` status apply to every page; missing mapping or a null
or invalid `scada.dim_farm.tz` blocks extraction.

Every snapshot has owner/farm identity, inclusive farm-local analysis dates,
original database terms (Decimal values as strings, nulls preserved), reporting
currency provenance, period FX and original observations, source file hashes,
source versions, content hash and readiness. Transaction/extraction timestamps
are excluded from material identity; original row timestamps remain included.
The suite version is explicitly `activation-deferred`: this standalone command
does not load the suite or claim a valuation run. The offline preparer captures
the actual suite version in its preparation outputs.

Reporting currency uses an explicit supported override, then the latest
non-synthetic financial filing across all linked entities ordered by period end
then period start, then currencies present in actual day-ahead `price_data`
observations in the requested window. Conflicting latest filings or mixed market
currencies need an override. Unsupported reported currency uses EUR. Missing
currency blocks. Non-GBP terms cannot target GBP; an explicit GBP request blocks,
and an automatic GBP choice falls back to EUR with a recorded reason.

FX is supplied by `ExchangeRateService.get_rate_for_period` without recalculating
the six-decimal averages or filling missing days. Cross-currency provenance only
includes matching daily observations. Calendar-year and full-period references
are clipped to known inclusive contract dates and the analysis window. Original
contract amounts stay unchanged. Translated nominal amounts are labelled
`reporting reference; not received PPA revenue`; these references do not describe
settlement clauses. No new `*_gbp` fields are used.

The output directory must be outside every repository, the workspace, shared
SCADA directories and sync roots. Paths are resolved before checks. The command
recognizes `SCADA_DATA_ROOT`, `ENERGYEXE_OPP_ROOT`, `ENERGYEXE_DATA_ROOT`, common
sync-root variables and standard macOS Dropbox/OneDrive/Google Drive/iCloud
locations. Declare additional roots via repeatable `--forbidden-root` or an
`os.pathsep` separated `SCADA_PRIVATE_FORBIDDEN_ROOTS`; the offline preparer uses
the same environment interface. Existing output directories must already be
operator-owned with mode 0700. New directories use mode 0700 and files 0600;
files are exclusively created and existing files or symlinks cannot be replaced.
No cloud sync/upload is performed.

Exit status 0 means a snapshot was prepared (possibly with a confirmed `no_terms`
state); it never permits valuation. Exit status 2 means blocked. A database read
failure produces `source_access_or_read_failure`, never `no_terms`, and does not
include a connection string, SQL parameters or exception text. Private snapshots
are for `scada opportunities prepare-offtake` only. Exact-date gaps, overlaps,
clauses and corrected-drop readiness are validated there. They must never be
passed to shared persistence.
