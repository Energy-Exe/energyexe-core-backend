# EPR-138 disposable PostgreSQL verification

Verified on 2026-09-15 against PostgreSQL 16.11 (Homebrew), server address
`127.0.0.1:5432`, with the backend and pipeline changes present in this workspace.
The final successful run created `epr138_verify_8e46c14f7175426e8a0c297c35bab84c`,
then dropped that database and queried `pg_database` to verify its absence.
No existing local database, staging data or production data was changed.

Reproduce from the backend checkout with its environment and the sibling pipeline
checkout/environment available:

```sh
.venv/bin/python scripts/verify_epr138_local_postgres.py \
  --admin-url postgresql://127.0.0.1:5432/postgres
.venv/bin/python -m py_compile scripts/verify_epr138_local_postgres.py
.venv/bin/python -m black --check scripts/verify_epr138_local_postgres.py
```

The [verification script](../scripts/verify_epr138_local_postgres.py) requires an
explicit loopback `/postgres` admin URL and rejects query options. It proves the
server address, generates a unique UUID database name, supplies explicit source
URLs to Alembic and backend connections, and drops only its generated database in
`finally`. Python optimization is rejected so assertions cannot be disabled. The
ordinary application `.env` cannot select the write destination. Separate local
checks rejected a remote hostname, an existing application database name and a
query-string host override before connecting. Compilation and Black checks passed.

SHA-256 inputs at verification:

```text
verify_epr138_local_postgres.py: b99406f467cdd8d61ea1d425612d6ab48f02722c567d1743202d37e5820df8ca
f138a6b7c8d9_opportunity_offtake_metadata.py: 3a1d54d130baf340e76de1822fce7a03b85e0a8d77bec28f5f3771e090e9a30f
golden/hot-2026-09-15-epr138/register_rows.json: 2a627920c562fc3e4fd21b292582273a828d81d3928b6eda1678a57904dbca90
```

| Check | Observed result |
|---|---|
| Real migrations | Full pipeline chain applied to `a9c3e7f1b2d4`; `f138a6b7c8d9` upgraded, downgraded, then upgraded again. |
| Legacy register row | Existing GBP `1.25` remained unchanged through the roundtrip. Both new metadata columns were nullable and read as JSON `null`. |
| Canonical fixture frames | `to_frames()` produced one run, 44 register, 110 annual and eight trend rows. These were inserted directly into the newly created fixture database. |
| Real backend reads and response schemas | List/detail/year service SQL executed against PostgreSQL and Pydantic serialized the resulting responses. Additive rows and headline agreed at GBP `779922`; class totals were `329280`, `72019`, and `378623`. |
| Shared metadata | Priced additive rows retained vendor `SPOT` and `UNKNOWN` strings; the legacy row retained null metadata. |
| Reclassified lifecycle | A synthetic `OPS_04/FLEET/RECOVERABLE` action remained `ACKNOWLEDGED` under its original key. The regenerated `CONTEXT` row was non-additive and had no joined action, the API's implicit `NEW` state. |
| Exporter connection | The actual exporter session reported `search_path=public`, `transaction_read_only=on` and `transaction_isolation=repeatable read`. |
| PostgreSQL enforcement | An attempted update through the exporter transaction failed with SQLSTATE `25006`. A change committed from a second scratch connection remained invisible to the exporter snapshot. |
| Complete owner-scoped pagination | The real PPA service returned 201 terms for owner 7, one for owner 8 and `no_terms` for owner 9, using the same farm. The 201-row case crossed the service's 200-row page boundary. |
| Scope and exact status | Another farm, null owner, `Draft`, and lowercase `active` records were excluded. |
| Native values and reporting reference | PostgreSQL Decimals `123.45` and `50.00` remained exact strings. Same-currency EUR schedules used rate one; all snapshots kept monetary activation false. |
| Failure semantics | Missing mapping returned `farm_mapping_missing`. An actual missing source table and an unavailable database returned blocked `source_access_or_read_failure`, distinct from the verified `no_terms` result. |
| Cleanup | Final generated database was dropped and its absence verified. |

This verifies migration execution, canonical frame compatibility, service SQL,
response serialization and exporter transaction behavior. The backend fixture
tables reuse the models' column names and PostgreSQL types but intentionally omit
unrelated core foreign keys and required fields. No private live-source terms were
read or exported. The 201 synthetic overlapping contracts exercise pagination,
not downstream readiness or valuation; the offline preparer must still block
unsupported overlaps.

The lifecycle action is synthetic and is not evidence about actual saved actions
in staging or production. Financial filing selection and cross-currency ECB
observations are covered by separate tests; this database run used empty filings
and an explicit EUR reporting choice. It exercised real service methods and
response models, not HTTP authentication or a running browser.

The golden replay was used only as a scratch fixture. The production persistence
gate was not bypassed or invoked. This result does **not** approve a release or
replace the fresh platform-silver/consensus calculation and exact-artifact
promotion gate. Actual PPA valuation remains disabled.
