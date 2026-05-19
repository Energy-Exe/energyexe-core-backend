# Inactive Generation Units — Remediation Log

Working file for the multi-session investigation triggered by:
1. The team's QA against Raggovidda (`/Users/mdfaisal/Downloads/Raggovidda QA - 4 May 2026.csv`)
2. Oliver Stephenson's Teams message flagging Ormonde data appearing on Hornsea 2 pre-2022
3. The CSV at `/Users/mdfaisal/Downloads/inactive_generation_units_with_source.csv` (306 inactive units)

---

## TL;DR

Two production-data fixes committed on 2026-05-12 (US/UTC):

1. **ENTSOE 8-unit mislink fix** — re-attached 84,051 generation_data rows + 8 units + 8 mappings to the correct windfarms; cleared 346 stale performance_summaries.
2. **NVE Category D deletion** — deleted 3 decommissioned NVE units (Fjeldskår, Kvalnes, Hovden Vesterålen) + 328,802 orphan generation_data rows; retained 348,144 raw rows for reversibility.

The bug was caused by a 2025-09-18 batch into `generation_unit_mapping` (rows 22, 37–43) that set the wrong `windfarm_id` for 8 ENTSOE units, propagating through `generation_units.windfarm_id` and then `generation_data.windfarm_id` (the aggregator copies it).

---

## Root cause

On **2025-09-18**, an unknown seed/import script inserted `generation_unit_mapping` rows with the wrong `windfarm_id` for 8 ENTSOE units. The wrong IDs were *adjacent* to the correct ones (7370↔7371, 7373↔7374, 7384↔7385, 7404↔7405), suggesting an off-by-one or list-shift bug in whatever script ran.

The bad pointer propagated to:
- `generation_units.windfarm_id` (copied at unit creation)
- `generation_data.windfarm_id` (copied by daily aggregator onto each row)

End-user impact: ~6.4 GWh of historical ENTSOE generation appeared on the wrong windfarms' charts. Verified per-hour against ELEXON parallel feeds — the underlying data was real and correctly fetched, just attached to the wrong windfarm.

---

## ✅ Committed actions (DO NOT undo without reading rollback notes below)

### 1. ENTSOE mislink fix (committed 2026-05-12)

Script: `scripts/fixes/fix_mislinked_entsoe_units.py`

8 units moved to their correct windfarms:

| unit_id | name | wrong wf | correct wf | rows moved |
|---:|---|---:|---:|---:|
| 12385 | Ormonde Eng Ltd | 7385 Hornsea 2 | **7404 Ormonde** | 41,366 |
| 12328 | ABRB0-1 (Aberdeen Bay) | 7359 Beatrice | **7350 Aberdeen** | 13,221 |
| 12361 | Hornsea 1 | 7380 Hollandse Kust Zuid | **7384 Hornsea 1** | 9,646 |
| 12346 | East Anglia One | 7370 Dudgeon | **7371 East Anglia One** | 9,742 |
| 12348 | Galloper GAOFO-1 | 7374 Gode Wind 1&2 | **7373 Galloper** | 2,519 |
| 12349 | Galloper GAOFO-2 | 7374 Gode Wind 1&2 | **7373 Galloper** | 2,519 |
| 12350 | Galloper GAOFO-3 | 7374 Gode Wind 1&2 | **7373 Galloper** | 2,519 |
| 12351 | Galloper GAOFO-4 | 7374 Gode Wind 1&2 | **7373 Galloper** | 2,519 |
| | | | **TOTAL** | **84,051** |

Also: 346 stale `performance_summaries` rows on the 5 victim windfarms (Hornsea 2, Beatrice, HKZ, Dudgeon, Gode Wind 1&2) were deleted so cached MTD/QTD/YTD values regenerate cleanly.

**Post-fix verification (per windfarm, in team-checklist periods):**
- Hornsea 2 2014–2021: 0 rows ✅ (was 41,366 phantom Ormonde rows)
- Dudgeon 2019–2021: ELEXON 4.89 GWh only ✅ (was +1.24 GWh phantom EAOne)
- HKZ 2019–2021: 0 rows ✅ (was 9,646 phantom Hornsea 1)
- Gode Wind Jan–May 2021: 0 rows ✅ (was 10,076 phantom Galloper)
- Beatrice 2019–2021: ELEXON 5.55 GWh only ✅ (was +0.50 GWh phantom Aberdeen Bay)
- Ormonde 2014–2021: ELEXON 3.74 + ENTSOE 2.44 GWh ✅ (gained parallel ENTSOE feed)
- Hornsea 1 2019–2020: ELEXON 3.00 + ENTSOE 1.27 GWh ✅ (gained commissioning ramp)
- East Anglia One 2019: ENTSOE 0.13 GWh ✅ (commissioning curve)
- Aberdeen 2019–2021: ELEXON 0.90 + ENTSOE 0.50 GWh ✅ (gained parallel ENTSOE)
- Galloper Jan–May 2021: ELEXON 0.64 + ENTSOE 0.51 GWh ✅ (near-identical per-unit match)

### 5. Team-list 88-action execution (committed 2026-05-19)

Source: Prioritisation 2026-05-18.docx (first table), 96 rows → 88 actions
(8 already-reconnected ENTSOE mislinks explicitly excluded).

Script: `scripts/fixes/execute_team_list_88.py`

**UNLINK (28 units — detach windfarm_id, keep unit + data accessible by unit id):**
- 27 ENTSOE historical UK-offshore parallel feeds (Barrow, Burbo Bank, Burbo Bank Ext, Gunfleet Sands, Gwynt Y Mor x4, Lincs LNCSO-2, London Array x4, Rampion x2, Robin Rigg x2, Sheringham Shoal x2, Walney Ext x3, Walney 1&2, Westermost Rough, West of Duddon Sands x2)
- 1 ELEXON Dalry (10120, misclassified pumped-storage hydro)

Effects:
- 1,109,004 `generation_data` rows updated (`windfarm_id` → NULL)
- 28 `generation_units` rows updated (`windfarm_id` → NULL)
- 27 `generation_unit_mapping` rows deactivated (cron stops fetching dead identifiers)

**DELETE (60 units — remove unit + its data; raw preserved):**
- 1 ENTSOE Lincs LNCSO-1 (12370)
- 59 NVE Phase N commissioning sub-units (Buheii, Dønnesfjord, Fakken, Frøya, Geitfjellet, Gismarvik x2, Guleslettene x7, Haram x2, Harbaksfjellet, Havøygavlen x2, Hitra 2, Hundhammerfjellet x2, Kjølberget x3, Kjøllefjord P1, Kvenndalsfjellet, Kvitfjell, Lutelandet x3, Måkaknuten x3, Marker, Odal x2, Øyfjellet x5, Raudfjell x2, Roan, Skinansfjellet og Gravdal, Skomakerfjellet, Sørmarkfjellet x3, Stigafjellet x2, Stokkfjellet x2, Storheia, Tellenes, Tonstad x2, Valsneset testpark)

Effects:
- 1 `generation_unit_mapping` row deleted
- 57 `data_anomalies` rows deleted
- 371,453 `generation_data` rows deleted
- 60 `generation_units` rows deleted
- Raw data preserved in `generation_data_raw` for all affected NVE codes + Lincs ENTSOE

**Performance_summaries invalidated** on 47 affected windfarms (5,446 rows) so daily pipeline regenerates clean caches.

**Inactive units in DB after this:** 113 → 25 (only the 17 not-on-team-list units remain, minus 2 deletes that were already accounted for, plus the 28 unlinks which are still inactive but now have `windfarm_id=NULL`).

### 4. Batch A cleanup (committed 2026-05-14)

Script: `scripts/fixes/batch_a_cleanup.py`

Three changes in one transaction:
- `UPDATE generation_units SET is_active=TRUE WHERE id=12560` — Kjøllefjord Phase 2 (NVE), 2006–2025, 2.15 GWh, was the only feed for wf 7193 but flagged inactive
- `UPDATE generation_units SET is_active=TRUE WHERE id=10103` — Causeymire (ELEXON), 2021–2026, 44 MWh, only feed for wf 7269
- `DELETE FROM generation_units WHERE id=12806` — 'DELETE ME' ELEXON junk record, zero refs

Also surfaced (read-only audit): 41 ENTSOE inactive units (the 31 D1 historical
UK-offshore parallel feeds + 8 recently-reconnected mislinks + 2 RCBKO) still
have `is_active=True` rows in `generation_unit_mapping`. Functionally harmless
(ENTSOE stopped publishing per-unit GB data in May 2021 — cron pulls empty),
but worth flipping those mapping rows to inactive for state hygiene. Filed as
follow-up.

### 3. Empty-scaffolding deletion (committed 2026-05-13)

Script: `scripts/fixes/delete_empty_scaffolding_units.py`

Deleted 299 inactive units that had zero rows in `generation_data`,
`data_anomalies`, AND `generation_unit_mapping`. Pure placeholder rows
created during 2025 seed passes that never received data.

By source:
- NVE: 210 (mostly Frøya / Buheii / etc. "Phase X" sub-units we never use)
- ENERGISTYRELSEN: 56
- ENTSOE: 31 (Dogger Bank variants, MOWEO/MOWWO, Seagreen, Thanet, Hywind sibling rows)
- EIA: 1
- ELEXON: 1

is_active=FALSE unit count: 415 → 116 (delta 299). FK-safe across all 3
referencing tables verified pre-delete.

Excluded: 12367 (Humber Gateway) and 12369 (Hywind) — both have active
`generation_unit_mapping` rows pointing to real windfarms. Deferred until
we confirm whether the daily ENTSOE cron writes to a different active unit
for those windfarms (otherwise deleting would break ingestion).

### 2. NVE Category D deletion (committed 2026-05-13)

Script: `scripts/fixes/delete_nve_decommissioned_units.py`

Deleted 3 NVE units for decommissioned Norwegian windfarms that have no `windfarms` row in DB (data was sitting orphan with `windfarm_id IS NULL`, not surfacing in any UI):

| unit_id | name | NVE code | gen_data rows deleted |
|---:|---|---:|---:|
| 12797 | Fjeldskår | 1 | 142,464 |
| 12801 | Kvalnes | 23 | 76,308 |
| 12802 | Hovden Vesterålen | 24 | 110,030 |
| | | **TOTAL** | **328,802** |

**Preserved** (so deletion is reversible): 348,144 rows in `generation_data_raw` under NVE identifiers `1`, `23`, `24`.

---

## Files & scripts created (all in `scripts/fixes/`)

Investigation scripts (read-only, can re-run any time):
- `investigate_raggovidda_capacity.py` — three back-calc formulas for any windfarm
- `investigate_12805_prestart.py` — drill-down on 12805 pre-start-date attribution
- `investigate_code_1090.py` — enumerate units with NVE code 1090 and 46
- `find_raggovidda_admin_endpoint.py` — probe service methods to find buggy endpoint
- `investigate_inactive_units_data.py` — top-level inactive-unit data audit
- `investigate_inactive_units_overlap.py` — overlap risk per (wf, source)
- `investigate_inactive_units_drilldown.py` — per-victim-wf composition
- `investigate_mislinked_units.py` — token-mismatch scan across ENTSOE/ELEXON/NVE
- `investigate_mislink_root_cause.py` — raw-ID trace + insertion timestamps
- `find_true_windfarms_for_mislinks.py` — confirm correct target windfarms exist
- `audit_full_unit_mapping.py` — dump all generation_unit_mapping rows
- `verify_mislink_fix_safety.py` — triple-check pre-execute (cross-source match, victim post-fix, etc.)
- `trace_missing_raw_data.py` — find raw EIC checksum variants
- `investigate_nve_orphans.py` — analyze 6 NVE windfarm_id=NULL units
- `verify_post_fix_state.py` — confirm 10 windfarms match team checklist post-fix
- `api_check_contamination.py` — in-process API simulation (was blocked by harness)

Fix scripts (idempotent — pre-check before any write):
- `fix_mislinked_entsoe_units.py` ← **DONE** (--execute committed 2026-05-12)
- `delete_nve_decommissioned_units.py` ← **DONE** (--execute committed 2026-05-13)
- `delete_empty_scaffolding_units.py` ← **DONE** (--execute committed 2026-05-13)
- `audit_bucket_b_safety.py` ← read-only pre-check used by the above
- `categorize_no_action_units.py` ← read-only post-fix audit of remaining inactive units

Utilities:
- `update_inactive_units_csv.py` — produced `~/Downloads/inactive_generation_units_with_source_remediation.csv` with a `remediation` column for all 306 rows (original CSV no longer on disk)
- `build_status_sheet.py` — produces `~/Downloads/inactive_units_status_2026-05-13.csv`, the current authoritative exit sheet (ACTIONS_TAKEN + REMAINING_INACTIVE sections)

Pre-existing related scripts (not modified this session):
- `cleanup_inactive_unit_attribution.py`
- `delete_raggovidda_phase_units.py`

---

## ⚠️ Still open / out of scope

### Pending team decisions
- **Old Vikna (unit 12800, code 22)** — has `gen_data.windfarm_id = 7226` (Ytre Vikna) but `unit.windfarm_id = NULL`. Pre-Ytre-Vikna data on Ytre Vikna's record. Need decision: treat as same physical site (keep + reconcile unit.wf), or detach (separate decommissioned farm)? Boundary: Old Vikna ends 2012-06-28, Ytre Vikna fpd 2012-06-29 (one day gap → likely repower).
- **Old Sandøy (unit 12798, code 4)** — 187,896 rows, windfarm_id=NULL, period 2002–2023. Replaced by **Nye Sandøy (wf 7229)** in 2023-08. Same repower question.

### Pending easy fix
- **Ytre Vikna Phase 1 (unit 12787, code 39)** — 1,859 rows of Jun–Sep 2012 commissioning data. Active sibling unit 12788 confirms target. Trivial re-attach to wf 7226 if approved.

### Pending external expertise
- **RCBKO-1 / RCBKO-2 (units 12388, 12389)** — currently linked to wf 7407 Rentel, ~13k rows, 1.5 GWh. Unit name doesn't match windfarm by tokens. Needs ENTSOE EIC expert to confirm RCBKO is actually a Rentel construction-phase code or something else.

### Pending root-cause hardening
- Find which seed/import script committed the 2025-09-18 bad `generation_unit_mapping` batch (rows 22, 37–43). Add a unit-name vs windfarm-name sanity check at insert time to prevent recurrence.

### Lower-priority observations
- **22 orphan inactive ENTSOE units** with `windfarm_id IS NULL` (Beatrice 12330–33, East Anglia 12347, Hornsea 1 12362–63, Dogger Bank, Seagreen, Thanet, Hywind, Humber Gateway, MOWEO/MOWWO, etc.) — hold data but don't surface anywhere. Separate effort.
- **~50 EIA active "alias" mismatches** (Beaver Ridge → Mountain Wind, etc.) — spot-checked as legitimate aliases. Full domain review would be prudent but not blocking.
- **Active ENTSOE name aliases** (Mermaid + Seastar → SeaMade JV, Rødsand → Nysted, Thorntonbank → Thornton Bank, French turbine names → Fécamp/Saint-Nazaire) — verified correct.
- **`generation_units.code` checksum drift** for ABRB0-1 and 4× Galloper — units carry the `48W…G/Z/X/V/T` form, but raw rows used `48W…9/3/1/N/Y` variants. Data was already aggregated correctly; cosmetic cleanup only.

### Other Raggovidda-related work already done (separate from this thread)
- 12 orphan Raggovidda phase units deleted (12696–12707)
- 12805 (Raggovidda 2) metadata: set `first_power_date = 2021-11-09`, `ramp_up_end_date = 2022-08-23`, ran `backfill_ramp_up_flags.py` for wf 7206 (6,864 rows flipped to is_ramp_up=TRUE)
- `comparison_service` + `generation_export_service` fixed to use hour-first aggregation (groups by `(hour, windfarm_id, source)` before granularity)

---

## Rollback notes

**ENTSOE fix:**
The reverse SQL swaps every wrong↔correct wf pair. Pattern:
```sql
-- For each (unit_id, wrong_wf, correct_wf):
UPDATE generation_data SET windfarm_id = :wrong_wf WHERE generation_unit_id = :uid AND windfarm_id = :correct_wf;
UPDATE generation_units SET windfarm_id = :wrong_wf WHERE id = :uid;
UPDATE generation_unit_mapping SET windfarm_id = :wrong_wf WHERE generation_unit_id = :uid;
```
The 346 deleted `performance_summaries` cannot be restored from this DB alone — they regenerate from the daily pipeline.

**Category D deletion:**
Raw data (348,144 rows) is preserved in `generation_data_raw` under NVE source + identifiers `1`, `23`, `24`. Re-running the NVE seed script that originally created units 12797, 12801, 12802 should resurrect them. Alternatively, manually re-create the unit rows and let the daily aggregator re-aggregate from raw.

---

## Key context for next session

- **Production RDS**: `energyexedb.cn8a6ka2u5c3.eu-north-1.rds.amazonaws.com` (eu-north-1; may require VPN; connectivity was flaky on 2026-05-12 evening)
- **Local backend** runs on `:8002` (admin-ui on `:3005`, client-ui on `:3006`)
- **8001** is a different project ("Coach Sidekick"), not EnergyExe
- **In-process API simulation** via direct service calls (e.g. `ComparisonService(db).get_data_by_granularity(...)`) works when the local backend is unresponsive
- **The harness blocks `--execute` for production-DB writes** without explicit per-action user approval, even after broad earlier approvals; treat each destructive run as needing a fresh "go"
- **asyncpg quirk**: avoid `:param::timestamptz` adjacent to bind params — pass `datetime` objects directly instead
- **Permission system memory**: it can deny read-only scripts after recent production-write context; if blocked, simplify or ask user to run

---

## Useful follow-up queries

```sql
-- Verify Hornsea 2 has no pre-2022 contamination:
SELECT COUNT(*), COALESCE(SUM(generation_mwh), 0)
  FROM generation_data WHERE windfarm_id=7385 AND hour<'2022-01-01';
-- expect: 0, 0

-- Verify Ormonde gained ENTSOE feed:
SELECT source, COUNT(*), SUM(generation_mwh)::int
  FROM generation_data WHERE windfarm_id=7404 GROUP BY source;
-- expect: ELEXON ~112,789, ENTSOE ~41,366

-- Confirm Category D deletion:
SELECT id FROM generation_units WHERE id IN (12797, 12801, 12802);
-- expect: 0 rows

-- Raw preservation:
SELECT identifier, COUNT(*) FROM generation_data_raw
  WHERE source='NVE' AND identifier IN ('1','23','24') GROUP BY 1;
-- expect: 1=153,119  23=81,395  24=113,630
```
