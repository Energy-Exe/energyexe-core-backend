# Capacity audit — 2026-10-31 (staging database)

Read-only audit behind the capacity-basis fix (follow-up to EPR-184, Aje's dev note of 2026-10-29). Every pipeline module and every capacity-factor view used to divide generation by today's nameplate capacity. This audit shows, for each wind farm with performance data, how much the installed capacity actually changed over its data years according to the turbine-unit records, and which records need fixing before the new basis can apply.

Produced by `scripts/audit_capacity_basis.py --today 2026-10-31` against the staging database (same unit registry as production). Files:

- `capacity_audit_farms.csv` — one row per farm with performance data (253): basis status, units in force today vs nameplate, capacity range inside the pipeline years (ramp-up excluded) and inside the capacity-factor views (all data years).
- `capacity_audit_units.csv` — unit records with impossible or missing values.
- `capacity_audit_phases.csv` — later build-out phases that have no ramp-up window of their own.

## Summary

| | Farms |
|---|---|
| With performance data | 253 |
| Basis applies (units agree with nameplate within 3 %) | 250 |
| Blocked (units disagree with nameplate by more than 3 %; nameplate kept) | 3 |
| Capacity changes inside the pipeline years | 12 |
| Capacity changes only in the capacity-factor views (pre-ramp-up years) | 8 |

## Farms whose pipeline results change

Capacity range over the years the pipeline analyses (ramp-up excluded).

| Id | Farm | Nameplate MW | Capacity in force MW | Swing | Data years |
|---|---|---|---|---|---|
| 7201 | Midtfjellet | 149.6 | 12.5 → 149.6 | 91.6 % | 2012-2025 |
| 7202 | Nygårdsfjellet | 32.2 | 6.9 → 32.2 | 78.6 % | 2008-2025 |
| 7238 | Greater Changhua 1 & 2A | 900.2 | 321.671 → 900.2 | 64.3 % | 2022-2025 |
| 8767 | METCentre Karmøy | 5.9 | 2.3 → 5.9 | 61.0 % | 2009-2025 |
| 7245 | Aikengall II & IIa | 142.5 | 60.8 → 142.5 | 57.3 % | 2017-2026 |
| 7434 | Chungwei | 125.1 | 75.9 → 125.1 | 39.3 % | 2020-2025 |
| 7200 | Mehuken | 25.3 | 18.4 → 29.9 | 38.5 % | 2010-2025 |
| 7259 | Black Law + Extension | 187.4 | 124.0 → 187.4 | 33.8 % | 2013-2026 |
| 7190 | Høg-Jæren | 73.6 | 59.8 → 73.6 | 18.8 % | 2011-2025 |
| 7428 | Chang Kong | 86.0 | 71.833 → 86.0 | 16.5 % | 2020-2025 |
| 7302 | Griffin & Calliacher | 188.6 | 157.8 → 188.6 | 16.3 % | 2013-2026 |
| 7213 | Smøla | 145.8 | 145.8 → 150.4 | 3.1 % | 2006-2025 |

## Farms whose capacity-factor views change in addition

Their pre-extension years were ramp-up flagged, so the pipeline never saw them, but the Generation and Comparison pages show those years.

| Id | Farm | Nameplate MW | Capacity in force MW | Swing | Data years |
|---|---|---|---|---|---|
| 7191 | Hundhammerfjellet | 55.0 | 4.6 → 59.6 | 92.3 % | 2020-2025 |
| 7392 | London Array | 630.0 | 154.8 → 630.0 | 75.4 % | 2013-2026 |
| 7272 | Clyde | 521.8 | 220.116 → 521.8 | 57.8 % | 2017-2026 |
| 7254 | Beinn An Tuirc 2+3 | 93.2 | 43.254 → 93.2 | 53.6 % | 2013-2026 |
| 7396 | Neart na Gaoithe (NnG) | 448.0 | 224.0 → 448.0 | 50.0 % | 2023-2026 |
| 8777 | North Kyle | 220.5 | 110.25 → 220.5 | 50.0 % | 2025-2026 |
| 7422 | Walney Extension | 659.0 | 356.0 → 659.0 | 46.0 % | 2017-2026 |
| 7299 | Gordonbush + Extension | 109.75 | 71.75 → 109.75 | 34.6 % | 2021-2026 |

## Blocked: unit records disagree with nameplate

The new basis is not applied to these farms; they keep today's constant-nameplate behaviour until the records are corrected.

| Id | Farm | Nameplate MW | Units in force today MW | Difference | Reason |
|---|---|---|---|---|---|
| 7279 | Crossdykes 1 & 2 | 46.0 | 23.0 | -50.0 % | today's unit sum differs from nameplate by > 3% |
| 7429 | Mailiao | 36.0 | 46.0 | 27.78 % | today's unit sum differs from nameplate by > 3% |
| 8783 | Douglas West Extension | 66.0 | 68.4 | 3.64 % | today's unit sum differs from nameplate by > 3% |

## Unit records to fix

| windfarm_id | windfarm | unit_id | unit | capacity_mw | is_active | start_date | first_power_date | end_date | problems |
|---|---|---|---|---|---|---|---|---|---|
| 7241 | Yunlin | 10071 | Yunlin (YunHu) | 360.0 | True |  |  |  | undated |
| 7241 | Yunlin | 10072 | Yunlin (YunSi) | 280.0 | True |  |  |  | undated |
| 7252 | Bad A Cheo | 10084 | Bad A Cheo | 26.65 | True | 2019-05-01 | 0018-12-04 |  | start_before_1980 |
| 7279 | Crossdykes 1 & 2 | 10115 | Crossdykes 1 | 23.0 | True | 2021-06-01 | 2121-06-01 |  | start_in_future |
| 7395 | Moray West | 10302 | Moray Offshore Wind West 2 | 220.5 | True | 2026-06-30 | 0024-07-03 |  | start_before_1980 |
| 8767 | METCentre Karmøy | 12799 | METCentre Karmøy |  | True | 2009-10-08 |  |  | null_or_zero_capacity |

## Build-out phases without their own ramp-up window (21)

The farm-level ramp-up flag only covers the first phase. These later phases jump to full capacity on their start date, so their first months read as under-performance until a unit-level first-power / ramp-up end date is recorded.

| windfarm_id | windfarm | unit_id | unit | capacity_mw | phase_start | farm_ramp_end | note |
|---|---|---|---|---|---|---|---|
| 7190 | Høg-Jæren | 12515 | Høg-Jæren Phase 2 | 73.6 | 2012-11-22 | 2011-07-21 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7200 | Mehuken | 12603 | Mehuken Phase 3 | 18.4 | 2015-06-14 | 2010-08-06 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7200 | Mehuken | 12604 | Mehuken Phase 4 | 25.3 | 2015-08-10 | 2010-08-06 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7201 | Midtfjellet | 12606 | Midtfjellet Phase 2 | 85.0 | 2012-12-31 | 2012-11-22 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7201 | Midtfjellet | 12607 | Midtfjellet Phase 3 | 110.0 | 2013-09-16 | 2012-11-22 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7201 | Midtfjellet | 12608 | Midtfjellet Phase 4 | 149.6 | 2018-07-24 | 2012-11-22 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7202 | Nygårdsfjellet | 12611 | Nygårdsfjellet Phase 2 | 32.2 | 2011-08-31 | 2008-01-19 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7213 | Smøla | 12838 | Smøla Phase 3 | 145.8 | 2024-02-01 | 2006-01-21 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7238 | Greater Changhua 1 & 2A | 10066 | Greater Changhua 2A (WoEr) Phase 2 | 294.8 | 2025-03-14 | 2025-03-06 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7238 | Greater Changhua 1 & 2A | 10068 | Greater Changhua 1 (WoYi) Phase 2 | 605.2 | 2025-03-14 | 2025-03-06 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7417 | Thornton Bank | 10342 | Thorntonbank - C-Power - Area SW | 177.6 | 2013-09-01 | 2012-09-27 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7428 | Chang Kong | 10359 | Chang Kong Phase 2 | 62.0 | 2010-07-01 | 2008-02-29 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7428 | Chang Kong | 10360 | Chang Kong Phase 3 | 72.0 | 2019-07-01 | 2008-02-29 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7428 | Chang Kong | 10361 | Chang Kong Phase 4 | 86.2 | 2021-07-01 | 2008-02-29 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7429 | Mailiao | 10363 | Mailiao RePowering | 46.0 | 2024-01-01 | 2001-02-27 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7432 | Luwei Changbin | 10367 | Luwei Changbin Phase 2 | 55.2 | 2009-01-01 | 2008-02-29 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7433 | Guanwei-Guanyin&Taowei-Xinwu | 10369 | Guanwei-Guanyin&Taowei-Xinwu Phase 2 | 43.7 | 2012-03-01 | 2010-02-01 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7433 | Guanwei-Guanyin&Taowei-Xinwu | 10370 | Guanwei-Guanyin&Taowei-Xinwu Phase 3 | 48.3 | 2013-01-01 | 2010-02-01 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7434 | Chungwei | 10372 | Zhongwei Da-an Phase 2 | 75.9 | 2013-02-01 | 2009-04-01 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 7434 | Chungwei | 10373 | ChuangWei Wind | 49.2 | 2023-10-26 | 2009-04-01 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
| 8767 | METCentre Karmøy | 12790 | METCentre Karmøy Phase 2 | 5.9 | 2021-12-02 | 2009-12-08 | no unit-level first_power/ramp_up_end/COD → commissioning months read as underperformance |
