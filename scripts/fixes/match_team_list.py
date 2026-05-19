"""Cross-reference the team's 'Unlink and delete' list (from
Prioritisation 2026-05-18.docx) against the current state of the DB.

For each row in the team's list (gen_unit_code, gen_unit_name, action):
  - Look up the unit in `generation_units` by (code, name)
  - Determine current state: deleted | reconnected | still pending
  - Aggregate by action type

Hand-transcribed from the doc's first table (96 rows).
"""
import asyncio
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import text
from app.core.database import get_session_factory


# Team's "Unlink and delete generation units" list — (code, name, action)
TEAM_LIST = [
    # ENTSOE units — most have "Unlink" action (keep raw, just unlink)
    ("48W00000ABRBO-19",   "ABRB0-1",                              "Unlink"),
    ("48W00000BOWLW-1K",   "Barrow Offshore Wind Farm BOWLW-1",    "Unlink"),
    ("1088",               "Buheii Phase 1",                       "Unlink and delete"),
    ("48W00000BURBW-1L",   "Burbo Wind Farm BURBW-1",              "Unlink"),
    ("48W00000BRBEO-17",   "Burbo Extension BRBEO-1",              "Unlink"),
    ("T_DALRD-1",          "Dalry",                                "Unlink"),
    ("1095",               "Dønnesfjord Phase 2",                  "Unlink and delete"),
    ("48W000000EAAO-1R",   "East Anglia One",                      "Unlink"),
    ("37",                 "Fakken Phase 1",                       "Unlink and delete"),
    ("1081",               "Frøya Phase 5",                        "Unlink and delete"),
    ("48W00000GAOFO-13",   "Galloper Offshore Wind Farm GAOFO-1",  "Unlink"),
    ("48W00000GAOFO-21",   "Galloper Offshore Wind Farm GAOFO-2",  "Unlink"),
    ("48W00000GAOFO-4Y",   "Galloper Offshore Wind Farm GAOFO-4",  "Unlink"),
    ("48W10000GAOFO-3N",   "Galloper Offshore Wind Farm GAOFO-3",  "Unlink"),
    ("1077",               "Geitfjellet Phase 2",                  "Unlink and delete"),
    ("1093",               "Gismarvik Phase 2",                    "Unlink and delete"),
    ("1093",               "Gismarvik Phase 1",                    "Unlink and delete"),
    ("1078",               "Guleslettene Phase 25",                "Unlink and delete"),
    ("1078",               "Guleslettene Phase 35",                "Unlink and delete"),
    ("1078",               "Guleslettene Phase 24",                "Unlink and delete"),
    ("1078",               "Guleslettene Phase 26",                "Unlink and delete"),
    ("1078",               "Guleslettene Phase 29",                "Unlink and delete"),
    ("1078",               "Guleslettene Phase 30",                "Unlink and delete"),
    ("1078",               "Guleslettene Phase 32",                "Unlink and delete"),
    ("48W00000GNFSW-1H",   "Gunfleet Sands GNFSW-1",               "Unlink"),
    ("48W00000GNFSW-2F",   "Gunfleet Sands GNFSW-2",               "Unlink"),
    ("48W0000GYMRO-15O",   "Gwynt Y Mor GYMRO-15",                 "Unlink"),
    ("48W0000GYMRO-17K",   "Gwynt Y Mor GYMRO-17",                 "Unlink"),
    ("48W0000GYMRO-26J",   "Gwynt Y Mor GYMRO-26",                 "Unlink"),
    ("48W0000GYMRO-28F",   "Gwynt Y Mor GYMRO-28",                 "Unlink"),
    ("1084",               "Haram Phase 4",                        "Unlink and delete"),
    ("1084",               "Haram Phase 5",                        "Unlink and delete"),
    ("1080",               "Harbaksfjellet Phase 2",               "Unlink and delete"),
    ("3",                  "Havøygavlen Phase 3",                  "Unlink and delete"),
    ("3",                  "Havøygavlen Phase 5",                  "Unlink and delete"),
    ("68",                 "Hitra 2 Phase 1",                      "Unlink and delete"),
    ("48W00000HOWAO-1M",   "Hornsea 1",                            "Unlink"),
    ("7",                  "Hundhammerfjellet Phase 22",           "Unlink and delete"),
    ("7",                  "Hundhammerfjellet Phase 21",           "Unlink and delete"),
    ("1082",               "Kjølberget Phase 9",                   "Unlink and delete"),
    ("1082",               "Kjølberget Phase 10",                  "Unlink and delete"),
    ("1082",               "Kjølberget Phase 7",                   "Unlink and delete"),
    ("2",                  "Kjøllefjord Phase 1",                  "Unlink and delete"),
    ("1074",               "Kvenndalsfjellet Phase 2",             "Unlink and delete"),
    ("70",                 "Kvitfjell Phase 2",                    "Unlink and delete"),
    ("48W00000LNCSO-1R",   "Lincs Wind Farm LNCSO-1",              "Unlink and delete"),
    ("48W00000LNCSO-2P",   "Lincs Wind Farm LNCSO-2",              "Unlink"),
    ("48W00000LARYO-1Z",   "London Array Wind Farm LARYO-1",       "Unlink"),
    ("48W00000LARYO-2X",   "London Array Wind Farm LARYO-2",       "Unlink"),
    ("48W00000LARYO-3V",   "London Array Wind Farm LARYO-3",       "Unlink"),
    ("48W00000LARYO-4T",   "London Array Wind Farm LARYO-4",       "Unlink"),
    ("1092",               "Lutelandet Phase 5",                   "Unlink and delete"),
    ("1092",               "Lutelandet Phase 8",                   "Unlink and delete"),
    ("1092",               "Lutelandet Phase 7",                   "Unlink and delete"),
    ("1075",               "Måkaknuten Phase 4",                   "Unlink and delete"),
    ("1075",               "Måkaknuten Phase 2",                   "Unlink and delete"),
    ("1075",               "Måkaknuten Phase 20",                  "Unlink and delete"),
    ("64",                 "Marker Phase 1",                       "Unlink and delete"),
    ("1094",               "Odal Phase 21",                        "Unlink and delete"),
    ("1094",               "Odal Phase 23",                        "Unlink and delete"),
    ("48W00000OMNDO-1J",   "Ormonde Eng Ltd",                      "Unlink"),
    ("1086",               "Øyfjellet Phase 52",                   "Unlink and delete"),
    ("1086",               "Øyfjellet Phase 51",                   "Unlink and delete"),
    ("1086",               "Øyfjellet Phase 53",                   "Unlink and delete"),
    ("1086",               "Øyfjellet Phase 18",                   "Unlink and delete"),
    ("1086",               "Øyfjellet Phase 50",                   "Unlink and delete"),
    ("48W00000RMPNO-17",   "Rampion Offshore Wind Farm 1",         "Unlink"),
    ("48W00000RMPNO-25",   "Rampion Offshore Windfarm",            "Unlink"),
    ("1076",               "Raudfjell Phase 7",                    "Unlink and delete"),
    ("1076",               "Raudfjell Phase 9",                    "Unlink and delete"),
    ("56",                 "Roan Phase 1",                         "Unlink and delete"),
    ("48W000000RREW-14",   "Robin Rigg East RREW-1",               "Unlink"),
    ("48W000000RRWW-1P",   "Robin Rigg West RRWW-1",               "Unlink"),
    ("48W00000SHRSO-1Y",   "Sheringham Shoal Wind Farm SHRSO-1",   "Unlink"),
    ("48W00000SHRSO-2W",   "Sheringham Shoal Wind Farm SHRSO-2",   "Unlink"),
    ("67",                 "Skinansfjellet og Gravdal Phase 1",    "Unlink and delete"),
    ("48",                 "Skomakerfjellet Phase 1",              "Unlink and delete"),
    ("1085",               "Sørmarkfjellet Phase 5",               "Unlink and delete"),
    ("1085",               "Sørmarkfjellet Phase 8",               "Unlink and delete"),
    ("1085",               "Sørmarkfjellet Phase 11",              "Unlink and delete"),
    ("1073",               "Stigafjellet Phase 4",                 "Unlink and delete"),
    ("1073",               "Stigafjellet Phase 2",                 "Unlink and delete"),
    ("1087",               "Stokkfjellet Phase 2",                 "Unlink and delete"),
    ("1087",               "Stokkfjellet Phase 10",                "Unlink and delete"),
    ("65",                 "Storheia Phase 1",                     "Unlink and delete"),
    ("51",                 "Tellenes Phase 1",                     "Unlink and delete"),
    ("69",                 "Tonstad Phase 6",                      "Unlink and delete"),
    ("69",                 "Tonstad Phase 1",                      "Unlink and delete"),
    ("40",                 "Valsneset testpark",                   "Unlink and delete"),
    ("48W00000WLNYW-1A",   "Walney Wind Farm WLNYW-1",             "Unlink"),
    ("48W00000WLNYO-23",   "Walney Ext 2",                         "Unlink"),
    ("48W00000WLNYO-31",   "Walney Ext 3",                         "Unlink"),
    ("48W00000WLNYO-4-",   "Walney Ext 4",                         "Unlink"),
    ("48W00000WTMSO-1M",   "Westermost Rough W/F WTMSO-1",         "Unlink"),
    ("48W00000WDNSO-1H",   "West of Duddon Sands WDNSO-1",         "Unlink"),
    ("48W00000WDNSO-2F",   "West of Duddon Sands WDNSO-2",         "Unlink"),
]


# Units we've already actioned in this thread
ENTSOE_RECONNECTED = {
    "ABRB0-1": 12328,                              # → Aberdeen (wf 7350)
    "Ormonde Eng Ltd": 12385,                      # → Ormonde (wf 7404)
    "Hornsea 1": 12361,                            # → Hornsea 1 (wf 7384)
    "East Anglia One": 12346,                      # → East Anglia One (wf 7371)
    "Galloper Offshore Wind Farm GAOFO-1": 12348,  # → Galloper (wf 7373)
    "Galloper Offshore Wind Farm GAOFO-2": 12349,
    "Galloper Offshore Wind Farm GAOFO-3": 12350,
    "Galloper Offshore Wind Farm GAOFO-4": 12351,
}


def banner(t):
    print()
    print("=" * 100)
    print(t)
    print("=" * 100)


async def main():
    print(f"Team's 'Unlink and delete' list size: {len(TEAM_LIST)} rows")
    S = get_session_factory()
    async with S() as db:
        # Pull all inactive units once
        rs = await db.execute(text("""
            SELECT gu.id, gu.source, gu.code, gu.name, gu.is_active, gu.windfarm_id,
                   w.name AS wf_name,
                   (SELECT COUNT(*) FROM generation_data WHERE generation_unit_id = gu.id) AS n_rows
            FROM generation_units gu
            LEFT JOIN windfarms w ON w.id = gu.windfarm_id
            WHERE gu.is_active = FALSE
        """))
        inactive_by_name = {}
        inactive_by_code_and_name = {}
        all_inactive = list(rs)
        for u in all_inactive:
            inactive_by_name.setdefault(u.name, []).append(u)
            inactive_by_code_and_name[(str(u.code), u.name)] = u
        print(f"Currently inactive in DB: {len(all_inactive)}")

        # Match team list rows
        matched = []
        not_in_db = []
        for code, name, action in TEAM_LIST:
            # Try exact (code, name) first
            u = inactive_by_code_and_name.get((code, name))
            if u is None:
                # Try by name only
                same_name = inactive_by_name.get(name, [])
                if same_name:
                    u = same_name[0]
            if u is None:
                not_in_db.append((code, name, action))
            else:
                matched.append((code, name, action, u))

        banner("Match summary")
        print(f"  Team list rows:                   {len(TEAM_LIST)}")
        print(f"  Found in current inactive units:  {len(matched)}")
        print(f"  NOT in current inactive units:    {len(not_in_db)}")

        # The "not in db" set should be the units already actioned (reconnected, deleted, flipped)
        banner("Team-list rows NOT in current inactive set (already actioned or missing)")
        for code, name, action in not_in_db:
            note = ""
            if name in ENTSOE_RECONNECTED:
                note = f" → already reconnected (unit {ENTSOE_RECONNECTED[name]}, windfarm_id corrected)"
            print(f"  {code:<22} '{name[:42]:<44}' [{action}]{note}")

        # Split matched by action
        unlink_only = [x for x in matched if x[2] == "Unlink"]
        unlink_and_delete = [x for x in matched if x[2] == "Unlink and delete"]
        print(f"\n  matched 'Unlink' (keep unit row but detach):       {len(unlink_only)}")
        print(f"  matched 'Unlink and delete' (remove entirely):     {len(unlink_and_delete)}")

        banner("Matched: Unlink only (detach windfarm_id, keep unit)")
        for code, name, action, u in unlink_only:
            print(f"  unit {u.id:>5} '{name[:42]:<44}' code={code:<22} "
                  f"src={u.source:<8} rows={u.n_rows:>7,} wf={u.windfarm_id} '{(u.wf_name or '')[:18]}'")

        banner("Matched: Unlink and delete (remove unit + its data)")
        for code, name, action, u in unlink_and_delete:
            print(f"  unit {u.id:>5} '{name[:42]:<44}' code={code:<22} "
                  f"src={u.source:<8} rows={u.n_rows:>7,} wf={u.windfarm_id} '{(u.wf_name or '')[:18]}'")

        # And which of the 113 are NOT on the team list?
        on_team_names = {name for _, name, _ in TEAM_LIST}
        leftover = [u for u in all_inactive if u.name not in on_team_names]
        banner(f"In-DB inactive units NOT on team list: {len(leftover)}")
        for u in leftover[:50]:
            print(f"  unit {u.id:>5} '{(u.name or '')[:42]:<44}' code={(u.code or '')[:14]:<14} "
                  f"src={u.source:<8} rows={u.n_rows:>7,} wf={u.windfarm_id}")
        if len(leftover) > 50:
            print(f"  ... and {len(leftover) - 50} more")


asyncio.run(main())
