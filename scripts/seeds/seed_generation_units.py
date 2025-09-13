#!/usr/bin/env python3
"""
Seed script for generation_units table
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.models.generation_unit import GenerationUnit
from app.models.windfarm import Windfarm

# Raw generation unit data
GENERATION_UNITS_DATA = [
    {
        "code": "17W000001445569U",
        "name": "Eoliennes Offshore des Hautes Falaises 1",
        "source": "ENTSOE",
        "windfarm_name": "Fécamp",
    },
    {
        "code": "48W00000ABRBO-19",
        "name": "ABRB0-1",
        "source": "ENTSOE",
        "windfarm_name": "Aberdeen",
    },
    {
        "code": "17W100P100P0842Y",
        "name": "ADP A1 DE LA FERME EOLIENNE DE BAIE-DE-ST-BRIEUC",
        "source": "ENTSOE",
        "windfarm_name": "Saint Brieuc",
    },
    {
        "code": "17W100P100P3382R",
        "name": "ADP A2 DE LA FERME EOLIENNE DE BAIE-DE-ST-BRIEUC",
        "source": "ENTSOE",
        "windfarm_name": "Saint Brieuc",
    },
    {
        "code": "45W000000000046I",
        "name": "Anholt Generation",
        "source": "ENTSOE",
        "windfarm_name": "Anholt",
    },
    {
        "code": "48W00000BOWLW-1K",
        "name": "Barrow Offshore Wind Farm BOWLW-1",
        "source": "ENTSOE",
        "windfarm_name": "Barrow",
    },
    {
        "code": "48W00000BEATO-1T",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "48W00000BEATO-3P",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "48W00000BEATO-2R",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "48W00000BEATO-4N",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "T_BEATO-1",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ELEXON",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "T_BEATO-2",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ELEXON",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "T_BEATO-3",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ELEXON",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "T_BEATO-4",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ELEXON",
        "windfarm_name": "Beatrice",
    },
    {
        "code": "22WBELWIN1500271",
        "name": "Belwind Phase 1",
        "source": "ENTSOE",
        "windfarm_name": "Belwind 1",
    },
    {
        "code": "48W00000BRBEO-17",
        "name": "Burbo Extension BRBEO-1",
        "source": "ENTSOE",
        "windfarm_name": "Burbo Bank Extension",
    },
    {
        "code": "48W00000BURBW-1L",
        "name": "Burbo Wind Farm BURBW-1",
        "source": "ENTSOE",
        "windfarm_name": "Burbo Bank",
    },
    {
        "code": "45W000000000126K",
        "name": "Danish Kriegers Flak Generation Unit",
        "source": "ENTSOE",
        "windfarm_name": "Kriegers Flak",
    },
    {
        "code": "48W00000DBBWO-1D",
        "name": "Dogger Bank B Unit 1",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBBWO-2B",
        "name": "Dogger Bank B Unit 2",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBBWO-39",
        "name": "Dogger Bank B Unit 3",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBBWO-47",
        "name": "Dogger Bank B Unit 4",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBBWO-55",
        "name": "Dogger Bank B Unit 5",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBAWO-1J",
        "name": "Dogger Bank Unit 1",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBAWO-2H",
        "name": "Dogger Bank Unit 2",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBAWO-3F",
        "name": "Dogger Bank Unit 3",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBAWO-5B",
        "name": "Dogger Bank Unit 5",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W00000DBAWO-4D",
        "name": "Doggerbank Unit 4",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "48W000000EAAO-1R",
        "name": "East Anglia One",
        "source": "ENTSOE",
        "windfarm_name": "East Anglia One",
    },
    {
        "code": "48W000000EAAO-2P",
        "name": "East Anglia One",
        "source": "ENTSOE",
        "windfarm_name": "East Anglia One",
    },
    {
        "code": "17W0000014455708",
        "name": "Eoliennes Offshore des Hautes Falaises 2",
        "source": "ENTSOE",
        "windfarm_name": "Fécamp",
    },
    {
        "code": "48W00000GAOFO-13",
        "name": "Galloper Offshore Wind Farm GAOFO-1",
        "source": "ENTSOE",
        "windfarm_name": "Galloper",
    },
    {
        "code": "48W00000GAOFO-21",
        "name": "Galloper Offshore Wind Farm GAOFO-2",
        "source": "ENTSOE",
        "windfarm_name": "Galloper",
    },
    {
        "code": "48W10000GAOFO-3N",
        "name": "Galloper Offshore Wind Farm GAOFO-3",
        "source": "ENTSOE",
        "windfarm_name": "Galloper",
    },
    {
        "code": "48W00000GAOFO-4Y",
        "name": "Galloper Offshore Wind Farm GAOFO-4",
        "source": "ENTSOE",
        "windfarm_name": "Galloper",
    },
    {
        "code": "48W00000GRGBW-1V",
        "name": "Greater Gabbard GRGBW-1",
        "source": "ENTSOE",
        "windfarm_name": "Greater Gabbard",
    },
    {
        "code": "48W00000GRGBW-2T",
        "name": "Greater Gabbard GRGBW-2",
        "source": "ENTSOE",
        "windfarm_name": "Greater Gabbard",
    },
    {
        "code": "48W00000GRGBW-3R",
        "name": "Greater Gabbard GRGBW-3",
        "source": "ENTSOE",
        "windfarm_name": "Greater Gabbard",
    },
    {
        "code": "48W00000GNFSW-1H",
        "name": "Gunfleet Sands GNFSW-1",
        "source": "ENTSOE",
        "windfarm_name": "Gunfleet Sands 1&2",
    },
    {
        "code": "48W00000GNFSW-2F",
        "name": "Gunfleet Sands GNFSW-2",
        "source": "ENTSOE",
        "windfarm_name": "Gunfleet Sands 1&2",
    },
    {
        "code": "48W0000GYMRO-15O",
        "name": "Gwynt Y Mor GYMRO-15",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor",
    },
    {
        "code": "48W0000GYMRO-17K",
        "name": "Gwynt Y Mor GYMRO-17",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor",
    },
    {
        "code": "48W0000GYMRO-26J",
        "name": "Gwynt Y Mor GYMRO-26",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor",
    },
    {
        "code": "48W0000GYMRO-28F",
        "name": "Gwynt Y Mor GYMRO-28",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor",
    },
    {
        "code": "45W000000000047G",
        "name": "Horns Rev A",
        "source": "ENTSOE",
        "windfarm_name": "Horns Rev",
    },
    {
        "code": "45W000000000048E",
        "name": "Horns Rev B",
        "source": "ENTSOE",
        "windfarm_name": "Horns Rev",
    },
    {
        "code": "45W000000000116N",
        "name": "Horns Rev C generation unit",
        "source": "ENTSOE",
        "windfarm_name": "Horns Rev",
    },
    {
        "code": "48W00000HOWAO-1M",
        "name": "Hornsea 1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 1",
    },
    {
        "code": "48W00000HOWAO-2K",
        "name": "Hornsea 1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 1",
    },
    {
        "code": "48W00000HOWAO-3I",
        "name": "Hornsea 1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 1",
    },
    {
        "code": "48W00000HOWBO-1H",
        "name": "HOWBO-1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 2",
    },
    {
        "code": "48W00000HOWBO-2F",
        "name": "HOWBO-2",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 2",
    },
    {
        "code": "48W00000HOWBO-3D",
        "name": "HOWBO-3",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 2",
    },
    {
        "code": "48W00000HMGTO-10",
        "name": "Humber Gateway Offshore Wind Farm HMGTO-1",
        "source": "ENTSOE",
        "windfarm_name": "Humber Gateway",
    },
    {
        "code": "48W00000HMGTO-2Z",
        "name": "Humber Gateway Offshore Wind Farm HMGTO-2",
        "source": "ENTSOE",
        "windfarm_name": "Humber Gateway",
    },
    {
        "code": "48W00000HYWDW-1G",
        "name": "Hywind Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Hywind Scotland",
    },
    {
        "code": "48W00000LNCSO-1R",
        "name": "Lincs Wind Farm LNCSO-1",
        "source": "ENTSOE",
        "windfarm_name": "Lincs",
    },
    {
        "code": "48W00000LNCSO-2P",
        "name": "Lincs Wind Farm LNCSO-2",
        "source": "ENTSOE",
        "windfarm_name": "Lincs",
    },
    {
        "code": "48W00000LARYO-1Z",
        "name": "London Array Wind Farm LARYO-1",
        "source": "ENTSOE",
        "windfarm_name": "London Array",
    },
    {
        "code": "48W00000LARYO-2X",
        "name": "London Array Wind Farm LARYO-2",
        "source": "ENTSOE",
        "windfarm_name": "London Array",
    },
    {
        "code": "48W00000LARYO-3V",
        "name": "London Array Wind Farm LARYO-3",
        "source": "ENTSOE",
        "windfarm_name": "London Array",
    },
    {
        "code": "48W00000LARYO-4T",
        "name": "London Array Wind Farm LARYO-4",
        "source": "ENTSOE",
        "windfarm_name": "London Array",
    },
    {"code": "22W20200608B---3", "name": "Mermaid", "source": "ENTSOE", "windfarm_name": "Seamade"},
    {
        "code": "48W000000MOWEO11",
        "name": "MOWEO-1",
        "source": "ENTSOE",
        "windfarm_name": "Moray East",
    },
    {
        "code": "48W00000MOWEO-2Y",
        "name": "MOWEO-2",
        "source": "ENTSOE",
        "windfarm_name": "Moray East",
    },
    {
        "code": "48W00000MOWEO-3W",
        "name": "MOWEO-3",
        "source": "ENTSOE",
        "windfarm_name": "Moray East",
    },
    {
        "code": "48WW00000MOWWO-Z",
        "name": "MOWWO-1",
        "source": "ENTSOE",
        "windfarm_name": "Moray West",
    },
    {
        "code": "48W00000MOWWO-2I",
        "name": "MOWWO-2",
        "source": "ENTSOE",
        "windfarm_name": "Moray West",
    },
    {
        "code": "48W00000MOWWO-3G",
        "name": "MOWWO-3",
        "source": "ENTSOE",
        "windfarm_name": "Moray West",
    },
    {
        "code": "48W00000MOWWO-4E",
        "name": "MOWWO-4",
        "source": "ENTSOE",
        "windfarm_name": "Moray West",
    },
    {
        "code": "48W00000NNGAO-13",
        "name": "Neart Na Gaoithe Offshore Wind NNGAO-1",
        "source": "ENTSOE",
        "windfarm_name": "Neart Na Gaoithe (NnG)",
    },
    {
        "code": "48W00000NNGAO-21",
        "name": "Neart Na Gaoithe Offshore Wind NNGAO-2",
        "source": "ENTSOE",
        "windfarm_name": "Neart Na Gaoithe (NnG)",
    },
    {
        "code": "22W20161115----Z",
        "name": "NOBELWIND Park",
        "source": "ENTSOE",
        "windfarm_name": "Nobelwind",
    },
    {
        "code": "22W201902132---O",
        "name": "Norther Offshore WP GU",
        "source": "ENTSOE",
        "windfarm_name": "Norther",
    },
    {
        "code": "22W201909151---M",
        "name": "Northwester 2 Zeebrugge PU",
        "source": "ENTSOE",
        "windfarm_name": "Northwester 2",
    },
    {
        "code": "22WNORTHW150187B",
        "name": "Northwind",
        "source": "ENTSOE",
        "windfarm_name": "Northwind",
    },
    {
        "code": "48W00000OMNDO-1J",
        "name": "Ormonde Eng Ltd",
        "source": "ENTSOE",
        "windfarm_name": "Ormonde",
    },
    {
        "code": "17W0000014455651",
        "name": "Parc du Banc de Guérande 1",
        "source": "ENTSOE",
        "windfarm_name": "Saint-Nazaire",
    },
    {
        "code": "17W000001445567Y",
        "name": "Parc du Banc de Guérande 2",
        "source": "ENTSOE",
        "windfarm_name": "Saint-Nazaire",
    },
    {
        "code": "48W00000RMPNO-17",
        "name": "Rampion Offshore Wind Farm 1",
        "source": "ENTSOE",
        "windfarm_name": "Rampion",
    },
    {
        "code": "48W00000RMPNO-25",
        "name": "Rampion Offshore Windfarm",
        "source": "ENTSOE",
        "windfarm_name": "Rampion",
    },
    {
        "code": "48W00000RCBKO-1S",
        "name": "RCBKO-1",
        "source": "ENTSOE",
        "windfarm_name": "Race Bank",
    },
    {
        "code": "48W00000RCBKO-2Q",
        "name": "RCBKO-2",
        "source": "ENTSOE",
        "windfarm_name": "Race Bank",
    },
    {
        "code": "22W20180615----H",
        "name": "Rentel Offshore WP PU",
        "source": "ENTSOE",
        "windfarm_name": "Rentel",
    },
    {
        "code": "48W000000RREW-14",
        "name": "Robin Rigg East RREW-1",
        "source": "ENTSOE",
        "windfarm_name": "Robin Rigg",
    },
    {
        "code": "48W000000RRWW-1P",
        "name": "Robin Rigg West RRWW-1",
        "source": "ENTSOE",
        "windfarm_name": "Robin Rigg",
    },
    {
        "code": "45W000000000044M",
        "name": "Rødsand 1_GU",
        "source": "ENTSOE",
        "windfarm_name": "Nysted",
    },
    {
        "code": "45W000000000045K",
        "name": "Rødsand 2_GU",
        "source": "ENTSOE",
        "windfarm_name": "Rødsand II",
    },
    {
        "code": "48W00000SGRWO-1L",
        "name": "Seagreen Windfarm SGRWO-1",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "48W00000SGRWO-2J",
        "name": "Seagreen Windfarm SGRWO-2",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "48W00000SGRWO-3H",
        "name": "Seagreen Windfarm SGRWO-3",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "48W00000SGRWO-4F",
        "name": "Seagreen Windfarm SGRWO-4",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "48W00000SGRWO-5D",
        "name": "Seagreen Windfarm SGRWO-5",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "48W00000SGRWO-6B",
        "name": "Seagreen Windfarm SGRWO-6",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen",
    },
    {"code": "22W20200608D---U", "name": "Seastar", "source": "ENTSOE", "windfarm_name": "Seamade"},
    {
        "code": "48W00000SHRSO-1Y",
        "name": "Sheringham Shoal Wind Farm SHRSO-1",
        "source": "ENTSOE",
        "windfarm_name": "Sheringham Shoal",
    },
    {
        "code": "48W00000SHRSO-2W",
        "name": "Sheringham Shoal Wind Farm SHRSO-2",
        "source": "ENTSOE",
        "windfarm_name": "Sheringham Shoal",
    },
    {
        "code": "48W00000THNTO-18",
        "name": "Thanet Offshore Wind THNTO-1",
        "source": "ENTSOE",
        "windfarm_name": "Thanet",
    },
    {
        "code": "48W00000THNTO-26",
        "name": "Thanet Offshore Wind THNTO-2",
        "source": "ENTSOE",
        "windfarm_name": "Thanet",
    },
    {
        "code": "22WTHORNT150237E",
        "name": "Thorntonbank - C-Power - Area NE",
        "source": "ENTSOE",
        "windfarm_name": "Thorntonbank II & III",
    },
    {
        "code": "22WTHORNT150238C",
        "name": "Thorntonbank - C-Power - Area SW",
        "source": "ENTSOE",
        "windfarm_name": "Thorntonbank II & III",
    },
    {
        "code": "45W000000000208I",
        "name": "Vesterhav Nord",
        "source": "ENTSOE",
        "windfarm_name": "Vesterhav Syd & Nord",
    },
    {
        "code": "45W000000000207K",
        "name": "Vesterhav Syd",
        "source": "ENTSOE",
        "windfarm_name": "Vesterhav Syd & Nord",
    },
    {
        "code": "48W00000WLNYO-23",
        "name": "Walney Ext 2",
        "source": "ENTSOE",
        "windfarm_name": "Walney 1&2",
    },
    {
        "code": "48W00000WLNYO-31",
        "name": "Walney Ext 3",
        "source": "ENTSOE",
        "windfarm_name": "Walney Extension",
    },
    {
        "code": "48W00000WLNYO-4-",
        "name": "Walney Ext 4",
        "source": "ENTSOE",
        "windfarm_name": "Walney Extension",
    },
    {
        "code": "48W00000WLNYW-1A",
        "name": "Walney Wind Farm WLNYW-1",
        "source": "ENTSOE",
        "windfarm_name": "Walney 1&2",
    },
    {
        "code": "48W00000WDNSO-1H",
        "name": "West of Duddon Sands WDNSO-1",
        "source": "ENTSOE",
        "windfarm_name": "West of Duddon Sands",
    },
    {
        "code": "48W00000WDNSO-2F",
        "name": "West of Duddon Sands WDNSO-2",
        "source": "ENTSOE",
        "windfarm_name": "West of Duddon Sands",
    },
    {
        "code": "48W00000WTMSO-1M",
        "name": "Westermost Rough W/F WTMSO-1",
        "source": "ENTSOE",
        "windfarm_name": "Westermost Rough",
    },
    # Additional Offshore Generation Units
    {
        "code": "ABRB0-1",
        "name": "Aberdeen Offshore Wind Farm",
        "source": "ELEXON",
        "windfarm_name": "Aberdeen",
    },
    {
        "code": "T_ABRBO-1",
        "name": "Aberdeen Offshore Wind Farm",
        "source": "ELEXON",
        "windfarm_name": "Aberdeen",
    },
    {
        "code": "T_BOWLW-1",
        "name": "Barrow",
        "source": "ELEXON",
        "windfarm_name": "Barrow",
    },
    {
        "code": "BURBW-1",
        "name": "Burbo Bank",
        "source": "ELEXON",
        "windfarm_name": "Burbo Bank",
    },
    {
        "code": "E_BURBO",
        "name": "Burbo Bank",
        "source": "ELEXON",
        "windfarm_name": "Burbo Bank",
    },
    {
        "code": "T_BRBEO-1",
        "name": "Burbo Bank Ext",
        "source": "ELEXON",
        "windfarm_name": "Burbo Bank Extension",
    },
    {
        "code": "T_DBBWO-1",
        "name": "Dogger Bank B Unit 1",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBBWO-2",
        "name": "Dogger Bank B Unit 2",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBBWO-3",
        "name": "Dogger Bank B Unit 3",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBBWO-4",
        "name": "Dogger Bank B Unit 4",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBBWO-5",
        "name": "Dogger Bank B Unit 5",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBAWO-1",
        "name": "Dogger Bank Unit 1",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBAWO-2",
        "name": "Dogger Bank Unit 2",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBAWO-3",
        "name": "Dogger Bank Unit 3",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBAWO-5",
        "name": "Dogger Bank Unit 5",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DBAWO-4",
        "name": "Doggerbank Unit 4",
        "source": "ELEXON",
        "windfarm_name": "Dogger Bank A&B",
    },
    {
        "code": "T_DDGNO-1",
        "name": "Dudgeon 1",
        "source": "ELEXON",
        "windfarm_name": "Dudgeon",
    },
    {
        "code": "T_DDGNO-2",
        "name": "Dudgeon 2",
        "source": "ELEXON",
        "windfarm_name": "Dudgeon",
    },
    {
        "code": "T_DDGNO-3",
        "name": "Dudgeon 3",
        "source": "ELEXON",
        "windfarm_name": "Dudgeon",
    },
    {
        "code": "T_DDGNO-4",
        "name": "Dudgeon 4",
        "source": "ELEXON",
        "windfarm_name": "Dudgeon",
    },
    {
        "code": "T_EAAO-1",
        "name": "East Anglia One Part 1",
        "source": "ELEXON",
        "windfarm_name": "East Anglia One",
    },
    {
        "code": "T_EAAO-2",
        "name": "East Anglia One Part 2",
        "source": "ELEXON",
        "windfarm_name": "East Anglia One",
    },
    {
        "code": "GAOFO-1",
        "name": "Galloper 1",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "T_GANW-11",
        "name": "Galloper 1",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "GAOFO-2",
        "name": "Galloper 2",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "T_GANW-22",
        "name": "Galloper 2",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "GAOFO-3",
        "name": "Galloper 3",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "T_GANW-13",
        "name": "Galloper 3",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "GAOFO-4",
        "name": "Galloper 4",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "T_GANW-24",
        "name": "Galloper 4",
        "source": "ELEXON",
        "windfarm_name": "Galloper",
    },
    {
        "code": "T_GRGBW-1",
        "name": "Greater Gabbard 1",
        "source": "ELEXON",
        "windfarm_name": "Greater Gabbard",
    },
    {
        "code": "T_GRGBW-2",
        "name": "Greater Gabbard 2",
        "source": "ELEXON",
        "windfarm_name": "Greater Gabbard",
    },
    {
        "code": "T_GRGBW-3",
        "name": "Greater Gabbard 3",
        "source": "ELEXON",
        "windfarm_name": "Greater Gabbard",
    },
    {
        "code": "T_GNFSW-1",
        "name": "Gunfleet Sands 1",
        "source": "ELEXON",
        "windfarm_name": "Gunfleet Sands",
    },
    {
        "code": "T_GNFSW-2",
        "name": "Gunfleet Sands 2",
        "source": "ELEXON",
        "windfarm_name": "Gunfleet Sands",
    },
    {
        "code": "GYMRO-15",
        "name": "Gwynt y Mor 15",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "T_GYMR-15",
        "name": "Gwynt y Mor 15",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "GYMRO-17",
        "name": "Gwynt y Mor 17",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "T_GYMR-17",
        "name": "Gwynt y Mor 17",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "GYMRO-26",
        "name": "Gwynt y Mor 26",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "T_GYMR-26",
        "name": "Gwynt y Mor 26",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "GYMRO-28",
        "name": "Gwynt y Mor 28",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "T_GYMR-28",
        "name": "Gwynt y Mor 28",
        "source": "ELEXON",
        "windfarm_name": "Gwynt y Mor",
    },
    {
        "code": "T_HOWAO-1",
        "name": "Hornsea A1",
        "source": "ELEXON",
        "windfarm_name": "Hornsea 1",
    },
    {
        "code": "T_HOWAO-2",
        "name": "Hornsea A2",
        "source": "ELEXON",
        "windfarm_name": "Hornsea 1",
    },
    {
        "code": "T_HOWAO-3",
        "name": "Hornsea A3",
        "source": "ELEXON",
        "windfarm_name": "Hornsea 1",
    },
    {
        "code": "T_HOWBO-1",
        "name": "Hornsea B1",
        "source": "ELEXON",
        "windfarm_name": "Hornsea 2",
    },
    {
        "code": "T_HOWBO-2",
        "name": "Hornsea B2",
        "source": "ELEXON",
        "windfarm_name": "Hornsea 2",
    },
    {
        "code": "T_HOWBO-3",
        "name": "Hornsea B3",
        "source": "ELEXON",
        "windfarm_name": "Hornsea 2",
    },
    {
        "code": "T_HMGTO-1",
        "name": "Humber Gateway 1",
        "source": "ELEXON",
        "windfarm_name": "Humber Gateway",
    },
    {
        "code": "T_HMGTO-2",
        "name": "Humber Gateway 2",
        "source": "ELEXON",
        "windfarm_name": "Humber Gateway",
    },
    {
        "code": "HYWDW-1",
        "name": "Hywind",
        "source": "ELEXON",
        "windfarm_name": "Hywind Scotland",
    },
    {
        "code": "E_HYWDW-1",
        "name": "Hywind",
        "source": "ELEXON",
        "windfarm_name": "Hywind Scotland",
    },
    {
        "code": "LNCSO-1",
        "name": "Lincs 1",
        "source": "ELEXON",
        "windfarm_name": "Lincs",
    },
    {
        "code": "T_LNCSW-1",
        "name": "Lincs 1",
        "source": "ELEXON",
        "windfarm_name": "Lincs",
    },
    {
        "code": "LNCSO-2",
        "name": "Lincs 2",
        "source": "ELEXON",
        "windfarm_name": "Lincs",
    },
    {
        "code": "T_LNCSW-2",
        "name": "Lincs 2",
        "source": "ELEXON",
        "windfarm_name": "Lincs",
    },
    {
        "code": "LARYO-1",
        "name": "London Array 1",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "T_LARYW-1",
        "name": "London Array 1",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "LARYO-2",
        "name": "London Array 2",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "T_LARYW-2",
        "name": "London Array 2",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "LARYO-3",
        "name": "London Array 3",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "T_LARYW-3",
        "name": "London Array 3",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "LARYO-4",
        "name": "London Array 4",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "T_LARYW-4",
        "name": "London Array 4",
        "source": "ELEXON",
        "windfarm_name": "London Array",
    },
    {
        "code": "T_MOWEO-1",
        "name": "Moray Firth Eastern 1",
        "source": "ELEXON",
        "windfarm_name": "Moray East",
    },
    {
        "code": "T_MOWEO-2",
        "name": "Moray Firth Eastern 2",
        "source": "ELEXON",
        "windfarm_name": "Moray East",
    },
    {
        "code": "T_MOWEO-3",
        "name": "Moray Firth Eastern 3",
        "source": "ELEXON",
        "windfarm_name": "Moray East",
    },
    {
        "code": "T_MOWWO-1",
        "name": "Moray Offshore Wind West 1",
        "source": "ELEXON",
        "windfarm_name": "Moray West",
    },
    {
        "code": "T_MOWWO-2",
        "name": "Moray Offshore Wind West 2",
        "source": "ELEXON",
        "windfarm_name": "Moray West",
    },
    {
        "code": "T_MOWWO-3",
        "name": "Moray Offshore Wind West 3",
        "source": "ELEXON",
        "windfarm_name": "Moray West",
    },
    {
        "code": "T_MOWWO-4",
        "name": "Moray Offshore Wind West 4",
        "source": "ELEXON",
        "windfarm_name": "Moray West",
    },
    {
        "code": "T_NNGAO-1",
        "name": "Neart Na Gaoithe Offshore Wind 1",
        "source": "ELEXON",
        "windfarm_name": "Neart Na Gaoithe (NnG)",
    },
    {
        "code": "T_NNGAO-2",
        "name": "Neart Na Gaoithe Offshore Wind 2",
        "source": "ELEXON",
        "windfarm_name": "Neart Na Gaoithe (NnG)",
    },
    {
        "code": "OMNDO-1",
        "name": "Ormonde Energy",
        "source": "ELEXON",
        "windfarm_name": "Ormonde",
    },
    {
        "code": "T_OMNDW-1",
        "name": "Ormonde Energy",
        "source": "ELEXON",
        "windfarm_name": "Ormonde",
    },
    {
        "code": "T_RCBKO-1",
        "name": "Race Bank 1",
        "source": "ELEXON",
        "windfarm_name": "Race Bank",
    },
    {
        "code": "T_RCBKO-2",
        "name": "Race Bank 2",
        "source": "ELEXON",
        "windfarm_name": "Race Bank",
    },
    {
        "code": "T_RMPNO-1",
        "name": "Rampion 1",
        "source": "ELEXON",
        "windfarm_name": "Rampion",
    },
    {
        "code": "T_RMPNO-2",
        "name": "Rampion 2",
        "source": "ELEXON",
        "windfarm_name": "Rampion",
    },
    {
        "code": "T_RREW-1",
        "name": "Robin Rigg East",
        "source": "ELEXON",
        "windfarm_name": "Robin Rigg",
    },
    {
        "code": "T_RRWW-1",
        "name": "Robin Rigg West",
        "source": "ELEXON",
        "windfarm_name": "Robin Rigg",
    },
    {
        "code": "T_SGRWO-1",
        "name": "Seagreen 1",
        "source": "ELEXON",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "T_SGRWO-2",
        "name": "Seagreen 2",
        "source": "ELEXON",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "T_SGRWO-3",
        "name": "Seagreen 3",
        "source": "ELEXON",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "T_SGRWO-4",
        "name": "Seagreen 4",
        "source": "ELEXON",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "T_SGRWO-5",
        "name": "Seagreen 5",
        "source": "ELEXON",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "T_SGRWO-6",
        "name": "Seagreen 6",
        "source": "ELEXON",
        "windfarm_name": "Seagreen",
    },
    {
        "code": "SHRSO-1",
        "name": "Sheringham Shoal 1",
        "source": "ELEXON",
        "windfarm_name": "Sheringham Shoal",
    },
    {
        "code": "T_SHRSW-1",
        "name": "Sheringham Shoal 1",
        "source": "ELEXON",
        "windfarm_name": "Sheringham Shoal",
    },
    {
        "code": "SHRSO-2",
        "name": "Sheringham Shoal 2",
        "source": "ELEXON",
        "windfarm_name": "Sheringham Shoal",
    },
    {
        "code": "T_SHRSW-2",
        "name": "Sheringham Shoal 2",
        "source": "ELEXON",
        "windfarm_name": "Sheringham Shoal",
    },
    {
        "code": "T_THNTO-1",
        "name": "Thanet 1",
        "source": "ELEXON",
        "windfarm_name": "Thanet",
    },
    {
        "code": "T_THNTO-2",
        "name": "Thanet 2",
        "source": "ELEXON",
        "windfarm_name": "Thanet",
    },
    {
        "code": "T_TKNEW-1",
        "name": "Triton Knoll East",
        "source": "ELEXON",
        "windfarm_name": "Triton Knoll",
    },
    {
        "code": "T_TKNWW-1",
        "name": "Triton Knoll West",
        "source": "ELEXON",
        "windfarm_name": "Triton Knoll",
    },
    {
        "code": "T_WLNYW-1",
        "name": "Walney 1",
        "source": "ELEXON",
        "windfarm_name": "Walney 1&2",
    },
    {
        "code": "T_WLNYO-2",
        "name": "Walney 2",
        "source": "ELEXON",
        "windfarm_name": "Walney 1&2",
    },
    {
        "code": "T_WLNYO-3",
        "name": "Walney 3",
        "source": "ELEXON",
        "windfarm_name": "Walney Extension",
    },
    {
        "code": "T_WLNYO-4",
        "name": "Walney 4",
        "source": "ELEXON",
        "windfarm_name": "Walney Extension",
    },
    {
        "code": "T_WDNSO-1",
        "name": "West of Duddon Sands 1",
        "source": "ELEXON",
        "windfarm_name": "West of Duddon Sands",
    },
    {
        "code": "T_WDNSO-2",
        "name": "West of Duddon Sands 2",
        "source": "ELEXON",
        "windfarm_name": "West of Duddon Sands",
    },
    {
        "code": "T_WTMSO-1",
        "name": "Westermost Rough",
        "source": "ELEXON",
        "windfarm_name": "Westermost Rough",
    },
    # Additional Onshore Generation Units
    {
        "code": "T_ACHRW-1",
        "name": "A'Chruach 1",
        "source": "ELEXON",
        "windfarm_name": "A'Chruach 1",
    },
    {
        "code": "T_AFTOW-1",
        "name": "Afton",
        "source": "ELEXON",
        "windfarm_name": "Afton",
    },
    {
        "code": "T_AKGLW-2",
        "name": "Aikengall II",
        "source": "ELEXON",
        "windfarm_name": "Aikengall II",
    },
    {
        "code": "T_AKGLW-3",
        "name": "Aikengall IIa",
        "source": "ELEXON",
        "windfarm_name": "Aikengall II",
    },
    {
        "code": "AIRSW-1",
        "name": "Airies",
        "source": "ELEXON",
        "windfarm_name": "Airies",
    },
    {
        "code": "E_AIRSW-1",
        "name": "Airies",
        "source": "ELEXON",
        "windfarm_name": "Airies",
    },
    {
        "code": "T_ANSUW-1",
        "name": "An Suidhe",
        "source": "ELEXON",
        "windfarm_name": "An Suidhe",
    },
    {
        "code": "ASHWW-1",
        "name": "Andershaw",
        "source": "ELEXON",
        "windfarm_name": "Andershaw",
    },
    {
        "code": "E_ASHWW-1",
        "name": "Andershaw",
        "source": "ELEXON",
        "windfarm_name": "Andershaw",
    },
    {
        "code": "T_ARCHW-1",
        "name": "Arecleoch",
        "source": "ELEXON",
        "windfarm_name": "Arecleoch",
    },
    {
        "code": "ASLVW-1",
        "name": "Assel Valley",
        "source": "ELEXON",
        "windfarm_name": "Assel Valley",
    },
    {
        "code": "E_ASLVW-1",
        "name": "Assel Valley",
        "source": "ELEXON",
        "windfarm_name": "Assel Valley",
    },
    {
        "code": "ABRTW-1",
        "name": "Auchrobert",
        "source": "ELEXON",
        "windfarm_name": "Auchrobert",
    },
    {
        "code": "E_ABRTW-1",
        "name": "Auchrobert",
        "source": "ELEXON",
        "windfarm_name": "Auchrobert",
    },
    {
        "code": "T_BDCHW-1",
        "name": "Bad A Cheo",
        "source": "ELEXON",
        "windfarm_name": "Bad A Cheo",
    },
    {
        "code": "BABAW-1",
        "name": "Baillie",
        "source": "ELEXON",
        "windfarm_name": "Baillie",
    },
    {
        "code": "E_BABAW-1",
        "name": "Baillie",
        "source": "ELEXON",
        "windfarm_name": "Baillie",
    },
    {
        "code": "BTUIW-2",
        "name": "Beinn An Tuirc 2",
        "source": "ELEXON",
        "windfarm_name": "Beinn An Tuirc 2+3",
    },
    {
        "code": "E_BTUIW-2",
        "name": "Beinn An Tuirc 2",
        "source": "ELEXON",
        "windfarm_name": "Beinn An Tuirc 2+3",
    },
    {
        "code": "BTUIW-3",
        "name": "Beinn An Tuirc 3",
        "source": "ELEXON",
        "windfarm_name": "Beinn An Tuirc 2+3",
    },
    {
        "code": "E_BTUIW-3",
        "name": "Beinn An Tuirc 3",
        "source": "ELEXON",
        "windfarm_name": "Beinn An Tuirc 2+3",
    },
    {
        "code": "BETHW-1",
        "name": "Beinn Tharsuinn",
        "source": "ELEXON",
        "windfarm_name": "Beinn Tharsuinn",
    },
    {
        "code": "E_BETHW-1",
        "name": "Beinn Tharsuinn",
        "source": "ELEXON",
        "windfarm_name": "Beinn Tharsuinn",
    },
    {
        "code": "T_BEINW-1",
        "name": "Beinneun 1",
        "source": "ELEXON",
        "windfarm_name": "Beinneun",
    },
    {
        "code": "T_BEINW-1",
        "name": "Beinneun 2",
        "source": "ELEXON",
        "windfarm_name": "Beinneun",
    },
    {
        "code": "BRYBW-1",
        "name": "Berry Burn",
        "source": "ELEXON",
        "windfarm_name": "Berry Burn",
    },
    {
        "code": "E_BRYBW-1",
        "name": "Berry Burn",
        "source": "ELEXON",
        "windfarm_name": "Berry Burn",
    },
    {
        "code": "T_BHLAW-1",
        "name": "Bhlaraidh",
        "source": "ELEXON",
        "windfarm_name": "Bhlaraidh",
    },
    {
        "code": "T_BLLA-1",
        "name": "Black Law",
        "source": "ELEXON",
        "windfarm_name": "Black Law",
    },
    {
        "code": "T_BLLA-2",
        "name": "Black Law Extension",
        "source": "ELEXON",
        "windfarm_name": "Black Law",
    },
    {
        "code": "T_BLKWW-1",
        "name": "Blackcraig",
        "source": "ELEXON",
        "windfarm_name": "Blackcraig",
    },
    {
        "code": "BLARW-1",
        "name": "Blary Hill",
        "source": "ELEXON",
        "windfarm_name": "Blary Hill",
    },
    {
        "code": "E_BLARW-1",
        "name": "Blary Hill",
        "source": "ELEXON",
        "windfarm_name": "Blary Hill",
    },
    {
        "code": "BRDUW-1",
        "name": "Braes of Doune",
        "source": "ELEXON",
        "windfarm_name": "Braes of Doune",
    },
    {
        "code": "E_BRDUW-1",
        "name": "Braes of Doune",
        "source": "ELEXON",
        "windfarm_name": "Braes of Doune",
    },
    {
        "code": "T_WISTW-2",
        "name": "Brockloch Rig",
        "source": "ELEXON",
        "windfarm_name": "Brockloch Rig",
    },
    {
        "code": "T_BROCW-1",
        "name": "Broken Cross",
        "source": "ELEXON",
        "windfarm_name": "Broken Cross",
    },
    {
        "code": "BRNLW-1",
        "name": "Brownieleys",
        "source": "ELEXON",
        "windfarm_name": "Brownieleys",
    },
    {
        "code": "E_BRNLW-1",
        "name": "Brownieleys",
        "source": "ELEXON",
        "windfarm_name": "Brownieleys",
    },
    {
        "code": "BNWKW-1",
        "name": "Burn of Whilk",
        "source": "ELEXON",
        "windfarm_name": "Burn of Whilk",
    },
    {
        "code": "E_BNWKW-1",
        "name": "Burn of Whilk",
        "source": "ELEXON",
        "windfarm_name": "Burn of Whilk",
    },
    {
        "code": "CMSTW-1",
        "name": "Camster",
        "source": "ELEXON",
        "windfarm_name": "Camster",
    },
    {
        "code": "2__PEDGE003",
        "name": "Camster",
        "source": "ELEXON",
        "windfarm_name": "Camster",
    },
    {
        "code": "T_CRGHW-1",
        "name": "Carraig Gheal",
        "source": "ELEXON",
        "windfarm_name": "Carraig Gheal",
    },
    {
        "code": "CAUSW-1",
        "name": "Causeymire",
        "source": "ELEXON",
        "windfarm_name": "Causeymire",
    },
    {
        "code": "2__PSMAE001",
        "name": "Causeymire",
        "source": "ELEXON",
        "windfarm_name": "Causeymire",
    },
    {
        "code": "CLFLW-1",
        "name": "Clachan Flats",
        "source": "ELEXON",
        "windfarm_name": "Clachan Flats",
    },
    {
        "code": "E_CLFLW-1",
        "name": "Clachan Flats",
        "source": "ELEXON",
        "windfarm_name": "Clachan Flats",
    },
    {
        "code": "CLDRW-1",
        "name": "Clashindarroch (Vattenfall)",
        "source": "ELEXON",
        "windfarm_name": "Clashindarroch (Vattenfall)",
    },
    {
        "code": "E_CLDRW-1",
        "name": "Clashindarroch (Vattenfall)",
        "source": "ELEXON",
        "windfarm_name": "Clashindarroch (Vattenfall)",
    },
    {
        "code": "T_CLDCW-1",
        "name": "Clyde North",
        "source": "ELEXON",
        "windfarm_name": "Clyde",
    },
    {
        "code": "T_CLDNW-1",
        "name": "Clyde Central",
        "source": "ELEXON",
        "windfarm_name": "Clyde",
    },
    {
        "code": "T_CLDSW-1",
        "name": "Clyde South",
        "source": "ELEXON",
        "windfarm_name": "Clyde",
    },
    {
        "code": "CNCLW-1",
        "name": "Coire Na Cloiche",
        "source": "ELEXON",
        "windfarm_name": "Coire Na Cloiche",
    },
    {
        "code": "E_CNCLW-1",
        "name": "Coire Na Cloiche",
        "source": "ELEXON",
        "windfarm_name": "Coire Na Cloiche",
    },
    {
        "code": "T_CGTHW-1",
        "name": "Corriegarth",
        "source": "ELEXON",
        "windfarm_name": "Corriegarth",
    },
    {
        "code": "T_CRMLW-1",
        "name": "Corriemoillie",
        "source": "ELEXON",
        "windfarm_name": "Corriemoillie",
    },
    {
        "code": "T_COUWW-1",
        "name": "Cour",
        "source": "ELEXON",
        "windfarm_name": "Cour",
    },
    {
        "code": "CRGTW-1",
        "name": "Craig",
        "source": "ELEXON",
        "windfarm_name": "Craig",
    },
    {
        "code": "E_CRGTW-1",
        "name": "Craig",
        "source": "ELEXON",
        "windfarm_name": "Craig",
    },
    {
        "code": "T_CREAW-1",
        "name": "Creag Riabhach",
        "source": "ELEXON",
        "windfarm_name": "Creag Riabhach",
    },
    {
        "code": "T_CRDEW-1",
        "name": "Crossdykes 1",
        "source": "ELEXON",
        "windfarm_name": "Crossdykes",
    },
    {
        "code": "T_CRDEW-2",
        "name": "Crossdykes 2",
        "source": "ELEXON",
        "windfarm_name": "Crossdykes",
    },
    {
        "code": "T_CRYRW-2",
        "name": "Crystal Rig II",
        "source": "ELEXON",
        "windfarm_name": "Crystal Rig 2 + 3",
    },
    {
        "code": "T_CRYRW-3",
        "name": "Crystal Rig III",
        "source": "ELEXON",
        "windfarm_name": "Crystal Rig 2 + 3",
    },
    {
        "code": "T_CUMHW-1",
        "name": "Cumberhead",
        "source": "ELEXON",
        "windfarm_name": "Cumberhead",
    },
    {
        "code": "T_DALRD-1",
        "name": "Dalry",
        "source": "ELEXON",
        "windfarm_name": "Dalry",
    },
    {
        "code": "T_DALQW-1",
        "name": "Dalquhandy",
        "source": "ELEXON",
        "windfarm_name": "Dalquhandy",
    },
    {
        "code": "DALSW-1",
        "name": "Dalswinton",
        "source": "ELEXON",
        "windfarm_name": "Dalswinton",
    },
    {
        "code": "E_DALSW-1",
        "name": "Dalswinton",
        "source": "ELEXON",
        "windfarm_name": "Dalswinton",
    },
    {
        "code": "T_DRSLW-1",
        "name": "Dersalloch",
        "source": "ELEXON",
        "windfarm_name": "Dersalloch",
    },
    {
        "code": "T_DOREW-1",
        "name": "Dorenell 1",
        "source": "ELEXON",
        "windfarm_name": "Dorenell",
    },
    {
        "code": "T_DOREW-2",
        "name": "Dorenell 2",
        "source": "ELEXON",
        "windfarm_name": "Dorenell",
    },
    {
        "code": "T_DOUGW-1",
        "name": "Douglas West",
        "source": "ELEXON",
        "windfarm_name": "Douglas West",
    },
    {
        "code": "T_DNLWW-1",
        "name": "Dun Law 2",
        "source": "ELEXON",
        "windfarm_name": "Dun Law 2",
    },
    {
        "code": "T_DUNGW-1",
        "name": "Dunmaglass",
        "source": "ELEXON",
        "windfarm_name": "Dunmaglass",
    },
    {
        "code": "T_EDINW-1",
        "name": "Edinbane",
        "source": "ELEXON",
        "windfarm_name": "Edinbane",
    },
    {
        "code": "T_EWHLW-1",
        "name": "Ewe Hill 2",
        "source": "ELEXON",
        "windfarm_name": "Ewe Hill 2",
    },
    {
        "code": "T_FALGW-1",
        "name": "Fallago Rig",
        "source": "ELEXON",
        "windfarm_name": "Fallago Rig",
    },
    {
        "code": "FAARW-1",
        "name": "Farr 1",
        "source": "ELEXON",
        "windfarm_name": "Farr",
    },
    {
        "code": "T_FARR-1",
        "name": "Farr 1",
        "source": "ELEXON",
        "windfarm_name": "Farr",
    },
    {
        "code": "FAARW-2",
        "name": "Farr 2",
        "source": "ELEXON",
        "windfarm_name": "Farr",
    },
    {
        "code": "T_FARR-2",
        "name": "Farr 2",
        "source": "ELEXON",
        "windfarm_name": "Farr",
    },
    {
        "code": "T_FSDLW-1",
        "name": "Freasdail",
        "source": "ELEXON",
        "windfarm_name": "Freasdail",
    },
    {
        "code": "T_GLWSW-1",
        "name": "Galawhistle",
        "source": "ELEXON",
        "windfarm_name": "Galawhistle",
    },
    {
        "code": "T_GNAPW-1",
        "name": "Glen App",
        "source": "ELEXON",
        "windfarm_name": "Glen App",
    },
    {
        "code": "T_GLNKW-1",
        "name": "Glen Kyllachy",
        "source": "ELEXON",
        "windfarm_name": "Glen Kyllachy",
    },
    {
        "code": "GFLDW-1",
        "name": "Goole Fields 1",
        "source": "ELEXON",
        "windfarm_name": "Goole Fields",
    },
    {
        "code": "E_GFLDW-1",
        "name": "Goole Fields 1",
        "source": "ELEXON",
        "windfarm_name": "Goole Fields",
    },
    {
        "code": "T_GORDW-1",
        "name": "Gordonbush",
        "source": "ELEXON",
        "windfarm_name": "Gordonbush",
    },
    {
        "code": "T_GORDW-2",
        "name": "Gordonbush Extension",
        "source": "ELEXON",
        "windfarm_name": "Gordonbush",
    },
    {
        "code": "GDSTW-1",
        "name": "Gordonstown Hill",
        "source": "ELEXON",
        "windfarm_name": "Gordonstown Hill",
    },
    {
        "code": "E_GDSTW-1",
        "name": "Gordonstown Hill",
        "source": "ELEXON",
        "windfarm_name": "Gordonstown Hill",
    },
    {
        "code": "GRGRW-1",
        "name": "Greengairs East",
        "source": "ELEXON",
        "windfarm_name": "Greengairs East",
    },
    {
        "code": "E_GRGRW-1",
        "name": "Greengairs East",
        "source": "ELEXON",
        "windfarm_name": "Greengairs East",
    },
    {
        "code": "T_GRIFW-1",
        "name": "Griffin 1",
        "source": "ELEXON",
        "windfarm_name": "Griffin",
    },
    {
        "code": "T_GRIFW-2",
        "name": "Griffin 2",
        "source": "ELEXON",
        "windfarm_name": "Griffin",
    },
    {
        "code": "T_HADHW-1",
        "name": "Hadyard Hill",
        "source": "ELEXON",
        "windfarm_name": "Hadyard Hill",
    },
    {
        "code": "T_HALSW-1",
        "name": "Halsary",
        "source": "ELEXON",
        "windfarm_name": "Halsary",
    },
    {
        "code": "HBHDW-1",
        "name": "Harburnhead",
        "source": "ELEXON",
        "windfarm_name": "Harburnhead",
    },
    {
        "code": "E_HBHDW-1",
        "name": "Harburnhead",
        "source": "ELEXON",
        "windfarm_name": "Harburnhead",
    },
    {
        "code": "HRHLW-1",
        "name": "Hare Hill Extension",
        "source": "ELEXON",
        "windfarm_name": "Hare Hill Extension",
    },
    {
        "code": "E_HRHLW-1",
        "name": "Hare Hill Extension",
        "source": "ELEXON",
        "windfarm_name": "Hare Hill Extension",
    },
    {
        "code": "T_HRSTW-1",
        "name": "Harestanes",
        "source": "ELEXON",
        "windfarm_name": "Harestanes",
    },
    {
        "code": "HLGLW-1",
        "name": "Hill Of Glaschyle",
        "source": "ELEXON",
        "windfarm_name": "Hill Of Glaschyle",
    },
    {
        "code": "E_HLGLW-1",
        "name": "Hill Of Glaschyle",
        "source": "ELEXON",
        "windfarm_name": "Hill Of Glaschyle",
    },
    {
        "code": "HLTWW-1",
        "name": "Hill Of Towie",
        "source": "ELEXON",
        "windfarm_name": "Hill Of Towie",
    },
    {
        "code": "E_HLTWW-1",
        "name": "Hill Of Towie",
        "source": "ELEXON",
        "windfarm_name": "Hill Of Towie",
    },
    {
        "code": "T_KTHLW-1",
        "name": "Keith Hill",
        "source": "ELEXON",
        "windfarm_name": "Keith Hill",
    },
    {
        "code": "T_KENNW-1",
        "name": "Kennoxhead 1",
        "source": "ELEXON",
        "windfarm_name": "Kennoxhead 1",
    },
    {
        "code": "T_KILBW-1",
        "name": "Kilbraur 1",
        "source": "ELEXON",
        "windfarm_name": "Kilbraur",
    },
    {
        "code": "T_KILBW-1",
        "name": "Kilbraur 2",
        "source": "ELEXON",
        "windfarm_name": "Kilbraur",
    },
    {
        "code": "T_KLGLW-1",
        "name": "Kilgallioch",
        "source": "ELEXON",
        "windfarm_name": "Kilgallioch",
    },
    {
        "code": "KHLLW-1",
        "name": "Kirk Hill",
        "source": "ELEXON",
        "windfarm_name": "Kirk Hill",
    },
    {
        "code": "E_KHLLW-1",
        "name": "Kirk Hill",
        "source": "ELEXON",
        "windfarm_name": "Kirk Hill",
    },
    {
        "code": "T_KYPEW-1",
        "name": "Kype Muir Extension",
        "source": "ELEXON",
        "windfarm_name": "Kype Muir Extension",
    },
    {
        "code": "T_KPMRW-1",
        "name": "Kype Muir",
        "source": "ELEXON",
        "windfarm_name": "Kype Muir",
    },
    {
        "code": "T_LIMKW-1",
        "name": "Limekiln",
        "source": "ELEXON",
        "windfarm_name": "Limekiln",
    },
    {
        "code": "T_LCLTW-1",
        "name": "Lochluichart 1+2",
        "source": "ELEXON",
        "windfarm_name": "Lochluichart 1+2",
    },
    {
        "code": "T_MKHLW-1",
        "name": "Mark Hill",
        "source": "ELEXON",
        "windfarm_name": "Mark Hill",
    },
    {
        "code": "MDHLW-1",
        "name": "Mid Hill 1",
        "source": "ELEXON",
        "windfarm_name": "Mid Hill",
    },
    {
        "code": "2__PSTAT002",
        "name": "Mid Hill 1",
        "source": "ELEXON",
        "windfarm_name": "Mid Hill",
    },
    {
        "code": "MDHLW-1",
        "name": "Mid Hill 2",
        "source": "ELEXON",
        "windfarm_name": "Mid Hill",
    },
    {
        "code": "2__PSTAT002",
        "name": "Mid Hill 2",
        "source": "ELEXON",
        "windfarm_name": "Mid Hill",
    },
    {
        "code": "T_MIDMW-1",
        "name": "Middle Muir",
        "source": "ELEXON",
        "windfarm_name": "Middle Muir",
    },
    {
        "code": "T_MILWW-1",
        "name": "Millennium 1+2",
        "source": "ELEXON",
        "windfarm_name": "Millennium",
    },
    {
        "code": "T_MILWW-1",
        "name": "Millennium 3",
        "source": "ELEXON",
        "windfarm_name": "Millennium",
    },
    {
        "code": "T_MYGPW-1",
        "name": "Minnygap",
        "source": "ELEXON",
        "windfarm_name": "Minnygap",
    },
    {
        "code": "MINSW-1",
        "name": "Minsca",
        "source": "ELEXON",
        "windfarm_name": "Minsca",
    },
    {
        "code": "E_MINSW-1",
        "name": "Minsca",
        "source": "ELEXON",
        "windfarm_name": "Minsca",
    },
    {
        "code": "MOYEW-1",
        "name": "Moy",
        "source": "ELEXON",
        "windfarm_name": "Moy",
    },
    {
        "code": "E_MOYEW-1",
        "name": "Moy",
        "source": "ELEXON",
        "windfarm_name": "Moy",
    },
    {
        "code": "PAUHW-1",
        "name": "Paul's Hill",
        "source": "ELEXON",
        "windfarm_name": "Paul's Hill",
    },
    {
        "code": "2__PENEC002",
        "name": "Paul's Hill",
        "source": "ELEXON",
        "windfarm_name": "Paul's Hill",
    },
    {
        "code": "T_PNYCW-1",
        "name": "Pen Y Cymoedd",
        "source": "ELEXON",
        "windfarm_name": "Pen Y Cymoedd",
    },
    {
        "code": "PIBUW-1",
        "name": "Pines Burn",
        "source": "ELEXON",
        "windfarm_name": "Pines Burn",
    },
    {
        "code": "E_PIBUW-1",
        "name": "Pines Burn",
        "source": "ELEXON",
        "windfarm_name": "Pines Burn",
    },
    {
        "code": "T_PGBIW-1",
        "name": "Pogbie",
        "source": "ELEXON",
        "windfarm_name": "Pogbie",
    },
    {
        "code": "RSHLW-1",
        "name": "Rosehall Hill Forest",
        "source": "ELEXON",
        "windfarm_name": "Rosehall Hill Forest",
    },
    {
        "code": "2__PEDGE004",
        "name": "Rosehall Hill Forest",
        "source": "ELEXON",
        "windfarm_name": "Rosehall Hill Forest",
    },
    {
        "code": "ROTHW-1",
        "name": "Rothes I",
        "source": "ELEXON",
        "windfarm_name": "Rothes",
    },
    {
        "code": "2__PENEC001",
        "name": "Rothes I",
        "source": "ELEXON",
        "windfarm_name": "Rothes",
    },
    {
        "code": "CAIRW-2",
        "name": "Rothes II",
        "source": "ELEXON",
        "windfarm_name": "Rothes",
    },
    {
        "code": "2__PSTAT001",
        "name": "Rothes II",
        "source": "ELEXON",
        "windfarm_name": "Rothes",
    },
    {
        "code": "T_SAKNW-1",
        "name": "Sandy Knowe",
        "source": "ELEXON",
        "windfarm_name": "Sandy Knowe",
    },
    {
        "code": "T_SANQW-1",
        "name": "Sanquhar Community",
        "source": "ELEXON",
        "windfarm_name": "Sanquhar Community",
    },
    {
        "code": "SWBKW-1",
        "name": "Solwaybank",
        "source": "ELEXON",
        "windfarm_name": "Solwaybank",
    },
    {
        "code": "E_SWBKW-1",
        "name": "Solwaybank",
        "source": "ELEXON",
        "windfarm_name": "Solwaybank",
    },
    {
        "code": "T_SOKYW-1",
        "name": "South Kyle",
        "source": "ELEXON",
        "windfarm_name": "South Kyle",
    },
    {
        "code": "T_STRNW-1",
        "name": "Strathy North",
        "source": "ELEXON",
        "windfarm_name": "Strathy North",
    },
    {
        "code": "T_STLGW-1",
        "name": "Stronelairg 1",
        "source": "ELEXON",
        "windfarm_name": "Stronelairg",
    },
    {
        "code": "T_STLGW-2",
        "name": "Stronelairg 2",
        "source": "ELEXON",
        "windfarm_name": "Stronelairg",
    },
    {
        "code": "T_STLGW-3",
        "name": "Stronelairg 3",
        "source": "ELEXON",
        "windfarm_name": "Stronelairg",
    },
    {
        "code": "T_TDBNW-1",
        "name": "Toddleburn",
        "source": "ELEXON",
        "windfarm_name": "Toddleburn",
    },
    {
        "code": "TMNCW-1",
        "name": "Tom Nan Clach",
        "source": "ELEXON",
        "windfarm_name": "Tom Nan Clach",
    },
    {
        "code": "C__PSTAT011",
        "name": "Tom Nan Clach",
        "source": "ELEXON",
        "windfarm_name": "Tom Nan Clach",
    },
    {
        "code": "T_TRLGW-1",
        "name": "Tralorg",
        "source": "ELEXON",
        "windfarm_name": "Tralorg",
    },
    {
        "code": "TULWW-1",
        "name": "Tullo",
        "source": "ELEXON",
        "windfarm_name": "Tullo & Twinshiels",
    },
    {
        "code": "E_TULWW-1",
        "name": "Tullo",
        "source": "ELEXON",
        "windfarm_name": "Tullo & Twinshiels",
    },
    {
        "code": "TULWW-2",
        "name": "Twinshiels",
        "source": "ELEXON",
        "windfarm_name": "Tullo & Twinshiels",
    },
    {
        "code": "E_TULWW-2",
        "name": "Twinshiels",
        "source": "ELEXON",
        "windfarm_name": "Tullo & Twinshiels",
    },
    {
        "code": "TLYMW-1",
        "name": "Tullymurdoch",
        "source": "ELEXON",
        "windfarm_name": "Tullymurdoch",
    },
    {
        "code": "E_TLYMW-1",
        "name": "Tullymurdoch",
        "source": "ELEXON",
        "windfarm_name": "Tullymurdoch",
    },
    {
        "code": "T_TWSHW-1",
        "name": "Twenty Shilling Hill",
        "source": "ELEXON",
        "windfarm_name": "Twenty Shilling Hill",
    },
    {
        "code": "T_VKNGW-1",
        "name": "Viking 1",
        "source": "ELEXON",
        "windfarm_name": "Viking",
    },
    {
        "code": "T_VKNGW-2",
        "name": "Viking 2",
        "source": "ELEXON",
        "windfarm_name": "Viking",
    },
    {
        "code": "T_VKNGW-3",
        "name": "Viking 3",
        "source": "ELEXON",
        "windfarm_name": "Viking",
    },
    {
        "code": "T_VKNGW-4",
        "name": "Viking 4",
        "source": "ELEXON",
        "windfarm_name": "Viking",
    },
    {
        "code": "T_WHILW-1",
        "name": "Whitelee",
        "source": "ELEXON",
        "windfarm_name": "Whitelee",
    },
    {
        "code": "T_WHILW-2",
        "name": "Whitelee Extension",
        "source": "ELEXON",
        "windfarm_name": "Whitelee",
    },
    {
        "code": "T_WHIHW-1",
        "name": "Whiteside Hill",
        "source": "ELEXON",
        "windfarm_name": "Whiteside Hill",
    },
    {
        "code": "T_WDRGW-1",
        "name": "Windy Rig",
        "source": "ELEXON",
        "windfarm_name": "Windy Rig",
    },
    {
        "code": "GLCHW-1",
        "name": "Glenchamber",
        "source": "ELEXON",
        "windfarm_name": "Glenchamber",
    },
    {
        "code": "E_GLCHW-1",
        "name": "Glenchamber",
        "source": "ELEXON",
        "windfarm_name": "Glenchamber",
    },
    {
        "code": "GLOFW-1",
        "name": "Glens Of Foudland",
        "source": "ELEXON",
        "windfarm_name": "Glens Of Foudland",
    },
    {
        "code": "E_GLOFW-1",
        "name": "Glens Of Foudland",
        "source": "ELEXON",
        "windfarm_name": "Glens Of Foudland",
    },
]


def lookup_windfarm_id(db: Session, windfarm_name: str) -> Optional[int]:
    """Look up windfarm ID by name"""
    if not windfarm_name or windfarm_name.strip() == "":
        return None

    windfarm = db.query(Windfarm).filter(Windfarm.name == windfarm_name.strip()).first()
    return windfarm.id if windfarm else None


def determine_fuel_type(name: str, windfarm_name: str) -> str:
    """Determine fuel type based on generation unit name and windfarm name"""
    # All these generation units are from offshore wind farms
    return "wind"


def determine_technology_type(name: str, windfarm_name: str) -> str:
    """Determine technology type based on generation unit name and windfarm name"""
    # All these generation units are from offshore wind farms
    return "wind_offshore"


def seed_generation_units(db: Session):
    """Seed generation_units table with initial data"""
    print(f"  Checking for existing generation units...")

    # Get existing generation unit codes
    existing_codes = {gu.code for gu in db.query(GenerationUnit.code).all()}

    success_count = 0
    failure_count = 0
    failures = []
    successful_entries = []

    for unit_data in GENERATION_UNITS_DATA:
        unit_code = unit_data["code"]

        # Skip if already exists
        if unit_code in existing_codes:
            print(f"    Skipping existing generation unit: {unit_code}")
            continue

        try:
            # Look up windfarm
            windfarm_id = lookup_windfarm_id(db, unit_data["windfarm_name"])
            if not windfarm_id:
                # Don't fail if windfarm not found, just log it
                print(
                    f"    Warning: Windfarm '{unit_data['windfarm_name']}' not found for unit {unit_code}"
                )

            # Get source, fuel type, and technology type
            source = unit_data["source"]
            fuel_type = determine_fuel_type(unit_data["name"], unit_data["windfarm_name"])
            technology_type = determine_technology_type(
                unit_data["name"], unit_data["windfarm_name"]
            )

            # Create generation unit
            generation_unit = GenerationUnit(
                code=unit_code,
                name=unit_data["name"],
                source=source,
                fuel_type=fuel_type,
                technology_type=technology_type,
                capacity_mw=None,  # Will be filled from other sources later
                windfarm_id=windfarm_id,
                notes=f"Linked to windfarm: {unit_data['windfarm_name']}"
                if windfarm_id
                else f"Windfarm '{unit_data['windfarm_name']}' not found in database",
            )

            db.add(generation_unit)
            db.flush()  # Get the generation unit ID

            success_count += 1
            successful_entries.append(
                {
                    "code": unit_code,
                    "name": unit_data["name"],
                    "source": source,
                    "fuel_type": fuel_type,
                    "technology_type": technology_type,
                    "windfarm_name": unit_data["windfarm_name"],
                    "windfarm_id": windfarm_id,
                    "windfarm_found": windfarm_id is not None,
                }
            )
            print(f"    ✅ Added generation unit: {unit_code} - {unit_data['name']}")

        except Exception as e:
            failure_count += 1
            error_msg = str(e)
            failures.append(
                {
                    "code": unit_code,
                    "name": unit_data["name"],
                    "windfarm_name": unit_data["windfarm_name"],
                    "error": error_msg,
                    "data": unit_data,
                }
            )
            print(f"    ❌ Failed to add generation unit: {unit_code} - {error_msg}")
            db.rollback()  # Rollback this transaction to allow subsequent units to be processed

    # Commit successful changes
    if success_count > 0:
        db.commit()
        print(f"  Successfully added {success_count} generation units")

    # Generate comprehensive report
    report_data = {
        "summary": {
            "total_attempted": len(GENERATION_UNITS_DATA),
            "successful": success_count,
            "failed": failure_count,
            "timestamp": datetime.now().isoformat(),
            "windfarms_not_found": sum(
                1 for entry in successful_entries if not entry["windfarm_found"]
            ),
        },
        "successful_entries": successful_entries,
        "failures": failures,
        "statistics": {
            "by_source": {},
            "by_fuel_type": {},
            "by_technology_type": {},
            "by_windfarm_found": {
                "found": sum(1 for entry in successful_entries if entry["windfarm_found"]),
                "not_found": sum(1 for entry in successful_entries if not entry["windfarm_found"]),
            },
        },
    }

    # Calculate statistics
    for entry in successful_entries:
        # Source statistics
        source = entry["source"]
        if source not in report_data["statistics"]["by_source"]:
            report_data["statistics"]["by_source"][source] = 0
        report_data["statistics"]["by_source"][source] += 1

        # Fuel type statistics
        fuel_type = entry["fuel_type"]
        if fuel_type not in report_data["statistics"]["by_fuel_type"]:
            report_data["statistics"]["by_fuel_type"][fuel_type] = 0
        report_data["statistics"]["by_fuel_type"][fuel_type] += 1

        # Technology type statistics
        tech_type = entry["technology_type"]
        if tech_type not in report_data["statistics"]["by_technology_type"]:
            report_data["statistics"]["by_technology_type"][tech_type] = 0
        report_data["statistics"]["by_technology_type"][tech_type] += 1

    # Generate report file
    report_path = Path(__file__).parent / "generation_units_seed_report.json"
    with open(report_path, "w") as f:
        json.dump(report_data, f, indent=2, default=str)

    print(f"  Generated comprehensive report: {report_path}")

    if failures:
        print(f"  Failed to add {failure_count} generation units - see report for details")

    # Print summary statistics
    windfarms_not_found = sum(1 for entry in successful_entries if not entry["windfarm_found"])
    if windfarms_not_found > 0:
        print(
            f"  Note: {windfarms_not_found} generation units could not be linked to windfarms (windfarms not found in database)"
        )

    print(
        f"  Generation unit seeding completed: {success_count} successful, {failure_count} failed"
    )
