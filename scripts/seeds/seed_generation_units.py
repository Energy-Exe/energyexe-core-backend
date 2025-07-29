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
        "windfarm_name": "Fécamp"
    },
    {
        "code": "48W00000ABRBO-19",
        "name": "ABRB0-1",
        "source": "ENTSOE",
        "windfarm_name": "Aberdeen"
    },
    {
        "code": "17W100P100P0842Y",
        "name": "ADP A1 DE LA FERME EOLIENNE DE BAIE-DE-ST-BRIEUC",
        "source": "ENTSOE",
        "windfarm_name": "Saint Brieuc"
    },
    {
        "code": "17W100P100P3382R",
        "name": "ADP A2 DE LA FERME EOLIENNE DE BAIE-DE-ST-BRIEUC",
        "source": "ENTSOE",
        "windfarm_name": "Saint Brieuc"
    },
    {
        "code": "45W000000000046I",
        "name": "Anholt Generation",
        "source": "ENTSOE",
        "windfarm_name": "Anholt"
    },
    {
        "code": "48W00000BOWLW-1K",
        "name": "Barrow Offshore Wind Farm BOWLW-1",
        "source": "ENTSOE",
        "windfarm_name": "Barrow"
    },
    {
        "code": "48W00000BEATO-1T",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice"
    },
    {
        "code": "48W00000BEATO-3P",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice"
    },
    {
        "code": "48W00000BEATO-2R",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice"
    },
    {
        "code": "48W00000BEATO-4N",
        "name": "Beatrice Offshore Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Beatrice"
    },
    {
        "code": "22WBELWIN1500271",
        "name": "Belwind Phase 1",
        "source": "ENTSOE",
        "windfarm_name": "Belwind 1"
    },
    {
        "code": "48W00000BRBEO-17",
        "name": "Burbo Extension BRBEO-1",
        "source": "ENTSOE",
        "windfarm_name": "Burbo Bank Extension"
    },
    {
        "code": "48W00000BURBW-1L",
        "name": "Burbo Wind Farm BURBW-1",
        "source": "ENTSOE",
        "windfarm_name": "Burbo Bank"
    },
    {
        "code": "45W000000000126K",
        "name": "Danish Kriegers Flak Generation Unit",
        "source": "ENTSOE",
        "windfarm_name": "Kriegers Flak"
    },
    {
        "code": "48W00000DBBWO-1D",
        "name": "Dogger Bank B Unit 1",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBBWO-2B",
        "name": "Dogger Bank B Unit 2",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBBWO-39",
        "name": "Dogger Bank B Unit 3",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBBWO-47",
        "name": "Dogger Bank B Unit 4",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBBWO-55",
        "name": "Dogger Bank B Unit 5",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBAWO-1J",
        "name": "Dogger Bank Unit 1",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBAWO-2H",
        "name": "Dogger Bank Unit 2",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBAWO-3F",
        "name": "Dogger Bank Unit 3",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBAWO-5B",
        "name": "Dogger Bank Unit 5",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W00000DBAWO-4D",
        "name": "Doggerbank Unit 4",
        "source": "ENTSOE",
        "windfarm_name": "Dogger Bank A&B"
    },
    {
        "code": "48W000000EAAO-1R",
        "name": "East Anglia One",
        "source": "ENTSOE",
        "windfarm_name": "East Anglia One"
    },
    {
        "code": "48W000000EAAO-2P",
        "name": "East Anglia One",
        "source": "ENTSOE",
        "windfarm_name": "East Anglia One"
    },
    {
        "code": "17W0000014455708",
        "name": "Eoliennes Offshore des Hautes Falaises 2",
        "source": "ENTSOE",
        "windfarm_name": "Fécamp"
    },
    {
        "code": "48W00000GAOFO-13",
        "name": "Galloper Offshore Wind Farm GAOFO-1",
        "source": "ENTSOE",
        "windfarm_name": "Galloper"
    },
    {
        "code": "48W00000GAOFO-21",
        "name": "Galloper Offshore Wind Farm GAOFO-2",
        "source": "ENTSOE",
        "windfarm_name": "Galloper"
    },
    {
        "code": "48W10000GAOFO-3N",
        "name": "Galloper Offshore Wind Farm GAOFO-3",
        "source": "ENTSOE",
        "windfarm_name": "Galloper"
    },
    {
        "code": "48W00000GAOFO-4Y",
        "name": "Galloper Offshore Wind Farm GAOFO-4",
        "source": "ENTSOE",
        "windfarm_name": "Galloper"
    },
    {
        "code": "48W00000GRGBW-1V",
        "name": "Greater Gabbard GRGBW-1",
        "source": "ENTSOE",
        "windfarm_name": "Greater Gabbard"
    },
    {
        "code": "48W00000GRGBW-2T",
        "name": "Greater Gabbard GRGBW-2",
        "source": "ENTSOE",
        "windfarm_name": "Greater Gabbard"
    },
    {
        "code": "48W00000GRGBW-3R",
        "name": "Greater Gabbard GRGBW-3",
        "source": "ENTSOE",
        "windfarm_name": "Greater Gabbard"
    },
    {
        "code": "48W00000GNFSW-1H",
        "name": "Gunfleet Sands GNFSW-1",
        "source": "ENTSOE",
        "windfarm_name": "Gunfleet Sands 1&2"
    },
    {
        "code": "48W00000GNFSW-2F",
        "name": "Gunfleet Sands GNFSW-2",
        "source": "ENTSOE",
        "windfarm_name": "Gunfleet Sands 1&2"
    },
    {
        "code": "48W0000GYMRO-15O",
        "name": "Gwynt Y Mor GYMRO-15",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor"
    },
    {
        "code": "48W0000GYMRO-17K",
        "name": "Gwynt Y Mor GYMRO-17",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor"
    },
    {
        "code": "48W0000GYMRO-26J",
        "name": "Gwynt Y Mor GYMRO-26",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor"
    },
    {
        "code": "48W0000GYMRO-28F",
        "name": "Gwynt Y Mor GYMRO-28",
        "source": "ENTSOE",
        "windfarm_name": "Gwynt Y Mor"
    },
    {
        "code": "45W000000000047G",
        "name": "Horns Rev A",
        "source": "ENTSOE",
        "windfarm_name": "Horns Rev"
    },
    {
        "code": "45W000000000048E",
        "name": "Horns Rev B",
        "source": "ENTSOE",
        "windfarm_name": "Horns Rev"
    },
    {
        "code": "45W000000000116N",
        "name": "Horns Rev C generation unit",
        "source": "ENTSOE",
        "windfarm_name": "Horns Rev"
    },
    {
        "code": "48W00000HOWAO-1M",
        "name": "Hornsea 1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 1"
    },
    {
        "code": "48W00000HOWAO-2K",
        "name": "Hornsea 1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 1"
    },
    {
        "code": "48W00000HOWAO-3I",
        "name": "Hornsea 1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 1"
    },
    {
        "code": "48W00000HOWBO-1H",
        "name": "HOWBO-1",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 2"
    },
    {
        "code": "48W00000HOWBO-2F",
        "name": "HOWBO-2",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 2"
    },
    {
        "code": "48W00000HOWBO-3D",
        "name": "HOWBO-3",
        "source": "ENTSOE",
        "windfarm_name": "Hornsea 2"
    },
    {
        "code": "48W00000HMGTO-10",
        "name": "Humber Gateway Offshore Wind Farm HMGTO-1",
        "source": "ENTSOE",
        "windfarm_name": "Humber Gateway"
    },
    {
        "code": "48W00000HMGTO-2Z",
        "name": "Humber Gateway Offshore Wind Farm HMGTO-2",
        "source": "ENTSOE",
        "windfarm_name": "Humber Gateway"
    },
    {
        "code": "48W00000HYWDW-1G",
        "name": "Hywind Wind Farm",
        "source": "ENTSOE",
        "windfarm_name": "Hywind Scotland"
    },
    {
        "code": "48W00000LNCSO-1R",
        "name": "Lincs Wind Farm LNCSO-1",
        "source": "ENTSOE",
        "windfarm_name": "Lincs"
    },
    {
        "code": "48W00000LNCSO-2P",
        "name": "Lincs Wind Farm LNCSO-2",
        "source": "ENTSOE",
        "windfarm_name": "Lincs"
    },
    {
        "code": "48W00000LARYO-1Z",
        "name": "London Array Wind Farm LARYO-1",
        "source": "ENTSOE",
        "windfarm_name": "London Array"
    },
    {
        "code": "48W00000LARYO-2X",
        "name": "London Array Wind Farm LARYO-2",
        "source": "ENTSOE",
        "windfarm_name": "London Array"
    },
    {
        "code": "48W00000LARYO-3V",
        "name": "London Array Wind Farm LARYO-3",
        "source": "ENTSOE",
        "windfarm_name": "London Array"
    },
    {
        "code": "48W00000LARYO-4T",
        "name": "London Array Wind Farm LARYO-4",
        "source": "ENTSOE",
        "windfarm_name": "London Array"
    },
    {
        "code": "22W20200608B---3",
        "name": "Mermaid",
        "source": "ENTSOE",
        "windfarm_name": "Seamade"
    },
    {
        "code": "48W000000MOWEO11",
        "name": "MOWEO-1",
        "source": "ENTSOE",
        "windfarm_name": "Moray East"
    },
    {
        "code": "48W00000MOWEO-2Y",
        "name": "MOWEO-2",
        "source": "ENTSOE",
        "windfarm_name": "Moray East"
    },
    {
        "code": "48W00000MOWEO-3W",
        "name": "MOWEO-3",
        "source": "ENTSOE",
        "windfarm_name": "Moray East"
    },
    {
        "code": "48WW00000MOWWO-Z",
        "name": "MOWWO-1",
        "source": "ENTSOE",
        "windfarm_name": "Moray West"
    },
    {
        "code": "48W00000MOWWO-2I",
        "name": "MOWWO-2",
        "source": "ENTSOE",
        "windfarm_name": "Moray West"
    },
    {
        "code": "48W00000MOWWO-3G",
        "name": "MOWWO-3",
        "source": "ENTSOE",
        "windfarm_name": "Moray West"
    },
    {
        "code": "48W00000MOWWO-4E",
        "name": "MOWWO-4",
        "source": "ENTSOE",
        "windfarm_name": "Moray West"
    },
    {
        "code": "48W00000NNGAO-13",
        "name": "Neart Na Gaoithe Offshore Wind NNGAO-1",
        "source": "ENTSOE",
        "windfarm_name": "Neart Na Gaoithe (NnG)"
    },
    {
        "code": "48W00000NNGAO-21",
        "name": "Neart Na Gaoithe Offshore Wind NNGAO-2",
        "source": "ENTSOE",
        "windfarm_name": "Neart Na Gaoithe (NnG)"
    },
    {
        "code": "22W20161115----Z",
        "name": "NOBELWIND Park",
        "source": "ENTSOE",
        "windfarm_name": "Nobelwind"
    },
    {
        "code": "22W201902132---O",
        "name": "Norther Offshore WP GU",
        "source": "ENTSOE",
        "windfarm_name": "Norther"
    },
    {
        "code": "22W201909151---M",
        "name": "Northwester 2 Zeebrugge PU",
        "source": "ENTSOE",
        "windfarm_name": "Northwester 2"
    },
    {
        "code": "22WNORTHW150187B",
        "name": "Northwind",
        "source": "ENTSOE",
        "windfarm_name": "Northwind"
    },
    {
        "code": "48W00000OMNDO-1J",
        "name": "Ormonde Eng Ltd",
        "source": "ENTSOE",
        "windfarm_name": "Ormonde"
    },
    {
        "code": "17W0000014455651",
        "name": "Parc du Banc de Guérande 1",
        "source": "ENTSOE",
        "windfarm_name": "Saint-Nazaire"
    },
    {
        "code": "17W000001445567Y",
        "name": "Parc du Banc de Guérande 2",
        "source": "ENTSOE",
        "windfarm_name": "Saint-Nazaire"
    },
    {
        "code": "48W00000RMPNO-17",
        "name": "Rampion Offshore Wind Farm 1",
        "source": "ENTSOE",
        "windfarm_name": "Rampion"
    },
    {
        "code": "48W00000RMPNO-25",
        "name": "Rampion Offshore Windfarm",
        "source": "ENTSOE",
        "windfarm_name": "Rampion"
    },
    {
        "code": "48W00000RCBKO-1S",
        "name": "RCBKO-1",
        "source": "ENTSOE",
        "windfarm_name": "Race Bank"
    },
    {
        "code": "48W00000RCBKO-2Q",
        "name": "RCBKO-2",
        "source": "ENTSOE",
        "windfarm_name": "Race Bank"
    },
    {
        "code": "22W20180615----H",
        "name": "Rentel Offshore WP PU",
        "source": "ENTSOE",
        "windfarm_name": "Rentel"
    },
    {
        "code": "48W000000RREW-14",
        "name": "Robin Rigg East RREW-1",
        "source": "ENTSOE",
        "windfarm_name": "Robin Rigg"
    },
    {
        "code": "48W000000RRWW-1P",
        "name": "Robin Rigg West RRWW-1",
        "source": "ENTSOE",
        "windfarm_name": "Robin Rigg"
    },
    {
        "code": "45W000000000044M",
        "name": "Rødsand 1_GU",
        "source": "ENTSOE",
        "windfarm_name": "Nysted"
    },
    {
        "code": "45W000000000045K",
        "name": "Rødsand 2_GU",
        "source": "ENTSOE",
        "windfarm_name": "Rødsand II"
    },
    {
        "code": "48W00000SGRWO-1L",
        "name": "Seagreen Windfarm SGRWO-1",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen"
    },
    {
        "code": "48W00000SGRWO-2J",
        "name": "Seagreen Windfarm SGRWO-2",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen"
    },
    {
        "code": "48W00000SGRWO-3H",
        "name": "Seagreen Windfarm SGRWO-3",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen"
    },
    {
        "code": "48W00000SGRWO-4F",
        "name": "Seagreen Windfarm SGRWO-4",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen"
    },
    {
        "code": "48W00000SGRWO-5D",
        "name": "Seagreen Windfarm SGRWO-5",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen"
    },
    {
        "code": "48W00000SGRWO-6B",
        "name": "Seagreen Windfarm SGRWO-6",
        "source": "ENTSOE",
        "windfarm_name": "Seagreen"
    },
    {
        "code": "22W20200608D---U",
        "name": "Seastar",
        "source": "ENTSOE",
        "windfarm_name": "Seamade"
    },
    {
        "code": "48W00000SHRSO-1Y",
        "name": "Sheringham Shoal Wind Farm SHRSO-1",
        "source": "ENTSOE",
        "windfarm_name": "Sheringham Shoal"
    },
    {
        "code": "48W00000SHRSO-2W",
        "name": "Sheringham Shoal Wind Farm SHRSO-2",
        "source": "ENTSOE",
        "windfarm_name": "Sheringham Shoal"
    },
    {
        "code": "48W00000THNTO-18",
        "name": "Thanet Offshore Wind THNTO-1",
        "source": "ENTSOE",
        "windfarm_name": "Thanet"
    },
    {
        "code": "48W00000THNTO-26",
        "name": "Thanet Offshore Wind THNTO-2",
        "source": "ENTSOE",
        "windfarm_name": "Thanet"
    },
    {
        "code": "22WTHORNT150237E",
        "name": "Thorntonbank - C-Power - Area NE",
        "source": "ENTSOE",
        "windfarm_name": "Thorntonbank II & III"
    },
    {
        "code": "22WTHORNT150238C",
        "name": "Thorntonbank - C-Power - Area SW",
        "source": "ENTSOE",
        "windfarm_name": "Thorntonbank II & III"
    },
    {
        "code": "45W000000000208I",
        "name": "Vesterhav Nord",
        "source": "ENTSOE",
        "windfarm_name": "Vesterhav Syd & Nord"
    },
    {
        "code": "45W000000000207K",
        "name": "Vesterhav Syd",
        "source": "ENTSOE",
        "windfarm_name": "Vesterhav Syd & Nord"
    },
    {
        "code": "48W00000WLNYO-23",
        "name": "Walney Ext 2",
        "source": "ENTSOE",
        "windfarm_name": "Walney 1&2"
    },
    {
        "code": "48W00000WLNYO-31",
        "name": "Walney Ext 3",
        "source": "ENTSOE",
        "windfarm_name": "Walney Extension"
    },
    {
        "code": "48W00000WLNYO-4-",
        "name": "Walney Ext 4",
        "source": "ENTSOE",
        "windfarm_name": "Walney Extension"
    },
    {
        "code": "48W00000WLNYW-1A",
        "name": "Walney Wind Farm WLNYW-1",
        "source": "ENTSOE",
        "windfarm_name": "Walney 1&2"
    },
    {
        "code": "48W00000WDNSO-1H",
        "name": "West of Duddon Sands WDNSO-1",
        "source": "ENTSOE",
        "windfarm_name": "West of Duddon Sands"
    },
    {
        "code": "48W00000WDNSO-2F",
        "name": "West of Duddon Sands WDNSO-2",
        "source": "ENTSOE",
        "windfarm_name": "West of Duddon Sands"
    },
    {
        "code": "48W00000WTMSO-1M",
        "name": "Westermost Rough W/F WTMSO-1",
        "source": "ENTSOE",
        "windfarm_name": "Westermost Rough"
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
                print(f"    Warning: Windfarm '{unit_data['windfarm_name']}' not found for unit {unit_code}")

            # Get source, fuel type, and technology type
            source = unit_data["source"]
            fuel_type = determine_fuel_type(unit_data["name"], unit_data["windfarm_name"])
            technology_type = determine_technology_type(unit_data["name"], unit_data["windfarm_name"])

            # Create generation unit
            generation_unit = GenerationUnit(
                code=unit_code,
                name=unit_data["name"],
                source=source,
                fuel_type=fuel_type,
                technology_type=technology_type,
                capacity_mw=None,  # Will be filled from other sources later
                windfarm_id=windfarm_id,
                notes=f"Linked to windfarm: {unit_data['windfarm_name']}" if windfarm_id else f"Windfarm '{unit_data['windfarm_name']}' not found in database",
            )

            db.add(generation_unit)
            db.flush()  # Get the generation unit ID

            success_count += 1
            successful_entries.append({
                "code": unit_code,
                "name": unit_data["name"],
                "source": source,
                "fuel_type": fuel_type,
                "technology_type": technology_type,
                "windfarm_name": unit_data["windfarm_name"],
                "windfarm_id": windfarm_id,
                "windfarm_found": windfarm_id is not None
            })
            print(f"    ✅ Added generation unit: {unit_code} - {unit_data['name']}")

        except Exception as e:
            failure_count += 1
            error_msg = str(e)
            failures.append({
                "code": unit_code,
                "name": unit_data["name"],
                "windfarm_name": unit_data["windfarm_name"],
                "error": error_msg,
                "data": unit_data
            })
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
            "windfarms_not_found": sum(1 for entry in successful_entries if not entry["windfarm_found"]),
        },
        "successful_entries": successful_entries,
        "failures": failures,
        "statistics": {
            "by_source": {},
            "by_fuel_type": {},
            "by_technology_type": {},
            "by_windfarm_found": {
                "found": sum(1 for entry in successful_entries if entry["windfarm_found"]),
                "not_found": sum(1 for entry in successful_entries if not entry["windfarm_found"])
            }
        }
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
        print(f"  Note: {windfarms_not_found} generation units could not be linked to windfarms (windfarms not found in database)")

    print(f"  Generation unit seeding completed: {success_count} successful, {failure_count} failed")