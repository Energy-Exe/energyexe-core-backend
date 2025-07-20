#!/usr/bin/env python3
"""
Seed script for control_areas table
"""

from sqlalchemy.orm import Session
from app.models.control_area import ControlArea
from app.models.country import Country


# Control area data with country mapping
CONTROL_AREAS_DATA = [
    {"code": "10Y1001A1001A016", "name": "NIE", "acronym": "NIE", "country_name": "United Kingdom"},
    {"code": "10Y1001A1001A39I", "name": "EE", "acronym": "EE", "country_name": "Estonia"},
    {"code": "10Y1001A1001A49F", "name": "RU", "acronym": "RU", "country_name": "Russia"},
    {"code": "10Y1001A1001A50U", "name": "RU-KGD", "acronym": "RU-KGD", "country_name": "Russia"},
    {"code": "10Y1001A1001A51S", "name": "BY", "acronym": "BY", "country_name": "Belarus"},
    {"code": "10Y1001A1001A796", "name": "DK", "acronym": "DK", "country_name": "Denmark"},
    {"code": "10Y1001A1001A869", "name": "UA-DobTPP", "acronym": "UA-DobTPP", "country_name": "Ukraine"},
    {"code": "10Y1001A1001A93C", "name": "MT", "acronym": "MT", "country_name": "Malta"},
    {"code": "10Y1001A1001A990", "name": "MD", "acronym": "MD", "country_name": "Moldova"},
    {"code": "10Y1001A1001B004", "name": "AM", "acronym": "AM", "country_name": "Armenia"},
    {"code": "10Y1001A1001B012", "name": "GE", "acronym": "GE", "country_name": "Georgia"},
    {"code": "10Y1001A1001B05V", "name": "AZ", "acronym": "AZ", "country_name": "Azerbaijan"},
    {"code": "10Y1001C--000182", "name": "UA-IPS", "acronym": "UA-IPS", "country_name": "Ukraine"},
    {"code": "10Y1001C--00100H", "name": "XK", "acronym": "XK", "country_name": "Kosovo"},
    {"code": "10YAL-KESH-----5", "name": "AL", "acronym": "AL", "country_name": "Albania"},
    {"code": "10YAT-APG------L", "name": "AT", "acronym": "AT", "country_name": "Austria"},
    {"code": "10YBA-JPCC-----D", "name": "BA", "acronym": "BA", "country_name": "Bosnia and Herzegovina"},
    {"code": "10YBE----------2", "name": "BE", "acronym": "BE", "country_name": "Belgium"},
    {"code": "10YCA-BULGARIA-R", "name": "BG", "acronym": "BG", "country_name": "Bulgaria"},
    {"code": "10YCH-SWISSGRIDZ", "name": "CH", "acronym": "CH", "country_name": "Switzerland"},
    {"code": "10YCS-CG-TSO---S", "name": "ME", "acronym": "ME", "country_name": "Montenegro"},
    {"code": "10YCS-SERBIATSOV", "name": "RS", "acronym": "RS", "country_name": "Serbia"},
    {"code": "10YCY-1001A0003J", "name": "CY", "acronym": "CY", "country_name": "Cyprus"},
    {"code": "10YCZ-CEPS-----N", "name": "CZ", "acronym": "CZ", "country_name": "Czech Republic"},
    {"code": "10YDE-ENBW-----N", "name": "DE(TransnetBW)", "acronym": "DE(TransnetBW)", "country_name": "Germany"},
    {"code": "10YDE-EON------1", "name": "DE(TenneT GER)", "acronym": "DE(TenneT GER)", "country_name": "Germany"},
    {"code": "10YDE-RWENET---I", "name": "DE(Amprion)", "acronym": "DE(Amprion)", "country_name": "Germany"},
    {"code": "10YDE-VE-------2", "name": "DE(50Hertz)", "acronym": "DE(50Hertz)", "country_name": "Germany"},
    {"code": "10YDOM-1001A082L", "name": "PL-CZ", "acronym": "PL-CZ", "country_name": "Poland"},  # Cross-border, assigned to Poland
    {"code": "10YES-REE------0", "name": "ES", "acronym": "ES", "country_name": "Spain"},
    {"code": "10YFI-1--------U", "name": "FI", "acronym": "FI", "country_name": "Finland"},
    {"code": "10YFR-RTE------C", "name": "FR", "acronym": "FR", "country_name": "France"},
    {"code": "10YGB----------A", "name": "National Grid", "acronym": "National Grid", "country_name": "United Kingdom"},
    {"code": "10YGR-HTSO-----Y", "name": "GR", "acronym": "GR", "country_name": "Greece"},
    {"code": "10YHR-HEP------M", "name": "HR", "acronym": "HR", "country_name": "Croatia"},
    {"code": "10YHU-MAVIR----U", "name": "HU", "acronym": "HU", "country_name": "Hungary"},
    {"code": "10YIE-1001A00010", "name": "IE", "acronym": "IE", "country_name": "Ireland"},
    {"code": "10YIT-GRTN-----B", "name": "IT", "acronym": "IT", "country_name": "Italy"},
    {"code": "10YLT-1001A0008Q", "name": "LT", "acronym": "LT", "country_name": "Lithuania"},
    {"code": "10YLU-CEGEDEL-NQ", "name": "LU", "acronym": "LU", "country_name": "Luxembourg"},
    {"code": "10YLV-1001A00074", "name": "LV", "acronym": "LV", "country_name": "Latvia"},
    {"code": "10YMK-MEPSO----8", "name": "MK", "acronym": "MK", "country_name": "North Macedonia"},
    {"code": "10YNL----------L", "name": "NL", "acronym": "NL", "country_name": "Netherlands"},
    {"code": "10YNO-0--------C", "name": "NO", "acronym": "NO", "country_name": "Norway"},
    {"code": "10YPL-AREA-----S", "name": "PL", "acronym": "PL", "country_name": "Poland"},
    {"code": "10YPT-REN------W", "name": "PT", "acronym": "PT", "country_name": "Portugal"},
    {"code": "10YRO-TEL------P", "name": "RO", "acronym": "RO", "country_name": "Romania"},
    {"code": "10YSE-1--------K", "name": "SE", "acronym": "SE", "country_name": "Sweden"},
    {"code": "10YSI-ELES-----O", "name": "SI", "acronym": "SI", "country_name": "Slovenia"},
    {"code": "10YSK-SEPS-----K", "name": "SK", "acronym": "SK", "country_name": "Slovakia"},
    {"code": "10YTR-TEIAS----W", "name": "TR", "acronym": "TR", "country_name": "Turkey"},
    {"code": "10YUA-WEPS-----0", "name": "UA-BEI", "acronym": "UA-BEI", "country_name": "Ukraine"},
]

# Country name to ISO3 code mapping
COUNTRY_NAME_TO_ISO3 = {
    "United Kingdom": "GBR",
    "Estonia": "EST",
    "Russia": "RUS",
    "Belarus": "BLR",
    "Denmark": "DNK",
    "Ukraine": "UKR",
    "Malta": "MLT",
    "Moldova": "MDA",
    "Armenia": "ARM",
    "Georgia": "GEO",
    "Azerbaijan": "AZE",
    "Kosovo": "XKX",
    "Albania": "ALB",
    "Austria": "AUT",
    "Bosnia and Herzegovina": "BIH",
    "Belgium": "BEL",
    "Bulgaria": "BGR",
    "Switzerland": "CHE",
    "Montenegro": "MNE",
    "Serbia": "SRB",
    "Cyprus": "CYP",
    "Czech Republic": "CZE",
    "Germany": "DEU",
    "Poland": "POL",
    "Spain": "ESP",
    "Finland": "FIN",
    "France": "FRA",
    "Greece": "GRC",
    "Croatia": "HRV",
    "Hungary": "HUN",
    "Ireland": "IRL",
    "Italy": "ITA",
    "Lithuania": "LTU",
    "Luxembourg": "LUX",
    "Latvia": "LVA",
    "North Macedonia": "MKD",
    "Netherlands": "NLD",
    "Norway": "NOR",
    "Portugal": "PRT",
    "Romania": "ROU",
    "Sweden": "SWE",
    "Slovenia": "SVN",
    "Slovakia": "SVK",
    "Turkey": "TUR",
}


def seed_control_areas(db: Session):
    """Seed control_areas table with initial data"""
    print(f"  Checking for existing control areas...")
    
    # Get existing control area codes
    existing_codes = {ca.code for ca in db.query(ControlArea.code).all()}
    
    # Get all countries for mapping
    countries = db.query(Country).all()
    country_map = {c.code: c for c in countries}
    
    # Filter out control areas that already exist
    control_areas_to_add = []
    skipped_count = 0
    warnings = []
    
    for ca_data in CONTROL_AREAS_DATA:
        if ca_data["code"] in existing_codes:
            skipped_count += 1
            continue
        
        # Map country name to ISO3 code
        country_name = ca_data["country_name"]
        country_iso3 = COUNTRY_NAME_TO_ISO3.get(country_name)
        
        country_id = None
        if country_iso3 and country_iso3 in country_map:
            country_id = country_map[country_iso3].id
        else:
            warnings.append(f"Country not found: {country_name} (ISO3: {country_iso3}) for control area {ca_data['name']}")
        
        # Create control area
        control_area = ControlArea(
            code=ca_data["code"],
            name=ca_data["name"],
            country_id=country_id
        )
        
        control_areas_to_add.append(control_area)
    
    if not control_areas_to_add:
        print(f"  Found {len(existing_codes)} existing control areas, no new control areas to add")
        if skipped_count > 0:
            print(f"  Skipped {skipped_count} existing control areas")
        return
    
    print(f"  Adding {len(control_areas_to_add)} new control areas...")
    if skipped_count > 0:
        print(f"  Skipping {skipped_count} existing control areas")
    
    # Print warnings
    for warning in warnings:
        print(f"  Warning: {warning}")
    
    db.add_all(control_areas_to_add)
    db.commit()
    
    print(f"  Successfully added {len(control_areas_to_add)} control areas")