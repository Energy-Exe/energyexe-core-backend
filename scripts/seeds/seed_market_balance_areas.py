#!/usr/bin/env python3
"""
Seed script for market_balance_areas table
"""

from sqlalchemy.orm import Session
from app.models.market_balance_area import MarketBalanceArea
from app.models.country import Country


# Market balance area data with country mapping
MARKET_BALANCE_AREAS_DATA = [
    {"code": "10Y1001A1001A016", "name": "SEM", "acronym": "SEM", "country_name": "United Kingdom"},  # Northern Ireland
    {"code": "10Y1001A1001A39I", "name": "EE", "acronym": "EE", "country_name": "Estonia"},
    {"code": "10Y1001A1001A44P", "name": "SE1", "acronym": "SE1", "country_name": "Sweden"},
    {"code": "10Y1001A1001A45N", "name": "SE2", "acronym": "SE2", "country_name": "Sweden"},
    {"code": "10Y1001A1001A46L", "name": "SE3", "acronym": "SE3", "country_name": "Sweden"},
    {"code": "10Y1001A1001A47J", "name": "SE4", "acronym": "SE4", "country_name": "Sweden"},
    {"code": "10Y1001A1001A48H", "name": "NO5", "acronym": "NO5", "country_name": "Norway"},
    {"code": "10Y1001A1001A49F", "name": "RU", "acronym": "RU", "country_name": "Russia"},
    {"code": "10Y1001A1001A50U", "name": "RU-KGD", "acronym": "RU-KGD", "country_name": "Russia"},
    {"code": "10Y1001A1001A51S", "name": "BY", "acronym": "BY", "country_name": "Belarus"},
    {"code": "10Y1001A1001A59C", "name": "IE(SEM)", "acronym": "IE(SEM)", "country_name": "Ireland"},
    {"code": "10Y1001A1001A699", "name": "IT-Z-Brindisi", "acronym": "IT-Z-Brindisi", "country_name": "Italy"},
    {"code": "10Y1001A1001A70O", "name": "IT-Z-Centre-North", "acronym": "IT-Z-Centre-North", "country_name": "Italy"},
    {"code": "10Y1001A1001A71M", "name": "IT-Z-Centre-South", "acronym": "IT-Z-Centre-South", "country_name": "Italy"},
    {"code": "10Y1001A1001A72K", "name": "IT-Z-Foggia", "acronym": "IT-Z-Foggia", "country_name": "Italy"},
    {"code": "10Y1001A1001A73I", "name": "IT-Z-North", "acronym": "IT-Z-North", "country_name": "Italy"},
    {"code": "10Y1001A1001A74G", "name": "IT-Z-Sardinia", "acronym": "IT-Z-Sardinia", "country_name": "Italy"},
    {"code": "10Y1001A1001A75E", "name": "IT-Z-Sicily", "acronym": "IT-Z-Sicily", "country_name": "Italy"},
    {"code": "10Y1001A1001A76C", "name": "IT-Z-Priolo", "acronym": "IT-Z-Priolo", "country_name": "Italy"},
    {"code": "10Y1001A1001A77A", "name": "IT-Z-Rossano", "acronym": "IT-Z-Rossano", "country_name": "Italy"},
    {"code": "10Y1001A1001A788", "name": "IT-Z-South", "acronym": "IT-Z-South", "country_name": "Italy"},
    {"code": "10Y1001A1001A82H", "name": "DE-LU", "acronym": "DE-LU", "country_name": "Germany"},  # Germany-Luxembourg, assigned to Germany
    {"code": "10Y1001A1001A84D", "name": "IT-MACRZONENORTH", "acronym": "IT-MACRZONENORTH", "country_name": "Italy"},
    {"code": "10Y1001A1001A85B", "name": "IT-MACRZONESOUTH", "acronym": "IT-MACRZONESOUTH", "country_name": "Italy"},
    {"code": "10Y1001A1001A93C", "name": "MT", "acronym": "MT", "country_name": "Malta"},
    {"code": "10Y1001A1001A990", "name": "MD", "acronym": "MD", "country_name": "Moldova"},
    {"code": "10Y1001A1001B012", "name": "GE", "acronym": "GE", "country_name": "Georgia"},
    {"code": "10Y1001C--00003F", "name": "UA", "acronym": "UA", "country_name": "Ukraine"},
    {"code": "10Y1001C--000182", "name": "UA-IPS", "acronym": "UA-IPS", "country_name": "Ukraine"},
    {"code": "10Y1001C--00096J", "name": "IT-Z-Calabria", "acronym": "IT-Z-Calabria", "country_name": "Italy"},
    {"code": "10Y1001C--00100H", "name": "XK", "acronym": "XK", "country_name": "Kosovo"},
    {"code": "10YAT-APG------L", "name": "AT", "acronym": "AT", "country_name": "Austria"},
    {"code": "10YBA-JPCC-----D", "name": "BA", "acronym": "BA", "country_name": "Bosnia and Herzegovina"},
    {"code": "10YBE----------2", "name": "BE", "acronym": "BE", "country_name": "Belgium"},
    {"code": "10YCH-SWISSGRIDZ", "name": "CH", "acronym": "CH", "country_name": "Switzerland"},
    {"code": "10YCS-CG-TSO---S", "name": "ME", "acronym": "ME", "country_name": "Montenegro"},
    {"code": "10YCS-SERBIATSOV", "name": "RS", "acronym": "RS", "country_name": "Serbia"},
    {"code": "10YCY-1001A0003J", "name": "CY", "acronym": "CY", "country_name": "Cyprus"},
    {"code": "10YCZ-CEPS-----N", "name": "CZ", "acronym": "CZ", "country_name": "Czech Republic"},
    {"code": "10YDK-1--------W", "name": "DK1", "acronym": "DK1", "country_name": "Denmark"},
    {"code": "10YDK-2--------M", "name": "DK2", "acronym": "DK2", "country_name": "Denmark"},
    {"code": "10YFI-1--------U", "name": "FI", "acronym": "FI", "country_name": "Finland"},
    {"code": "10YFR-RTE------C", "name": "FR", "acronym": "FR", "country_name": "France"},
    {"code": "10YGB----------A", "name": "GB", "acronym": "GB", "country_name": "United Kingdom"},
    {"code": "10YGR-HTSO-----Y", "name": "GR", "acronym": "GR", "country_name": "Greece"},
    {"code": "10YHR-HEP------M", "name": "HR", "acronym": "HR", "country_name": "Croatia"},
    {"code": "10YHU-MAVIR----U", "name": "HU", "acronym": "HU", "country_name": "Hungary"},
    {"code": "10YIE-1001A00010", "name": "SEM(EirGrid)", "acronym": "SEM(EirGrid)", "country_name": "Ireland"},
    {"code": "10YIT-GRTN-----B", "name": "IT", "acronym": "IT", "country_name": "Italy"},
    {"code": "10YLT-1001A0008Q", "name": "LT", "acronym": "LT", "country_name": "Lithuania"},
    {"code": "10YLV-1001A00074", "name": "LV", "acronym": "LV", "country_name": "Latvia"},
    {"code": "10YMK-MEPSO----8", "name": "MK", "acronym": "MK", "country_name": "North Macedonia"},
    {"code": "10YNO-0--------C", "name": "NO", "acronym": "NO", "country_name": "Norway"},
    {"code": "10YNO-1--------2", "name": "NO1", "acronym": "NO1", "country_name": "Norway"},
    {"code": "10YNO-2--------T", "name": "NO2", "acronym": "NO2", "country_name": "Norway"},
    {"code": "10YNO-3--------J", "name": "NO3", "acronym": "NO3", "country_name": "Norway"},
    {"code": "10YNO-4--------9", "name": "NO4", "acronym": "NO4", "country_name": "Norway"},
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
    "Sweden": "SWE",
    "Norway": "NOR",
    "Russia": "RUS",
    "Belarus": "BLR",
    "Ireland": "IRL",
    "Italy": "ITA",
    "Germany": "DEU",
    "Malta": "MLT",
    "Moldova": "MDA",
    "Georgia": "GEO",
    "Ukraine": "UKR",
    "Kosovo": "XKX",
    "Austria": "AUT",
    "Bosnia and Herzegovina": "BIH",
    "Belgium": "BEL",
    "Switzerland": "CHE",
    "Montenegro": "MNE",
    "Serbia": "SRB",
    "Cyprus": "CYP",
    "Czech Republic": "CZE",
    "Denmark": "DNK",
    "Finland": "FIN",
    "France": "FRA",
    "Greece": "GRC",
    "Croatia": "HRV",
    "Hungary": "HUN",
    "Lithuania": "LTU",
    "Latvia": "LVA",
    "North Macedonia": "MKD",
    "Poland": "POL",
    "Portugal": "PRT",
    "Romania": "ROU",
    "Slovenia": "SVN",
    "Slovakia": "SVK",
    "Turkey": "TUR",
}


def seed_market_balance_areas(db: Session):
    """Seed market_balance_areas table with initial data"""
    print(f"  Checking for existing market balance areas...")
    
    # Get existing market balance area codes
    existing_codes = {mba.code for mba in db.query(MarketBalanceArea.code).all()}
    
    # Get all countries for mapping
    countries = db.query(Country).all()
    country_map = {c.code: c for c in countries}
    
    # Filter out market balance areas that already exist
    market_balance_areas_to_add = []
    skipped_count = 0
    warnings = []
    
    for mba_data in MARKET_BALANCE_AREAS_DATA:
        if mba_data["code"] in existing_codes:
            skipped_count += 1
            continue
        
        # Map country name to ISO3 code
        country_name = mba_data["country_name"]
        country_iso3 = COUNTRY_NAME_TO_ISO3.get(country_name)
        
        country_id = None
        if country_iso3 and country_iso3 in country_map:
            country_id = country_map[country_iso3].id
        else:
            warnings.append(f"Country not found: {country_name} (ISO3: {country_iso3}) for market balance area {mba_data['name']}")
        
        # Create market balance area
        market_balance_area = MarketBalanceArea(
            code=mba_data["code"],
            name=mba_data["name"],
            country_id=country_id
        )
        
        market_balance_areas_to_add.append(market_balance_area)
    
    if not market_balance_areas_to_add:
        print(f"  Found {len(existing_codes)} existing market balance areas, no new market balance areas to add")
        if skipped_count > 0:
            print(f"  Skipped {skipped_count} existing market balance areas")
        return
    
    print(f"  Adding {len(market_balance_areas_to_add)} new market balance areas...")
    if skipped_count > 0:
        print(f"  Skipping {skipped_count} existing market balance areas")
    
    # Print warnings
    for warning in warnings:
        print(f"  Warning: {warning}")
    
    db.add_all(market_balance_areas_to_add)
    db.commit()
    
    print(f"  Successfully added {len(market_balance_areas_to_add)} market balance areas")