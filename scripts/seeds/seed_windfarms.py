#!/usr/bin/env python3
"""
Seed script for windfarms table with ownership relationships
"""

import json
from datetime import datetime, date
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from sqlalchemy.orm import Session

from app.models.country import Country
from app.models.state import State
from app.models.region import Region
from app.models.bidzone import Bidzone
from app.models.market_balance_area import MarketBalanceArea
from app.models.control_area import ControlArea
from app.models.owner import Owner
from app.models.project import Project
from app.models.windfarm import Windfarm
from app.models.windfarm_owner import WindfarmOwner

# Raw windfarm data converted from Excel
WINDFARMS_DATA = [
    {
        "name": "Aberdeen",
        "total_turbine_count": 11,
        "country": "UK",
        "state": "",
        "region": "Scottish North Sea",
        "bidzone": "10YGB----------A",
        "market_balance_area": "10YGB----------A", 
        "control_area": "10YGB----------A",
        "nameplate_capacity_mw": 96.8,
        "project": "",
        "commercial_operational_date": "9/7/2018",
        "first_power_date": "5/7/2018",
        "centroid_latitude": 57.225694,
        "centroid_longitude": -1.996571,
        "foundation_type": "Offshore jacket",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Vattenfall", "percentage": 100.0}
        ]
    },
    {
        "name": "Albatros / Hohe See",
        "total_turbine_count": 87,
        "country": "Germany",
        "state": "",
        "region": "German Bight",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-EON------1",
        "nameplate_capacity_mw": 610,
        "project": "",
        "commercial_operational_date": "1/1/2020",
        "first_power_date": "",
        "centroid_latitude": 54.452174,
        "centroid_longitude": 6.313244,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "EnBW", "percentage": 50.1},
            {"name": "Enbridge", "percentage": 49.9}
        ]
    },
    {
        "name": "Alpha Ventus",
        "total_turbine_count": 12,
        "country": "Germany",
        "state": "",
        "region": "German Bight",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-EON------1",
        "nameplate_capacity_mw": 60,
        "project": "",
        "commercial_operational_date": "4/1/2010",
        "first_power_date": "",
        "centroid_latitude": 54.01078,
        "centroid_longitude": 6.60685,
        "foundation_type": "Offshore jacket",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "EWE", "percentage": 47.5},
            {"name": "Vattenfall", "percentage": 26.25},
            {"name": "RWE", "percentage": 26.25}
        ]
    },
    {
        "name": "Amrumbank West",
        "total_turbine_count": 80,
        "country": "Germany",
        "state": "",
        "region": "German Bight",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-EON------1",
        "nameplate_capacity_mw": 302.4,
        "project": "",
        "commercial_operational_date": "10/1/2015",
        "first_power_date": "",
        "centroid_latitude": 54.52295,
        "centroid_longitude": 7.705657,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "RWE", "percentage": 100.0}
        ]
    },
    {
        "name": "Anholt",
        "total_turbine_count": 111,
        "country": "Denmark",
        "state": "",
        "region": "Kattegatt",
        "bidzone": "10YDK-1--------W",
        "market_balance_area": "10YDK-1--------W",
        "control_area": "10Y1001A1001A796",
        "nameplate_capacity_mw": 400,
        "project": "",
        "commercial_operational_date": "9/4/2013",
        "first_power_date": "",
        "centroid_latitude": 56.600244,
        "centroid_longitude": 11.217641,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Ørsted", "percentage": 50.0},
            {"name": "PKA", "percentage": 20.0},
            {"name": "Pension Danmark", "percentage": 30.0}
        ]
    },
    {
        "name": "Arcadis Ost 1",
        "total_turbine_count": 27,
        "country": "Germany",
        "state": "",
        "region": "Baltic Sea",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-VE-------2",
        "nameplate_capacity_mw": 257,
        "project": "",
        "commercial_operational_date": "12/5/2023",
        "first_power_date": "",
        "centroid_latitude": 54.823589,
        "centroid_longitude": 13.644274,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "JERA Nex bp", "percentage": 100.0}
        ]
    },
    {
        "name": "Arkona",
        "total_turbine_count": 60,
        "country": "Germany",
        "state": "",
        "region": "Baltic Sea",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-VE-------2",
        "nameplate_capacity_mw": 385,
        "project": "",
        "commercial_operational_date": "4/1/2019",
        "first_power_date": "",
        "centroid_latitude": 54.782856,
        "centroid_longitude": 14.121069,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "RWE", "percentage": 50.0},
            {"name": "Equinor", "percentage": 25.0},
            {"name": "Energy Infrastructure Partners (EIP)", "percentage": 25.0}
        ]
    },
    {
        "name": "Avedøre",
        "total_turbine_count": 3,
        "country": "Denmark",
        "state": "",
        "region": "Baltic Sea",
        "bidzone": "10YDK-2--------M",
        "market_balance_area": "10YDK-2--------M",
        "control_area": "10Y1001A1001A796",
        "nameplate_capacity_mw": 10.8,
        "project": "",
        "commercial_operational_date": "11/1/2009",
        "first_power_date": "",
        "centroid_latitude": 55.60223,
        "centroid_longitude": 12.461203,
        "foundation_type": "offshore gravity based",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Ørsted", "percentage": 100.0}
        ]
    },
    {
        "name": "Baltic 1",
        "total_turbine_count": 21,
        "country": "Germany",
        "state": "",
        "region": "Baltic Sea",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-VE-------2",
        "nameplate_capacity_mw": 48,
        "project": "Baltic 1 and 2",
        "commercial_operational_date": "4/1/2011",
        "first_power_date": "",
        "centroid_latitude": 54.628902,
        "centroid_longitude": 12.691839,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "EnBW", "percentage": 100.0}
        ]
    },
    {
        "name": "Baltic 2",
        "total_turbine_count": 80,
        "country": "Germany",
        "state": "",
        "region": "Baltic Sea",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-VE-------2",
        "nameplate_capacity_mw": 288,
        "project": "Baltic 1 and 2",
        "commercial_operational_date": "9/1/2015",
        "first_power_date": "",
        "centroid_latitude": 54.982701,
        "centroid_longitude": 13.162384,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "EnBW", "percentage": 50.1},
            {"name": "Ärzteversorgung Westfalen-Lippe", "percentage": 22.5},
            {"name": "Vårgrønn", "percentage": 27.4}
        ]
    },
    {
        "name": "Baltic Eagle",
        "total_turbine_count": 50,
        "country": "Germany",
        "state": "",
        "region": "Baltic Sea",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-VE-------2",
        "nameplate_capacity_mw": 476,
        "project": "",
        "commercial_operational_date": "12/31/2024",
        "first_power_date": "",
        "centroid_latitude": 54.828346,
        "centroid_longitude": 13.860219,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Iberdrola", "percentage": 50.0},
            {"name": "Masdar", "percentage": 50.0}
        ]
    },
    {
        "name": "Baltic Power",
        "total_turbine_count": 76,
        "country": "Poland",
        "state": "",
        "region": "Baltic Sea",
        "bidzone": "10YPL-AREA-----S",
        "market_balance_area": "10YPL-AREA-----S",
        "control_area": "10YPL-AREA-----S",
        "nameplate_capacity_mw": 1140,
        "project": "",
        "commercial_operational_date": "12/31/2026",
        "first_power_date": "",
        "centroid_latitude": None,
        "centroid_longitude": None,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "under_installation",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Orlen", "percentage": 51.0},
            {"name": "Northland Power", "percentage": 49.0}
        ]
    },
    {
        "name": "BARD Offshore 1",
        "total_turbine_count": 80,
        "country": "Germany",
        "state": "",
        "region": "German Bight",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-EON------1",
        "nameplate_capacity_mw": 400,
        "project": "",
        "commercial_operational_date": "9/1/2013",
        "first_power_date": "",
        "centroid_latitude": 54.355288,
        "centroid_longitude": 5.980843,
        "foundation_type": "Offshore tripod",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Macquarie", "percentage": 100.0}
        ]
    },
    {
        "name": "Barrow",
        "total_turbine_count": 30,
        "country": "UK",
        "state": "",
        "region": "Irish Sea",
        "bidzone": "10YGB----------A",
        "market_balance_area": "10YGB----------A",
        "control_area": "10YGB----------A",
        "nameplate_capacity_mw": 90,
        "project": "",
        "commercial_operational_date": "9/1/2006",
        "first_power_date": "",
        "centroid_latitude": 53.991471,
        "centroid_longitude": -3.295966,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Ørsted", "percentage": 100.0}
        ]
    },
    {
        "name": "Beatrice",
        "total_turbine_count": 84,
        "country": "UK",
        "state": "",
        "region": "Scottish North Sea",
        "bidzone": "10YGB----------A",
        "market_balance_area": "10YGB----------A",
        "control_area": "10YGB----------A",
        "nameplate_capacity_mw": 588,
        "project": "",
        "commercial_operational_date": "8/5/2019",
        "first_power_date": "",
        "centroid_latitude": 58.254059,
        "centroid_longitude": -2.884547,
        "foundation_type": "Offshore jacket",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "SSE", "percentage": 40.0},
            {"name": "TRIG", "percentage": 17.5},
            {"name": "SDIC Red Rock Power", "percentage": 25.0},
            {"name": "Equitix", "percentage": 17.5}
        ]
    },
    {
        "name": "Belwind 1",
        "total_turbine_count": 56,
        "country": "Belgium",
        "state": "",
        "region": "Southern North Sea",
        "bidzone": "10YBE----------2",
        "market_balance_area": "10YBE----------2",
        "control_area": "10YBE----------2",
        "nameplate_capacity_mw": 171,
        "project": "",
        "commercial_operational_date": "12/1/2010",
        "first_power_date": "",
        "centroid_latitude": 51.668763,
        "centroid_longitude": 2.804548,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "Bligh Bank",
        "owners": [
            {"name": "Meewind", "percentage": 20.0},
            {"name": "Sumitomo", "percentage": 39.0},
            {"name": "JERA Nex bp", "percentage": 41.0}
        ]
    },
    {
        "name": "Block Island",
        "total_turbine_count": 5,
        "country": "USA",
        "state": "RI",
        "region": "US Atlantic",
        "bidzone": "",
        "market_balance_area": "",
        "control_area": "",
        "nameplate_capacity_mw": 30,
        "project": "",
        "commercial_operational_date": "12/1/2016",
        "first_power_date": "",
        "centroid_latitude": None,
        "centroid_longitude": None,
        "foundation_type": "Offshore jacket",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Ørsted", "percentage": 100.0}
        ]
    },
    {
        "name": "Borkum Riffgat",
        "total_turbine_count": 30,
        "country": "Germany",
        "state": "",
        "region": "German Bight",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-EON------1",
        "nameplate_capacity_mw": 133,
        "project": "",
        "commercial_operational_date": "2/1/2014",
        "first_power_date": "",
        "centroid_latitude": 53.692416,
        "centroid_longitude": 6.475472,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "According to EWE has capacity of 108 MW",
        "alternate_name": "",
        "owners": [
            {"name": "EWE", "percentage": 99.58},
            {"name": "ENOVA", "percentage": 0.42}
        ]
    },
    {
        "name": "Borkum Riffgrund 1",
        "total_turbine_count": 78,
        "country": "Germany",
        "state": "",
        "region": "German Bight",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-EON------1",
        "nameplate_capacity_mw": 312,
        "project": "",
        "commercial_operational_date": "10/1/2015",
        "first_power_date": "",
        "centroid_latitude": 53.967364,
        "centroid_longitude": 6.554129,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Ørsted", "percentage": 50.0},
            {"name": "Schroders Greencoat", "percentage": 50.0}
        ]
    },
    {
        "name": "Borkum Riffgrund 2",
        "total_turbine_count": 56,
        "country": "Germany",
        "state": "",
        "region": "German Bight",
        "bidzone": "10Y1001A1001A82H",
        "market_balance_area": "10Y1001A1001A82H",
        "control_area": "10YDE-EON------1",
        "nameplate_capacity_mw": 450,
        "project": "",
        "commercial_operational_date": "6/1/2019",
        "first_power_date": "",
        "centroid_latitude": 53.951742,
        "centroid_longitude": 6.487407,
        "foundation_type": "Offshore monopile",
        "location_type": "offshore",
        "status": "operational",
        "notes": "",
        "alternate_name": "",
        "owners": [
            {"name": "Gulf Energy Development Public Co", "percentage": 25.0},
            {"name": "Ørsted", "percentage": 50.0},
            {"name": "Keppel Corp and Keppel Infrastructure Fund", "percentage": 25.0}
        ]
    }
]

# Country mapping from names to database codes
COUNTRY_MAPPING = {
    "UK": "GBR",
    "Germany": "DEU", 
    "Denmark": "DNK",
    "Poland": "POL",
    "Belgium": "BEL",
    "USA": "USA"
}

def parse_date(date_str: str) -> Optional[date]:
    """Parse date string in various formats"""
    if not date_str or date_str.strip() == "":
        return None
        
    try:
        # Try MM/DD/YYYY format first
        if "/" in date_str:
            month, day, year = date_str.strip().split("/")
            return date(int(year), int(month), int(day))
        # Add other date formats if needed
        return None
    except (ValueError, AttributeError):
        return None

def lookup_country_id(db: Session, country_name: str) -> Optional[int]:
    """Look up country ID by name"""
    country_code = COUNTRY_MAPPING.get(country_name)
    if not country_code:
        return None
        
    country = db.query(Country).filter(Country.code == country_code).first()
    return country.id if country else None

def lookup_state_id(db: Session, state_code: str, country_id: int) -> Optional[int]:
    """Look up state ID by code and country, create offshore state if needed"""
    if not state_code or state_code.strip() == "":
        # For offshore windfarms, create or find an "offshore" state
        country = db.query(Country).filter(Country.id == country_id).first()
        if not country:
            return None
            
        offshore_code = f"{country.code}_OFFSHORE"
        offshore_state = db.query(State).filter(
            State.code == offshore_code,
            State.country_id == country_id
        ).first()
        
        if not offshore_state:
            # Create offshore state for this country
            offshore_state = State(
                code=offshore_code,
                name=f"{country.name} Offshore Waters",
                country_id=country_id,
                lat=None,
                lng=None
            )
            db.add(offshore_state)
            db.flush()  # Get the ID
            print(f"    Created offshore state for {country.name}: {offshore_state.name}")
        
        return offshore_state.id if offshore_state else None
        
    state = db.query(State).filter(
        State.code == state_code.strip(),
        State.country_id == country_id
    ).first()
    return state.id if state else None

def lookup_region_id(db: Session, region_name: str) -> Optional[int]:
    """Look up region ID by name"""
    if not region_name or region_name.strip() == "":
        return None
        
    region = db.query(Region).filter(Region.name == region_name.strip()).first()
    return region.id if region else None

def lookup_bidzone_id(db: Session, bidzone_code: str) -> Optional[int]:
    """Look up bidzone ID by code"""
    if not bidzone_code or bidzone_code.strip() == "":
        return None
        
    bidzone = db.query(Bidzone).filter(Bidzone.code == bidzone_code.strip()).first()
    return bidzone.id if bidzone else None

def lookup_market_balance_area_id(db: Session, mba_code: str) -> Optional[int]:
    """Look up market balance area ID by code"""
    if not mba_code or mba_code.strip() == "":
        return None
        
    mba = db.query(MarketBalanceArea).filter(MarketBalanceArea.code == mba_code.strip()).first()
    return mba.id if mba else None

def lookup_control_area_id(db: Session, ca_code: str) -> Optional[int]:
    """Look up control area ID by code"""
    if not ca_code or ca_code.strip() == "":
        return None
        
    ca = db.query(ControlArea).filter(ControlArea.code == ca_code.strip()).first()
    return ca.id if ca else None

def lookup_owner_id(db: Session, owner_name: str) -> Optional[int]:
    """Look up owner ID by name"""
    if not owner_name or owner_name.strip() == "":
        return None
        
    owner = db.query(Owner).filter(Owner.name == owner_name.strip()).first()
    return owner.id if owner else None

def lookup_project_id(db: Session, project_name: str) -> Optional[int]:
    """Look up project ID by name"""
    if not project_name or project_name.strip() == "":
        return None
        
    project = db.query(Project).filter(Project.name == project_name.strip()).first()
    return project.id if project else None

def generate_windfarm_code(name: str) -> str:
    """Generate a code from windfarm name"""
    # Simple approach: take uppercase letters and numbers, replace spaces/special chars with underscores
    import re
    code = re.sub(r'[^A-Za-z0-9]', '_', name.upper())
    code = re.sub(r'_+', '_', code)  # Replace multiple underscores with single
    code = code.strip('_')  # Remove leading/trailing underscores
    return code[:50]  # Limit to 50 characters

def seed_windfarms(db: Session):
    """Seed windfarms table with initial data and create ownership relationships"""
    print(f"  Checking for existing windfarms...")
    
    # Get existing windfarm names
    existing_names = {wf.name for wf in db.query(Windfarm.name).all()}
    
    success_count = 0
    failure_count = 0
    failures = []
    
    for windfarm_data in WINDFARMS_DATA:
        windfarm_name = windfarm_data["name"]
        
        # Skip if already exists
        if windfarm_name in existing_names:
            print(f"    Skipping existing windfarm: {windfarm_name}")
            continue
            
        try:
            # Look up foreign key relationships
            country_id = lookup_country_id(db, windfarm_data["country"])
            if not country_id:
                raise ValueError(f"Country '{windfarm_data['country']}' not found")
                
            state_id = lookup_state_id(db, windfarm_data["state"], country_id)
            region_id = lookup_region_id(db, windfarm_data["region"])
            bidzone_id = lookup_bidzone_id(db, windfarm_data["bidzone"])
            mba_id = lookup_market_balance_area_id(db, windfarm_data["market_balance_area"])
            ca_id = lookup_control_area_id(db, windfarm_data["control_area"])
            project_id = lookup_project_id(db, windfarm_data["project"])
            
            # Parse dates
            commercial_date = parse_date(windfarm_data["commercial_operational_date"])
            first_power_date = parse_date(windfarm_data["first_power_date"])
            
            # Validate ownership percentages
            total_percentage = sum(owner["percentage"] for owner in windfarm_data["owners"])
            if abs(total_percentage - 100.0) > 0.01:  # Allow small rounding differences
                raise ValueError(f"Ownership percentages sum to {total_percentage}%, not 100%")
            
            # Look up owner IDs
            owner_lookups = []
            for owner_data in windfarm_data["owners"]:
                owner_id = lookup_owner_id(db, owner_data["name"])
                if not owner_id:
                    raise ValueError(f"Owner '{owner_data['name']}' not found")
                owner_lookups.append({
                    "owner_id": owner_id,
                    "percentage": Decimal(str(owner_data["percentage"]))
                })
            
            # Create windfarm
            windfarm = Windfarm(
                code=generate_windfarm_code(windfarm_name),
                name=windfarm_name,
                country_id=country_id,
                state_id=state_id,
                region_id=region_id,
                bidzone_id=bidzone_id,
                market_balance_area_id=mba_id,
                control_area_id=ca_id,
                nameplate_capacity_mw=windfarm_data.get("nameplate_capacity_mw"),
                project_id=project_id,
                commercial_operational_date=commercial_date,
                first_power_date=first_power_date,
                lat=windfarm_data.get("centroid_latitude"),
                lng=windfarm_data.get("centroid_longitude"),
                foundation_type=windfarm_data.get("foundation_type") or None,
                location_type=windfarm_data.get("location_type") or None,
                status=windfarm_data.get("status") or None,
                notes=windfarm_data.get("notes") or None,
                alternate_name=windfarm_data.get("alternate_name") or None
            )
            
            db.add(windfarm)
            db.flush()  # Get the windfarm ID
            
            # Create ownership relationships
            for owner_lookup in owner_lookups:
                windfarm_owner = WindfarmOwner(
                    windfarm_id=windfarm.id,
                    owner_id=owner_lookup["owner_id"],
                    ownership_percentage=owner_lookup["percentage"]
                )
                db.add(windfarm_owner)
            
            success_count += 1
            print(f"    ✅ Added windfarm: {windfarm_name} with {len(owner_lookups)} owners")
            
        except Exception as e:
            failure_count += 1
            error_msg = str(e)
            failures.append({
                "windfarm_name": windfarm_name,
                "error": error_msg,
                "data": windfarm_data
            })
            print(f"    ❌ Failed to add windfarm: {windfarm_name} - {error_msg}")
            db.rollback()  # Rollback this transaction to allow subsequent windfarms to be processed
    
    # Commit successful changes
    if success_count > 0:
        db.commit()
        print(f"  Successfully added {success_count} windfarms")
    
    # Generate failure report
    if failures:
        report_path = Path(__file__).parent / "windfarms_seed_failures.json"
        with open(report_path, 'w') as f:
            json.dump({
                "summary": {
                    "total_attempted": len(WINDFARMS_DATA),
                    "successful": success_count,
                    "failed": failure_count,
                    "timestamp": datetime.now().isoformat()
                },
                "failures": failures
            }, f, indent=2, default=str)
        print(f"  Generated failure report: {report_path}")
        print(f"  Failed to add {failure_count} windfarms - see report for details")
    
    print(f"  Windfarm seeding completed: {success_count} successful, {failure_count} failed")