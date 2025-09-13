#!/usr/bin/env python3
"""
Step 1: Preload all lookup data from Excel and database into JSON cache.
This script reads the Excel file and preloads all geography lookups to avoid repeated queries.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Set

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.config import get_settings
from app.models.bidzone import Bidzone
from app.models.control_area import ControlArea
from app.models.country import Country
from app.models.generation_unit import GenerationUnit
from app.models.market_balance_area import MarketBalanceArea
from app.models.owner import Owner
from app.models.region import Region
from app.models.state import State
from app.models.windfarm import Windfarm


def get_sync_session():
    """Get synchronous database session."""
    settings = get_settings()
    engine = create_engine(settings.database_url_sync)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


def extract_unique_values(df: pd.DataFrame) -> Dict[str, Set[str]]:
    """Extract all unique values from Excel for each lookup field."""
    unique_values = {
        'countries': set(),
        'states': set(),
        'regions': set(),
        'bidzones': set(),
        'market_balance_areas': set(),
        'control_areas': set(),
        'owners': set(),
        'windfarm_names': set(),
        'generation_unit_names': set(),
    }
    
    for _, row in df.iterrows():
        # Extract geography values
        if pd.notna(row.get('country')):
            unique_values['countries'].add(str(row['country']).strip())
        if pd.notna(row.get('state')):
            unique_values['states'].add(str(row['state']).strip())
        if pd.notna(row.get('region')):
            unique_values['regions'].add(str(row['region']).strip())
        if pd.notna(row.get('geography_bidzone')):
            unique_values['bidzones'].add(str(row['geography_bidzone']).strip())
        if pd.notna(row.get('geography_market_balance_area')):
            unique_values['market_balance_areas'].add(str(row['geography_market_balance_area']).strip())
        if pd.notna(row.get('geography_control_area')):
            unique_values['control_areas'].add(str(row['geography_control_area']).strip())
        
        # Extract windfarm and generation unit names
        if pd.notna(row.get('windfarm_name')):
            unique_values['windfarm_names'].add(str(row['windfarm_name']).strip())
        if pd.notna(row.get('generation_unit_name')):
            unique_values['generation_unit_names'].add(str(row['generation_unit_name']).strip())
        
        # Extract owners
        owner_columns = ['owner_1', 'owner_2', 'owner_3', 'owner_4', 'owner_5']
        for col in owner_columns:
            if col in row and pd.notna(row[col]):
                unique_values['owners'].add(str(row[col]).strip())
    
    # Convert sets to lists for JSON serialization
    return {k: list(v) for k, v in unique_values.items()}


def preload_database_lookups(db: Session) -> Dict:
    """Load all existing lookup data from database."""
    print("Loading existing database records...")
    
    lookups = {
        'countries': {},
        'states': {},
        'regions': {},
        'bidzones': {},
        'market_balance_areas': {},
        'control_areas': {},
        'owners': {},
        'existing_windfarms': {},
        'existing_generation_units': set(),
    }
    
    # Load countries
    for country in db.query(Country).all():
        lookups['countries'][country.name] = country.id
        # Also map by code for common aliases
        if country.code == 'USA':
            lookups['countries']['USA'] = country.id
        if country.code in ['GB', 'UK']:
            lookups['countries']['United Kingdom'] = country.id
    print(f"  Loaded {len(lookups['countries'])} countries")
    
    # Load states
    for state in db.query(State).all():
        lookups['states'][state.name] = state.id
    print(f"  Loaded {len(lookups['states'])} states")
    
    # Load regions
    for region in db.query(Region).all():
        lookups['regions'][region.name] = region.id
    print(f"  Loaded {len(lookups['regions'])} regions")
    
    # Load bidzones
    for bidzone in db.query(Bidzone).all():
        lookups['bidzones'][bidzone.code] = bidzone.id
    print(f"  Loaded {len(lookups['bidzones'])} bidzones")
    
    # Load market balance areas
    for mba in db.query(MarketBalanceArea).all():
        lookups['market_balance_areas'][mba.code] = mba.id
    print(f"  Loaded {len(lookups['market_balance_areas'])} market balance areas")
    
    # Load control areas
    for ca in db.query(ControlArea).all():
        lookups['control_areas'][ca.code] = ca.id
    print(f"  Loaded {len(lookups['control_areas'])} control areas")
    
    # Load owners
    for owner in db.query(Owner).all():
        lookups['owners'][owner.name] = owner.id
    print(f"  Loaded {len(lookups['owners'])} owners")
    
    # Load existing windfarms (name -> id mapping)
    for wf in db.query(Windfarm.id, Windfarm.name, Windfarm.code).all():
        lookups['existing_windfarms'][wf.name] = {
            'id': wf.id,
            'code': wf.code
        }
    print(f"  Loaded {len(lookups['existing_windfarms'])} existing windfarms")
    
    # Load existing generation unit codes
    for gu in db.query(GenerationUnit.code).all():
        lookups['existing_generation_units'].add(gu.code)
    lookups['existing_generation_units'] = list(lookups['existing_generation_units'])
    print(f"  Loaded {len(lookups['existing_generation_units'])} existing generation unit codes")
    
    return lookups


def main():
    """Main function to preload all lookup data."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Preload lookup data for windfarm import')
    parser.add_argument('--excel', default='scripts/seeds/generation_unit_seed.xlsx',
                        help='Path to Excel file')
    parser.add_argument('--output', default='scripts/seeds/windfarm_and_generation_unit/lookup_cache.json',
                        help='Output JSON file path')
    args = parser.parse_args()
    
    excel_path = Path(args.excel)
    if not excel_path.exists():
        print(f"Error: Excel file not found: {excel_path}")
        sys.exit(1)
    
    print("="*60)
    print("STEP 1: PRELOADING LOOKUP DATA")
    print("="*60)
    
    # Read Excel file
    print(f"\nReading Excel file: {excel_path}")
    df = pd.read_excel(excel_path)
    print(f"  Found {len(df)} rows")
    
    # Extract unique values from Excel
    print("\nExtracting unique values from Excel...")
    excel_values = extract_unique_values(df)
    
    for key, values in excel_values.items():
        if key not in ['windfarm_names', 'generation_unit_names']:  # Skip these as they're too many
            print(f"  {key}: {len(values)} unique values")
    
    # Load database lookups
    db = get_sync_session()
    try:
        db_lookups = preload_database_lookups(db)
    finally:
        db.close()
    
    # Combine Excel values and database lookups
    cache_data = {
        'excel_unique_values': excel_values,
        'database_lookups': db_lookups,
        'total_rows': len(df),
        'columns': list(df.columns)
    }
    
    # Save to JSON
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(cache_data, f, indent=2, default=str)
    
    print(f"\n✓ Lookup cache saved to: {output_path}")
    print(f"  File size: {output_path.stat().st_size / 1024:.1f} KB")
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Total Excel rows: {len(df)}")
    print(f"Unique windfarms in Excel: {len(excel_values['windfarm_names'])}")
    print(f"Unique generation units in Excel: {len(excel_values['generation_unit_names'])}")
    print(f"Existing windfarms in database: {len(db_lookups['existing_windfarms'])}")
    print(f"Existing generation units in database: {len(db_lookups['existing_generation_units'])}")
    print("="*60)


if __name__ == "__main__":
    main()