#!/usr/bin/env python3
"""
Step 2: Bulk import windfarms and generation units using preloaded cache.
Optimized for performance with bulk operations and minimal database queries.
"""

import argparse
import json
import sys
import time
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
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
from app.models.windfarm_owner import WindfarmOwner


def get_sync_session():
    """Get synchronous database session."""
    settings = get_settings()
    engine = create_engine(settings.database_url_sync)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal(), engine


def parse_date(date_str):
    """Parse date from various formats."""
    if pd.isna(date_str) or not date_str:
        return None
    if isinstance(date_str, datetime):
        return date_str.date()
    try:
        for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']:
            try:
                return datetime.strptime(str(date_str), fmt).date()
            except ValueError:
                continue
    except:
        pass
    return None


def parse_decimal(value):
    """Parse decimal value."""
    if pd.isna(value) or value == '':
        return None
    try:
        return float(value)  # Use float for bulk operations
    except:
        return None


def map_status(status_value):
    """Map Excel status to database status."""
    if pd.isna(status_value) or not status_value:
        return None
    
    status_str = str(status_value).strip().lower()
    status_mapping = {
        'operational': 'operational',
        'decommissioned': 'decommissioned',
        'under installation': 'under_installation',
        'expanded': 'expanded'
    }
    return status_mapping.get(status_str, None)


class BulkImporter:
    """Handles bulk import operations with optimized performance."""
    
    def __init__(self, db: Session, engine, cache_data: dict):
        self.db = db
        self.engine = engine
        self.cache = cache_data
        self.db_lookups = cache_data['database_lookups']
        self.excel_values = cache_data['excel_unique_values']
        
        # Statistics
        self.stats = {
            'countries_created': 0,
            'states_created': 0,
            'regions_created': 0,
            'bidzones_created': 0,
            'mbas_created': 0,
            'cas_created': 0,
            'windfarms_created': 0,
            'windfarms_updated': 0,
            'generation_units_created': 0,
            'owners_created': 0,
            'relationships_created': 0,
            'errors': []
        }
        
        # Track what we've created in this session
        self.created_windfarm_codes = set()
        self.created_gen_unit_codes = set(self.db_lookups['existing_generation_units'])
    
    def ensure_geography_entities(self):
        """Create all missing geography entities in bulk."""
        print("\nEnsuring geography entities exist...")
        
        # Countries
        missing_countries = [
            name for name in self.excel_values['countries']
            if name not in self.db_lookups['countries']
        ]
        if missing_countries:
            countries_to_insert = [
                {'code': name[:3].upper(), 'name': name} 
                for name in missing_countries
            ]
            stmt = insert(Country).values(countries_to_insert).on_conflict_do_nothing(index_elements=['code'])
            result = self.db.execute(stmt.returning(Country.id, Country.name))
            for row in result:
                self.db_lookups['countries'][row.name] = row.id
                self.stats['countries_created'] += 1
            print(f"  Created {len(missing_countries)} countries")
        
        # States
        missing_states = [
            name for name in self.excel_values['states']
            if name not in self.db_lookups['states']
        ]
        if missing_states:
            # Get USA country ID (most states will be USA)
            usa_id = self.db_lookups['countries'].get('USA')
            if not usa_id:
                # Try to find USA by code
                usa_result = self.db.query(Country).filter(Country.code == 'USA').first()
                if usa_result:
                    usa_id = usa_result.id
                    self.db_lookups['countries']['USA'] = usa_id
                else:
                    # Create USA if it doesn't exist
                    usa_country = Country(code='USA', name='USA')
                    self.db.add(usa_country)
                    self.db.flush()
                    usa_id = usa_country.id
                    self.db_lookups['countries']['USA'] = usa_id
            
            states_to_insert = []
            for name in missing_states:
                # Default to USA for all states (can be refined later)
                country_id = usa_id
                states_to_insert.append({
                    'code': name.replace(' ', '_').replace(',', '_').replace('+', '_')[:50].upper(),
                    'name': name,
                    'country_id': country_id
                })
            
            stmt = insert(State).values(states_to_insert).on_conflict_do_nothing(index_elements=['code'])
            result = self.db.execute(stmt.returning(State.id, State.name))
            for row in result:
                self.db_lookups['states'][row.name] = row.id
                self.stats['states_created'] += 1
            print(f"  Created {len(missing_states)} states")
        
        # Regions
        missing_regions = [
            name for name in self.excel_values['regions']
            if name not in self.db_lookups['regions']
        ]
        if missing_regions:
            regions_to_insert = []
            for name in missing_regions:
                # Determine location type based on region name
                name_lower = name.lower()
                if 'sea' in name_lower or 'north sea' in name_lower or 'ocean' in name_lower:
                    location_type = 'sea'
                else:
                    location_type = 'land'
                
                regions_to_insert.append({
                    'code': name.replace(' ', '_')[:50].upper(),
                    'name': name,
                    'location_type': location_type
                })
            
            stmt = insert(Region).values(regions_to_insert).on_conflict_do_nothing(index_elements=['code'])
            result = self.db.execute(stmt.returning(Region.id, Region.name))
            for row in result:
                self.db_lookups['regions'][row.name] = row.id
                self.stats['regions_created'] += 1
            print(f"  Created {len(missing_regions)} regions")
        
        # Bidzones
        missing_bidzones = [
            code for code in self.excel_values['bidzones']
            if code not in self.db_lookups['bidzones']
        ]
        if missing_bidzones:
            bidzones_to_insert = [{'code': code, 'name': code} for code in missing_bidzones]
            stmt = insert(Bidzone).values(bidzones_to_insert).on_conflict_do_nothing(index_elements=['code'])
            result = self.db.execute(stmt.returning(Bidzone.id, Bidzone.code))
            for row in result:
                self.db_lookups['bidzones'][row.code] = row.id
                self.stats['bidzones_created'] += 1
            print(f"  Created {len(missing_bidzones)} bidzones")
        
        # Market Balance Areas
        missing_mbas = [
            code for code in self.excel_values['market_balance_areas']
            if code not in self.db_lookups['market_balance_areas']
        ]
        if missing_mbas:
            mbas_to_insert = [{'code': code, 'name': code} for code in missing_mbas]
            stmt = insert(MarketBalanceArea).values(mbas_to_insert).on_conflict_do_nothing(index_elements=['code'])
            result = self.db.execute(stmt.returning(MarketBalanceArea.id, MarketBalanceArea.code))
            for row in result:
                self.db_lookups['market_balance_areas'][row.code] = row.id
                self.stats['mbas_created'] += 1
            print(f"  Created {len(missing_mbas)} market balance areas")
        
        # Control Areas
        missing_cas = [
            code for code in self.excel_values['control_areas']
            if code not in self.db_lookups['control_areas']
        ]
        if missing_cas:
            cas_to_insert = [{'code': code, 'name': code} for code in missing_cas]
            stmt = insert(ControlArea).values(cas_to_insert).on_conflict_do_nothing(index_elements=['code'])
            result = self.db.execute(stmt.returning(ControlArea.id, ControlArea.code))
            for row in result:
                self.db_lookups['control_areas'][row.code] = row.id
                self.stats['cas_created'] += 1
            print(f"  Created {len(missing_cas)} control areas")
        
        # Commit geography entities
        self.db.commit()
    
    def prepare_windfarm_batch(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare windfarm data for bulk insert."""
        windfarms_to_insert = []
        windfarms_processed = set()
        
        for _, row in df.iterrows():
            windfarm_name = str(row.get('windfarm_name', '')).strip()
            if not windfarm_name or windfarm_name in windfarms_processed:
                continue
            
            windfarms_processed.add(windfarm_name)
            
            # Skip if already exists
            if windfarm_name in self.db_lookups['existing_windfarms']:
                continue
            
            # Get country (required)
            country_name = str(row.get('country', '')).strip() if pd.notna(row.get('country')) else None
            if not country_name:
                self.stats['errors'].append(f"Windfarm {windfarm_name}: Missing country")
                continue
            
            country_id = self.db_lookups['countries'].get(country_name)
            if not country_id:
                self.stats['errors'].append(f"Windfarm {windfarm_name}: Country '{country_name}' not found")
                continue
            
            # Generate unique code
            base_code = windfarm_name.replace(' ', '_').upper()[:45]
            code = base_code
            counter = 1
            while code in self.created_windfarm_codes:
                code = f"{base_code}_{counter}"[:50]
                counter += 1
            self.created_windfarm_codes.add(code)
            
            # Determine location type
            foundation = str(row.get('foundation_type', '')).strip().lower() if pd.notna(row.get('foundation_type')) else ''
            location_type = 'offshore' if foundation in ['fixed', 'floating'] else 'onshore'
            
            windfarm_data = {
                'code': code,
                'name': windfarm_name,
                'country_id': country_id,
                'state_id': self.db_lookups['states'].get(str(row.get('state', '')).strip()) if pd.notna(row.get('state')) else None,
                'region_id': self.db_lookups['regions'].get(str(row.get('region', '')).strip()) if pd.notna(row.get('region')) else None,
                'bidzone_id': self.db_lookups['bidzones'].get(str(row.get('geography_bidzone', '')).strip()) if pd.notna(row.get('geography_bidzone')) else None,
                'market_balance_area_id': self.db_lookups['market_balance_areas'].get(str(row.get('geography_market_balance_area', '')).strip()) if pd.notna(row.get('geography_market_balance_area')) else None,
                'control_area_id': self.db_lookups['control_areas'].get(str(row.get('geography_control_area', '')).strip()) if pd.notna(row.get('geography_control_area')) else None,
                'nameplate_capacity_mw': parse_decimal(row.get('nameplate_capacity_mw')),
                'commercial_operational_date': parse_date(row.get('commercial_operational_date')),
                'first_power_date': parse_date(row.get('first_power_date')),
                'lat': parse_decimal(row.get('centroid_latitude')),
                'lng': parse_decimal(row.get('centroid_longitude')),
                'status': map_status(row.get('status')),
                'notes': str(row.get('notes', '')).strip()[:300] if pd.notna(row.get('notes')) else None,
                'alternate_name': str(row.get('alternate_name', '')).strip() if pd.notna(row.get('alternate_name')) else None,
                'foundation_type': str(row.get('foundation_type', '')).strip() if pd.notna(row.get('foundation_type')) else None,
                'location_type': location_type
            }
            
            windfarms_to_insert.append(windfarm_data)
        
        return windfarms_to_insert
    
    def bulk_insert_windfarms(self, windfarms_data: List[Dict]):
        """Bulk insert windfarms and update lookup cache."""
        if not windfarms_data:
            return
        
        print(f"\nBulk inserting {len(windfarms_data)} windfarms...")
        
        # Use INSERT ... RETURNING to get the IDs
        stmt = insert(Windfarm).values(windfarms_data).returning(Windfarm.id, Windfarm.name, Windfarm.code)
        result = self.db.execute(stmt)
        
        for row in result:
            self.db_lookups['existing_windfarms'][row.name] = {
                'id': row.id,
                'code': row.code
            }
            self.stats['windfarms_created'] += 1
        
        self.db.commit()
        print(f"  ✓ Created {self.stats['windfarms_created']} windfarms")
    
    def prepare_generation_unit_batch(self, df: pd.DataFrame) -> List[Dict]:
        """Prepare generation unit data for bulk insert."""
        units_to_insert = []
        
        for idx, row in df.iterrows():
            gen_unit_name = str(row.get('generation_unit_name', '')).strip()
            windfarm_name = str(row.get('windfarm_name', '')).strip()
            
            if not gen_unit_name or not windfarm_name:
                continue
            
            # Get windfarm ID
            windfarm_info = self.db_lookups['existing_windfarms'].get(windfarm_name)
            if not windfarm_info:
                self.stats['errors'].append(f"Row {idx+1}: Windfarm '{windfarm_name}' not found for unit '{gen_unit_name}'")
                continue
            
            # Generate unique code
            base_code = str(row.get('data_source_code', gen_unit_name))[:40]
            code = base_code
            counter = 1
            while code in self.created_gen_unit_codes:
                code = f"{base_code}_{counter}"[:50]
                counter += 1
                if counter > 1000:  # Safety limit
                    code = f"{base_code}_{datetime.now().strftime('%H%M%S%f')}"[:50]
                    break
            self.created_gen_unit_codes.add(code)
            
            # Determine technology type
            foundation = str(row.get('foundation_type', '')).strip().lower() if pd.notna(row.get('foundation_type')) else ''
            technology_type = 'offshore_wind' if foundation in ['fixed', 'floating'] else 'onshore_wind'
            
            unit_data = {
                'name': gen_unit_name,
                'code': code,
                'source': str(row.get('data_source', 'UNKNOWN')).strip(),
                'fuel_type': 'wind',
                'technology_type': technology_type,
                'capacity_mw': parse_decimal(row.get('nameplate_capacity_mw')),
                'status': map_status(row.get('status')),
                'start_date': parse_date(row.get('commercial_operational_date')),
                'end_date': parse_date(row.get('decommissioning_date')),
                'windfarm_id': windfarm_info['id'],
                'is_active': True
            }
            
            units_to_insert.append(unit_data)
        
        return units_to_insert
    
    def bulk_insert_generation_units(self, units_data: List[Dict]):
        """Bulk insert generation units."""
        if not units_data:
            return
        
        print(f"\nBulk inserting {len(units_data)} generation units...")
        
        # Split into smaller batches to avoid memory issues
        batch_size = 500
        for i in range(0, len(units_data), batch_size):
            batch = units_data[i:i+batch_size]
            stmt = insert(GenerationUnit).values(batch)
            self.db.execute(stmt)
            self.stats['generation_units_created'] += len(batch)
            
            if (i + batch_size) % 1000 == 0:
                print(f"  Inserted {i + batch_size} units...")
        
        self.db.commit()
        print(f"  ✓ Created {self.stats['generation_units_created']} generation units")
    
    def process_owners(self, df: pd.DataFrame):
        """Process and create owners and relationships."""
        print("\nProcessing owners...")
        
        # First, ensure all owners exist
        owners_to_create = set()
        owner_columns = ['owner_1', 'owner_2', 'owner_3', 'owner_4', 'owner_5']
        
        for _, row in df.iterrows():
            for col in owner_columns:
                if col in row and pd.notna(row[col]):
                    owner_name = str(row[col]).strip()
                    if owner_name and owner_name not in self.db_lookups['owners']:
                        owners_to_create.add(owner_name)
        
        if owners_to_create:
            owners_data = []
            for owner_name in owners_to_create:
                code = owner_name.replace(' ', '_').upper()[:50]
                owners_data.append({'code': code, 'name': owner_name})
            
            stmt = insert(Owner).values(owners_data).on_conflict_do_nothing(index_elements=['code'])
            result = self.db.execute(stmt.returning(Owner.id, Owner.name))
            for row in result:
                self.db_lookups['owners'][row.name] = row.id
                self.stats['owners_created'] += 1
            
            self.db.commit()
            print(f"  Created {self.stats['owners_created']} owners")
        
        # Now create windfarm-owner relationships
        relationships_to_create = []
        windfarms_with_owners = set()
        
        for _, row in df.iterrows():
            windfarm_name = str(row.get('windfarm_name', '')).strip()
            if not windfarm_name or windfarm_name in windfarms_with_owners:
                continue
            
            windfarm_info = self.db_lookups['existing_windfarms'].get(windfarm_name)
            if not windfarm_info:
                continue
            
            windfarms_with_owners.add(windfarm_name)
            
            # Process each owner column
            owner_cols = [
                ('owner_1', 'percentage1'),
                ('owner_2', 'percentage2'),
                ('owner_3', 'percentage3'),
                ('owner_4', 'percentage4'),
                ('owner_5', 'percentage5'),
            ]
            
            for owner_col, pct_col in owner_cols:
                if owner_col in row and pd.notna(row[owner_col]):
                    owner_name = str(row[owner_col]).strip()
                    owner_id = self.db_lookups['owners'].get(owner_name)
                    
                    if owner_id:
                        percentage = 0.0
                        if pct_col in row and pd.notna(row[pct_col]):
                            try:
                                pct_str = str(row[pct_col]).replace('%', '').strip()
                                percentage = float(pct_str)
                                if percentage <= 1:
                                    percentage = percentage * 100
                            except:
                                percentage = 0.0
                        
                        relationships_to_create.append({
                            'windfarm_id': windfarm_info['id'],
                            'owner_id': owner_id,
                            'ownership_percentage': percentage
                        })
        
        if relationships_to_create:
            # Delete existing relationships for these windfarms
            windfarm_ids = list(set(r['windfarm_id'] for r in relationships_to_create))
            self.db.query(WindfarmOwner).filter(WindfarmOwner.windfarm_id.in_(windfarm_ids)).delete(synchronize_session=False)
            
            # Insert new relationships
            self.db.execute(insert(WindfarmOwner).values(relationships_to_create))
            self.stats['relationships_created'] = len(relationships_to_create)
            self.db.commit()
            print(f"  Created {self.stats['relationships_created']} owner relationships")


def main():
    """Main import function."""
    parser = argparse.ArgumentParser(description='Bulk import windfarms and generation units')
    parser.add_argument('--excel', default='scripts/seeds/generation_unit_seed.xlsx',
                        help='Path to Excel file')
    parser.add_argument('--cache', default='scripts/seeds/windfarm_and_generation_unit/lookup_cache.json',
                        help='Path to lookup cache JSON')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of rows to process')
    parser.add_argument('--skip-geography', action='store_true',
                        help='Skip creating geography entities')
    parser.add_argument('--skip-owners', action='store_true',
                        help='Skip processing owners')
    args = parser.parse_args()
    
    # Load cache
    cache_path = Path(args.cache)
    if not cache_path.exists():
        print(f"Error: Cache file not found: {cache_path}")
        print("Please run step1_preload_lookups.py first")
        sys.exit(1)
    
    with open(cache_path, 'r') as f:
        cache_data = json.load(f)
    
    # Read Excel
    excel_path = Path(args.excel)
    if not excel_path.exists():
        print(f"Error: Excel file not found: {excel_path}")
        sys.exit(1)
    
    print("="*60)
    print("STEP 2: BULK IMPORT")
    print("="*60)
    
    print(f"\nReading Excel file: {excel_path}")
    df = pd.read_excel(excel_path, nrows=args.limit)
    print(f"  Processing {len(df)} rows")
    
    # Initialize database
    db, engine = get_sync_session()
    importer = BulkImporter(db, engine, cache_data)
    
    try:
        start_time = time.time()
        
        # Step 1: Ensure all geography entities exist
        if not args.skip_geography:
            importer.ensure_geography_entities()
        
        # Step 2: Bulk insert windfarms
        windfarms_data = importer.prepare_windfarm_batch(df)
        importer.bulk_insert_windfarms(windfarms_data)
        
        # Step 3: Bulk insert generation units
        units_data = importer.prepare_generation_unit_batch(df)
        importer.bulk_insert_generation_units(units_data)
        
        # Step 4: Process owners
        if not args.skip_owners:
            importer.process_owners(df)
        
        elapsed_time = time.time() - start_time
        
        # Print summary
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        print(f"✓ Countries created: {importer.stats['countries_created']}")
        print(f"✓ States created: {importer.stats['states_created']}")
        print(f"✓ Regions created: {importer.stats['regions_created']}")
        print(f"✓ Bidzones created: {importer.stats['bidzones_created']}")
        print(f"✓ Market Balance Areas created: {importer.stats['mbas_created']}")
        print(f"✓ Control Areas created: {importer.stats['cas_created']}")
        print(f"✓ Windfarms created: {importer.stats['windfarms_created']}")
        print(f"✓ Generation units created: {importer.stats['generation_units_created']}")
        print(f"✓ Owners created: {importer.stats['owners_created']}")
        print(f"✓ Owner relationships created: {importer.stats['relationships_created']}")
        
        if importer.stats['errors']:
            print(f"\n⚠ Errors: {len(importer.stats['errors'])}")
            for error in importer.stats['errors'][:10]:
                print(f"  - {error}")
        
        print(f"\n⏱ Time elapsed: {elapsed_time:.1f} seconds")
        print(f"📊 Processing rate: {len(df)/elapsed_time:.1f} rows/second")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()