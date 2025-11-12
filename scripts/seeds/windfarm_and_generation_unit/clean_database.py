#!/usr/bin/env python3
"""
Clean database - Remove all windfarms, generation units, and related data.
Use with caution! This will delete data.
"""

import argparse
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.config import get_settings


def clean_database(force=False):
    """Clean all windfarm and generation unit related data."""
    if not force:
        response = input("⚠️  This will DELETE all windfarms, generation units, and related data. Continue? (yes/no): ")
        if response.lower() != 'yes':
            print("Aborted.")
            return
    
    settings = get_settings()
    engine = create_engine(settings.database_url_sync)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    
    try:
        print("\n" + "="*60)
        print("CLEANING DATABASE")
        print("="*60)
        
        # Get counts before deletion
        counts = {}
        tables = [
            'generation_units',
            'windfarm_owners',
            'windfarms',
            'owners',
            'turbine_units',
            'elexon_generation_data',
            'eia_generation_data',
            'entsoe_generation_data',
            'backfill_tasks',
            'backfill_jobs'
        ]
        
        print("\nCurrent record counts:")
        for table in tables:
            try:
                result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                counts[table] = count
                print(f"  {table}: {count}")
            except:
                counts[table] = 0
        
        print("\nDeleting data...")
        
        # Delete in order of dependencies
        deletion_order = [
            'elexon_generation_data',
            'eia_generation_data',
            'entsoe_generation_data',
            'taipower_generation_data',
            'backfill_tasks',
            'backfill_jobs',
            'generation_units',
            'windfarm_owners',
            'turbine_units',
            'windfarms',
        ]
        
        for table in deletion_order:
            try:
                if counts.get(table, 0) > 0:
                    db.execute(text(f"DELETE FROM {table}"))
                    print(f"  ✓ Deleted from {table}")
            except Exception as e:
                print(f"  ✗ Error deleting from {table}: {e}")
        
        # Optionally delete owners if no other references
        try:
            db.execute(text("""
                DELETE FROM owners 
                WHERE id NOT IN (
                    SELECT DISTINCT owner_id FROM windfarm_owners
                )
            """))
            print(f"  ✓ Cleaned up orphaned owners")
        except Exception as e:
            print(f"  ⚠ Could not clean owners: {e}")
        
        db.commit()
        
        print("\nVerifying deletion:")
        for table in ['generation_units', 'windfarms', 'windfarm_owners']:
            result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            print(f"  {table}: {count}")
        
        print("\n✓ Database cleaned successfully!")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        db.rollback()
        import traceback
        traceback.print_exc()
    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description='Clean windfarm and generation unit data')
    parser.add_argument('--force', action='store_true',
                        help='Skip confirmation prompt')
    args = parser.parse_args()
    
    clean_database(force=args.force)


if __name__ == "__main__":
    main()