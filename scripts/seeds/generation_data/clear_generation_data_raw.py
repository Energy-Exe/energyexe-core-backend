"""Clear all data from generation_data_raw table."""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from sqlalchemy import select, func, delete, text


async def clear_all_data():
    """Clear all data from generation_data_raw table."""
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        print("=" * 60)
        print("CLEAR GENERATION_DATA_RAW TABLE")
        print("=" * 60)
        
        # Check current data volume
        print("\nChecking current data volume...")
        count_result = await db.execute(
            select(func.count(GenerationDataRaw.id))
        )
        current_count = count_result.scalar() or 0
        print(f"Current total records: {current_count:,}")
        
        if current_count > 0:
            # Get table size
            size_result = await db.execute(
                text("SELECT pg_size_pretty(pg_total_relation_size('generation_data_raw'))")
            )
            table_size = size_result.scalar()
            print(f"Table size: {table_size}")
            
            # Confirm deletion
            print("\n⚠️  WARNING: This will DELETE ALL DATA from generation_data_raw table!")
            response = input("Are you sure you want to proceed? Type 'DELETE ALL' to confirm: ")
            
            if response == 'DELETE ALL':
                print("\nDeleting all data from generation_data_raw...")
                
                # Use TRUNCATE for faster deletion of all rows
                try:
                    await db.execute(text("TRUNCATE TABLE generation_data_raw RESTART IDENTITY"))
                    await db.commit()
                    print(f"✅ Successfully truncated table (all {current_count:,} records deleted)")
                except Exception as e:
                    # Fallback to DELETE if TRUNCATE fails (e.g., due to foreign keys)
                    print("TRUNCATE failed, using DELETE instead...")
                    delete_stmt = delete(GenerationDataRaw)
                    result = await db.execute(delete_stmt)
                    await db.commit()
                    print(f"✅ Deleted {result.rowcount:,} records")
                
                # Run VACUUM to reclaim space
                print("\nRunning VACUUM to reclaim disk space...")
                try:
                    await db.execute(text("VACUUM FULL ANALYZE generation_data_raw"))
                    await db.commit()
                    print("✅ Space reclaimed successfully")
                except Exception as e:
                    print(f"⚠️  VACUUM FULL failed: {e}")
                    print("   Running regular VACUUM instead...")
                    await db.execute(text("VACUUM ANALYZE generation_data_raw"))
                    await db.commit()
                    print("✅ Regular VACUUM completed")
                
                # Verify deletion
                verify_result = await db.execute(
                    select(func.count(GenerationDataRaw.id))
                )
                final_count = verify_result.scalar() or 0
                
                if final_count == 0:
                    print("\n✅ Verification: Table is now empty")
                    
                    # Check new table size
                    size_result = await db.execute(
                        text("SELECT pg_size_pretty(pg_total_relation_size('generation_data_raw'))")
                    )
                    new_table_size = size_result.scalar()
                    print(f"New table size: {new_table_size}")
                else:
                    print(f"\n⚠️  Warning: {final_count} records still remain")
            else:
                print("\n❌ Deletion cancelled - confirmation text did not match")
        else:
            print("\n✅ Table is already empty")
        
        print("\n" + "=" * 60)
        print("COMPLETE")
        print("=" * 60)


if __name__ == "__main__":
    print("\n⚠️  This script will DELETE ALL DATA from the generation_data_raw table!")
    print("   This action cannot be undone.")
    
    initial_confirm = input("\nDo you want to continue? (yes/no): ")
    
    if initial_confirm.lower() == 'yes':
        asyncio.run(clear_all_data())
    else:
        print("Operation cancelled")