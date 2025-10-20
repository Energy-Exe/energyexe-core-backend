"""Clear all TAIPOWER data from generation_data_raw table."""

import asyncio
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent.parent))

from app.core.database import get_session_factory
from app.models.generation_data import GenerationDataRaw
from sqlalchemy import select, text, func


async def clear_taipower_data():
    """Clear all TAIPOWER records from generation_data_raw."""
    
    print("="*80)
    print(" "*20 + "🗑️  CLEAR TAIPOWER DATA 🗑️")
    print("="*80)
    
    AsyncSessionLocal = get_session_factory()
    
    async with AsyncSessionLocal() as db:
        # Count existing records
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'TAIPOWER')
        )
        count = result.scalar() or 0
        
        if count == 0:
            print("\n✅ No TAIPOWER data to clear")
            return
        
        print(f"\n⚠️  Found {count:,} TAIPOWER records in database")
        
        # Ask for confirmation
        response = input("\n❓ Are you sure you want to delete all TAIPOWER data? (yes/no): ")
        
        if response.lower() != 'yes':
            print("\n❌ Cancelled - no data was deleted")
            return
        
        print(f"\n🗑️  Deleting {count:,} records...")
        
        # Delete records
        await db.execute(
            text("DELETE FROM generation_data_raw WHERE source = 'TAIPOWER'")
        )
        await db.commit()
        
        print(f"✅ Successfully deleted {count:,} TAIPOWER records")
        
        # Verify deletion
        result = await db.execute(
            select(func.count(GenerationDataRaw.id))
            .where(GenerationDataRaw.source == 'TAIPOWER')
        )
        remaining = result.scalar() or 0
        
        if remaining == 0:
            print("✅ Verification: All TAIPOWER data has been cleared")
        else:
            print(f"⚠️  Warning: {remaining:,} records still remain")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    asyncio.run(clear_taipower_data())