"""Test the complete API flow for Anholt windfarm."""

import asyncio
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import get_settings
from app.services.entsoe_service import ENTSOEService
from sqlalchemy import select, text

async def test_anholt_api():
    settings = get_settings()
    engine = create_async_engine(str(settings.DATABASE_URL))
    AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with AsyncSessionLocal() as db:
        # Get Anholt windfarm details
        result = await db.execute(text('''
            SELECT w.id, w.name, w.code, ca.code as control_area_code, ca.name as control_area_name
            FROM windfarms w
            LEFT JOIN control_areas ca ON w.control_area_id = ca.id
            WHERE LOWER(w.name) LIKE '%anholt%'
        '''))
        windfarm = result.fetchone()
        
        if not windfarm:
            print("Anholt windfarm not found!")
            return
        
        print(f"Testing API flow for: {windfarm.name}")
        print(f"Control Area: {windfarm.control_area_name} ({windfarm.control_area_code})")
        print("-" * 50)
        
        # Get generation units using raw SQL to avoid model import issues
        result = await db.execute(text('''
            SELECT id, name, code, fuel_type, capacity_mw, source
            FROM generation_units
            WHERE windfarm_id = :wf_id
            AND is_active = true
            AND source = 'ENTSOE'
        '''), {'wf_id': windfarm.id})
        generation_units_raw = result.fetchall()
        
        # Create mock generation unit objects for the service
        class MockGenerationUnit:
            def __init__(self, id, name, code, fuel_type, capacity_mw, source):
                self.id = id
                self.name = name
                self.code = code
                self.fuel_type = fuel_type
                self.capacity_mw = capacity_mw
                self.source = source
        
        generation_units = [
            MockGenerationUnit(gu.id, gu.name, gu.code, gu.fuel_type, gu.capacity_mw, gu.source)
            for gu in generation_units_raw
        ]
        
        print(f"Found {len(generation_units)} generation units")
        for unit in generation_units:
            print(f"  - {unit.name}: EIC={unit.code}")
        
        # Test fetching data
        service = ENTSOEService(db)
        
        # Use historical date
        end_date = datetime(2025, 1, 15, 0, 0, 0)
        start_date = end_date - timedelta(days=1)
        
        print(f"\nFetching data from {start_date} to {end_date}")
        print("-" * 50)
        
        try:
            result = await service.fetch_generation_per_unit(
                start_date=start_date,
                end_date=end_date,
                area_code=windfarm.control_area_code,
                generation_units=generation_units,
                current_user=None,
                store_data=False  # Don't store for test
            )
            
            print(f"Success: {result.get('metadata', {}).get('total_records', 0) > 0}")
            print(f"Total records: {result.get('metadata', {}).get('total_records', 0)}")
            print(f"Units found: {result.get('metadata', {}).get('units_found', 0)}")
            print(f"Units list: {result.get('metadata', {}).get('units_found_list', [])}")
            
            if result.get("data"):
                print(f"\nSample data (first 3 records):")
                for i, record in enumerate(result["data"][:3]):
                    print(f"  {i+1}. {record.get('timestamp')}: {record.get('value')} MW (EIC: {record.get('eic_code')})")
            
            if result.get("metadata", {}).get("errors"):
                print(f"\nErrors: {result['metadata']['errors']}")
                
        except Exception as e:
            print(f"Error: {e}")
    
    await engine.dispose()
    print("\n" + "=" * 50)
    print("Test completed")

if __name__ == "__main__":
    asyncio.run(test_anholt_api())