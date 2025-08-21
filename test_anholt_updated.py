"""Test updated ENTSOE client for Anholt."""

import asyncio
from datetime import datetime, timedelta
from app.services.entsoe_client import ENTSOEClient

async def test_anholt():
    client = ENTSOEClient()
    
    # Test with historical date
    end = datetime(2025, 1, 15, 0, 0, 0)
    start = end - timedelta(days=1)
    
    print("Testing updated ENTSOE client for Anholt")
    print("-" * 50)
    
    # Fetch per-unit data
    df, metadata = await client.fetch_generation_per_unit(
        start=start,
        end=end,
        area_code="10Y1001A1001A796",  # DK
        eic_codes=["45W000000000046I"],  # Anholt EIC
        production_types=["wind"]
    )
    
    print(f"Success: {metadata.get('success')}")
    print(f"Records: {metadata.get('records', 0)}")
    print(f"Units found: {metadata.get('units_found', [])}")
    
    if not df.empty:
        print(f"\nDataFrame shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Check if Anholt data is present
        if "eic_code" in df.columns:
            anholt_data = df[df["eic_code"] == "45W000000000046I"]
            if not anholt_data.empty:
                print(f"\n✓ Found {len(anholt_data)} data points for Anholt")
                print(f"Sample data:")
                print(anholt_data.head())
            else:
                print("\n✗ No data for Anholt EIC code in results")
    else:
        print("\n✗ No data returned")
    
    print("\n" + "=" * 50)
    print("Test completed")

if __name__ == "__main__":
    asyncio.run(test_anholt())