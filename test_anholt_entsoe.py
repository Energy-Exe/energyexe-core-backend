"""Test ENTSOE API for Anholt windfarm EIC code."""

import asyncio
from datetime import datetime, timedelta
import pandas as pd
from entsoe import EntsoePandasClient
from app.core.config import get_settings

async def test_anholt_eic():
    settings = get_settings()
    client = EntsoePandasClient(api_key=settings.ENTSOE_API_KEY)
    
    # Test parameters
    area_code = "10Y1001A1001A796"  # DK control area
    eic_code = "45W000000000046I"  # Anholt EIC code
    # Use historical date range that should have data
    end = datetime(2025, 1, 15, 0, 0, 0)  # Mid January 2025
    start = end - timedelta(days=1)
    
    print(f"Testing ENTSOE API for Anholt windfarm")
    print(f"Control Area: {area_code}")
    print(f"EIC Code: {eic_code}")
    print(f"Period: {start} to {end}")
    print("-" * 50)
    
    # Test 1: Try query_generation_per_plant with Wind Onshore (B19)
    try:
        print("\n1. Testing query_generation_per_plant (Wind Onshore - B19)...")
        df = client.query_generation_per_plant(
            area_code,
            start=pd.Timestamp(start, tz="UTC"),
            end=pd.Timestamp(end, tz="UTC"),
            psr_type="B19",  # Wind Onshore
            include_eic=True,
        )
        
        if df is not None and not df.empty:
            print(f"   Success! DataFrame shape: {df.shape}")
            print(f"   Columns: {df.columns.tolist()[:5]}...")  # Show first 5 columns
            
            # Check if our EIC code is in the columns
            if isinstance(df.columns, pd.MultiIndex):
                # The EIC code is at position 3 in the tuple (name, type, aggregation, eic_code)
                eic_codes_found = [col[3] for col in df.columns if len(col) > 3 and isinstance(col[3], str)]
                if eic_code in eic_codes_found:
                    print(f"   ✓ Found Anholt EIC code in results!")
                    # Get the column with Anholt data
                    for col in df.columns:
                        if len(col) > 3 and col[3] == eic_code:
                            print(f"   Unit name: {col[0]}")
                            unit_data = df[col].dropna()
                            if not unit_data.empty:
                                print(f"   Data points: {len(unit_data)}")
                                print(f"   Latest value: {unit_data.iloc[-1]:.2f} MW")
                else:
                    print(f"   ✗ Anholt EIC code NOT found. Found {len(eic_codes_found)} other units")
                    print(f"   Sample EIC codes: {eic_codes_found[:5]}")
        else:
            print("   No data returned")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test 2: Try query_generation_per_plant with Wind Offshore (B18)
    try:
        print("\n2. Testing query_generation_per_plant (Wind Offshore - B18)...")
        df = client.query_generation_per_plant(
            area_code,
            start=pd.Timestamp(start, tz="UTC"),
            end=pd.Timestamp(end, tz="UTC"),
            psr_type="B18",  # Wind Offshore
            include_eic=True,
        )
        
        if df is not None and not df.empty:
            print(f"   Success! DataFrame shape: {df.shape}")
            print(f"   Columns: {df.columns.tolist()[:5]}...")
            
            # Check if our EIC code is in the columns
            if isinstance(df.columns, pd.MultiIndex):
                # The EIC code is at position 3 in the tuple (name, type, aggregation, eic_code)
                eic_codes_found = [col[3] for col in df.columns if len(col) > 3 and isinstance(col[3], str)]
                if eic_code in eic_codes_found:
                    print(f"   ✓ Found Anholt EIC code in results!")
                    # Get the column with Anholt data
                    for col in df.columns:
                        if len(col) > 3 and col[3] == eic_code:
                            print(f"   Unit name: {col[0]}")
                            unit_data = df[col].dropna()
                            if not unit_data.empty:
                                print(f"   Data points: {len(unit_data)}")
                                print(f"   Latest value: {unit_data.iloc[-1]:.2f} MW")
                else:
                    print(f"   ✗ Anholt EIC code NOT found. Found {len(eic_codes_found)} other units")
                    print(f"   Sample EIC codes: {eic_codes_found[:5]}")
        else:
            print("   No data returned")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Test 3: Try query_generation without PSR type filter
    try:
        print("\n3. Testing query_generation_per_plant (All types)...")
        df = client.query_generation_per_plant(
            area_code,
            start=pd.Timestamp(start, tz="UTC"),
            end=pd.Timestamp(end, tz="UTC"),
            psr_type=None,  # All types
            include_eic=True,
        )
        
        if df is not None and not df.empty:
            print(f"   Success! DataFrame shape: {df.shape}")
            
            # Check if our EIC code is in the columns
            if isinstance(df.columns, pd.MultiIndex):
                # Try different positions for EIC code (it varies by PSR type)
                eic_codes_found = []
                for col in df.columns:
                    # EIC code could be at position 1 or 3 depending on structure
                    if len(col) > 3 and isinstance(col[3], str) and "W" in col[3]:
                        eic_codes_found.append(col[3])
                    elif len(col) > 1 and isinstance(col[1], str) and "W" in col[1]:
                        eic_codes_found.append(col[1])
                
                if eic_code in eic_codes_found:
                    print(f"   ✓ Found Anholt EIC code in results!")
                    # Get the data for this unit
                    for col in df.columns:
                        if (len(col) > 3 and col[3] == eic_code) or (len(col) > 1 and col[1] == eic_code):
                            unit_data = df[col].dropna()
                            if not unit_data.empty:
                                print(f"   Unit name: {col[0]}")
                                print(f"   Data points: {len(unit_data)}")
                                print(f"   Latest value: {unit_data.iloc[-1]:.2f} MW")
                else:
                    print(f"   ✗ Anholt EIC code NOT found. Found {len(eic_codes_found)} other units")
                    # Search for partial matches
                    partial_matches = [eic for eic in eic_codes_found if "45W" in eic]
                    if partial_matches:
                        print(f"   Partial matches starting with 45W: {partial_matches}")
        else:
            print("   No data returned")
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n" + "=" * 50)
    print("Test completed")

if __name__ == "__main__":
    asyncio.run(test_anholt_eic())