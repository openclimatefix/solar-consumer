#!/usr/bin/env python
"""
Local test script for Belgium generation data fetching and saving.
This script tests the complete pipeline for fetching and saving Belgium generation data.
"""

import os
import sys
import asyncio
from datetime import datetime, timezone
import pandas as pd
from pathlib import Path

# Add the project to the path
sys.path.insert(0, str(Path(__file__).parent))

from solar_consumer.fetch_data import fetch_data
from solar_consumer.save.save_csv import save_forecasts_to_csv


async def test_be_generation_local():
    """
    Test fetching Belgium generation data and saving to CSV.
    """
    print("=" * 80)
    print("Testing Belgium Generation Data Pipeline")
    print("=" * 80)
    
    # Step 1: Test importing the functions
    print("\n[1/4] Verifying function imports...")
    try:
        from solar_consumer.data.fetch_be_data import (
            fetch_be_data_generation,
            BASE_URL_GENERATION,
        )
        print("✓ Successfully imported fetch_be_data_generation")
        print(f"  API Endpoint: {BASE_URL_GENERATION}")
    except Exception as e:
        print(f"✗ Failed to import: {e}")
        return False
    
    # Step 2: Test the fetch_data function
    print("\n[2/4] Testing fetch_data function with be_generation country code...")
    try:
        df = fetch_data(country="be_generation", historic_or_forecast="generation")
        print(f"✓ fetch_data executed successfully")
        print(f"  Shape: {df.shape}")
        print(f"  Columns: {list(df.columns)}")
        
        # Verify schema
        expected_columns = {"target_datetime_utc", "solar_generation_kw", "region"}
        if expected_columns.issubset(df.columns):
            print(f"✓ DataFrame has correct schema")
        else:
            print(f"✗ DataFrame schema mismatch. Expected: {expected_columns}, Got: {set(df.columns)}")
            return False
        
        if not df.empty:
            print(f"  Sample data (first 3 rows):")
            print(df.head(3).to_string())
        else:
            print("  Note: Empty result (may be due to API data availability)")
            
    except Exception as e:
        print(f"✗ Error fetching data: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 3: Test direct function call
    print("\n[3/4] Testing fetch_be_data_generation directly...")
    try:
        df_direct = fetch_be_data_generation(days=1)
        print(f"✓ fetch_be_data_generation executed successfully")
        print(f"  Shape: {df_direct.shape}")
        
        if not df_direct.empty:
            print(f"  Regions found: {df_direct['region'].unique().tolist()}")
            print(f"  Date range: {df_direct['target_datetime_utc'].min()} to {df_direct['target_datetime_utc'].max()}")
        else:
            print("  Note: Empty result (may be due to API data availability)")
            
    except Exception as e:
        print(f"✗ Error with direct call: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 4: Test CSV saving
    print("\n[4/4] Testing CSV saving...")
    try:
        csv_dir = "/tmp/solar_consumer_test"
        os.makedirs(csv_dir, exist_ok=True)
        
        # Create sample data for testing if API returned empty
        if df.empty:
            print("  Creating sample data for CSV test...")
            df = pd.DataFrame({
                "target_datetime_utc": pd.date_range(start="2026-01-18", periods=5, freq="h", tz="UTC"),
                "solar_generation_kw": [100, 150, 200, 180, 120],
                "region": ["Belgium", "Flanders", "Belgium", "Wallonia", "Flanders"]
            })
        
        save_forecasts_to_csv(df, csv_dir=csv_dir)
        
        csv_file = os.path.join(csv_dir, "forecast_data.csv")
        if os.path.exists(csv_file):
            print(f"✓ CSV saved successfully to {csv_file}")
            df_loaded = pd.read_csv(csv_file)
            print(f"  Loaded CSV shape: {df_loaded.shape}")
            print(f"  Loaded CSV columns: {list(df_loaded.columns)}")
        else:
            print(f"✗ CSV file not found at {csv_file}")
            return False
            
    except Exception as e:
        print(f"✗ Error saving CSV: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "=" * 80)
    print("✓ All tests passed!")
    print("=" * 80)
    return True


if __name__ == "__main__":
    success = asyncio.run(test_be_generation_local())
    sys.exit(0 if success else 1)
