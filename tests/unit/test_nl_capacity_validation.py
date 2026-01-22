import pandas as pd
from datetime import datetime, timezone
from solar_consumer.save.save_site_database import validate_nl_capacities


def test_valid_capacities_pass():
    """Test that valid capacity data passes validation"""
    timestamp = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    
    data = [
        {'target_datetime_utc': timestamp, 'region_id': 0, 'capacity_kw': 30_000, 'solar_generation_kw': 15_000}
    ]
    # 12 regions, each 2,500 kW = 30,000 kW total (matches national)
    for region_id in range(1, 13):
        data.append({
            'target_datetime_utc': timestamp,
            'region_id': region_id,
            'capacity_kw': 2_500,
            'solar_generation_kw': 1_250
        })
    
    df = pd.DataFrame(data)
    assert validate_nl_capacities(df, tolerance=0.001) is True


def test_funky_high_capacities_fail():
    """Test that funky high regional capacities fail validation"""
    timestamp = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    
    data = [
        {'target_datetime_utc': timestamp, 'region_id': 0, 'capacity_kw': 30_000, 'solar_generation_kw': 15_000}
    ]
    # Regional sum: 60,000 kW (double the national!)
    for region_id in range(1, 13):
        data.append({
            'target_datetime_utc': timestamp,
            'region_id': region_id,
            'capacity_kw': 5_000,  # Way too high
            'solar_generation_kw': 2_500
        })
    
    df = pd.DataFrame(data)
    assert validate_nl_capacities(df, tolerance=0.001) is False


def test_funky_low_capacities_fail():
    """Test that funky low regional capacities fail validation"""
    timestamp = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    
    data = [
        {'target_datetime_utc': timestamp, 'region_id': 0, 'capacity_kw': 30_000, 'solar_generation_kw': 15_000}
    ]
    # Regional sum: 12,000 kW (way too low)
    for region_id in range(1, 13):
        data.append({
            'target_datetime_utc': timestamp,
            'region_id': region_id,
            'capacity_kw': 1_000,  # Too low
            'solar_generation_kw': 500
        })
    
    df = pd.DataFrame(data)
    assert validate_nl_capacities(df, tolerance=0.001) is False


def test_within_30mw_tolerance_passes():
    """Test the specific 30MW tolerance mentioned in the issue"""
    timestamp = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    
    data = [
        {'target_datetime_utc': timestamp, 'region_id': 0, 'capacity_kw': 30_000, 'solar_generation_kw': 15_000}
    ]
    # Regional sum: 30,029 kW (29 kW = 0.029 MW difference - within tolerance)
    for region_id in range(1, 13):
        capacity = 2_529 if region_id == 1 else 2_500
        data.append({
            'target_datetime_utc': timestamp,
            'region_id': region_id,
            'capacity_kw': capacity,
            'solar_generation_kw': 1_250
        })
    
    df = pd.DataFrame(data)
    assert validate_nl_capacities(df, tolerance=0.001) is True