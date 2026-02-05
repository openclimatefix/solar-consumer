import pytest
from datetime import datetime, timezone
import pandas as pd
from solar_consumer.save.save_site_database import save_generation_to_site_db
from pvsite_datamodel.sqlmodels import GenerationSQL


@pytest.mark.integration
def test_save_aborts_with_funky_capacity(db_site_session):
    """
    Test that save_generation_to_site_db aborts when validation fails
    """
    timestamp = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    
    # Create funky data where regional sum (60,000) doesn't match national (30,000)
    data = [
        {'target_datetime_utc': timestamp, 'region_id': 0, 'capacity_kw': 30_000, 'solar_generation_kw': 15_000}
    ]
    for region_id in range(1, 13):
        data.append({
            'target_datetime_utc': timestamp,
            'region_id': region_id,
            'capacity_kw': 5_000,  # 12 * 5000 = 60,000 (funky!)
            'solar_generation_kw': 2_500
        })
    
    funky_df = pd.DataFrame(data)
    
    # Try to save - should abort
    save_generation_to_site_db(
        generation_data=funky_df,
        session=db_site_session,
        country="nl"
    )
    
    # Verify nothing was saved
    saved_data = db_site_session.query(GenerationSQL).all()
    assert len(saved_data) == 0, "No data should be saved when validation fails"


@pytest.mark.integration
def test_save_succeeds_with_valid_capacity(db_site_session):
    """
    Test that save_generation_to_site_db saves when validation passes
    """
    timestamp = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
    
    # Create valid data where regional sum (30,000) matches national (30,000)
    data = [
        {'target_datetime_utc': timestamp, 'region_id': 0, 'capacity_kw': 30_000, 'solar_generation_kw': 15_000}
    ]
    for region_id in range(1, 13):
        data.append({
            'target_datetime_utc': timestamp,
            'region_id': region_id,
            'capacity_kw': 2_500,  # 12 * 2500 = 30,000 (valid!)
            'solar_generation_kw': 1_250
        })
    
    valid_df = pd.DataFrame(data)
    
    # Save should succeed
    save_generation_to_site_db(
        generation_data=valid_df,
        session=db_site_session,
        country="nl"
    )
    
    # Verify data was saved (13 sites: 1 national + 12 regions)
    saved_data = db_site_session.query(GenerationSQL).all()
    assert len(saved_data) == 13, f"Expected 13 records, got {len(saved_data)}"