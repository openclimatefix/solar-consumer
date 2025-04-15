from neso_solar_consumer.save_forecast import save_generation_to_site_db
from pvsite_datamodel.sqlmodels import GenerationSQL
import pandas as pd


def test_save_generation_to_site_db(db_site_session):

    """
    Test the save_generation_to_site_db function.
    """
    # Prepare mock data
    generation_data = {
        "target_datetime_utc": ["2023-10-01 00:00:00", "2023-10-01 01:00:00"],
        "solar_generation_kw": [100, 150],
    }

    # Convert to DataFrame
    generation_df = pd.DataFrame(generation_data)

    # Call the function
    save_generation_to_site_db(
        generation_data=generation_df,
        session=db_site_session,
    )

    # Check if data is saved correctly in the database
    saved_data = db_site_session.query(GenerationSQL).all()

    assert len(saved_data) == len(generation_df)


