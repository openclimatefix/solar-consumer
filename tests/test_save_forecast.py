from solar_consumer.save_forecast import save_generation_to_site_db, save_forecasts_to_site_db
from pvsite_datamodel.sqlmodels import GenerationSQL, ForecastSQL, ForecastValueSQL, LocationSQL
import pandas as pd


def test_save_generation_to_site_db(db_site_session):

    """
    Test the save_generation_to_site_db function.
    """
    # Prepare mock data
    generation_data = {
        "target_datetime_utc": ["2023-10-01 00:00:00", "2023-10-01 01:00:00"],
        "solar_generation_kw": [100, 150],
        "capacity_kw": [20_000_000, 20_000_002],
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

    sites = db_site_session.query(LocationSQL).all()
    assert len(sites) == 1
    assert sites[0].capacity_kw == 20_000_002
    assert sites[0].client_location_name == "nl_national"


def test_save_forecasts_to_site_db(db_site_session):

    """
    Test the save_generation_to_site_db function.
    """
    # Prepare mock data
    forecast_data = {
        "target_datetime_utc": ["2023-10-01 00:00:00+00:00", "2023-10-01 01:00:00+00:00"],
        "solar_generation_kw": [100, 150],
        "capacity_kw": [200, 201],
    }

    # Convert to DataFrame
    forecast_df = pd.DataFrame(forecast_data)
    forecast_df["target_datetime_utc"] = pd.to_datetime(forecast_df["target_datetime_utc"])

    # Call the function
    save_forecasts_to_site_db(
        forecast_data=forecast_df,
        session=db_site_session,
        model_tag="test-model",
        model_version="1.0",
    )

    # Check if data is saved correctly in the database
    assert len(db_site_session.query(ForecastSQL).all()) == 1
    saved_data = db_site_session.query(ForecastValueSQL).all()

    assert len(saved_data) == len(forecast_df)

