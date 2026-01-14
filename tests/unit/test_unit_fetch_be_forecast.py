import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from solar_consumer.data.fetch_be_data import fetch_be_data_forecast

# Mock API records for testing
MOCK_RECORDS = [
    {
        "datetime": "2026-01-13T10:00:00Z",
        "mostrecentforecast": 0.5,  # MW
        "region": "Belgium",
        "monitoredcapacity": 2,     # MW
    },
    {
        "datetime": "2026-01-13T11:00:00Z",
        "mostrecentforecast": 0.3,
        "region": "Flanders",
        "monitoredcapacity": 1,
    },
]

class TestFetchBeForecast:
    """Unit tests for fetch_be_data_forecast"""

    @patch("solar_consumer.data.fetch_be_data._fetch_records_time_window")
    def test_fetch_national_and_regional(self, mock_fetch):
        """Test normal fetch with mock records (national + regional)"""
        # Arrange: mock API call
        mock_fetch.return_value = MOCK_RECORDS

        # Act: call the fetch function
        df = fetch_be_data_forecast(days=1)

        # Assert: check DataFrame type
        assert isinstance(df, pd.DataFrame)

        # Assert: all expected columns exist
        expected_cols = [
            "target_datetime_utc",
            "solar_generation_kw",
            "region",
            "forecast_type",
            "gsp_id",
            "regime",
            "capacity_mwp",
        ]
        for col in expected_cols:
            assert col in df.columns

        # Assert: Belgium row has gsp_id = 0 and solar generation in kW
        belgium_row = df[df["region"] == "Belgium"].iloc[0]
        assert belgium_row["gsp_id"] == 0
        assert belgium_row["solar_generation_kw"] == 500  # 0.5 MW -> 500 kW

        # Assert: Other regions have gsp_id = NaN
        flanders_row = df[df["region"] == "Flanders"].iloc[0]
        assert np.isnan(flanders_row["gsp_id"])

    @patch("solar_consumer.data.fetch_be_data._fetch_records_time_window")
    def test_empty_response(self, mock_fetch):
        """Test handling of empty API response"""
        # Arrange: mock API returns empty list
        mock_fetch.return_value = []

        # Act
        df = fetch_be_data_forecast(days=1)

        # Assert: DataFrame is empty but columns exist
        assert df.empty
        assert list(df.columns) == [
            "target_datetime_utc",
            "solar_generation_kw",
            "region",
            "forecast_type",
            "gsp_id",
            "regime",
            "capacity_mwp",
        ]

    @patch("solar_consumer.data.fetch_be_data._fetch_records_time_window")
    def test_conversion_and_regime(self, mock_fetch):
        """Test MW->kW conversion and 'regime' field assignment"""
        # Arrange
        mock_fetch.return_value = MOCK_RECORDS

        # Act
        df = fetch_be_data_forecast(days=1)

        # Assert: check conversion MW -> kW
        assert df["solar_generation_kw"].iloc[0] == 500
        assert df["solar_generation_kw"].iloc[1] == 300

        # Assert: 'regime' is always 'in-day'
        assert all(df["regime"] == "in-day")

    @patch("solar_consumer.data.fetch_be_data._fetch_records_time_window")
    def test_forecast_type_field(self, mock_fetch):
        """Test that forecast_type is set correctly"""
        # Arrange
        mock_fetch.return_value = MOCK_RECORDS

        # Act
        df = fetch_be_data_forecast(days=1)

        # Assert
        assert all(df["forecast_type"] == "most_recent")
