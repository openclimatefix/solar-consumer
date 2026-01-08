"""
Unit tests for fetch_ind_rajasthan_data.

These tests validate the behavior of the RUVNL data fetcher using mocked API responses.

Test cases covered:
- Valid JSON response with both solar and wind data
- Valid response with negative power values
- Valid response with missing solar generation data
- Non-200 HTTP response codes
- Invalid JSON responses
- Connection timeout and retry failure handling
"""

import pandas as pd
import pytest
import requests
from freezegun import freeze_time

from solar_consumer.data.fetch_ind_rajasthan_data import (
    DEFAULT_DATA_URL,
    fetch_ind_rajasthan_data,
)

retry_interval = 0


# Loading mock response from file
def load_mock_response(response_file_name: str) -> str:
    with open(response_file_name) as f:
        return f.read()


class TestFetchIndRajasthanData:
    """
    Test suite for fetching data from RUVNL
    """

    @freeze_time("2021-01-31T10:01:00Z")
    def test_fetch_data(self, requests_mock):
        """Test for correctly fetching data"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response("tests/unit/mock/responses/ruvnl-valid-response.json"),
        )
        result = fetch_ind_rajasthan_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

        assert isinstance(result, pd.DataFrame)

        # Assert correct num rows/cols and column names
        assert result.shape == (2, 3)
        for col in ["energy_type", "target_datetime_utc", "solar_generation_kw"]:
            assert col in result.columns

        # Ensure 1 solar and wind value
        result.sort_values(by="energy_type", inplace=True)
        assert result.iloc[0]["energy_type"] == "solar"
        assert result.iloc[1]["energy_type"] == "wind"

        for vals in result[["target_datetime_utc", "solar_generation_kw"]]:
            assert not pd.isna(vals)

    @freeze_time("2021-01-31T10:01:00Z")
    def test_fetch_data_with_negative_power(self, requests_mock, caplog):
        """Test for fetching data with negative power values"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response(
                "tests/unit/mock/responses/ruvnl-valid-response-negative-power.json"
            ),
        )
        result = fetch_ind_rajasthan_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

        assert result.empty
        assert "WARNING" in caplog.text

    @freeze_time("2021-01-31T10:01:00Z")
    def test_fetch_data_with_missing_asset(self, requests_mock, caplog):
        """Test for fetching data with missing asset type"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response(
                "tests/unit/mock/responses/ruvnl-valid-response-missing-solar.json"
            ),
        )
        result = fetch_ind_rajasthan_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

        assert result.shape[0] == 1
        assert result.iloc[0]["energy_type"] == "wind"
        assert "No generation data for asset type: solar" in caplog.text

    def test_catch_bad_response_code(self, requests_mock):
        """Test for handling bad response code by returning empty DataFrame"""

        requests_mock.get(DEFAULT_DATA_URL, status_code=404, reason="Not Found")
        result = fetch_ind_rajasthan_data(DEFAULT_DATA_URL, retry_interval=retry_interval)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_old_fetch_data(self, requests_mock):
        """Test for correctly fetching data"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response("tests/unit/mock/responses/ruvnl-valid-response.json"),
        )

        # we now just get a warning
        fetch_ind_rajasthan_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

    def test_catch_bad_response_json(self, requests_mock):
        """Test for catching invalid response JSON"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response("tests/unit/mock/responses/ruvnl-invalid-response.json"),
        )
        with pytest.raises(requests.exceptions.JSONDecodeError):
            fetch_ind_rajasthan_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

    def test_call_bad_url(self, requests_mock):
        """Test to check timeout raises error after max retries"""

        requests_mock.get(DEFAULT_DATA_URL, exc=requests.exceptions.ConnectTimeout)

        with pytest.raises(RuntimeError, match=r"Failed to fetch data after \d+ attempts from.*"):
            fetch_ind_rajasthan_data(DEFAULT_DATA_URL, retry_interval=retry_interval)
