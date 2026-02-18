import pandas as pd
from freezegun import freeze_time
import requests
from unittest.mock import patch

from solar_consumer.data.fetch_be_data import (
    fetch_be_data,
    BASE_URL_FORECAST,
)

pytest_plugins = ["requests_mock"]


# Helper utilities
def load_mock_response(filename: str) -> str:
    """
    Load a mocked Elia API response from disk.

    Args:
        filename: JSON file name under tests/unit/mock/responses/

    Returns:
        Raw JSON string
    """
    path = f"tests/unit/mock/responses/{filename}"
    with open(path, "r") as f:
        return f.read()


def build_mocked_session(requests_mock) -> requests.Session:
    """
    Build a real requests.Session wired to requests-mock.

    This allows the production code to use its normal session-based
    logic while still hitting mocked HTTP responses.
    """
    session = requests.Session()
    session.mount("https://", requests_mock._adapter)
    return session


# Unit test: national + regional records (pagination covered)
@freeze_time("2026-01-18T10:00:00Z")
def test_fetch_be_forecast_mixed_regions(requests_mock):
    """
    Validate successful forecast ingestion with:
    - mocked Elia API URL
    - national + regional records
    - MW -> kW conversion
    - cursor-based pagination termination
    """

    # Simulate cursor-based pagination:
    # 1) First request returns data
    # 2) Second request returns an empty page to stop the loop
    requests_mock.get(
        BASE_URL_FORECAST,
        [
            {
                "text": load_mock_response("elia_be_mixed_regions.json"),
                "status_code": 200,
            },
            {
                "json": {"results": []},
                "status_code": 200,
            },
        ],
    )

    session = build_mocked_session(requests_mock)

    # Force the fetcher to use the mocked session
    with patch(
        "solar_consumer.data.fetch_be_data._build_session",
        return_value=session,
    ):
        df = fetch_be_data(historic_or_forecast="forecast")

    # Basic sanity checks
    assert not df.empty
    assert len(df) == 2

    expected_columns = {
        "target_datetime_utc",
        "solar_generation_kw",
        "region",
        "forecast_type",
        "capacity_kw",
    }
    assert expected_columns.issubset(df.columns)

    # MW -> kW conversion
    assert df.loc[df["region"] == "Belgium", "solar_generation_kw"].iloc[0] == 1200
    assert df.loc[df["region"] == "Flanders", "solar_generation_kw"].iloc[0] == 300

    # Static metadata
    assert (df["forecast_type"] == "most_recent").all()

    # Rolling window sanity check
    now_utc = pd.Timestamp("2026-01-18T10:00:00Z")
    one_day_ago = now_utc - pd.Timedelta(days=1)

    assert df["target_datetime_utc"].min() >= one_day_ago
    assert df["target_datetime_utc"].max() <= now_utc


# Unit test: empty API response
def test_fetch_be_forecast_empty_response(requests_mock):
    """
    Ensure an empty API response results in:
    - an empty DataFrame
    - correct output schema
    """

    requests_mock.get(
        BASE_URL_FORECAST,
        text=load_mock_response("elia_be_empty.json"),
        status_code=200,
    )

    session = build_mocked_session(requests_mock)

    with patch(
        "solar_consumer.data.fetch_be_data._build_session",
        return_value=session,
    ):
        df = fetch_be_data(historic_or_forecast="forecast")

    assert df.empty
    assert list(df.columns) == [
        "target_datetime_utc",
        "solar_generation_kw",
        "region",
        "forecast_type",
        "capacity_kw",
    ]


# Unit test: malformed / unexpected payload
def test_fetch_be_forecast_invalid_payload(requests_mock):
    """
    Ensure unexpected / malformed payloads do not crash the fetcher
    and result in an empty DataFrame.
    """

    requests_mock.get(
        BASE_URL_FORECAST,
        text=load_mock_response("elia_be_invalid.json"),
        status_code=200,
    )

    session = build_mocked_session(requests_mock)

    with patch(
        "solar_consumer.data.fetch_be_data._build_session",
        return_value=session,
    ):
        df = fetch_be_data(historic_or_forecast="forecast")

    assert df.empty


# Unit test: API timeout / retry handling
def test_fetch_be_forecast_timeout(requests_mock):
    """
    Validate ReadTimeout handling without entering an infinite loop.

    Scenario:
    - First request raises ReadTimeout
    - Second request returns an empty page
    - Pagination loop terminates naturally
    """

    requests_mock.get(
        BASE_URL_FORECAST,
        [
            {"exc": requests.exceptions.ReadTimeout},
            {"json": {"results": []}, "status_code": 200},
        ],
    )

    session = build_mocked_session(requests_mock)

    with patch(
        "solar_consumer.data.fetch_be_data._build_session",
        return_value=session,
    ):
        df = fetch_be_data(historic_or_forecast="forecast")

    assert df.empty