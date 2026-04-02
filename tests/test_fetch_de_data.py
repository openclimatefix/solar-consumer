import pytest
import requests
import pandas as pd
import solar_consumer.data.fetch_de_data as de_mod
from solar_consumer.data.fetch_de_data import fetch_de_data
from solar_consumer.data.fetch_de_data import fetch_de_data_range


@pytest.fixture(autouse=True)
def _mock_entsoe_client(monkeypatch, request):
    # Monkey-patch EntsoePandasClient unless marked @live
    if "live" not in request.keywords:

        class DummyClient:
            def __init__(self, api_key):
                self.api_key = api_key

            def query_generation(self, country_code, start, end, psr_type=None):
                # Return empty series for testing empty windows
                if pd.Timestamp(start).year <= 1999:
                    return pd.Series(dtype=float)

                # Return values in mw as it is later converted to kW
                idx = pd.date_range(
                    "2024-06-01T00:00Z", periods=4, freq="15min", tz="UTC"
                )
                return pd.Series([2.0, 3.0, 0.0, 0.0], index=idx)

        monkeypatch.setattr(de_mod, "EntsoePandasClient", DummyClient)
    yield


@pytest.fixture(autouse=True)
def _set_entsoe_key(monkeypatch):
    # Make sure code under test sees non-empty API key
    monkeypatch.setenv("ENTSOE_API_KEY", "dummy")


def test_only_solar_rows_returned():
    df = fetch_de_data()
    # 4 points, 3 cols, all from Germany-Luxembourg (DE_LU) Zone
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (4, 3) and all(df["tso_zone"] == "10Y1001A1001A82H")


def test_quantity_and_timestamp_conversion():
    df = fetch_de_data()
    # Check kilowatts conversion and timestamps dtype check
    assert df.iloc[0]["solar_generation_kw"] == pytest.approx(2_000)
    assert pd.api.types.is_datetime64tz_dtype(df["target_datetime_utc"])


def test_assert_on_invalid_mode():
    with pytest.raises(AssertionError):
        fetch_de_data(historic_or_forecast="forecast")


def test_http_error(monkeypatch):
    class BadClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def query_generation(self, country_code, start, end, psr_type=None):
            raise requests.HTTPError("HTTP 500")

    monkeypatch.setattr(
        de_mod, "EntsoePandasClient", BadClient
    )
    with pytest.raises(requests.HTTPError):
        fetch_de_data()


def test_range_fetch_returns_rows():
    # 2-hour window spanning the mocked points returned by the dummy client
    start = pd.Timestamp("2024-06-01T00:00Z")
    end = pd.Timestamp("2024-06-01T01:00Z")
    df = fetch_de_data_range(start.to_pydatetime(), end.to_pydatetime(), chunk_hours=1)
    assert not df.empty
    assert {"target_datetime_utc", "solar_generation_kw", "tso_zone"} <= set(df.columns)
    assert df.shape == (4, 3) and all(df["tso_zone"] == "10Y1001A1001A82H")


def test_range_fetch_handles_empty_windows():
    # Ensures that passing in time outside mocked window doesn't error, instead returning 
    # expected columns
    start = pd.Timestamp("1999-01-01T00:00Z")
    end = pd.Timestamp("1999-01-01T01:00Z")
    df = fetch_de_data_range(start.to_pydatetime(), end.to_pydatetime(), chunk_hours=1)
    assert isinstance(df, pd.DataFrame)
    assert {"target_datetime_utc", "solar_generation_kw", "tso_zone"} <= set(df.columns)


# Tests index normalisation
def test_datetime_index_is_normalised_to_utc(monkeypatch):
    class NormalisationClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def query_generation(self, country_code, start, end, psr_type=None):
            idx = pd.date_range("2024-06-01 00:00", periods=2, freq="15min")
            return pd.Series([1.0, 2.0], index=idx)

    monkeypatch.setattr(de_mod, "EntsoePandasClient", NormalisationClient)
    df = fetch_de_data()
    assert pd.api.types.is_datetime64tz_dtype(df["target_datetime_utc"])
    assert str(df["target_datetime_utc"].dt.tz) == "UTC"


# Tests that non-hourly resolution is preserved (no resampling)
def test_non_hourly_resolution_preservation(monkeypatch):
    class PreservationClient:
        def __init__(self, api_key):
            self.api_key = api_key

        def query_generation(self, country_code, start, end, psr_type=None):
            idx = pd.date_range("2024-06-01T00:00Z", periods=4, freq="15min", tz="UTC")
            return pd.Series([1.0, 1.0, 1.0, 1.0], index=idx)

    monkeypatch.setattr(de_mod, "EntsoePandasClient", PreservationClient)
    df = fetch_de_data()
    assert len(df) == 4

# Live test only executes if $env ENT­SOE_API_KEY set
@pytest.mark.skip(
    reason="Live ENTSOE endpoint often returns empty rows for the most recent 24h;\
                  mocked suite covers parsing"
)
@pytest.mark.live
def test_live_fetch_returns_rows():
    df = fetch_de_data()
    assert not df.empty
    assert {"target_datetime_utc", "solar_generation_kw", "tso_zone"} <= set(df.columns)
