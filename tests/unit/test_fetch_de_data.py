"""
Test Suite for `fetch_de_data` for Germany
"""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from entsoe.exceptions import NoMatchingDataError

from solar_consumer.constants import DE_AREAS
from solar_consumer.data.fetch_de_data import fetch_de_data


def make_generation_df(values_mw: list[float], multi_index_columns: bool = False) -> pd.DataFrame:
    """Make a generation DataFrame, in the shape entsoe-py returns it.

    The index is in the timezone of the area, and the values are in MW.
    """
    index = pd.date_range("2025-07-11 02:00", periods=len(values_mw), freq="15min", tz="Europe/Berlin")

    if multi_index_columns:
        columns = pd.MultiIndex.from_tuples(
            [("Solar", "Actual Aggregated"), ("Wind Onshore", "Actual Aggregated")]
        )
        return pd.DataFrame(
            {columns[0]: values_mw, columns[1]: [42.0] * len(values_mw)}, index=index
        )

    return pd.DataFrame({"Solar": values_mw, "Wind Onshore": [42.0] * len(values_mw)}, index=index)


def make_capacity_df(values_mw: list[float]) -> pd.DataFrame:
    """Make an installed-capacity DataFrame, in the shape entsoe-py returns it (one row/year)."""
    index = pd.date_range("2024-01-01", periods=len(values_mw), freq="YS", tz="Europe/Berlin")
    return pd.DataFrame({"Solar": values_mw}, index=index)


def make_client(gen_df: pd.DataFrame, cap_df: pd.DataFrame | None = None) -> MagicMock:
    """Build a mock ENTSO-E client returning the given generation and capacity."""
    client = MagicMock()
    client.query_generation.return_value = gen_df
    if cap_df is not None:
        client.query_installed_generation_capacity.return_value = cap_df
    return client


@pytest.fixture(autouse=True)
def _set_api_key(monkeypatch):
    monkeypatch.setenv("APIKEY_ENTSOE", "test-key")
    monkeypatch.delenv("DE_FORECAST_TYPE", raising=False)


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data(mock_client_class):
    """One row per area and timestamp, with MW converted to kW, plus capacity_kw."""
    mock_client_class.return_value = make_client(
        make_generation_df([1.0, 2.0]), make_capacity_df([18000.0, 19000.0, 20000.0])
    )

    df = fetch_de_data()

    assert isinstance(df, pd.DataFrame)
    assert set(df.columns) == {
        "target_datetime_utc",
        "solar_generation_kw",
        "capacity_kw",
        "region",
        "tso_zone",
    }
    # 2 timestamps for each of the national and the four TSO areas
    assert len(df) == 2 * len(DE_AREAS)
    assert set(df["region"]) == set(DE_AREAS)

    # MW -> kW
    assert sorted(df["solar_generation_kw"].unique()) == [1_000.0, 2_000.0]

    # capacity_kw is the most recent year (2000.0 MW -> kW), the same for every row
    assert (df["capacity_kw"] == 20_000_000.0).all()

    # timestamps are converted from the area timezone to UTC
    assert df["target_datetime_utc"].dt.tz is not None
    assert str(df["target_datetime_utc"].min()) == "2025-07-11 00:00:00+00:00"

    # only solar is requested from ENTSO-E
    assert mock_client_class.return_value.query_generation.call_args.kwargs["psr_type"] == "B16"


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_capacity_unavailable(mock_client_class):
    """If ENTSO-E has no installed capacity, capacity_kw is NaN (save falls back to max gen)."""
    client = make_client(make_generation_df([1.0]))
    client.query_installed_generation_capacity.side_effect = Exception("boom")
    mock_client_class.return_value = client

    df = fetch_de_data()

    assert df["capacity_kw"].isna().all()


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_capacity_latest_year_when_out_of_order(mock_client_class):
    """Capacity comes from the latest year, whatever order ENTSO-E returns the rows in."""
    # newest year first, so the last row is the oldest capacity
    capacity_df = make_capacity_df([18000.0, 19000.0, 20000.0]).iloc[::-1]
    mock_client_class.return_value = make_client(
        make_generation_df([1.0]), capacity_df
    )

    df = fetch_de_data()

    assert (df["capacity_kw"] == 20_000_000.0).all()


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_tso_zones(mock_client_class):
    """The TSO names line up with the regions, and the national values have no TSO."""
    mock_client_class.return_value = make_client(
        make_generation_df([1.0]), make_capacity_df([20000.0])
    )

    df = fetch_de_data().set_index("region")

    assert df.loc["de", "tso_zone"] is None
    assert df.loc["50hertz", "tso_zone"] == "50Hertz"
    assert df.loc["amprion", "tso_zone"] == "Amprion"
    assert df.loc["tennet", "tso_zone"] == "TenneT"
    assert df.loc["transnetbw", "tso_zone"] == "TransnetBW"


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_multi_index_columns(mock_client_class):
    """ENTSO-E can return columns of production type and business type."""
    mock_client_class.return_value = make_client(
        make_generation_df([1.0, 2.0], multi_index_columns=True), make_capacity_df([20000.0])
    )

    df = fetch_de_data()

    assert len(df) == 2 * len(DE_AREAS)
    assert sorted(df["solar_generation_kw"].unique()) == [1_000.0, 2_000.0]


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_one_area_missing(mock_client_class):
    """If one area has no data, we still return the others."""
    client = MagicMock()
    client.query_installed_generation_capacity.return_value = make_capacity_df([20000.0])

    def query_generation(area, start, end, psr_type):
        if area == DE_AREAS["amprion"]:
            raise NoMatchingDataError()
        return make_generation_df([1.0])

    client.query_generation.side_effect = query_generation
    mock_client_class.return_value = client

    df = fetch_de_data()

    assert set(df["region"]) == set(DE_AREAS) - {"amprion"}


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_no_data(mock_client_class):
    """No data at all gives an empty DataFrame, rather than an error."""
    client = MagicMock()
    client.query_generation.side_effect = NoMatchingDataError()
    mock_client_class.return_value = client

    df = fetch_de_data()

    assert df.empty
    assert list(df.columns) == [
        "target_datetime_utc",
        "solar_generation_kw",
        "capacity_kw",
        "region",
        "tso_zone",
    ]


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_no_solar_column(mock_client_class):
    """ENTSO-E returning something other than solar does not blow up."""
    client = MagicMock()
    client.query_generation.return_value = pd.DataFrame(
        {"Wind Onshore": [42.0]},
        index=pd.date_range("2025-07-11 02:00", periods=1, freq="15min", tz="Europe/Berlin"),
    )
    mock_client_class.return_value = client

    assert fetch_de_data().empty


def test_fetch_de_data_no_api_key(monkeypatch):
    monkeypatch.delenv("APIKEY_ENTSOE", raising=False)

    with pytest.raises(ValueError, match="api key"):
        fetch_de_data()


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_forecast(mock_client_class):
    """Fetch the German national day ahead solar forecast from ENTSO-E."""
    client = MagicMock()
    client.query_wind_and_solar_forecast.return_value = make_generation_df([1.0, 2.0])
    mock_client_class.return_value = client

    df = fetch_de_data(historic_or_forecast="forecast")

    assert set(df.columns) == {
        "target_datetime_utc",
        "solar_generation_kw",
        "region",
        "forecast_type",
    }
    assert len(df) == 2
    assert set(df["region"]) == {"de"}
    assert set(df["forecast_type"]) == {"day_ahead"}
    assert sorted(df["solar_generation_kw"]) == [1_000.0, 2_000.0]
    assert str(df["target_datetime_utc"].min()) == "2025-07-11 00:00:00+00:00"

    # the national area, solar only, and the day ahead forecast by default
    assert client.query_wind_and_solar_forecast.call_args.args[0] == DE_AREAS["de"]
    assert client.query_wind_and_solar_forecast.call_args.kwargs["psr_type"] == "B16"
    assert client.query_wind_and_solar_forecast.call_args.kwargs["process_type"] == "A01"


@pytest.mark.parametrize(
    ("forecast_type", "process_type"),
    [("day_ahead", "A01"), ("intraday", "A40"), ("current", "A18")],
)
@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_forecast_types(mock_client_class, monkeypatch, forecast_type, process_type):
    """DE_FORECAST_TYPE picks which of the three ENTSO-E forecasts we fetch."""
    monkeypatch.setenv("DE_FORECAST_TYPE", forecast_type)
    client = MagicMock()
    client.query_wind_and_solar_forecast.return_value = make_generation_df([1.0])
    mock_client_class.return_value = client

    df = fetch_de_data(historic_or_forecast="forecast")

    assert set(df["forecast_type"]) == {forecast_type}
    assert client.query_wind_and_solar_forecast.call_args.kwargs["process_type"] == process_type


def test_fetch_de_data_forecast_unknown_type(monkeypatch):
    monkeypatch.setenv("DE_FORECAST_TYPE", "next_week")

    with pytest.raises(ValueError, match="Unknown DE forecast type"):
        fetch_de_data(historic_or_forecast="forecast")


@patch("solar_consumer.data.fetch_de_data.EntsoePandasClient")
def test_fetch_de_data_forecast_no_data(mock_client_class):
    """ENTSO-E having no forecast gives an empty DataFrame, rather than raising an error."""
    client = MagicMock()
    client.query_wind_and_solar_forecast.side_effect = NoMatchingDataError()
    mock_client_class.return_value = client

    df = fetch_de_data(historic_or_forecast="forecast")

    assert df.empty
    assert list(df.columns) == [
        "target_datetime_utc",
        "solar_generation_kw",
        "region",
        "forecast_type",
    ]
