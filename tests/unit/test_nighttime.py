from datetime import datetime, timezone
import pandas as pd
from solar_consumer.data.nighttime import make_night_time_zeros


class TestMakeNightTimeZeros:

    def test_midnight_winter_returns_zeros(self):
        # Winter midnight should return zeros
        start = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 15, 2, 0, tzinfo=timezone.utc)

        df = make_night_time_zeros(start, end, gsp_id=0, regime="in-day")

        assert not df.empty
        assert (df["generation_mw"] == 0.0).all()
        assert "datetime_gmt" in df.columns
        assert "installedcapacity_mwp" in df.columns
        assert "capacity_mwp" in df.columns
        assert "updated_gmt" in df.columns

    def test_midday_summer_returns_empty(self):
        start = datetime(2025, 7, 15, 11, 0, tzinfo=timezone.utc)
        end = datetime(2025, 7, 15, 13, 0, tzinfo=timezone.utc)

        df = make_night_time_zeros(start, end, gsp_id=0, regime="in-day")

        assert df.empty

    def test_non_inday_regime_returns_empty(self):
        # Only works for in day regime
        start = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 15, 2, 0, tzinfo=timezone.utc)

        df = make_night_time_zeros(start, end, gsp_id=0, regime="day-after")

        assert df.empty

    def test_invalid_gsp_id_returns_empty(self):
        # Unknown GSP ID should return empty
        start = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 15, 2, 0, tzinfo=timezone.utc)

        df = make_night_time_zeros(start, end, gsp_id=9999, regime="in-day")

        assert df.empty

    def test_dataframe_schema(self):
        start = datetime(2025, 1, 15, 0, 0, tzinfo=timezone.utc)
        end = datetime(2025, 1, 15, 4, 0, tzinfo=timezone.utc)

        df = make_night_time_zeros(start, end, gsp_id=0, regime="in-day")

        assert not df.empty
        expected_columns = {
            "generation_mw",
            "datetime_gmt",
            "installedcapacity_mwp",
            "capacity_mwp",
            "updated_gmt",
        }
        assert set(df.columns) == expected_columns
        assert (df["generation_mw"] == 0.0).all()
        timestamps = df["datetime_gmt"].sort_values()
        if len(timestamps) > 1:
            diffs = timestamps.diff().dropna()
            assert (diffs == pd.Timedelta(minutes=30)).all()
