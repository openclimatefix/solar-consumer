# tests/data/test_nighttime.py
import pandas as pd

from solar_consumer.data.nighttime import make_night_time_zeros
from solar_consumer.data.uk_gsp_locations import GSP_LOCATIONS


def test_nighttime_zeroing_based_on_elevation():
    """Verify that generation is zeroed correctly at night."""
    # London-ish coords
    lat, lon = 51.5, -0.1

    df = pd.DataFrame({
        "datetime_gmt": pd.to_datetime([
            "2025-07-01 00:00Z",  # night
            "2025-07-01 12:00Z",  # day
        ]),
        "generation_mw": [5.0, 5.0],
    })

    out = make_night_time_zeros(
        df,
        latitude=lat,
        longitude=lon,
        timestamp_col="datetime_gmt",
        generation_col="generation_mw",
    )

    # first row is night → zeroed
    assert out["generation_mw"].iloc[0] == 0.0

    # second row is daytime → unchanged
    assert out["generation_mw"].iloc[1] == 5.0


def test_load_gsp_locations():
    """Verify the bundled GSP locations CSV loads and contains expected columns."""
    assert GSP_LOCATIONS is not None
    assert not GSP_LOCATIONS.empty
    assert "latitude" in GSP_LOCATIONS.columns
    assert "longitude" in GSP_LOCATIONS.columns
    assert 0 in GSP_LOCATIONS.index  # national GSP always expected


def test_make_night_time_zeros_uses_gsp_id_fallback():
    """Ensure make_night_time_zeros can look up coords using gsp_id and run."""
    gsp_id = 0
    times = pd.date_range("2025-07-01 00:00Z", periods=2, freq="12H", tz="UTC")
    df = pd.DataFrame({"datetime_gmt": times, "generation_mw": [5.0, 5.0]})

    out = make_night_time_zeros(
        df,
        gsp_id=gsp_id,
        timestamp_col="datetime_gmt",
        generation_col="generation_mw",
    )

    # first row (night) should be zeroed, second (day) unchanged
    assert out["generation_mw"].iloc[0] == 0.0
    assert out["generation_mw"].iloc[1] == 5.0