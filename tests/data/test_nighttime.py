import pandas as pd
from solar_consumer.data.nighttime import make_night_time_zeros

def test_nighttime_zeroing_based_on_elevation():
    # London-ish coords
    lat, lon = 51.5, -0.1

    df = pd.DataFrame({
        "datetime_gmt": pd.to_datetime([
            "2025-07-01 00:00Z",  # night
            "2025-07-01 12:00Z",  # day
        ]),
        "generation_mw": [5.0, 5.0]
    })

    out = make_night_time_zeros(
        df,
        latitude=lat,
        longitude=lon,
        ts_col="datetime_gmt",
        mw_col="generation_mw",
    )

    # first row is night → 0
    assert out["generation_mw"].iloc[0] == 0.0

    # second row is daytime → unchanged
    assert out["generation_mw"].iloc[1] == 5.0