import pandas as pd
from solar_consumer.data.nighttime import make_night_time_zeros

def test_make_night_time_zeros_sets_night_values_to_zero():
    times = pd.date_range("2025-07-01 00:00Z", periods=4, freq="6H", tz="UTC")
    df = pd.DataFrame({
        "target_time_utc": times,
        "generation_mw": [5.0, 5.0, 5.0, 5.0],
        "sunrise_utc": [pd.Timestamp("2025-07-01 04:30Z")]*4,
        "sunset_utc":  [pd.Timestamp("2025-07-01 20:30Z")]*4,
    })

    out = make_night_time_zeros(df)

    # 00:00 and 24:00 should be night (zero)
    assert out["generation_mw"].iloc[0] == 0.0
    assert out["generation_mw"].iloc[-1] == 0.0

    # 06:00 and 12:00 should stay 5.0 (daytime)
    assert out["generation_mw"].iloc[1] == 5.0
    assert out["generation_mw"].iloc[2] == 5.0