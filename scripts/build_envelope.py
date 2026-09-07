"""
Builds v3 from v2. Generation stays the same, capacity is replaced.

The ENTSO-E capacity includes rooftop that is missing from the generation
series, so the capacity factor drops off over the years. This swaps it for a
trailing 365 day p99.9 of each TSO's own generation, which keeps the capacity
factor flat.

This one is a rebuild from notes, not the original script. It writes to
v3_rebuild so it will not touch the real v3. Compare the two before using it.

Set BASE below.
"""

import numpy as np
import pandas as pd
import xarray as xr

BASE = "PLACEHOLDER"

SRC = f"{BASE}/DE_tso_4_2020_2025_v2.zarr"
OUT = f"{BASE}/DE_tso_4_2020_2025_v3_rebuild.zarr"

WINDOW_DAYS = 365
QUANTILE = 0.999
MIN_DAYS = 90  # early period has no full trailing year, use what is there


def envelope(series):
    """Trailing WINDOW_DAYS p99.9 of a 15-min series, evaluated once per day."""
    idx = series.index
    days = pd.date_range(idx[0].normalize(), idx[-1].normalize(), freq="D")
    vals = series.values
    starts = np.searchsorted(idx.values, days.values - np.timedelta64(WINDOW_DAYS, "D"))
    ends = np.searchsorted(idx.values, days.values + np.timedelta64(1, "D"))
    min_pts = MIN_DAYS * 96

    out = np.full(len(days), np.nan)
    for i, (a, b) in enumerate(zip(starts, ends)):
        if b - a < min_pts:
            b_seed = np.searchsorted(idx.values, days[0].to_datetime64() + np.timedelta64(MIN_DAYS, "D"))
            chunk = vals[:b_seed]
        else:
            chunk = vals[a:b]
        chunk = chunk[~np.isnan(chunk)]
        if len(chunk):
            out[i] = np.quantile(chunk, QUANTILE)

    daily = pd.Series(out, index=days).ffill().bfill()
    return daily.reindex(idx, method="ffill").bfill()


def main():
    ds = xr.open_zarr(SRC)
    names = [str(n) for n in ds.tso_name.values]

    gen = ds.generation_mw.compute().to_pandas().T
    gen.columns = names
    gen.index = pd.to_datetime(gen.index)

    cap = pd.DataFrame(
        {n: envelope(gen[n]) for n in names}, index=gen.index
    )

    cf = gen / cap
    print("=== p99 capacity factor by year (should be flat, ~0.90-0.97) ===")
    print(cf.resample("YE").quantile(0.99).round(3))
    print("\nmax CF:", cf.max().round(3).to_dict())
    print("frac gen>cap:", (gen > cap).mean().round(5).to_dict())
    print("\nmean envelope capacity (MW):", cap.mean().round(0).to_dict())

    new = xr.Dataset(
        {
            "generation_mw": (
                ("location_id", "time_utc"),
                gen[names].T.values.astype("float32"),
            ),
            "capacity_mwp": (
                ("location_id", "time_utc"),
                cap[names].T.values.astype("float32"),
            ),
        },
        coords={
            "location_id": ds.location_id.values,
            "time_utc": gen.index.values,
            "latitude": ("location_id", ds.latitude.values),
            "longitude": ("location_id", ds.longitude.values),
            "tso_name": ("location_id", names),
        },
        attrs={
            "source": "ENTSO-E per control area, psr_type=B16",
            "capacity_basis": (
                f"trailing {WINDOW_DAYS}-day p{QUANTILE * 100:g} empirical envelope of own "
                "generation, not reported installed capacity"
            ),
            "created": str(pd.Timestamp.now()),
        },
    )

    new.chunk({"location_id": 1, "time_utc": 39444}).to_zarr(OUT, mode="w")
    print("written:", OUT)


if __name__ == "__main__":
    main()
