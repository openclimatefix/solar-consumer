"""
Generation stays same, capacity is replaced.

ENTSO-E capacity includes rooftop that is missing from generation, so the
capacity factor drops off over the years. This swaps it for a
trailing 365 day p99.9 of each TSO's own generation.

The p99.9 is set by the top 35 or so quarter hours in corresponding window, 
which are always peak summer days - hence no step down.

No decrease in capacity factor over the specified year was observed.

Be wary of a strong summer dropping out and a weaker one replacing it: this 
would show up as a step down in August or September.

This one is a rebuild from notes - not original script.
"""

import logging

import numpy as np
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

BASE = "PLACEHOLDER"

SRC = f"{BASE}/DE_tso_4_2020_2025_v2.zarr"
OUT = f"{BASE}/DE_tso_4_2020_2025_v3_rebuild.zarr"

WINDOW_DAYS = 365
QUANTILE = 0.999
MIN_DAYS = 90

STEPS_PER_DAY = 96


def envelope(series):
    """Trailing WINDOW_DAYS p99.9 of a 15-min series, evaluated once per day."""
    idx = series.index
    days = pd.date_range(idx[0].normalize(), idx[-1].normalize(), freq="D")
    vals = series.to_numpy()
    starts = np.searchsorted(idx.values, days.values - np.timedelta64(WINDOW_DAYS, "D"))
    ends = np.searchsorted(idx.values, days.values + np.timedelta64(1, "D"))
    min_pts = MIN_DAYS * STEPS_PER_DAY

    seed_end = np.searchsorted(
        idx.values, days[0].to_datetime64() + np.timedelta64(MIN_DAYS, "D")
    )

    out = np.full(len(days), np.nan)
    for i, (a, b) in enumerate(zip(starts, ends, strict=True)):
        chunk = vals[:seed_end] if b - a < min_pts else vals[a:b]
        chunk = chunk[~np.isnan(chunk)]
        if len(chunk):
            out[i] = np.quantile(chunk, QUANTILE)

    daily = pd.Series(out, index=days).ffill().bfill()
    return daily.reindex(idx, method="ffill").bfill()


def build_dataset(ds, gen, cap, names):
    return xr.Dataset(
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
                f"trailing {WINDOW_DAYS}-day p{QUANTILE * 100:g} empirical "
                "envelope of own generation, not reported installed capacity"
            ),
            "created": str(pd.Timestamp.now()),
        },
    )


def main():
    ds = xr.open_zarr(SRC)
    names = [str(n) for n in ds.tso_name.values]

    gen = ds.generation_mw.compute().to_pandas().T
    gen.columns = names
    gen.index = pd.to_datetime(gen.index)

    cap = pd.DataFrame({n: envelope(gen[n]) for n in names}, index=gen.index)

    cf = gen / cap
    logger.info(
        "Capacity factor p99 by year:\n%s", cf.resample("YE").quantile(0.99).round(3)
    )
    logger.info("Capacity factor max by TSO: %s", cf.max().round(3).to_dict())
    logger.info(
        "Fraction of steps with generation above capacity: %s",
        (gen > cap).mean().round(5).to_dict(),
    )
    logger.info("Mean envelope capacity, MW: %s", cap.mean().round(0).to_dict())

    new = build_dataset(ds, gen, cap, names)
    new.chunk({"location_id": 1, "time_utc": 39444}).to_zarr(OUT, mode="w")
    logger.info("Wrote %s", OUT)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    main()
