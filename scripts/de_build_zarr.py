"""
Builds zarr from the downloaded parquets.

generation_mw is the real ENTSO-E generation per TSO at 15 min.

capacity_mwp is the ENTSO-E installed capacity, interpolated to 15 min.

"""

import logging
import pathlib

import numpy as np
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)

BASE = "PLACEHOLDER"

CACHE = pathlib.Path(BASE) / "data" / "power" / "entsoe_cache"
OUT = f"{BASE}/DE_tso_4_2020_2025_v2.zarr"

NAMES = ["50Hertz", "Amprion", "TenneT", "TransnetBW"]

# approximate centroids
LATLON = {
    "50Hertz": (52.5, 13.5),
    "Amprion": (51.5, 7.5),
    "TenneT": (51.0, 10.5),
    "TransnetBW": (48.5, 9.0),
}

START = "2020-01-01"
END = "2025-12-31 23:45"

MIN_SHARE_STD = 0.01
MIN_NATIONAL_MW = 1000


def load_generation():
    gen = {}
    for n in NAMES:
        parts = [
            pd.read_parquet(CACHE / f"gen_{n}_{y}.parquet")["mw"]
            for y in range(2020, 2026)
        ]
        s = pd.concat(parts)
        s.index = pd.to_datetime(s.index, utc=True).tz_localize(None)
        gen[n] = s[~s.index.duplicated()].sort_index()
    gen = pd.DataFrame(gen).resample("15min").mean().interpolate(limit=4)
    return gen.loc[START:END]


def load_capacity(index):
    cap_yr = pd.DataFrame(
        {n: pd.read_parquet(CACHE / f"cap_{n}.parquet")["mw"] for n in NAMES}
    )
    cap_yr.index = pd.to_datetime(cap_yr.index)
    return cap_yr.reindex(cap_yr.index.union(index)).interpolate("time").reindex(index)


def build_dataset(gen, cap):
    return xr.Dataset(
        {
            "generation_mw": (
                ("location_id", "time_utc"),
                gen[NAMES].T.values.astype("float32"),
            ),
            "capacity_mwp": (
                ("location_id", "time_utc"),
                cap[NAMES].T.values.astype("float32"),
            ),
        },
        coords={
            "location_id": np.arange(4),
            "time_utc": gen.index.values,
            "latitude": ("location_id", [LATLON[n][0] for n in NAMES]),
            "longitude": ("location_id", [LATLON[n][1] for n in NAMES]),
            "tso_name": ("location_id", NAMES),
        },
        attrs={
            "source": "ENTSO-E per control area, psr_type=B16",
            "capacity_basis": (
                "ENTSO-E reported installed capacity, annual, time-interpolated"
            ),
            "created": str(pd.Timestamp.now()),
        },
    )


def main():
    gen = load_generation()
    cap = load_capacity(gen.index)

    annual_twh = (gen.sum(axis=1).resample("YE").sum() * 0.25 / 1e6).round(1)
    logger.info("Annual national generation, TWh:\n%s", annual_twh)

    total = gen.sum(axis=1)
    share = gen.div(total, axis=0)[total > MIN_NATIONAL_MW]
    share_std = share.std()
    logger.info("Generation share std by TSO:\n%s", share_std.round(5))
    logger.info(
        "Generation share range:\n%s",
        pd.DataFrame({"min": share.min(), "max": share.max()}).round(3),
    )
    logger.info("Cross-TSO correlation:\n%s", gen.corr().round(4))

    cf = gen / cap
    logger.info(
        "Capacity factor p99 by year:\n%s", cf.resample("YE").quantile(0.99).round(3)
    )
    logger.info("Capacity factor max by TSO: %s", cf.max().round(3).to_dict())
    logger.info(
        "Fraction of steps with generation above capacity: %s",
        (gen > cap).mean().round(5).to_dict(),
    )

    if share_std.min() <= MIN_SHARE_STD:
        logger.error(
            "Generation shares are near constant, data is not regional. Not writing %s",
            OUT,
        )
        return

    ds = build_dataset(gen, cap)
    ds.chunk({"location_id": 1, "time_utc": 39444}).to_zarr(OUT, mode="w")
    logger.info("Wrote %s", OUT)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    main()
