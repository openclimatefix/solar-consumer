"""
Builds zarr from the downloaded parquets.

generation_mw is the real ENTSO-E generation per TSO at 15 min.

capacity_mwp is the ENTSO-E installed capacity, interpolated to 15 min.

"""

import pathlib

import numpy as np
import pandas as pd
import xarray as xr

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


def load_generation():
    gen = {}
    for n in NAMES:
        s = pd.concat(
            [pd.read_parquet(CACHE / f"gen_{n}_{y}.parquet")["mw"] for y in range(2020, 2026)]
        )
        s.index = pd.to_datetime(s.index, utc=True).tz_localize(None)
        gen[n] = s[~s.index.duplicated()].sort_index()
    gen = pd.DataFrame(gen).resample("15min").mean().interpolate(limit=4)
    return gen.loc[START:END]


def load_capacity(index):
    cap_yr = pd.DataFrame({n: pd.read_parquet(CACHE / f"cap_{n}.parquet")["mw"] for n in NAMES})
    cap_yr.index = pd.to_datetime(cap_yr.index)
    return cap_yr.reindex(cap_yr.index.union(index)).interpolate("time").reindex(index)


def main():
    gen = load_generation()
    cap = load_capacity(gen.index)

    print("=== annual TWh (national) ===")
    print((gen.sum(axis=1).resample("YE").sum() * 0.25 / 1e6).round(1))

    tot = gen.sum(axis=1)
    sh = gen.div(tot, axis=0)[tot > 1000]

    print("\n=== CHECK 1 - generation share std (need > 0.01) ===")
    print(sh.std().round(5))
    print("\nshare range:")
    print(pd.DataFrame({"min": sh.min(), "max": sh.max()}).round(3))

    print("\n=== CHECK 2 - cross-TSO correlation (expect 0.85-0.95) ===")
    print(gen.corr().round(4))

    cf = gen / cap
    print("\n=== CHECK 3 - p99 capacity factor by year (must differ) ===")
    print(cf.resample("YE").quantile(0.99).round(3))
    print("\nmax CF:", cf.max().round(3).to_dict())
    print("frac gen>cap:", (gen > cap).mean().round(5).to_dict())

    if sh.std().min() <= 0.01:
        print("\n>>> STILL SYNTHETIC - NOT WRITING")
        return

    print("\n>>> REGIONAL SIGNAL PRESENT")

    ds = xr.Dataset(
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
            "capacity_basis": "ENTSO-E reported installed capacity, annual, time-interpolated",
            "created": str(pd.Timestamp.now()),
        },
    )

    ds.chunk({"location_id": 1, "time_utc": 39444}).to_zarr(OUT, mode="w")
    print("written:", OUT)


if __name__ == "__main__":
    main()
