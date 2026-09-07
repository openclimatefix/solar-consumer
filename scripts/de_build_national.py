"""
Adds a national.

location_id 0 is national total, 1-4 are four TSOs.
"""

import numpy as np
import pandas as pd
import xarray as xr

BASE = "PLACEHOLDER"

SRC = f"{BASE}/DE_tso_4_2020_2025_v3.zarr"
OUT = f"{BASE}/DE_tso_4_2020_2025_v4_national.zarr"

NATIONAL_LATLON = (51.0, 10.0)


def main():
    ds = xr.open_zarr(SRC)

    nat_gen = ds.generation_mw.sum(dim="location_id")
    nat_cap = ds.capacity_mwp.sum(dim="location_id")

    gen_tso = ds.generation_mw.drop_vars(
        ["latitude", "longitude", "tso_name"], errors="ignore"
    )
    cap_tso = ds.capacity_mwp.drop_vars(
        ["latitude", "longitude", "tso_name"], errors="ignore"
    )

    gen_all = np.concatenate([nat_gen.values[None, :], gen_tso.values], axis=0)
    cap_all = np.concatenate([nat_cap.values[None, :], cap_tso.values], axis=0)

    lat = np.concatenate([[NATIONAL_LATLON[0]], ds.latitude.values])
    lon = np.concatenate([[NATIONAL_LATLON[1]], ds.longitude.values])

    if "tso_name" in ds.coords:
        names = ["National"] + [str(n) for n in ds.tso_name.values]
    else:
        names = ["National", "50Hertz", "Amprion", "TenneT", "TransnetBW"]

    new = xr.Dataset(
        {
            "generation_mw": (("location_id", "time_utc"), gen_all),
            "capacity_mwp": (("location_id", "time_utc"), cap_all),
        },
        coords={
            "location_id": np.array([0, 1, 2, 3, 4]),
            "time_utc": ds.time_utc.values,
            "latitude": ("location_id", lat),
            "longitude": ("location_id", lon),
            "tso_name": ("location_id", names),
        },
        attrs={
            "source": "ENTSO-E per control area, psr_type=B16",
            "built_from": SRC,
            "capacity_basis": ds.attrs.get(
                "capacity_basis",
                "inherited from v3 - trailing 365-day p99.9 envelope of own generation",
            ),
            "location_convention": "0 = national sum, 1-4 = TSOs",
            "created": str(pd.Timestamp.now()),
        },
    )

    new.to_zarr(OUT, mode="w")

    print("WRITTEN", OUT)
    print("location_ids:", new.location_id.values)
    print("national cap mean:", round(float(nat_cap.mean()), 0), "MW")
    print("gen dims:", new.generation_mw.dims, new.generation_mw.shape)


if __name__ == "__main__":
    main()
