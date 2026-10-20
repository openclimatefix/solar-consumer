"""
Convert a NED.nl regional solar CSV (from get_ned_nl_generation.py) to zarr.

Output has dims (location_id, time_utc) with variables generation_mw and
capacity_mwp, plus latitude/longitude coords taken from a site metadata CSV
(columns: site_id, latitude, longitude).

The API's "capacity (kW)" column is really generation, so installed capacity is
worked out as generation / percentage, then forward/back-filled per site to
cover night-time gaps.

If --existing is given, only timestamps after the end of that zarr are added,
and the combined data is written to --out.

Usage:
    python nednl_csv_to_zarr.py --csv nednl_2025.csv --meta site_meta.csv --out NL_regional.zarr
    python nednl_csv_to_zarr.py --csv nednl_2025.csv --meta site_meta.csv \
        --existing NL_regional.zarr --out NL_regional_with_2025.zarr
"""

import argparse

import pandas as pd
import xarray as xr


def csv_to_dataset(csv_path, meta_path):
    df = pd.read_csv(csv_path)
    df = df.rename(columns={"site_id": "location_id", "capacity (kW)": "generation_kw"})
    df["time_utc"] = pd.to_datetime(df["validfrom (UTC)"], utc=True).dt.tz_localize(None)
    df = df.sort_values(["location_id", "time_utc"]).drop_duplicates(["location_id", "time_utc"])

    # Installed capacity, only where percentage is a valid fraction
    valid = (df["percentage"] > 0) & (df["percentage"] <= 1)
    df["capacity_kw"] = (df["generation_kw"] / df["percentage"]).where(valid)
    df["capacity_kw"] = df.groupby("location_id")["capacity_kw"].transform(lambda s: s.ffill().bfill())

    df["generation_mw"] = df["generation_kw"] / 1000
    df["capacity_mwp"] = df["capacity_kw"] / 1000

    ds = df.set_index(["location_id", "time_utc"])[["generation_mw", "capacity_mwp"]].to_xarray()

    meta = pd.read_csv(meta_path).set_index("site_id").loc[ds.location_id.values]
    return ds.assign_coords(
        latitude=("location_id", meta["latitude"].values),
        longitude=("location_id", meta["longitude"].values),
    )


def main(csv, meta, out, existing=None):
    ds = csv_to_dataset(csv, meta)
    print(f"CSV data: {ds.time_utc.values[0]} to {ds.time_utc.values[-1]}, {dict(ds.sizes)}")

    if existing:
        ds_old = xr.open_zarr(existing, chunks=None)
        cutoff = ds_old.time_utc.values[-1]
        ds_new = ds.sel(time_utc=ds.time_utc > cutoff)
        if ds_new.sizes["time_utc"] == 0:
            raise SystemExit(f"No timestamps after {cutoff} to add")
        ds = xr.concat([ds_old, ds_new], dim="time_utc").sortby("time_utc")

    ds.drop_encoding().to_zarr(out, mode="w")

    check = xr.open_zarr(out, chunks=None)
    print(f"Saved {out}: {check.time_utc.values[0]} to {check.time_utc.values[-1]}, {dict(check.sizes)}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="CSV from get_ned_nl_generation.py")
    p.add_argument("--meta", required=True, help="Site metadata CSV with site_id, latitude, longitude")
    p.add_argument("--out", required=True, help="Output zarr path")
    p.add_argument("--existing", help="Optional existing zarr to append to")
    a = p.parse_args()
    main(a.csv, a.meta, a.out, a.existing)
