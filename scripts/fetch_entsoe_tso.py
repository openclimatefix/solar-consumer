"""
Downloads ENTSO-E solar data for the four German TSOs.

Gets generation (B16) for 2020-2025 and installed capacity for 2020-2026.
Saves one parquet per zone per year to the cache folder. Rerunning skips
anything already downloaded.

Set BASE below and export ENTSOE_KEY first.
"""

import os
import pathlib

import pandas as pd
from entsoe import EntsoePandasClient

BASE = "PLACEHOLDER"

CACHE = pathlib.Path(BASE) / "data" / "power" / "entsoe_cache"

ZONES = [
    ("DE_50HZ", "50Hertz"),
    ("DE_AMPRION", "Amprion"),
    ("DE_TENNET", "TenneT"),
    ("DE_TRANSNET", "TransnetBW"),
]

GEN_YEARS = range(2020, 2026)
CAP_YEARS = range(2020, 2027)


def main():
    key = os.environ.get("ENTSOE_KEY")
    if not key:
        raise SystemExit("ENTSOE_KEY not set in environment")

    CACHE.mkdir(parents=True, exist_ok=True)
    client = EntsoePandasClient(api_key=key)

    for zone, name in ZONES:
        for yr in GEN_YEARS:
            f = CACHE / f"gen_{name}_{yr}.parquet"
            if f.exists():
                print(f"skip {name} {yr}", flush=True)
                continue
            start = pd.Timestamp(f"{yr}-01-01", tz="Europe/Berlin")
            end = pd.Timestamp(f"{yr + 1}-01-01", tz="Europe/Berlin")
            try:
                df = client.query_generation(zone, start=start, end=end, psr_type="B16")
                ser = df.iloc[:, 0] if getattr(df, "ndim", 1) > 1 else df
                ser.to_frame("mw").to_parquet(f)
                print(f"OK {name} {yr}: {len(ser)}", flush=True)
            except Exception as ex:
                print(f"FAIL {name} {yr}: {type(ex).__name__} {ex}", flush=True)

        f = CACHE / f"cap_{name}.parquet"
        if f.exists():
            print(f"skip cap {name}", flush=True)
            continue
        vals = {}
        for yr in CAP_YEARS:
            try:
                df = client.query_installed_generation_capacity(
                    zone,
                    start=pd.Timestamp(f"{yr}-01-01", tz="Europe/Berlin"),
                    end=pd.Timestamp(f"{yr}-12-31", tz="Europe/Berlin"),
                    psr_type="B16",
                )
                vals[pd.Timestamp(f"{yr}-01-01")] = float(df.iloc[0, 0])
            except Exception as ex:
                print(f"  cap {name} {yr} failed: {type(ex).__name__}", flush=True)
        pd.Series(vals).to_frame("mw").to_parquet(f)
        print(f"OK cap {name}", flush=True)

    print("DONE")


if __name__ == "__main__":
    main()
