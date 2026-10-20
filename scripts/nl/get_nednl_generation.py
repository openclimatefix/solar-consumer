"""
Pull 15-minute regional solar generation from the NED.nl API.

Loops over the 13 NED.nl points (0 = national, 1-12 = provinces) one day at a
time and writes everything to a single CSV.

Note: the API's "capacity" field is actually generation (kW), not installed
capacity. Installed capacity = capacity / percentage. NED.nl also revised its
historic data on 16 Dec 2025, so fresh pulls may not match older stored data.

Usage:
    export APIKEY_NEDNL=...
    python scripts/nl/get_nednl_generation.py --start 2025-01-01 --end 2026-01-01 --out nednl_2025.csv
"""

import argparse
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from tqdm import tqdm

URL = "https://api.ned.nl/v1/utilizations"
N_POINTS = 13


def fetch(session, params, max_retries=3, initial_delay=5):
    for attempt in range(max_retries):
        try:
            r = session.get(URL, params=params, allow_redirects=False)
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return []

        if r.status_code == 200:
            time.sleep(0.1)  # stay under the rate limit (~180 requests / 5 min)
            return r.json()["hydra:member"]
        if r.status_code == 429:
            wait = initial_delay * 2**attempt
            print(f"Rate limited, waiting {wait}s")
            time.sleep(wait)
            continue

        print(f"Error {r.status_code}: {r.text[:200]}")
        return []

    print("Max retries reached")
    return []


def main(start, end, out):
    session = requests.Session()
    session.headers.update({
        "X-AUTH-TOKEN": os.environ["APIKEY_NEDNL"],
        "Accept": "application/ld+json",
    })

    n_days = (end - start).days
    rows = []
    with tqdm(total=n_days * N_POINTS) as pbar:
        for point in range(N_POINTS):
            day = start
            while day < end:
                nxt = day + timedelta(days=1)
                rows += fetch(session, {
                    "point": point,
                    "type": 2,  # solar
                    "granularity": 4,  # 15 min
                    "granularitytimezone": 0,  # UTC
                    "classification": 2,  # measured (1 = forecast)
                    "activity": 1,  # production
                    "validfrom[after]": day.strftime("%Y-%m-%d"),
                    "validfrom[strictly_before]": nxt.strftime("%Y-%m-%d"),
                })
                day = nxt
                pbar.update(1)

    if not rows:
        raise SystemExit("No data returned")

    df = pd.DataFrame({
        "site_id": [int(u["point"].rsplit("/", 1)[-1]) for u in rows],
        "capacity (kW)": [u["capacity"] for u in rows],
        "volume (kWh)": [u["volume"] for u in rows],
        "percentage": [u["percentage"] for u in rows],
        "validfrom (UTC)": pd.to_datetime([u["validfrom"] for u in rows]),
        "validto (UTC)": pd.to_datetime([u["validto"] for u in rows]),
        "lastupdate (UTC)": pd.to_datetime([u["lastupdate"] for u in rows]),
    }).sort_values(["site_id", "validfrom (UTC)"])

    df.to_csv(out, index=False)
    print(f"Saved {len(df)} rows to {out}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True, type=datetime.fromisoformat)
    p.add_argument("--end", required=True, type=datetime.fromisoformat)
    p.add_argument("--out", default="nednl_15m_solar_regional.csv")
    a = p.parse_args()
    main(a.start, a.end, a.out)
