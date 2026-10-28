"""
Pull 15-minute regional solar generation from the NED.nl API.

Loops over the 13 NED.nl points (0 = national, 1-12 = provinces) one day at a
time and writes everything to a single CSV.

Note: the API's "capacity" field is actually generation (kW), not installed
capacity. Installed capacity = capacity / percentage. NED.nl also revised its
historic data on 16 Dec 2025, so fresh pulls may not match older stored data.

Requests are paced to stay under the API limit of ~180 requests per 5 minutes,
so a full year (13 points x 365 days) takes around 2 hours.

Usage:
    export APIKEY_NEDNL=...
    python scripts/nl/get_nednl_generation.py --start 2025-01-01 --end 2026-01-01 --out nednl_2025.csv
"""

import os
import time
from datetime import timedelta

import click
import pandas as pd
import requests
from tqdm import tqdm

API_KEY = os.getenv("APIKEY_NEDNL")

URL = "https://api.ned.nl/v1/utilizations"
N_POINTS = 13
REQUEST_INTERVAL_S = 300 / 180  # at most 180 requests per 5 minutes


class FetchError(RuntimeError):
    """Raised when a request fails for good, so the pull stops instead of saving partial data."""


def fetch(session, params, max_retries=5, initial_delay=60):
    for attempt in range(max_retries):
        try:
            r = session.get(URL, params=params, allow_redirects=False, timeout=60)
        except requests.RequestException as e:
            raise FetchError(f"Request failed for {params}: {e}") from e

        if r.status_code == 200:
            time.sleep(REQUEST_INTERVAL_S)
            return r.json()["hydra:member"]
        if r.status_code == 429:
            wait = initial_delay * 2**attempt
            print(f"Rate limited, waiting {wait}s")
            time.sleep(wait)
            continue

        raise FetchError(f"Error {r.status_code} for {params}: {r.text[:200]}")

    raise FetchError(f"Max retries reached for {params}")


@click.command()
@click.option("--start", required=True, type=click.DateTime(["%Y-%m-%d"]), help="Start date (inclusive)")
@click.option("--end", required=True, type=click.DateTime(["%Y-%m-%d"]), help="End date (exclusive)")
@click.option("--out", default="nednl_15m_solar_regional.csv", show_default=True, help="Output CSV path")
def main(start, end, out):
    if not API_KEY:
        raise click.ClickException("APIKEY_NEDNL environment variable is not set")

    session = requests.Session()
    session.headers.update({
        "X-AUTH-TOKEN": API_KEY,
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
        raise click.ClickException("No data returned")

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
    main()
