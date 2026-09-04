import pandas as pd
from entsoe import EntsoePandasClient

API_KEY = ""
client  = EntsoePandasClient(api_key=API_KEY)

TSOS = {
    "50hz":     "DE_50HZ",
    "amprion":  "DE_AMPRION",
    "tennet":   "DE_TENNET",
    "transnet": "DE_TRANSNET",
}

MONTHS = [(2024, 5), (2025, 5), (2026, 5)]

for tso_name, tso_code in TSOS.items():
    chunks = []
    for year, month in MONTHS:
        print(f"Fetching {tso_name} {year}-{month:02d}...")
        start = pd.Timestamp(f"{year}-{month:02d}-01", tz="Europe/Berlin")
        end   = start + pd.offsets.MonthEnd(1) + pd.Timedelta("1D")
        try:
            df = client.query_generation(tso_code, start=start, end=end, psr_type="B16")
            df = df.iloc[:, 0].to_frame(name="solar_mw")
            df.index = df.index.tz_convert("UTC")
            df["year"]  = year
            df["month"] = month
            chunks.append(df)
            print(f"  {len(df)} rows")
        except Exception as e:
            print(f"  ERROR: {e}")

    if chunks:
        out = pd.concat(chunks).sort_index()
        path = f"data/power/entsoe_solar_{tso_name}.parquet"
        out.to_parquet(path, compression="snappy")
        print(f"Saved {path} — {len(out)} rows")
