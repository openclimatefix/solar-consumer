import os
from datetime import datetime, timedelta, timezone
from solar_consumer.data.fetch_de_data import fetch_de_data_range


def main():
    # Backfill from 01/01/2020 to yesterday and write to CSV
    out_path = os.path.join("solar_consumer", "exports", "de_5_year_repopulate.csv")

    now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end = (now_utc - timedelta(days=1))

    # Start on 01/01/2020 for clean boundaries
    start = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Perform backfill using week-long chunks
    df = fetch_de_data_range(
        start, end, chunk_hours=168
    )  # Adjust if you hit API limits

    # Write to file (done with temp to avoid empty file if failure midway)
    temp = out_path + ".tmp"
    df.to_csv(temp, index=False)
    os.replace(temp, out_path)
    print(
        f"FINISHED: WROTE {len(df)} ROWS OF SOLAR GENERATION DATA TO FILE: {out_path}"
    )


if __name__ == "__main__":
    main()
