import pandas as pd
import urllib.request
import urllib.parse
import json


def fetch_gb_data():
    """
    Fetch data from the NESO API and process it into a Pandas DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing two columns:
                      - `Datetime_GMT`: Combined date and time in UTC.
                      - `solar_forecast_kw`: Estimated solar forecast in kW.
    """

    meta_url = "https://api.neso.energy/api/3/action/datapackage_show?id=embedded-wind-and-solar-forecasts"
    response = urllib.request.urlopen(meta_url)
    data = json.loads(response.read().decode("utf-8"))

    # we take the latest path, which is the most recent forecast
    url = data["result"]["resources"][0]["path"]

    df = pd.read_csv(url)

    # Parse and combine DATE_GMT and TIME_GMT into Datetime_GMT
    df["Datetime_GMT"] = pd.to_datetime(
        df["DATE_GMT"].str[:10] + " " + df["TIME_GMT"].str.strip(),
        format="%Y-%m-%d %H:%M",
        errors="coerce",
    ).dt.tz_localize("UTC")

    # Rename and select necessary columns
    df["solar_forecast_kw"] = df["EMBEDDED_SOLAR_FORECAST"] * 1000
    df = df[["Datetime_GMT", "solar_forecast_kw"]]

    # Drop rows with invalid Datetime_GMT
    df = df.dropna(subset=["Datetime_GMT"])

    # rename columns to match the schema
    df.rename(
        columns={
            "solar_forecast_kw": "solar_generation_kw",
            "Datetime_GMT": "target_datetime_utc",
        },
        inplace=True,
    )

    return df
