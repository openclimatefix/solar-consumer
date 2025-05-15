import urllib.request
import json
import pandas as pd
from datetime import datetime

def fetch_in_data(historic_or_forecast="generation"):
    if historic_or_forecast != "generation":
        raise NotImplementedError("Only 'generation' is supported for India.")

    url = (
        "https://www.upsldc.org/real-time-data?"
        "p_p_id=upgenerationsummary_WAR_UPSLDCDynamicDisplayportlet"
        "&p_p_lifecycle=2"
        "&p_p_state=normal"
        "&p_p_mode=view"
        "&p_p_resource_id=realtimedata"
        "&p_p_cacheability=cacheLevelPage"
        "&p_p_col_id=column-1"
        "&p_p_col_count=1"
        "&_upgenerationsummary_WAR_UPSLDCDynamicDisplayportlet_cmd=realtimedata"
    )

    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read().decode("utf-8"))

        for item in data:
            obj = json.loads(item["daynamic_obj"])
            if "solar generation" in obj.get("point_desc", "").lower():
                solar_value = float(obj["point_val"])
                print(f"✅ Solar Generation: {solar_value} MW")

                utc_now = datetime.utcnow()

                return pd.DataFrame([{
                    "target_datetime_utc": utc_now,
                    "solar_generation_kw": solar_value * 1000  # Convert MW to kW
                }])

        raise ValueError("Solar generation entry not found in API response.")

    except Exception as e:
        raise RuntimeError(f"Error fetching UPSLDC API data: {e}")
