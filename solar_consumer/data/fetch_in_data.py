from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def fetch_in_data(historic_or_forecast="generation"):
    if historic_or_forecast != "generation":
        raise NotImplementedError("Only 'generation' mode is supported for India UPSLDC.")

    # Setup headless browser
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get("https://www.upsldc.org/real-time-data")
        driver.implicitly_wait(5)  # wait for JS to load

        html = driver.page_source
        soup = BeautifulSoup(html, "html.parser")
        all_cells = soup.find_all("td")

        solar_value = None
        for i, cell in enumerate(all_cells):
            if "solar generation" in cell.text.strip().lower():
                try:
                    solar_value = float(all_cells[i + 1].text.strip())
                    break
                except Exception as e:
                    raise ValueError(f"Found label but couldn't parse value: {e}")

        if solar_value is None:
            raise ValueError("Solar generation value not found in UPSLDC page")

        # Timestamp = current UTC time
        ist_now = datetime.now()
        utc_now = ist_now - pd.Timedelta(hours=5, minutes=30)

        df = pd.DataFrame([{
            "target_datetime_utc": utc_now,
            "solar_generation_kw": solar_value * 1000
        }])

        return df

    finally:
        driver.quit()
