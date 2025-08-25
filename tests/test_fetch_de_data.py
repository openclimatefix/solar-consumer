import pytest
import requests
import pandas as pd
from solar_consumer.data.fetch_de_data import fetch_de_data

# Combined XML fixture: includes wind offshore (B18), wind onshore (B19)
# and solar (A-10Y1001A1001A83H), as shown in ENTSOE API docs
SAMPLE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<GL_MarketDocument>
  <TimeSeries>
    <MktPSRType><psrType>B18</psrType></MktPSRType>
    <inBiddingZone_Domain><Mrid>WIND_ZONE</Mrid></inBiddingZone_Domain>
    <Period>
      <Point>
        <timeInterval><start>2025-07-11T00:00Z</start></timeInterval>
        <quantity>100</quantity>
      </Point>
    </Period>
  </TimeSeries>
  <TimeSeries>
    <MktPSRType><psrType>B19</psrType></MktPSRType>
    <inBiddingZone_Domain><Mrid>WIND_ZONE</Mrid></inBiddingZone_Domain>
    <Period>
      <Point>
        <timeInterval><start>2025-07-11T01:00Z</start></timeInterval>
        <quantity>150</quantity>
      </Point>
    </Period>
  </TimeSeries>
  <TimeSeries>
    <MktPSRType><psrType>A-10Y1001A1001A83H</psrType></MktPSRType>
    <inBiddingZone_Domain><Mrid>TEST_ZONE</Mrid></inBiddingZone_Domain>
    <Period>
      <Point>
        <timeInterval><start>2025-07-11T02:00Z</start></timeInterval>
        <quantity>1.00</quantity>
      </Point>
      <Point>
        <timeInterval><start>2025-07-11T03:00Z</start></timeInterval>
        <quantity>2.00</quantity>
      </Point>
    </Period>
  </TimeSeries>
</GL_MarketDocument>
"""

class DummyResp:
    def __init__(self, status_code = 200, content = SAMPLE_XML):
        self.status_code = status_code
        self.content = content.encode('utf-8')
        
    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise requests.HTTPError(f"HTTP {self.status_code}")
          
    @property
    def url(self):
        return "https://dummy.entsoe.eu/api"

@pytest.fixture(autouse=True)
def _mock_session_get(monkeypatch, request):
    # Monkey-patch requests.Session.get unless marked @live
    if "live" not in request.keywords:
        def dummy_get(self, url, params=None):
            return DummyResp()
        monkeypatch.setattr(requests.Session, "get", dummy_get)
    yield
    
@pytest.fixture(autouse=True)
def _set_entsoe_key(monkeypatch):
    #Make sure code under test sees non-empty API key
    monkeypatch.setenv("ENTSOE_API_KEY", "dummy")
    monkeypatch.setattr(de_mod, "API_KEY", "dummy", raising=False)

def test_only_solar_rows_returned():
    df = fetch_de_data()
    # 2 points, 3 cols, all from TEST_ZONE
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 3) and all(df['tso_zone'] == 'TEST_ZONE')

def test_quantity_and_timestamp_conversion():
    df = fetch_de_data()
    # Check kilowatts conversion and timesatmps dtype check
    assert df.iloc[0]["solar_generation_kw"] == pytest.approx(1_000)
    assert pd.api.types.is_datetime64tz_dtype(df['target_datetime_utc'])

def test_assert_on_invalid_mode():
    with pytest.raises(AssertionError):
        fetch_de_data(historic_or_forecast = 'forecast')


def test_http_error(monkeypatch):
    class BadResp(DummyResp):
        def __init__(self):
            super().__init__(status_code=500)
    monkeypatch.setattr(requests.Session, 'get', lambda self, url, params=None: BadResp())
    with pytest.raises(requests.HTTPError):
        fetch_de_data()
        
def test_range_fetch_returns_rows():
    # 2-hour window spanning the 2 sample points in SAMPLE_XML
    start = pd.Timestamp("2025-07-11T02:00Z")
    end = pd.Timestamp("2025-07-11T04:00Z")
    df = fetch_de_data.fetch_de_data_range(start.to_pydatetime(), end.to_pydatetime(), 
                                           chunk_hours = 1)
    assert not df.empty
    assert {"target_datetime_utc", "solar_generation_kw", "tso_zone"} <= set(df.columns)
    
    #should be 2 points, - both solar and zone as in the fitxure
    assert df.shape == (2, 3) and all(df["tso_zone"] == "TEST_ZONE")

def test_range_fetch_handles_empty_windows():
    # Time outside sample XML gives mocked response, but this ensures the function doesn't 
    # error and returns expected columns when empty
    start = pd.Timestamp("1999-01-01T00:00Z")
    end = pd.Timestamp("1999-01-01T01:00Z")
    df = fetch_de_data.fetch_de_data_range(start.to_pydatetime(), end.to_pydatetime(), 
                                           chunk_hours = 1)
    assert isinstance(df, pd.DataFrame)
    assert {"target_datetime_utc", "solar_generation_kw", "tso_zone"} <= set(df.columns)


# Live test only executes if $env ENT­SOE_API_KEY set
@pytest.mark.skip(reason = "Live ENTSOE endpoint often returns empty rows for the most recent 24h;\
                  mocked suite covers parsing")
@pytest.mark.live
def test_live_fetch_returns_rows():
    df = fetch_de_data()
    assert not df.empty
    assert {"target_datetime_utc", "solar_generation_kw", "tso_zone"} <= set(df.columns)
