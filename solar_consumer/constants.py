# Belgium (Elia) API URLs
BE_FORECAST_URL = (
    "https://opendata.elia.be/api/explore/v2.1/"
    "catalog/datasets/ods032/records"
)
BE_GENERATION_URL = (
    "https://opendata.elia.be/api/explore/v2.1/"
    "catalog/datasets/ods087/records"
)

# Germany (ENTSO-E)
# The ENTSO-E psrType for solar generation
DE_SOLAR_PSR_TYPE = "B16"

# The ENTSO-E areas we fetch generation for, keyed by the data platform join key.
# "DE" is the whole of Germany (national), the others are the four German control areas (TSOs).
DE_AREAS = {
    "de": "DE",
    "50hertz": "DE_50HZ",
    "amprion": "DE_AMPRION",
    "tennet": "DE_TENNET",
    "transnetbw": "DE_TRANSNET",
}

# TSO names, as used by the legacy site database sites
DE_TSO_NAMES = {
    "50hertz": "50Hertz",
    "amprion": "Amprion",
    "tennet": "TenneT",
    "transnetbw": "TransnetBW",
}

# Great Britain - NESO forecast API URL
GB_NESO_FORECAST_URL = (
    "https://api.neso.energy/api/3/action/datapackage_show"
    "?id=embedded-wind-and-solar-forecasts"
)

# Great Britain - NESO datastore SQL API URL
GB_NESO_DATASTORE_URL = "https://api.neso.energy/api/3/action/datastore_search_sql"

# Great Britain - PVLive domain URL
GB_PVLIVE_DOMAIN_URL = "api.pvlive.uk"

# Netherlands (NED) API base URL
NL_BASE_URL = "https://api.ned.nl/v1"

# India - Rajasthan SLDC data URL
IND_RAJASTHAN_URL = "http://sldc.rajasthan.gov.in/rrvpnl/read-sftp?type=overview"
