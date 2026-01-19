import docker
import pandas as pd
import pytest

from solar_consumer.save.save_site_database import save_generation_to_site_db


docker_available = True
try:
    docker.from_env().ping()
except Exception:
    docker_available = False

pytestmark = pytest.mark.skipif(
    not docker_available,
    reason="Docker not available for testcontainers-based test",
)


def test_save_ruvnl_generation_to_site_db(db_site_session):
    """
    Ensure RUVNL (India) generation data with both solar and wind
    can be saved to the site_database without errors.
    """
    df = pd.DataFrame(
        {
            "target_datetime_utc": pd.date_range(
                "2024-01-01", periods=4, freq="15min", tz="UTC"
            ),
            "solar_generation_kw": [1000, 1200, 900, 800],
            "energy_type": ["solar", "wind", "solar", "wind"],
        }
    )

    save_generation_to_site_db(
        generation_data=df,
        session=db_site_session,
        country="in",
    )
