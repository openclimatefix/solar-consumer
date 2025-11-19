# solar_consumer/data/uk_gsp_locations.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
from loguru import logger

# Directory this file lives in
_DIR = Path(__file__).parent

def _load_gsp_locations() -> pd.DataFrame | None:
    """
    Load the UK GSP latitude/longitude table from a bundled CSV.

    Returns
    -------
    pd.DataFrame | None
        DataFrame indexed by 'gsp_id' with at least columns ['latitude', 'longitude'].
        Returns None if the CSV cannot be found.
    """
    candidates = [
        _DIR / "uk_gsp_locations_20250109.csv",
        _DIR / "data" / "uk_gsp_locations_20250109.csv",
    ]
    for path in candidates:
        if path.exists():
            df = pd.read_csv(path)
            if "gsp_id" in df.columns:
                return df.set_index("gsp_id")

    logger.warning("GSP locations CSV not found; skipping night-time zeroing.")
    return None

# Public constant used by other modules
GSP_LOCATIONS: pd.DataFrame | None = _load_gsp_locations()

__all__ = ["GSP_LOCATIONS"]