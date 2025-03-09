""" Pydantic and Sqlalchemy models for the database

The following class are made
1. Reusable classes (utils.py)
2. Location objects, where the forecast is for (models.py)
3. Model object, what forecast model is used (models.py)
4. ForecastValue objects, specific values of a forecast and time (models.py)
5. Input data status, shows when the data was collected (models.py)
6. Forecasts, a forecast that is made for one gsp,
    for several time steps into the future (models.py)
7. PV system for storing PV data (pv.py)
8. PV yield for storing PV data (pv.py)

Current these models have a primary index of 'id'.
This keeps things very simple at the start.
But there can be multiple forecasts for similar values.

Later on it would be good to add a forecast latest table,
    where the latest forecast can be read.
The primary keys could be 'gsp_id' and 'target_datetime_utc'.
"""

from .api import *  # noqa F403
from .forecast import *  # noqa F403
from .gsp import *  # noqa F403
from .metric import *  # noqa F403
from .models import *  # noqa F403
from .pv import *  # noqa F403
