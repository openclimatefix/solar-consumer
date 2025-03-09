""" Read functions for models"""

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from nowcasting_datamodel.models import MLModelSQL
from nowcasting_datamodel.models.forecast import ForecastSQL
from nowcasting_datamodel.read.read import logger


def get_models(
    session, with_forecasts: Optional[bool] = False, forecast_created_utc: Optional[datetime] = None
) -> MLModelSQL:
    """
    Get all models from the database, distinct on name

    Can also filter if the model has forecasts, and if they are made after a certain time

    :param session: sql session
    :param with_forecasts: only get models with forecasts
    :param forecast_created_utc: only look at forecast created after this time
    :return:
    """

    query = session.query(MLModelSQL)
    query = query.distinct(MLModelSQL.name)

    if with_forecasts:
        query = query.join(ForecastSQL)

        if forecast_created_utc:
            query = query.filter(ForecastSQL.created_utc > forecast_created_utc)

    # order by
    query = query.order_by(MLModelSQL.name)

    # get all results
    models = query.all()

    return models


def get_model(session: Session, name: str, version: Optional[str] = None) -> MLModelSQL:
    """
    Get model object from name and version

    :param session: database session
    :param name: name of the model
    :param version: version of the model

    return: Model object

    """

    # start main query
    query = session.query(MLModelSQL)

    # filter on gsp_id
    query = query.filter(MLModelSQL.name == name)
    if version is not None:
        query = query.filter(MLModelSQL.version == version)

    # gets the latest version
    query = query.order_by(MLModelSQL.version.desc())

    # get all results
    models = query.all()

    if len(models) == 0:
        logger.debug(
            f"Model for name {name} and version {version} does not exist so going to add it"
        )

        model = MLModelSQL(name=name, version=version)
        session.add(model)
        session.commit()

    else:
        model = models[0]

    return model
