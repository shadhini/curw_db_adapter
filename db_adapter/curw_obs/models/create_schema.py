from db_adapter.base import get_engine, CurwObsBase
from db_adapter.constants import (
    CURW_OBS_USERNAME, CURW_OBS_PASSWORD, CURW_OBS_HOST, CURW_OBS_PORT,
    CURW_OBS_DATABASE,
    )
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL

from db_adapter.logger import logger


def create_curw_obs_db():

    logger.info("Creating curw_obs schema.")
    # connect to the MySQL engine

    engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_OBS_HOST, CURW_OBS_PORT, CURW_OBS_DATABASE,
            CURW_OBS_USERNAME, CURW_OBS_PASSWORD)

    # create the schema using classes defined
    CurwObsBase.metadata.create_all(engine)

    logger.info("curw_obs schema generated.")


