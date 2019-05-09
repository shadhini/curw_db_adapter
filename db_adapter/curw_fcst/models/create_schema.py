from db_adapter.base import get_engine, CurwFcstBase
from db_adapter.constants import (
    CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_HOST, CURW_FCST_PORT,
    CURW_FCST_DATABASE,
    )
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL
from db_adapter.curw_fcst.models import Run, Data, Source, Variable, Unit, Station

from db_adapter.logger import logger


def create_curw_fcst_db():

    logger.info("Creating curw_fcst schema.")
    # connect to the MySQL engine

    engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_FCST_HOST, CURW_FCST_PORT, CURW_FCST_DATABASE,
            CURW_FCST_USERNAME, CURW_FCST_PASSWORD)

    # create the schema using classes defined
    CurwFcstBase.metadata.create_all(engine)

    logger.info("curw_fcst schema generated.")


