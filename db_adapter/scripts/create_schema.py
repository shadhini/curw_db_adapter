from db_adapter.base import get_engine, CurwFcstBase
from db_adapter.constants import (
    CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_HOST, CURW_FCST_PORT,
    CURW_FCST_DATABASE,
    )
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL
import sqlalchemy
from db_adapter.models import Run, Data, Station, Source, Variable, Unit


def create_curw_fcst_db():
    # connect to the MySQL engine

    engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_FCST_HOST, CURW_FCST_PORT, CURW_FCST_DATABASE,
            CURW_FCST_USERNAME, CURW_FCST_PASSWORD)

    # create the schema using classes defined
    CurwFcstBase.metadata.create_all(engine)



