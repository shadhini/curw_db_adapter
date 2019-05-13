from db_adapter.base import get_engine, CurwFcstBase
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL
from db_adapter.curw_fcst.models import Run, Data, Variable, Unit, Station, Source

from db_adapter.logger import logger


USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "curw_fcst"

print("Create test schema")


def create_test_schema_db():

    # connect to the MySQL engine
    engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, HOST, PORT, DATABASE,
            USERNAME, PASSWORD)

    # create the schema using classes defined
    CurwFcstBase.metadata.create_all(engine)

    logger.info("test_schema schema generated.")


create_test_schema_db()