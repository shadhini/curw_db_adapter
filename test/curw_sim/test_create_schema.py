from db_adapter.base import get_engine, CurwSimBase
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL
from db_adapter.curw_sim.models import Run, Data

from db_adapter.logger import logger


USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"

print("Create test schema")


def create_test_schema_db():

    # connect to the MySQL engine
    engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, HOST, PORT, DATABASE,
            USERNAME, PASSWORD)

    # create the schema using classes defined
    CurwSimBase.metadata.create_all(engine)

    logger.info("test_schema schema generated.")


create_test_schema_db()