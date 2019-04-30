from db_adapter.temp.base import get_engine, CurwFcstBase
from db_adapter.temp.constants import DIALECT_MYSQL, DRIVER_PYMYSQL

from db_adapter.temp.logger import logger


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
    CurwFcstBase.metadata.create_all(engine)

    logger.info("test_schema schema generated.")


