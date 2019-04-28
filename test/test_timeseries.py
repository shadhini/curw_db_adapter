from db_adapter.base import get_engine, get_sessionmaker
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL

from db_adapter.curw_fcst.timeseries import Timeseries

USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "curw_fcst"


engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, HOST, PORT, DATABASE,
            USERNAME, PASSWORD)


ts = Timeseries(engine)

print("########### Check Time Series Exist #################################")
# print(ts.is_id_exists("092a55ff5239a63047adbcdc8f15e4156f72ca4e1531ccf29205f6c59f25a382"))
print(ts.is_id_exists(""))