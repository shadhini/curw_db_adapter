import traceback
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_fcst.timeseries import Timeseries


try:

    USERNAME = "root"
    PASSWORD = "password"
    HOST = "127.0.0.1"
    PORT = 3306
    DATABASE = "curw_fcst"

    pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)

    ts = Timeseries(pool=pool)


    ts.update_latest_fgt(id_="02fef97c984cf709f98b57c7278f6b8ccdd6ae165c68e204c2695d8c7fb8e32e", fgt="2019-07-20 23:00:00")

except Exception as e:
    traceback.print_exc()
finally:
    destroy_Pool(pool=pool)
    print("Process Finished.")
