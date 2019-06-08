import os
import time
from datetime import datetime, timedelta


def get_file_last_modified_time(file_path):
    # os.path.getmtime() returns modified time as seconds since the epoch
    # time.gmtime() converts seconds since the epoch	time.struct_time in UTC
    # time.localtime() converts seconds since the epoch	time.struct_time in localime

    # returns local time (UTC + 5 30)
    modified_time = time.gmtime(os.path.getmtime(file_path) + 19800)

    return time.strftime('%Y-%m-%d %H:%M:%S', modified_time)


print(get_file_last_modified_time("/home/shadhini/dev/repos/shadhini/curw_helpers/netcdf_utils/d03_RAINNC_2019-05-06_A.csv"))