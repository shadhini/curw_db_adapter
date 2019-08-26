import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import CURW_OBS_DATABASE, CURW_OBS_PASSWORD, CURW_OBS_USERNAME, CURW_OBS_PORT, CURW_OBS_HOST
from db_adapter.constants import HOST, PASSWORD, PORT, DATABASE, USERNAME
from db_adapter.curw_sim.timeseries.discharge import Timeseries
from db_adapter.logger import logger


# for bulk insertion for a given one grid interpolation method
def update_discharge_estimated(method, timeseries, station_name, target_model=None):
    # this is an hourly timeseries

    """
    Update rainfall observations for flo2d models
    :param target_model: target model
    :param method: value interpolation method
    :param timeseries: list of [time, value] pairs
    :return:
    """


    try:

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        TS = Timeseries(pool=curw_sim_pool)

        # [station_name,latitude,longitude,target]
        extract_stations = read_csv('grids/discharge_stations/extract_stations.csv')
        extract_stations_dict = { }  # keys: station_name , value: [latitude, longitude, target_model]

        for obs_index in range(len(extract_stations)):
            extract_stations_dict[extract_stations[obs_index][0]] = [extract_stations[obs_index][1],
                                                                     extract_stations[obs_index][2],
                                                                     extract_stations[obs_index][3]]
        if target_model is None:
            target_model = extract_stations_dict.get(station_name)[2]

        meta_data = {
            'latitude': float('%.6f' % float(extract_stations_dict.get(station_name)[0])),
            'longitude': float('%.6f' % float(extract_stations_dict.get(station_name)[1])),
            'model': target_model, 'method': method,
            'grid_id': 'discharge_{}'.format(station_name)
        }

        tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

        if tms_id is None:
            tms_id = TS.generate_timeseries_id(meta_data=meta_data)
            meta_data['id'] = tms_id
            TS.insert_run(meta_data=meta_data)

        if timeseries is not None and len(timeseries) > 0:
            TS.insert_data(timeseries=timeseries, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred while updating obs rainfalls in curw_sim.")
    finally:
        destroy_Pool(pool=curw_sim_pool)
