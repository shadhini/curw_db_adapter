import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import CURW_OBS_DATABASE, CURW_OBS_PASSWORD, CURW_OBS_USERNAME, CURW_OBS_PORT, CURW_OBS_HOST
from db_adapter.constants import HOST, PASSWORD, PORT, DATABASE, USERNAME
from db_adapter.curw_sim.timeseries import Timeseries
from db_adapter.curw_sim.common import process_5_min_ts, process_15_min_ts, \
    extract_obs_rain_5_min_ts, extract_obs_rain_15_min_ts
from db_adapter.logger import logger


# for bulk insertion for a given one grid interpolation method
def update_rainfall_obs(target_model, method, grid_interpolation, timestep):

    """
    Update rainfall observations for flo2d models
    :param model: target model
    :param method: value interpolation method
    :param grid_interpolation: grid interpolation method
    :param timestep:
    :return:
    """

    now = datetime.now()
    OBS_START_STRING = (now - timedelta(days=10)).strftime('%Y-%m-%d %H:00:00')
    OBS_START = datetime.strptime(OBS_START_STRING, '%Y-%m-%d %H:%M:%S')

    try:

        # Connect to the database
        curw_obs_pool = get_Pool(host=CURW_OBS_HOST, user=CURW_OBS_USERNAME, password=CURW_OBS_PASSWORD,
                port=CURW_OBS_PORT, db=CURW_OBS_DATABASE)

        curw_obs_connection = curw_obs_pool.connection()

        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        TS = Timeseries(pool=curw_sim_pool)

        # [hash_id, station_id, station_name, latitude, longitude]
        active_obs_stations = read_csv('grids/obs_stations/rainfall/curw_active_rainfall_obs_stations.csv')
        obs_stations_dict = { }  # keys: obs station id , value: [hash id, name, latitude, longitude]

        for obs_index in range(len(active_obs_stations)):
            obs_stations_dict[active_obs_stations[obs_index][1]] = [active_obs_stations[obs_index][0],
                                                                    active_obs_stations[obs_index][2],
                                                                    active_obs_stations[obs_index][3],
                                                                    active_obs_stations[obs_index][4]]

        for obs_id in obs_stations_dict.keys():
            obs_start = OBS_START
            meta_data = {
                    'latitude': float('%.6f' % float(obs_stations_dict.get(obs_id)[2])),
                    'longitude': float('%.6f' % float(obs_stations_dict.get(obs_id)[3])),
                    'model': target_model, 'method': method,
                    'grid_id': 'rainfall_{}_{}'.format(obs_stations_dict.get(obs_id)[1], grid_interpolation)
                    }

            tms_id = TS.get_timeseries_id_if_exists(meta_data=meta_data)

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                TS.insert_run(meta_data=meta_data)

            obs_end = TS.get_obs_end(id_=tms_id)

            if obs_end is not None:
                obs_start = obs_end

            obs_hash_id = obs_stations_dict.get(obs_id)[0]

            obs_timeseries = []

            if timestep == 5:
                ts = extract_obs_rain_5_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs_hash_id)
                if ts is not None and len(ts) > 1:
                    obs_timeseries.extend(process_5_min_ts(newly_extracted_timeseries=ts, expected_start=obs_start)[1:])
                    # obs_start = ts[-1][0]
            elif timestep == 15:
                ts = extract_obs_rain_15_min_ts(connection=curw_obs_connection, start_time=obs_start, id=obs_hash_id)
                if ts is not None and len(ts) > 1:
                    obs_timeseries.extend(process_15_min_ts(newly_extracted_timeseries=ts, expected_start=obs_start)[1:])
                    # obs_start = ts[-1][0]

            for i in range(len(obs_timeseries)):
                if obs_timeseries[i][1] == -99999:
                    obs_timeseries[i][1] = 0

            if obs_timeseries is not None and len(obs_timeseries) > 0:
                TS.insert_data(timeseries=obs_timeseries, tms_id=tms_id, upsert=True)
                TS.update_latest_obs(id_=tms_id, obs_end=(obs_timeseries[-1][1]))

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred while updating obs rainfalls in curw_sim.")
    finally:
        curw_obs_connection.close()
        destroy_Pool(pool=curw_sim_pool)
        destroy_Pool(pool=curw_obs_pool)
