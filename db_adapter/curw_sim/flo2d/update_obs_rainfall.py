import traceback
import pymysql
from datetime import datetime, timedelta

from db_adapter.csv_utils import read_csv
from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import HOST, PASSWORD, PORT, DATABASE, USERNAME
from db_adapter.curw_sim.grids import get_flo2d_cells_to_obs_grid_mappings
from db_adapter.curw_sim.timeseries import Timeseries
from db_adapter.curw_sim.common import process_5_min_ts, process_15_min_ts, fill_missing_values, \
    extract_obs_rain_5_min_ts, extract_obs_rain_15_min_ts
from db_adapter.logger import logger
from db_adapter.constants import COMMON_DATE_TIME_FORMAT


# for bulk insertion for a given one grid interpolation method
def update_rainfall_obs(flo2d_model, method, grid_interpolation, timestep):

    """
    Update rainfall observations for flo2d models
    :param flo2d_model: flo2d model
    :param method: value interpolation method
    :param grid_interpolation: grid interpolation method
    :param timestep: output timeseries timestep
    :return:
    """

    now = datetime.now()
    OBS_START_STRING = (now - timedelta(days=10)).strftime('%Y-%m-%d %H:00:00')
    OBS_START = datetime.strptime(OBS_START_STRING, '%Y-%m-%d %H:%M:%S')

    try:

        # Connect to the database
        curw_connection = pymysql.connect(host='104.198.0.87',
                user='root',
                password='cfcwm07',
                db='curw',
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor)

        pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        # test ######
        # pool = get_Pool(host=HOST, user=USERNAME, password=PASSWORD, port=PORT, db=DATABASE)

        TS = Timeseries(pool=pool)

        # [hash_id, run_name, station_id, station_name, latitude, longitude]
        active_obs_stations = read_csv('grids/obs_stations/rainfall/curw_active_rainfall_obs_stations.csv')
        flo2d_grids = read_csv('grids/flo2d/{}m.csv'.format(flo2d_model))  # [Grid_ ID, X(longitude), Y(latitude)]

        stations_dict_for_obs = { }  # keys: obs station id , value: hash id

        for obs_index in range(len(active_obs_stations)):
            stations_dict_for_obs[active_obs_stations[obs_index][2]] = active_obs_stations[obs_index][0]

        flo2d_obs_mapping = get_flo2d_cells_to_obs_grid_mappings(pool=pool, grid_interpolation=grid_interpolation, flo2d_model=flo2d_model)

        for flo2d_index in range(len(flo2d_grids)):
            obs_start = OBS_START
            lat = flo2d_grids[flo2d_index][2]
            lon = flo2d_grids[flo2d_index][1]
            cell_id = flo2d_grids[flo2d_index][0]
            meta_data = {
                    'latitude': float('%.6f' % float(lat)), 'longitude': float('%.6f' % float(lon)),
                    'model': flo2d_model, 'method': method,
                    'grid_id': '{}_{}_{}'.format(flo2d_model, grid_interpolation, (str(cell_id)).zfill(10))
                    }

            tms_id = TS.get_timeseries_id(grid_id=meta_data.get('grid_id'), method=meta_data.get('method'))

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                TS.insert_run(meta_data=meta_data)

            obs_end = TS.get_obs_end(id_=tms_id)

            if obs_end is not None:
                obs_start = obs_end

            obs1_hash_id = stations_dict_for_obs.get(str(flo2d_obs_mapping.get(meta_data['grid_id'])[0]))
            obs2_hash_id = stations_dict_for_obs.get(str(flo2d_obs_mapping.get(meta_data['grid_id'])[1]))
            obs3_hash_id = stations_dict_for_obs.get(str(flo2d_obs_mapping.get(meta_data['grid_id'])[2]))

            obs_timeseries = []

            if timestep == 5:
                ts = extract_obs_rain_5_min_ts(connection=curw_connection, start_time=obs_start, id=obs1_hash_id)
                if ts is not None and len(ts) > 1:
                    obs_timeseries.extend(process_5_min_ts(newly_extracted_timeseries=ts, expected_start=obs_start)[1:])
                    # obs_start = ts[-1][0]

                ts2 = extract_obs_rain_5_min_ts(connection=curw_connection, start_time=obs_start, id=obs2_hash_id)
                if ts2 is not None and len(ts2) > 1:
                    obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts2, OBS_TS=obs_timeseries)
                    if obs_timeseries is not None and len(obs_timeseries) > 0:
                        expected_start = obs_timeseries[-1][0]
                    else:
                        expected_start= obs_start
                    obs_timeseries.extend(process_5_min_ts(newly_extracted_timeseries=ts2, expected_start=expected_start)[1:])
                    # obs_start = ts2[-1][0]

                ts3 = extract_obs_rain_5_min_ts(connection=curw_connection, start_time=obs_start, id=obs3_hash_id)
                if ts3 is not None and len(ts3) > 1 and len(obs_timeseries) > 0:
                    obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts3, OBS_TS=obs_timeseries)
                    if obs_timeseries is not None:
                        expected_start = obs_timeseries[-1][0]
                    else:
                        expected_start= obs_start
                    obs_timeseries.extend(process_5_min_ts(newly_extracted_timeseries=ts3, expected_start=expected_start)[1:])
            elif timestep == 15:
                ts = extract_obs_rain_15_min_ts(connection=curw_connection, start_time=obs_start, id=obs1_hash_id)
                if ts is not None and len(ts) > 1:
                    obs_timeseries.extend(process_15_min_ts(newly_extracted_timeseries=ts, expected_start=obs_start)[1:])
                    # obs_start = ts[-1][0]

                ts2 = extract_obs_rain_15_min_ts(connection=curw_connection, start_time=obs_start, id=obs2_hash_id)
                if ts2 is not None and len(ts2) > 1:
                    obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts2, OBS_TS=obs_timeseries)
                    if obs_timeseries is not None and len(obs_timeseries) > 0:
                        expected_start = obs_timeseries[-1][0]
                    else:
                        expected_start = obs_start
                    obs_timeseries.extend(process_15_min_ts(newly_extracted_timeseries=ts2, expected_start=expected_start)[1:])
                    # obs_start = ts2[-1][0]

                ts3 = extract_obs_rain_15_min_ts(connection=curw_connection, start_time=obs_start, id=obs3_hash_id)
                if ts3 is not None and len(ts3) > 1 and len(obs_timeseries) > 0:
                    obs_timeseries = fill_missing_values(newly_extracted_timeseries=ts3, OBS_TS=obs_timeseries)
                    if obs_timeseries is not None:
                        expected_start = obs_timeseries[-1][0]
                    else:
                        expected_start = obs_start
                    obs_timeseries.extend(process_15_min_ts(newly_extracted_timeseries=ts3, expected_start=expected_start)[1:])

            for i in range(len(obs_timeseries)):
                if obs_timeseries[i][1] == -99999:
                    obs_timeseries[i][1] = 0

            if obs_timeseries is not None and len(obs_timeseries) > 0:
                logger.info("Update observed rainfall timeseries in curw_sim for id {}".format(tms_id))
                TS.insert_data(timeseries=obs_timeseries, tms_id=tms_id, upsert=True)
                logger.info("Update latest obs {}".format(obs_timeseries[-1][1]))
                TS.update_latest_obs(id_=tms_id, obs_end=(obs_timeseries[-1][1]))

        destroy_Pool(pool=pool)

    except Exception as e:
        traceback.print_exc()
        logger.error("Exception occurred while updating obs rainfalls in curw_sim.")
    finally:
        logger.info("Process finished")


