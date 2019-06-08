import traceback
import pymysql
from datetime import datetime, timedelta
import csv

from db_adapter.base import get_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.curw_sim.grids import get_flo2d_to_obs_grid_mappings, GridInterpolationEnum, \
    FLO2D_250, FLO2D_150, FLO2D_30
from db_adapter.curw_sim.timeseries import  Timeseries, MethodEnum


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][1:]

    return data


def extract_rain_ts(connection, id, start_time):
    """
    Extract obs station timeseries (15 min intervals)
    :param connection: connection to curw database
    :param stations_dict: dictionary with station_id as keys and run_ids as values
    :param start_time: start of timeseries
    :return:
    """

    timeseries = []

    try:
        # Extract per 5 min observed timeseries
        with connection.cursor() as cursor1:
            sql_statement = "select `time`, `value`  from data where `id`=%s and `time` > %s ;"
            print(id, start_time)
            rows = cursor1.execute(sql_statement, (id, start_time))
            if rows > 0:
                results = cursor1.fetchall()
                for result in results:
                    timeseries.append([result.get('time'), result.get('value')])
        if len(timeseries) > 1:
            print(id, "::", len(timeseries), "::", timeseries[1][0], "::", timeseries[-1][0])
        return timeseries

    except Exception as ex:
        traceback.print_exc()
    # finally:
    #     if connection is not None:
    #         connection.close()


# for bulk insertion for a given one grid interpolation method
def update_rainfall_obs(flo2d_model, method, grid_interpolation):

    """
    Update rainfall observations for flo2d models
    :param flo2d_model: flo2d model
    :param method: value interpolation method
    :param grid_interpolation: grid interpolation method
    :return:
    """

    print("Update rainfall observations")

    now = datetime.now()
    OBS_START = (now - timedelta(hours=12)).strftime('%Y-%m-%d %H:%M:%S')

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

        TS = Timeseries(pool=pool)

        active_obs_stations = read_csv('curw_active_rainfall_obs_stations.csv')
        flo2d_grids = read_csv('{}m.csv'.format(flo2d_model))

        stations_dict_for_obs = { }  # keys: obs station id , value: hash id

        for obs_index in range(len(active_obs_stations)):
            stations_dict_for_obs[active_obs_stations[obs_index][2]] = active_obs_stations[obs_index][0]

        flo2d_obs_mapping = get_flo2d_to_obs_grid_mappings(pool=pool, grid_interpolation=grid_interpolation, flo2d_model=flo2d_model)

        for flo2d_index in range(len(flo2d_grids)):
            obs_start = OBS_START
            meta_data = {
                    'latitude': float('%.6f' % float(flo2d_grids[flo2d_index][2])), 'longitude': float('%.6f' % float(flo2d_grids[flo2d_index][1])),
                    'model': flo2d_model, 'method': method,
                    'grid_id': '{}_{}_{}'.format(flo2d_model, flo2d_grids[flo2d_index][0], grid_interpolation)
                    }

            tms_id = TS.get_timeseries_by_grid_id(meta_data.get('grid_id'))

            if tms_id is None:
                tms_id = TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                print("{} : Insert entry to run table with id={}".format(datetime.now(), tms_id))
                TS.insert_run(meta_data=meta_data)

            obs_end = TS.get_obs_end(id_=tms_id)

            if obs_end is not None:
                obs_start = obs_end

            obs1_hash_id = stations_dict_for_obs.get(str(flo2d_obs_mapping.get(meta_data['grid_id'])[0]))
            obs2_hash_id = stations_dict_for_obs.get(str(flo2d_obs_mapping.get(meta_data['grid_id'])[1]))
            obs3_hash_id = stations_dict_for_obs.get(str(flo2d_obs_mapping.get(meta_data['grid_id'])[2]))

            obs_timeseries = []

            ts = extract_rain_ts(connection=curw_connection, start_time=obs_start, id=obs1_hash_id)
            if ts is not None and len(ts) > 1:
                obs_timeseries.extend(ts[1:])
                print("{} : Extracted timeseries from obs station 1 till {}".format(datetime.now(), ts[-1][0]))
                obs_start = ts[-1][0]

            ts2 = extract_rain_ts(connection=curw_connection, start_time=obs_start, id=obs2_hash_id)
            if ts2 is not None and len(ts2) > 1:
                obs_timeseries.extend(ts2[1:])
                print("{} : Extracted timeseries from obs station 2 till {}".format(datetime.now(), ts2[-1][0]))
                obs_start = ts2[-1][0]

            ts3 = extract_rain_ts(connection=curw_connection, start_time=obs_start, id=obs3_hash_id)
            if ts3 is not None and len(ts3) > 1:
                obs_timeseries.extend(ts3[1:])
                print("{} : Extracted timeseries from obs station 3 till {}".format(datetime.now(), ts3[-1][0]))

            print("{} : Insert timeseries to database".format(datetime.now()))
            TS.insert_data(timeseries=obs_timeseries, tms_id=tms_id, upsert=True)

            if obs_timeseries is not None and len(obs_timeseries) > 0:
                print("{} : Update latest obs {}".format(datetime.now(), obs_timeseries[-1][1]))
                TS.update_latest_obs(id_=tms_id, obs_end=(obs_timeseries[-1][1]))

    except Exception as e:
        traceback.print_exc()
    finally:
        pool.destroy()


method = MethodEnum.getAbbreviation(MethodEnum.MME)
grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

print("{} : ####### Insert obs rainfall for FLO2D 250 grids".format(datetime.now()))
update_rainfall_obs(flo2d_model=FLO2D_250, method=method, grid_interpolation=grid_interpolation)

print("{} : ####### Insert obs rainfall for FLO2D 150 grids".format(datetime.now()))
update_rainfall_obs(flo2d_model=FLO2D_150, method=method, grid_interpolation=grid_interpolation)

print("{} : ####### Insert obs rainfall for FLO2D 30 grids".format(datetime.now()))
update_rainfall_obs(flo2d_model=FLO2D_30, method=method, grid_interpolation=grid_interpolation)

print("{} : ####### obs rainfall insertion process finished #######".format(datetime.now()))

