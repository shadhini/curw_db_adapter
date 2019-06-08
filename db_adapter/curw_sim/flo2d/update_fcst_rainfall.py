import traceback
import pymysql
from datetime import datetime, timedelta
import csv
import json

from db_adapter.base import get_Pool
from db_adapter.constants import CURW_SIM_DATABASE, CURW_SIM_PASSWORD, CURW_SIM_USERNAME, CURW_SIM_PORT, CURW_SIM_HOST
from db_adapter.constants import CURW_FCST_DATABASE, CURW_FCST_PASSWORD, CURW_FCST_USERNAME, CURW_FCST_PORT, CURW_FCST_HOST
from db_adapter.curw_sim.grids import get_flo2d_to_wrf_grid_mappings, GridInterpolationEnum, \
    FLO2D_250, FLO2D_150, FLO2D_30
from db_adapter.curw_sim.timeseries import Timeseries as Sim_Timeseries, MethodEnum
from db_adapter.curw_fcst.timeseries import Timeseries as Fcst_Timeseries
from db_adapter.curw_fcst.source import get_source_id


def read_csv(file_name):
    """
    Read csv file
    :param file_name: <file_path/file_name>.csv
    :return: list of lists which contains each row of the csv file
    """

    with open(file_name, 'r') as f:
        data = [list(line) for line in csv.reader(f)][1:]

    return data


def read_attribute_from_config_file(attribute, config):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    else:
        print("{} not specified in config file.".format(attribute))
        exit(1)


# for bulk insertion for a given one grid interpolation method
def update_rainfall_fcsts(flo2d_model, method, grid_interpolation):

    """
    Update rainfall forecasts for flo2d models
    :param flo2d_model: flo2d model
    :param method: value interpolation method
    :param grid_interpolation: grid interpolation method
    :return:
    """

    print("Update rainfall forecasts")


    try:

        config = json.loads(open('config.json').read())

        # source details
        model = read_attribute_from_config_file('model', config)
        version = read_attribute_from_config_file('version', config)

        # Connect to the database
        curw_sim_pool = get_Pool(host=CURW_SIM_HOST, user=CURW_SIM_USERNAME, password=CURW_SIM_PASSWORD,
                port=CURW_SIM_PORT, db=CURW_SIM_DATABASE)

        curw_fcst_pool = get_Pool(host=CURW_FCST_HOST, user=CURW_FCST_USERNAME, password=CURW_FCST_PASSWORD,
                port=CURW_FCST_PORT, db=CURW_FCST_DATABASE)

        Sim_TS = Sim_Timeseries(pool=curw_sim_pool)
        Fcst_TS = Fcst_Timeseries(pool=curw_fcst_pool)

        flo2d_grids = read_csv('{}m.csv'.format(flo2d_model))

        flo2d_wrf_mapping = get_flo2d_to_wrf_grid_mappings(pool=curw_sim_pool, grid_interpolation=grid_interpolation, flo2d_model=flo2d_model)

        source_id = get_source_id(pool=curw_fcst_pool, model=model, version=version)

        for flo2d_index in range(len(flo2d_grids)):
            meta_data = {
                    'latitude': float('%.6f' % float(flo2d_grids[flo2d_index][2])), 'longitude': float('%.6f' % float(flo2d_grids[flo2d_index][1])),
                    'model': flo2d_model, 'method': method,
                    'grid_id': '{}_{}_{}'.format(flo2d_model, flo2d_grids[flo2d_index][0], grid_interpolation)
                    }

            tms_id = Sim_TS.get_timeseries_by_grid_id(meta_data.get('grid_id'))

            if tms_id is None:
                tms_id = Sim_TS.generate_timeseries_id(meta_data=meta_data)
                meta_data['id'] = tms_id
                print("{} : Insert entry to run table with id={}".format(datetime.now(), tms_id))
                Sim_TS.insert_run(meta_data=meta_data)

            obs_end = Sim_TS.get_obs_end(id_=tms_id)

            fcst_timeseries = []

            if obs_end is not None:
                fcst_timeseries = Fcst_TS.get_latest_timeseries(sim_tag="evening_18hrs",
                        station_id=flo2d_wrf_mapping.get(meta_data['grid_id']), start=obs_end,
                        source_id=source_id, variable_id=1, unit_id=1)
            else:
                fcst_timeseries = Fcst_TS.get_latest_timeseries(sim_tag="evening_18hrs",
                        station_id=flo2d_wrf_mapping.get(meta_data['grid_id']),
                        source_id=source_id, variable_id=1, unit_id=1)

            print("{} : Insert timeseries to database".format(datetime.now()))
            Sim_TS.insert_data(timeseries=fcst_timeseries, tms_id=tms_id, upsert=True)

    except Exception as e:
        traceback.print_exc()
    finally:
        curw_fcst_pool.destroy()
        curw_sim_pool.destroy()


method = MethodEnum.getAbbreviation(MethodEnum.MME)
grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

print("{} : ####### Insert fcst rainfall for FLO2D 250 grids".format(datetime.now()))
update_rainfall_fcsts(flo2d_model=FLO2D_250, method=method, grid_interpolation=grid_interpolation)

print("{} : ####### Insert fcst rainfall for FLO2D 150 grids".format(datetime.now()))
update_rainfall_fcsts(flo2d_model=FLO2D_150, method=method, grid_interpolation=grid_interpolation)

print("{} : ####### Insert fcst rainfall for FLO2D 30 grids".format(datetime.now()))
update_rainfall_fcsts(flo2d_model=FLO2D_30, method=method, grid_interpolation=grid_interpolation)

print("{} : ####### fcst rainfall insertion process finished #######".format(datetime.now()))
