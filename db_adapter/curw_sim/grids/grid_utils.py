import traceback
import csv
import pkg_resources

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError


def add_flo2d_raincell_grid_mappings(pool, grid_interpolation, flo2d_model):

    """
    Add flo2d grid mappings to the database
    :param pool:  database connection pool
    :param grid_interpolation: grid interpolation method
    :param flo2d_model: string: flo2d model (e.g. FLO2D_250, FLO2D_150, FLO2D_30)
    :return: True if the insertion is successful, else False
    """

    # [flo2d_grid_id,nearest_d03_station_id,dist]
    with open('grid_maps/flo2d/{}_{}_d03_stations_mapping.csv'.format(grid_interpolation, flo2d_model), 'r') as f1:
        flo2d_d03_mapping=[line for line in csv.reader(f1)][1:]

    # [flo2d_250_station_id,ob_1_id,ob_1_dist,ob_2_id,ob_2_dist,ob_3_id,ob_3_dist]
    with open('grid_maps/flo2d/{}_{}_obs_mapping.csv'.format(grid_interpolation, flo2d_model), 'r') as f2:
        flo2d_obs_mapping=[line for line in csv.reader(f2)][1:]

    grid_mappings_list = []

    for index in range(len(flo2d_obs_mapping)):
        cell_id = flo2d_obs_mapping[index][0]
        obs1 = flo2d_obs_mapping[index][1]
        obs2 = flo2d_obs_mapping[index][3]
        obs3 = flo2d_obs_mapping[index][5]
        fcst = flo2d_d03_mapping[index][1]
        grid_mapping = ['{}_{}_{}'.format(flo2d_model, grid_interpolation, (str(cell_id)).zfill(10)),
                        obs1, obs2, obs3, fcst]
        grid_mappings_list.append(tuple(grid_mapping))

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "INSERT INTO `grid_map_flo2d_raincell` (`grid_id`, `obs1`, `obs2`, `obs3`, `fcst`)" \
                            " VALUES ( %s, %s, %s, %s, %s) "\
                            "ON DUPLICATE KEY UPDATE `obs1`=VALUES(`obs1`), `obs2`=VALUES(`obs2`), " \
                            "`obs3`=VALUES(`obs3`), `fcst`=VALUES(`fcst`);"
            row_count = cursor.executemany(sql_statement, grid_mappings_list)
        connection.commit()
        return row_count
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of flo2d raincell grid mappings failed."
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_flo2d_cells_to_obs_grid_mappings(pool, grid_interpolation, flo2d_model):

    """
    Retrieve flo2d to obs grid mappings
    :param pool: database connection pool
    :param grid_interpolation: grid interpolation method
    :param flo2d_model: string: flo2d model (e.g. FLO2D_250, FLO2D_150, FLO2D_30)
    :return: dictionary with grid ids as keys and corresponding obs1, obs2, obs3 station ids as a list
    """

    flo2d_grid_mappings = {}

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `grid_map_flo2d_raincell` WHERE `grid_id` like %s ESCAPE '$'"
            row_count = cursor.execute(sql_statement, "flo2d$_{}$_{}$_%".format(flo2d_model.split('_')[1], grid_interpolation))
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    flo2d_grid_mappings[dict.get("grid_id")] = [dict.get("obs1"), dict.get("obs2"), dict.get("obs3")]
                return flo2d_grid_mappings
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving flo2d cells to obs grid mappings failed"
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_flo2d_cells_to_wrf_grid_mappings(pool, grid_interpolation, flo2d_model):

    """
    Retrieve flo2d to wrf stations mappings
    :param pool: database connection pool
    :param grid_interpolation: grid interpolation method
    :param flo2d_model: string: flo2d model (e.g. FLO2D_250, FLO2D_150, FLO2D_30)
    :return: dictionary with grid ids as keys and corresponding wrf station ids as values
    """

    flo2d_grid_mappings = {}

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT `grid_id`, `fcst` FROM `grid_map_flo2d_raincell` WHERE `grid_id` like %s ESCAPE '$'"
            row_count = cursor.execute(sql_statement, "flo2d$_{}$_{}$_%".format(flo2d_model.split('_')[1], grid_interpolation))
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    flo2d_grid_mappings[dict.get("grid_id")] = dict.get("fcst")
                return flo2d_grid_mappings
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving flo2d cells to obs grid mappings failed"
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_obs_to_d03_grid_mappings_for_rainfall(pool, grid_interpolation):

    """
    Add observational stations grid mappings to the database
    :param pool:  database connection pool
    :param grid_interpolation: grid interpolation method
    :return: True if the insertion is successful, else False
    """
    # [obs_grid_id,d03_1_id,d03_1_dist,d03_2_id,d03_2_dist,d03_3_id,d03_3_dist]
    with open('grid_maps/obs_stations/rainfall/{}_obs_d03_stations_mapping.csv'.format(grid_interpolation), 'r') as f1:
        obs_d03_mapping=[line for line in csv.reader(f1)][1:]

    # [hash_id,station_id,station_name,latitude,longitude]
    with open('grids/obs_stations/rainfall/curw_active_rainfall_obs_stations.csv', 'r') as f2:
        obs_stations=[line for line in csv.reader(f2)][1:]

    obs_dict = {}

    for i in range(len(obs_stations)):
        station_id = obs_stations[i][1]
        station_name = obs_stations[i][2]
        obs_dict[station_id] = [station_name]

    grid_mappings_list = []

    for index in range(len(obs_d03_mapping)):
        obs_id = obs_d03_mapping[index][0]
        grid_mapping = ['rainfall_{}_{}'.format(obs_dict.get(obs_id)[0], grid_interpolation),
                        obs_d03_mapping[index][1], obs_d03_mapping[index][3], obs_d03_mapping[index][5]]
        grid_mappings_list.append(tuple(grid_mapping))

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "INSERT INTO `grid_map_obs` (`grid_id`, `d03_1`, `d03_2`, `d03_3`)" \
                            " VALUES ( %s, %s, %s, %s) " \
                            "ON DUPLICATE KEY UPDATE `d03_1`=VALUES(`d03_1`), `d03_2`=VALUES(`d03_2`), " \
                            "`d03_3`=VALUES(`d03_3`);"
            row_count = cursor.executemany(sql_statement, grid_mappings_list)
        connection.commit()
        return row_count
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of flo2d grid mappings failed."
        logger.error(error_message)
        traceback.print_exc()
        return False
    finally:
        if connection is not None:
            connection.close()


def get_obs_to_d03_grid_mappings_for_rainfall(pool, grid_interpolation):

    """
    Retrieve obs to d03 grid mappings
    :param pool: database connection pool
    :param grid_interpolation: grid interpolation method
    :return: dictionary with grid ids as keys and corresponding obs1, obs2, obs3 station ids as a list
    """

    obs_grid_mappings = {}

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT `grid_id`,`d03_1`,`d03_2`,`d03_3` FROM `grid_map_obs` " \
                            "WHERE `grid_id` like %s ESCAPE '$'"
            row_count = cursor.execute(sql_statement, "rainfall$_%$_{}".format(grid_interpolation))
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    obs_grid_mappings[dict.get("grid_id")] = [dict.get("d03_1"), dict.get("d03_2"), dict.get("d03_3")]
                return obs_grid_mappings
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving flo2d to obs grid mappings failed"
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_flo2d_initial_conditions(pool, flo2d_model, initial_condition_file_path):

    """
    Add flo2d grid mappings to the database
    :param pool:  database connection pool
    :param flo2d_model: string: flo2d model (e.g. enum values of FLO2D_250, FLO2D_150, FLO2D_30)
    :param initial_condition_file_path: path to the file with flo2d initial conditions
    :return: True if the insertion is successful, else False
    """

    with open(initial_condition_file_path, 'r') as f1:
        flo2d_init_cond=[line for line in csv.reader(f1)][1:]

    grid_mappings_list = []

    for index in range(len(flo2d_init_cond)):
        upstrm = flo2d_init_cond[index][0]
        downstrm = flo2d_init_cond[index][1]
        obs_wl = flo2d_init_cond[index][2]
        canal = flo2d_init_cond[index][3]
        grid_mapping = ['{}_{}_{}'.format(flo2d_model, upstrm, downstrm),
                        upstrm, downstrm, canal, obs_wl]
        grid_mappings_list.append(tuple(grid_mapping))

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "INSERT INTO `grid_map_flo2d_initial_cond` (`grid_id`, `up_strm`, `down_strm`, `canal_seg`, `obs_wl`)" \
                            " VALUES ( %s, %s, %s, %s, %s) "\
                            "ON DUPLICATE KEY UPDATE `up_strm`=VALUES(`up_strm`), `down_strm`=VALUES(`down_strm`), " \
                            "`canal_seg`=VALUES(`canal_seg`), `obs_wl`=VALUES(`obs_wl`);"
            row_count = cursor.executemany(sql_statement, grid_mappings_list)
        connection.commit()
        return row_count
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of flo2d initial conditions failed."
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_flo2d_initial_conditions(pool, flo2d_model):

    """
    Retrieve flo2d initial conditions
    :param pool: database connection pool
    :param flo2d_model: string: flo2d model (e.g. FLO2D_250, FLO2D_150, FLO2D_30)
    :return: dictionary with grid ids as keys and corresponding up_strm, down_strm, canal_seg, and obs_wl as a list
    """

    initial_conditions = {}

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT `grid_id`,`up_strm`,`down_strm`,`obs_wl` FROM `grid_map_flo2d_initial_cond` " \
                            "WHERE `grid_id` like %s ESCAPE '$'"
            row_count = cursor.execute(sql_statement, "{}$_%".format(flo2d_model))
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    initial_conditions[dict.get("grid_id")] = [dict.get("up_strm"), dict.get("down_strm"), dict.get("obs_wl")]
                return initial_conditions
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving {} initial conditions failed".format(flo2d_model)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
