import traceback
import csv
import pkg_resources

from db_adapter.logger import logger


def add_flo2d_raincell_grid_mappings(pool, grid_interpolation, flo2d_model, obs_map_file_path, d03_map_file_path=None):

    """
    Add flo2d grid mappings to the database
    :param pool:  database connection pool
    :param grid_interpolation: grid interpolation method
    :param flo2d_model: string: flo2d model (e.g. FLO2D_250, FLO2D_150, FLO2D_30)
    :param obs_map_file_path: path to file containing flo2d grids to rainfall observational stations mapping
    :param d03_map_file_path: path to file containing flo2d grids to d03 stations mapping
    :return: True if the insertion is successful, else False
    """

    # [flo2d_250_station_id,ob_1_id,ob_1_dist,ob_2_id,ob_2_dist,ob_3_id,ob_3_dist]
    with open(obs_map_file_path, 'r') as f2:
        flo2d_obs_mapping=[line for line in csv.reader(f2)][1:]

    grid_mappings_list = []

    if d03_map_file_path is not None:
        # [flo2d_grid_id,nearest_d03_station_id,dist]
        with open(d03_map_file_path, 'r') as f1:
            flo2d_d03_mapping=[line for line in csv.reader(f1)][1:]

        for index in range(len(flo2d_obs_mapping)):
            cell_id = flo2d_obs_mapping[index][0]
            obs1 = flo2d_obs_mapping[index][1]
            obs2 = flo2d_obs_mapping[index][3]
            obs3 = flo2d_obs_mapping[index][5]
            fcst = flo2d_d03_mapping[index][1]
            grid_mapping = ['{}_{}_{}'.format(flo2d_model, grid_interpolation, (str(cell_id)).zfill(10)),
                            obs1, obs2, obs3, fcst]
            grid_mappings_list.append(tuple(grid_mapping))

        sql_statement = "INSERT INTO `grid_map_flo2d_raincell` (`grid_id`, `obs1`, `obs2`, `obs3`, `fcst`)" \
                        " VALUES ( %s, %s, %s, %s, %s) " \
                        "ON DUPLICATE KEY UPDATE `obs1`=VALUES(`obs1`), `obs2`=VALUES(`obs2`), " \
                        "`obs3`=VALUES(`obs3`), `fcst`=VALUES(`fcst`);"
    else:

        for index in range(len(flo2d_obs_mapping)):
            cell_id = flo2d_obs_mapping[index][0]
            obs1 = flo2d_obs_mapping[index][1]
            obs2 = flo2d_obs_mapping[index][3]
            obs3 = flo2d_obs_mapping[index][5]
            grid_mapping = ['{}_{}_{}'.format(flo2d_model, grid_interpolation, (str(cell_id)).zfill(10)),
                            obs1, obs2, obs3]
            grid_mappings_list.append(tuple(grid_mapping))

        sql_statement = "INSERT INTO `grid_map_flo2d_raincell` (`grid_id`, `obs1`, `obs2`, `obs3`)" \
                        " VALUES ( %s, %s, %s, %s) " \
                        "ON DUPLICATE KEY UPDATE `obs1`=VALUES(`obs1`), `obs2`=VALUES(`obs2`), " \
                        "`obs3`=VALUES(`obs3`);"

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
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
            row_count = cursor.execute(sql_statement, "flo2d$_{}$_{}$_%".format('$_'.join(flo2d_model.split('_')[1:]), grid_interpolation))
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
            row_count = cursor.execute(sql_statement, "flo2d$_{}$_{}$_%".format('$_'.join(flo2d_model.split('_')[1:]), grid_interpolation))
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
        error_message = "Insertion of {} initial conditions failed.".format(flo2d_model)
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
            sql_statement = "SELECT `grid_id`,`up_strm`,`down_strm`,`obs_wl`, `obs_wl_down_strm` FROM `grid_map_flo2d_initial_cond` " \
                            "WHERE `grid_id` like %s ESCAPE '$'"
            row_count = cursor.execute(sql_statement, "{}$_%".format(flo2d_model))
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    initial_conditions[dict.get("grid_id")] = [dict.get("up_strm"), dict.get("down_strm"),
                                                               dict.get("obs_wl"), dict.get("obs_wl_down_strm")]
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


def clear_initial_conditions(pool, flo2d_model):
    """
    Clear existing initial conditions of a given flo2d model from database
    :param pool: database connection pool
    :param flo2d_model: string: flo2d model (e.g. FLO2D_250, FLO2D_150, FLO2D_30)
    :return: affected row count if successful
    """

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `grid_map_flo2d_initial_cond` " \
                            "WHERE `grid_id` like %s ESCAPE '$'"
            row_count = cursor.execute(sql_statement, "{}$_%".format(flo2d_model))

        connection.commit()
        return row_count
    except Exception as exception:
        connection.rollback()
        error_message = "Deletion of {} initial conditions failed.".format(flo2d_model)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
