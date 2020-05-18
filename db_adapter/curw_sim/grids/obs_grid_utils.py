import traceback
import csv
import pkg_resources

from db_adapter.logger import logger


def add_obs_to_d03_grid_mappings_for_rainfall(pool, grid_interpolation, obs_to_d03_map_path, active_obs_path):

    """
    Add observational stations grid mappings to the database
    :param pool:  database connection pool
    :param grid_interpolation: grid interpolation method
    :param obs_to_d03_map_path: path to file containing curw rainfall observational stations to d03 stations mapping
    :param active_obs_path: path to file containing all active curw rainfall observation stations
    :return: True if the insertion is successful, else False
    """
    # [obs_grid_id,d03_1_id,d03_1_dist,d03_2_id,d03_2_dist,d03_3_id,d03_3_dist]
    with open(obs_to_d03_map_path, 'r') as f1:
        obs_d03_mapping=[line for line in csv.reader(f1)][1:]

    # [hash_id,station_id,station_name,latitude,longitude]
    with open(active_obs_path, 'r') as f2:
        obs_stations=[line for line in csv.reader(f2)][1:]

    obs_dict = {}

    for i in range(len(obs_stations)):
        station_id = obs_stations[i][1]
        station_name = obs_stations[i][2]
        obs_dict[station_id] = [station_name]

    grid_mappings_list = []

    for index in range(len(obs_d03_mapping)):
        obs_id = obs_d03_mapping[index][0]
        grid_mapping = ['rainfall_{}_{}_{}'.format(obs_id, obs_dict.get(obs_id)[0], grid_interpolation),
                        obs_d03_mapping[index][1], obs_d03_mapping[index][3],
                        obs_d03_mapping[index][5], obs_d03_mapping[index][7]]
        grid_mappings_list.append(tuple(grid_mapping))

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "INSERT INTO `grid_map_obs` (`grid_id`, `d03_1`, `d03_2`, `d03_3`, `d03_4`)" \
                            " VALUES ( %s, %s, %s, %s, %s) " \
                            "ON DUPLICATE KEY UPDATE `d03_1`=VALUES(`d03_1`), `d03_2`=VALUES(`d03_2`), " \
                            "`d03_3`=VALUES(`d03_3`), `d03_4`=VALUES(`d03_4`);"
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
            sql_statement = "SELECT `grid_id`,`d03_1`,`d03_2`,`d03_3`,`d03_4` FROM `grid_map_obs` " \
                            "WHERE `grid_id` like %s ESCAPE '$'"
            row_count = cursor.execute(sql_statement, "rainfall$_%$_{}".format(grid_interpolation))
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    obs_grid_mappings[dict.get("grid_id")] = [dict.get("d03_1"), dict.get("d03_2"),
                                                              dict.get("d03_3"), dict.get("d03_4")]
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
