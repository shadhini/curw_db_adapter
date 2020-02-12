import json
import traceback

from db_adapter.exceptions import DatabaseAdapterError
from db_adapter.logger import logger


def get_curw_sim_discharge_id(pool, model, method, grid_id):
    """
    Retrieve curw_sim discharge id
    :param pool: Database connection pool
    :param model: target forecast model
    :param method: timeseries population method
    :param grid_id: timeseries grid id
    :return: curw_sim discharge hash id corresponds to specified mode, method and grid_id
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `id` FROM curw_sim.dis_run WHERE `model`=%s AND `method`=%s AND `grid_id`=%s;"
            row_count = cursor.execute(sql_statement, (model, method, grid_id))
            if row_count > 0:
                return cursor.fetchone()['id']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving curw_sim discharge id failed :: {} :: {} :: {}".format(model, method, grid_id)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_curw_sim_tidal_id(pool, model, method, grid_id):
    """
    Retrieve curw_sim discharge id
    :param pool: Database connection pool
    :param model: target forecast model
    :param method: timeseries population method
    :param grid_id: timeseries grid id
    :return: curw_sim discharge hash id corresponds to specified mode, method and grid_id
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `id` FROM curw_sim.tide_run WHERE `model`=%s AND `method`=%s AND `grid_id`=%s;"
            row_count = cursor.execute(sql_statement, (model, method, grid_id))
            if row_count > 0:
                return cursor.fetchone()['id']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving curw_sim tidal id failed :: {} :: {} :: {}".format(model, method, grid_id)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()

