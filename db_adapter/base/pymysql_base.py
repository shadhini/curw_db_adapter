# from pymysqlpool.pool import Pool

import pymysql
import traceback
from DBUtils.PooledDB import PooledDB

from db_adapter.logger import logger


def get_Pool(host, port, user, password, db):

    pool = PooledDB(creator=pymysql, maxconnections=4, blocking=True,
            host=host, port=port, user=user, password=password, db=db, autocommit=False, cursorclass=pymysql.cursors.DictCursor)

    return pool


def destroy_Pool(pool):

    pool.close()


def execute_read_query(pool, query, params):
    """

    :param pool: connection pool
    :param query: sql query with wild cards
    :param params: tuple, parameters need to be passed in to the sql query
    :return:
    """

    connection = pool.connection()

    try:
        with connection.cursor() as cursor:
            row_count= cursor.execute(query, params)
            if row_count > 0:
                return cursor.fetchall()
        return None
    except Exception as ex:
        error_message = "Executing sql query {} with params {} failed".format(query, params)
        logger.error(error_message)
        traceback.print_exc()
    finally:
        if connection is not None:
            connection.close()


def execute_write_query(pool, query, params):
    """

      :param pool: connection pool
      :param query: sql query with wild cards
      :param params: tuple, parameters need to be passed in to the sql query
      :return:
      """

    connection = pool.connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
        connection.commit()
        return True
    except Exception as ex:
        connection.rollback()
        error_message = "Executing sql query {} with params {} failed".format(query, params)
        logger.error(error_message)
        traceback.print_exc()
        return False
    finally:
        if connection is not None:
            connection.close()


# for bulk data retrieval
def get_connection_for_iterable_cursor(host, port, user, password, db):
    return pymysql.connect(host=host, user=user, password=password, db=db, port=port,
                           cursorclass=pymysql.cursors.DictCursor)
