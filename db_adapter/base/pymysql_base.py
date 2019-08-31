# from pymysqlpool.pool import Pool

import pymysql
import traceback
from DBUtils.PooledDB import PooledDB

from db_adapter.logger import logger


def get_Pool(host, port, user, password, db):

    pool = PooledDB(creator=pymysql, maxconnections=5, blocking=True,
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


#  Conn = pool.connection()
#  After each time you need a database connection, you can use the connection() function to get the connection.
# cur=conn.cursor()
# SQL="select * from table"
# count=cur.execute(SQL)
# results=cur.fetchall()
# cur.close()
# conn.close()


# Connect to the database
# connection = pymysql.connect(host='localhost',
#                              user='user',
#                              password='passwd',
#                              db='db',
#                              charset='utf8mb4',
#                              cursorclass=pymysql.cursors.DictCursor)
#
# try:
#     with connection.cursor() as cursor:
#         # Create a new record
#         sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
#         cursor.execute(sql, ('webmaster@python.org', 'very-secret'))
#
#     # connection is not autocommit by default. So you must commit to save
#     # your changes.
#     connection.commit()
#
#     with connection.cursor() as cursor:
#         # Read a single record
#         sql = "SELECT `id`, `password` FROM `users` WHERE `email`=%s"
#         cursor.execute(sql, ('webmaster@python.org',))
#         result = cursor.fetchone()
#         print(result)
# finally:
#     connection.close()