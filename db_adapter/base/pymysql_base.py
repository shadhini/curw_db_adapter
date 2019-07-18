# from pymysqlpool.pool import Pool

import pymysql
from DBUtils.PooledDB import PooledDB


def get_Pool(host, port, user, password, db):

    pool = PooledDB(creator=pymysql, maxconnections=6, blocking=True,
            host=host, port=port, user=user, password=password, db=db, autocommit=False, cursorclass=pymysql.cursors.DictCursor)

    return pool


def destroy_Pool(pool):

    pool.close()


    #  Conn = pool.connection() # After each time you need a database connection, you can use the connection() function to get the connection.
    #
    # cur=conn.cursor()
    #
    # SQL="select * from table"
    #
    # count=cur.execute(SQL)
    #
    # results=cur.fetchall()
    #
    # cur.close()
    #
    # conn.close()


# def get_Pool(host, port, user, password, db):
#     # uses pymysql.cursors.DictCursor
#     pool = Pool(host=host, port=port, user=user, password=password, db=db, autocommit=False, max_size=5)
#     pool.init()
#     return pool

# connection = pool.connection()
# cur = connection.cursor()
# cur.execute('SELECT * FROM `pet` WHERE `name`=%s', args=("Puffball", ))
# print(cur.fetchone())
#
# connection.close()

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