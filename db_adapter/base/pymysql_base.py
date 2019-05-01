from pymysqlpool.pool import Pool


def get_Pool(host, port, user, password, db):
    # uses pymysql.cursors.DictCursor
    pool = Pool(host=host, port=port, user=user, password=password, db=db, autocommit=False, max_size=5)
    pool.init()

# connection = pool.get_conn()
# cur = connection.cursor()
# cur.execute('SELECT * FROM `pet` WHERE `name`=%s', args=("Puffball", ))
# print(cur.fetchone())
#
# pool.release(connection)

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