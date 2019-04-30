import pymysql


def create_db_connection(host, user, password, database, port):
    # Open database connection
    db = pymysql.connect(host=host, user=user, password=password, database=database, port=port, autocommit=True)
    return db


def get_cursor(connection):
    # prepare a cursor object using cursor() method
    cursor = connection.cursor()

# # execute SQL query using execute() method.
# cursor.execute("SELECT VERSION()")
#
# # Drop table if it already exist using execute() method.
# cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
#
# # Create table as per requirement
# sql = """CREATE TABLE EMPLOYEE (
#    FIRST_NAME  CHAR(20) NOT NULL,
#    LAST_NAME  CHAR(20),
#    AGE INT,
#    SEX CHAR(1),
#    INCOME FLOAT )"""
#
# cursor.execute(sql)


# # Fetch a single row using fetchone() method.
# data = cursor.fetchone()
# print ("Database version : %s " % data)

# disconnect from server

def close_connection(connection):
    connection.close()