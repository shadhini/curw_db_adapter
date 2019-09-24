import traceback

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError

"""
Variable JSON Object would looks like this
e.g.:
    {
        'variable'     : 'mm',
    }
"""


def get_variable_by_id(pool, id_):
    """
    Retrieve variable by id
    :param pool: database connection pool
    :param id_: variable id
    :return: Variable if variable exists in the db, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `variable` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            if row_count > 0:
                return cursor.fetchone()
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving variable with variable_id {} failed".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_variable_id(pool, variable) -> str:
    """
    Retrieve Variable id
    :param pool: database connection pool
    :param variable:
    :return: str: variable id if variable exists in the db, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `id` FROM `variable` WHERE `variable`=%s"
            row_count = cursor.execute(sql_statement, variable)
            if row_count > 0:
                return cursor.fetchone()['id']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving variable id: variable={} failed.".format(variable)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_variable(pool, variable):
    """
    Insert variables into the database
    :param pool: database connection pool
    :param variable: string
    :return: True if the variable has been added to the "Variable" table of the database, else False
    """

    connection = pool.connection()
    try:
        if get_variable_id(pool=pool, variable=variable) is None:
            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `variable` (`variable`) VALUES ( %s)"
                row_count = cursor.execute(sql_statement, variable)
                connection.commit()
                return True if row_count > 0 else False
        else:
            logger.info("Variable with variable={} already exists in the database".format(variable))
            return False
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of variable: variable={} failed".format(variable)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_variables(variables, pool):
    """
    Add variables into Variable table
    :param variables: list of json objects that define variable attributes
    e.g.:
    {
        'variable'     : 'mm',
    }
    :param pool: database connection pool
    :return:
    """

    for variable in variables:

        print(add_variable(pool=pool, variable=variable.get('variable')))
        print(variable.get('variable'))


def delete_variable(pool, variable):
    """
    Delete variable from Variable table, given variable name
    :param pool: database connection pool
    :param variable: string
    :return: True if the deletion was successful, else False
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `variable` WHERE `variable`=%s"
            row_count = cursor.execute(sql_statement, variable)
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of variable in the database with variable={}".format(variable))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting variable with variable={} failed.".format(variable)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def delete_variable_by_id(pool, id_):
    """
    Delete variable from Variable table by id
    :param pool: database connection pool
    :param id_:
    :return: True if the deletion was successful, else False
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `variable` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of variable in the database with the variable id {}".format(id_))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting variable with id {} failed.".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
