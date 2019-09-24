import traceback

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError

"""
Unit JSON Object would looks like this
e.g.:
    {
        'unit'     : 'mm',
        'unit_type': UnitType.Accumulative
    }
    {
        'unit'     : 'm3/s',
        'unit_type': UnitType.Instantaneous
    }
"""


def get_unit_by_id(pool, id_):
    """
    Retrieve unit by id
    :param pool: database connection pool
    :param id_: unit id
    :return: Unit if unit exists in the db, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `unit` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            if row_count > 0:
                return cursor.fetchone()
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving unit with unit id {} failed".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_unit_id(pool, unit, unit_type) -> str:
    """
    Retrieve Unit id
    :param pool: database connection pool
    :param unit:
    :param unit_type: UnitType enum value. This value can be any of {Accumulative, Instantaneous, Mean} set
    :return: str: unit id if unit exists in the db, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `id` FROM `unit` WHERE `unit`=%s and `type`=%s"
            row_count = cursor.execute(sql_statement, (unit, unit_type.value))
            if row_count > 0:
                return cursor.fetchone()['id']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving unit id: unit={} and unit_type={} failed.".format(unit, unit_type)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_unit(pool, unit, unit_type):
    """
    Insert units into the database
    :param pool: database connection pool
    :param unit: string
    :param unit_type: UnitType enum value. This value can be any of {Accumulative, Instantaneous, Mean} set
    :return: True if the unit has been added to the "Unit" table of the database, else False
    """

    connection = pool.connection()
    try:
        if get_unit_id(pool=pool, unit=unit, unit_type=unit_type) is None:
            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `unit` (`unit`, `type`) VALUES ( %s, %s)"
                row_count = cursor.execute(sql_statement, (unit, unit_type.value))
                connection.commit()
                return True if row_count > 0 else False
        else:
            logger.info("Unit with unit={}, unit_type={} already exists in the database".format(unit, unit_type))
            return False
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of unit: unit={}, unit_type={} failed".format(unit, unit_type)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_units(units, pool):
    """
    Add units into Unit table
    :param units: list of json objects that define unit attributes
    e.g.:
    {
        'unit'     : 'mm',
        'unit_type': UnitType.Accumulative
    }
    {
        'unit'     : 'm3/s',
        'unit_type': UnitType.Instantaneous
    }
    :param pool: database connection pool
    :return:
    """

    for unit in units:

        print(add_unit(pool=pool, unit=unit.get('unit'), unit_type=unit.get('unit_type')))
        print(unit.get('unit'))


def delete_unit(pool, unit, unit_type):
    """
    Delete unit from Unit table, given unit and unit_type
    :param pool: database connection pool
    :param unit: string
    :param unit_type: UnitType enum value. This value can be any of {Accumulative, Instantaneous, Mean} set
    :return: True if the deletion was successful, else False
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `unit` WHERE `unit`=%s and `type`=%s"
            row_count = cursor.execute(sql_statement, (unit, unit_type.value))
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of unit in the database with unit={} and unit_type={}".format(unit,
                        unit_type))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting unit with unit={} and unit_type={} failed.".format(unit, unit_type)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def delete_unit_by_id(pool, id_):
    """
    Delete unit from Unit table by id
    :param pool: database connection pool
    :param id_:
    :return: True if the deletion was successful, else False
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `unit` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of unit in the database with the unit id {}".format(id_))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting unit with id {} failed.".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
