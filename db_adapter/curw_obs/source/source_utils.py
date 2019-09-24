import json
import traceback

from db_adapter.exceptions import DatabaseAdapterError
from db_adapter.logger import logger
"""
Source JSON Object would looks like this 
e.g.:
   {
        'source'     : 'CUrW_WeatherStation',
        'parameters' : { }
   },
   {
        'source'     : 'CUrW_WeatherStation',
        'parameters' : { }
   }
"""


def get_source_by_id(pool, id_):
    """
    Retrieve source by id
    :param pool: database connection pool
    :param id_: source id
    :return: Source if source exists in the database, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `source` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            if row_count > 0:
                return cursor.fetchone()
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving source with source_id {} failed".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_source_id(pool, source) -> str:
    """
    Retrieve Source id
    :param pool: database connection pool
    :param source:
    :return: str: source id if source exists in the database, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `id` FROM `source` WHERE `source`=%s"
            row_count = cursor.execute(sql_statement, source)
            if row_count > 0:
                return cursor.fetchone()['id']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving source id: source={} failed.".format(source)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_source(pool, source, parameters=None):
    """
    Insert sources into the database
    :param pool: database connection pool
    :param source: string
    :param parameters: JSON
    :return: True if the source has been added to the "Source' table of the database, else False
    """

    connection = pool.connection()
    try:
        if get_source_id(pool=pool, source=source) is None:
            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `source` (`source`, `parameters`) VALUES ( %s, %s)"
                row_count = cursor.execute(sql_statement, (source, json.dumps(parameters)))
                connection.commit()
                return True if row_count > 0 else False
        else:
            logger.info("Source with source={} already exists in the database".format(source))
            return False
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of source: source={}, and parameters={} failed".format(source, parameters)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_sources(sources, pool):
    """
    Add sources into Source table
    :param sources: list of json objects that define source attributes
    e.g.:
   {
        'source'     : 'wrfSE',
        'parameters': { }
    }
    {
        'source'     : 'OBS_WATER_LEVEL',
        'parameters': {
                "CHANNEL_CELL_MAP"               : {
                        "594" : "Wellawatta", "1547": "Ingurukade", "3255": "Yakbedda", "3730": "Wellampitiya",
                        "7033": "Janakala Kendraya"
                        }, "FLOOD_PLAIN_CELL_MAP": { }
                }
    }
    :return:
    """

    for source in sources:

        print(add_source(pool=pool, source=source.get('source'), parameters=source.get('parameters')))
        print(source.get('source'))


def delete_source(pool, source):
    """
    Delete source from Source table, given source
    :param pool: database connection pool
    :param source: str
    :return: True if the deletion was successful
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `source` WHERE `source`=%s"
            row_count = cursor.execute(sql_statement, source)
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of source in the database with source={}".format(source))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting source with source={} failed.".format(source)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def delete_source_by_id(pool, id_):
    """
    Delete source from Source table by id
    :param pool: database connection pool
    :param id_:
    :return: True if the deletion was successful, else False
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `source` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            connection.commit()
            if row_count > 0 :
                return True
            else:
                logger.info("There's no record of source in the database with the source id {}".format(id_))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting source with id {} failed.".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
