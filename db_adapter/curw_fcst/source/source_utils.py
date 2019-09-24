import json
import traceback

from db_adapter.exceptions import DatabaseAdapterError
from db_adapter.logger import logger
"""
Source JSON Object would looks like this 
e.g.:
   {
        'model'     : 'wrfSE',
        'version'   : 'v3',
        'parameters': { }
    }
    {
        'model'     : 'OBS_WATER_LEVEL',
        'version'   : '',
        'parameters': {
                "CHANNEL_CELL_MAP"               : {
                        "594" : "Wellawatta", "1547": "Ingurukade", "3255": "Yakbedda", "3730": "Wellampitiya",
                        "7033": "Janakala Kendraya"
                        }, "FLOOD_PLAIN_CELL_MAP": { }
                }
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


def get_source_id(pool, model, version) -> str:
    """
    Retrieve Source id
    :param pool: database connection pool
    :param model:
    :param version:
    :return: str: source id if source exists in the database, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `id` FROM `source` WHERE `model`=%s and `version`=%s"
            row_count = cursor.execute(sql_statement, (model, version))
            if row_count > 0:
                return cursor.fetchone()['id']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving source id: model={} and version={} failed.".format(model, version)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_source(pool, model, version, parameters=None):
    """
    Insert sources into the database
    :param pool: database connection pool
    :param model: string
    :param version: string
    :param parameters: JSON
    :return: True if the source has been added to the "Source' table of the database, else False
    """

    connection = pool.connection()
    try:
        if get_source_id(pool=pool, model=model, version=version) is None:
            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `source` (`model`, `version`, `parameters`) VALUES ( %s, %s, %s)"
                row_count = cursor.execute(sql_statement, (model, version, json.dumps(parameters)))
                connection.commit()
                return True if row_count > 0 else False
        else:
            logger.info("Source with model={} and version={} already exists in the database".format(model, version))
            return False
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of source: model={}, version={} and parameters={} failed".format(model, version, parameters)
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
        'model'     : 'wrfSE',
        'version'   : 'v3',
        'parameters': { }
    }
    {
        'model'     : 'OBS_WATER_LEVEL',
        'version'   : '',
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

        print(add_source(pool=pool, model=source.get('model'), version=source.get('version'),
                parameters=source.get('parameters')))
        print(source.get('model'))


def delete_source(pool, model, version):
    """
    Delete source from Source table, given model and version
    :param pool: database connection pool
    :param model: str
    :param version: str
    :return: True if the deletion was successful
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `source` WHERE `model`=%s and `version`=%s"
            row_count = cursor.execute(sql_statement, (model, version))
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of source in the database with model={} and version={}".format(model, version))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting source with model={} and version={} failed.".format(model, version)
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


def get_source_parameters(pool, model, version):
    """
        Retrieve Source parameters
        :param pool: database connection pool
        :param model:
        :param version:
        :return: str: json object parameters if source exists in the database, else None
        """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `parameters` FROM `source` WHERE `model`=%s and `version`=%s"
            row_count = cursor.execute(sql_statement, (model, version))
            if row_count > 0:
                return cursor.fetchone()['parameters']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving source parameters: model={} and version={} failed.".format(model, version)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
