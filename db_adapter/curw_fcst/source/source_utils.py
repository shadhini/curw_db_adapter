from db_adapter.curw_fcst.models import Source
from db_adapter.logger import logger
import traceback

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
    :param pool: thread pool made for the database connection
    :param id_: source id
    :return: Source if source exists in the database, else None
    """

    connection = pool.get()
    # try:
    #     with connection.cursor() as cursor:
    #         sql = "SELECT 1 FROM `run` WHERE `id`=%s"
    #
    #         cursor.execute(sql, possible_id)
    #         is_exist = cursor.fetchone()
    #         if is_exist is not None:
    #             event_id = possible_id
    #     return event_id
    # except Exception as ex:
    #     error_message = 'Error in retrieving event_id for meta data: %s.' % meta_data
    #     # TODO logging and raising is considered as a cliche' and bad practice.
    #     logging.error(error_message)
    #     raise DatabaseAdapterError(error_message, ex)

    try:
        source_row = pool.query(Source).get(id_)
        return None if source_row is None else source_row
    except Exception as e:
        logger.error("Exception occurred while retrieving source with source_id {}".format(id_))
        traceback.print_exc()
        return False
    finally:
        if connection is not None:
            pool.put(connection)


def get_source_id(pool, model, version) -> str:
    """
    Retrieve Source id
    :param pool: pool made by poolmaker for the database engine
    :param model:
    :param version:
    :return: str: source id if source exists in the database, else None
    """

    try:
        source_row = pool.query(Source) \
            .filter_by(model=model) \
            .filter_by(version=version) \
            .first()
        return None if source_row is None else source_row.id
    except Exception as e:
        logger.error("Exception occurred while retrieving source id: model={} and version={}".format(model, version))
        traceback.print_exc()
        return False
    finally:
        pool.close()


def add_source(pool, model, version, parameters):
    """
    Insert sources into the database
    :param pool: pool made by poolmaker for the database engine
    :param model: string
    :param version: string
    :param parameters: JSON
    :return: True if the source has been added to the "Source' table of the database, else False
    """

    try:
        source = Source(
                model=model,
                version=version,
                parameters=parameters
                )

        pool.add(source)
        pool.commit()

        return True
    except Exception as e:
        logger.error("Exception occurred while adding source: model={}, version={} and parameters={}"
            .format(model, version, parameters))
        traceback.print_exc()
        return False
    finally:
        pool.close()


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
    :param pool: pool made by poolmaker for the database engine
    :param model: str
    :param version: str
    :return: True if the deletion was successful, else False
    """

    id_ = get_source_id(pool=pool, model=model, version=version)

    try:
        if id_ is not None:
            return delete_source_by_id(pool, id_)
        else:
            print("There's no record in the database with the source id ", id_)
            logger.info("There's no record in the database with the source id {}".format(id_))
            return False
    finally:
        pool.close()


def delete_source_by_id(pool, id_):
    """
    Delete source from Source table by id
    :param pool: pool made by poolmaker for the database engine
    :param id_:
    :return: True if the deletion was successful, else False
    """

    try:
        source = pool.query(Source).get(id_)
        if source is not None:
            pool.delete(source)
            pool.commit()
            status = pool.query(Source).filter_by(id=id_).count()
            return True if status==0 else False
        else:
            print("There's no record in the database with the source id ", id_)
            logger.info("There's no record in the database with the source id {}".format(id_))
            return False
    except Exception as e:
        logger.error("Exception occurred while deleting source with it {}".format(id_))
        traceback.print_exc()
        return False
    finally:
        pool.close()
