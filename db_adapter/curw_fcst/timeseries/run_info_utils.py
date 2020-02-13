import traceback
from db_adapter.logger import logger
import json


def insert_run_metadata(pool, source_id, sim_tag, fgt, metadata):
    """
    Insert new run info entry
    :param source_id:
    :param sim_tag:
    :param fgt:
    :param metadata:
    :return:
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "INSERT INTO `run_info` (`source`, `sim_tag`, `fgt`, `metadata`) " \
                            "VALUES ( %s, %s, %s, %s)"
            cursor.execute(sql_statement, ())

        connection.commit()
        return True
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion failed for run info enty with source={}, sim_tag={}, fgt={}, metadata={}" \
            .format(source_id, sim_tag, fgt, json.dumps(metadata))
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()