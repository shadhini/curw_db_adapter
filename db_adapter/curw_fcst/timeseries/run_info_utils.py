import traceback
from db_adapter.logger import logger
import json


def insert_run_metadata(pool, sim_tag, source_id, variable_id, fgt, metadata, template=None):
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

        sql_statement = "INSERT INTO `run_info` (`sim_tag`, `source`, `variable`, `fgt`, `metadata`) " \
                        "VALUES ( %s, %s, %s, %s, %s)"
        data = (sim_tag, source_id, variable_id, fgt, json.dumps(metadata))

        if template is not None:
            sql_statement = "INSERT INTO `run_info` (`sim_tag`, `source`, `variable`, `fgt`, `metadata`, `template`) " \
                                "VALUES ( %s, %s, %s, %s, %s, LOAD_FILE(%s))"
            data = (sim_tag, source_id, variable_id, fgt, json.dumps(metadata), template)

        with connection.cursor() as cursor:
            cursor.execute(sql_statement, data)

        connection.commit()

        return True
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion failed for run info entry with source={}, variable={}, sim_tag={}, fgt={}, metadata={}" \
            .format(source_id, variable_id, sim_tag, fgt, json.dumps(metadata))
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()