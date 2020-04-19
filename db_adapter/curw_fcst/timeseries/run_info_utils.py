import traceback
from db_adapter.logger import logger
import json


def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData


def write_file(data, filename):
    # Convert binary data to proper format and write it on Hard Disk
    with open(filename, 'wb') as file:
        file.write(data)


def insert_run_metadata(pool, sim_tag, source_id, variable_id, fgt, metadata, template_path=None):
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

        if template_path is not None:
            template = convertToBinaryData(template_path)
            sql_statement = "INSERT INTO `run_info` (`sim_tag`, `source`, `variable`, `fgt`, `metadata`, `template`) " \
                                "VALUES ( %s, %s, %s, %s, %s, %s)"
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


def read_template(pool, sim_tag, source_id, variable_id, fgt, output_file_path):
    """
    Read template (convert BLOB to a file)
    :param source_id:
    :param sim_tag:
    :param fgt:
    :param output_file_path: where to write the output
    :return:
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `template` FROM `run_info` WHERE `sim_tag`=%s and `source`=%s and " \
                            "`variable`=%s and `fgt`=%s"
            row_count = cursor.execute(sql_statement, (sim_tag, source_id, variable_id, fgt))
            if row_count > 0:
                template_data = cursor.fetchone()['template']
                write_file(data=template_data, filename=output_file_path)
            else:
                return None

        return True
    except Exception as exception:
        error_message = "Retrieving template failed for run info entry with source={}, variable={}, sim_tag={}, fgt={}" \
            .format(source_id, variable_id, sim_tag, fgt)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()