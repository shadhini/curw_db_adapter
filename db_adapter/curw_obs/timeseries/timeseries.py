import pandas as pd
import hashlib
import json
import traceback

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError


class Timeseries:
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def generate_timeseries_id(meta_data: object) -> object:
        """
        Generate the event id for given metadata
        Only 'latitude', 'longitude', 'source', 'variable', 'unit', 'unit_type'
        are used to generate the id (i.e. hash value)

        :param meta_data: Dict with 'sim_tag', 'scheduled_date', 'latitude',
        'longitude', 'model', 'version', 'variable', 'unit', 'unit_type' keys
        :return: str: sha256 hash value in hex format (length of 64 characters)
        """

        sha256 = hashlib.sha256()
        hash_data = {
                'latitude' : '',
                'longitude': '',
                'source'   : '',
                'variable' : '',
                'unit'     : '',
                'unit_type': ''
                }

        for key in hash_data.keys():
            hash_data[key] = meta_data[key]

        sha256.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        event_id = sha256.hexdigest()
        return event_id

    def get_timeseries_id_if_exists(self, meta_data):

        """
        Check whether a timeseries id exists in the database for a given set of meta data
        :param meta_data: Dict with 'latitude', 'longitude', 'source', 'variable', 'unit', 'unit_type' keys
        :return: timeseries id if exist else raise DatabaseAdapterError
        """
        event_id = self.generate_timeseries_id(meta_data)

        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                sql_statement = "SELECT 1 FROM `run` WHERE `id`=%s"
                is_exist = cursor.execute(sql_statement, event_id)
            return event_id if is_exist > 0 else None
        except Exception as ex:
            error_message = "Retrieving timeseries id for metadata={} failed.".format(meta_data)
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                connection.close()

    def is_id_exists(self, id_):
        """
        Check whether a given timeseries id exists in the database
        :param id_:
        :return: True, if id is in the database, False otherwise
        """
        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                sql_statement = "SELECT 1 FROM `run` WHERE `id`=%s"
                is_exist = cursor.execute(sql_statement, id_)
            return True if is_exist > 0 else False
        except Exception as ex:
            error_message = "Check operation to find timeseries id {} in the run table failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise False
        finally:
            if connection is not None:
                connection.close()

    def insert_data(self, timeseries, upsert=False):
        """
        Insert timeseries to Data table in the database
        :param tms_id: hash value
        :param timeseries: list of [tms_id, time, value] lists
        :param boolean upsert: If True, upsert existing values ON DUPLICATE KEY. Default is False.
        Ref: 1). https://stackoverflow.com/a/14383794/1461060
             2). https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
        :return: row count if insertion was successful, else raise DatabaseAdapterError
        """

        row_count = 0
        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                if upsert:
                    sql_statement = "INSERT INTO `data` (`id`, `time`, `value`) VALUES (%s, %s, %s) " \
                                    "ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"
                else:
                    sql_statement = "INSERT INTO `data` (`id`, `time`, `value`) VALUES (%s, %s, %s)"
                row_count = cursor.executemany(sql_statement, timeseries)
            connection.commit()
            return row_count
        except Exception as ex:
            connection.rollback()
            error_message = "Data insertion to data table for tms id {}, upsert={} failed.".format(timeseries[0][0],
                    upsert)
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)

        finally:
            if connection is not None:
                connection.close()

    def insert_timeseries(self, timeseries, latitude, longitude, source, variable, unit, unit_type):
        """
        Insert new timeseries into the Run table and Data table, this will generate the tieseries id from the given meta data
        :param timeseries: list of [time, value] lists
        :param latitude:
        :param longitude:
        :param source:
        :param variable:
        :param unit:
        :param unit_type: str value
        :return: str: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """
        tms_meta = {
                'latitude' : latitude,
                'longitude': longitude,
                'source'   : source,
                'variable' : variable,
                'unit'     : unit,
                'unit_type': unit_type
                }

        tms_id = Timeseries.get_timeseries_id_if_exists(tms_meta)

        connection = self.pool.connection()

        if tms_id is None:

            try:
                sql_statements = [
                        "SELECT `id` as `source_id` FROM `source` WHERE `source`=%s",
                        "SELECT `id` as `station_id` FROM `station` WHERE `latitude`=%s and `longitude`=%s",
                        "SELECT `id` as `unit_id` FROM `unit` WHERE `unit`=%s and `type`=%s",
                        "SELECT `id` as `variable_id` FROM `variable` WHERE `variable`=%s"
                        ]

                station_id = None
                source_id = None
                variable_id = None
                unit_id = None

                with connection.cursor() as cursor1:
                    source_id = cursor1.execute(sql_statements[0], source).fetchone()
                with connection.cursor() as cursor2:
                    station_id = cursor2.execute(sql_statements[1], (latitude, longitude)).fetchone()
                with connection.cursor() as cursor3:
                    unit_id = cursor3.execute(sql_statements[2], (unit, unit_type)).fetchone()
                with connection.cursor() as cursor4:
                    variable_id = cursor4.execute(sql_statements[3], variable).fetchone()

                with connection.cursor() as cursor5:
                    sql_statement = "INSERT INTO `run` (`id`, `station`, `source`, `variable`, `unit`) " \
                                    "VALUES ( %s, %s, %s, %s, %s)"
                    sql_values = (tms_id, station_id, source_id, variable_id, unit_id)
                    cursor5.execute(sql_statement, sql_values)

                connection.commit()

            except Exception as ex:
                connection.rollback()
                error_message = "Insertion to run table failed for timeseries with latitude={}, longitude={}," \
                                " source={}, variable={}, unit={}, unit_type={}" \
                    .format(latitude, longitude, source, variable, unit, unit_type)
                logger.error(error_message)
                traceback.print_exc()
                raise DatabaseAdapterError(error_message, ex)
            finally:
                if connection is not None:
                    connection.close()

        try:
            new_timeseries = []
            for t in [i for i in timeseries]:
                if len(t) > 1:
                    # Insert EventId in front of timestamp, value list
                    t.insert(0, tms_id)
                    new_timeseries.append(t)
                else:
                    logger.warning('Invalid timeseries data:: %s', t)
            self.insert_data(new_timeseries, True)

            return tms_id
        except Exception as ex:
            connection.rollback()
            error_message = "Insertion to data table failed for timeseries with latitude={}, longitude={}," \
                            " source={}, variable={}, unit={}, unit_type={}" \
                .format(latitude, longitude, source, variable, unit, unit_type)
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                connection.close()

    def insert_timeseries(self, timeseries, run_tuple):

        """
        Insert new timeseries into the Run table and Data table, for given timeseries id
        :param tms_id:
        :param timeseries: list of [tms_id, time, fgt, value] lists
        :param run_tuple: tuples like
        (tms_id[0], station_id[1], source_id[2], variable_id[3], unit_id[4])
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `run` (`id`, `station`, `source`, `variable`, `unit`) " \
                                "VALUES ( %s, %s, %s, %s, %s)"
                sql_values = run_tuple
                cursor.execute(sql_statement, sql_values)

            connection.commit()
            self.insert_data(timeseries, True)
            return run_tuple[0]
        except Exception as ex:
            connection.rollback()
            error_message = "Insertion failed for timeseries with tms_id={}, station_id={}, source_id={}," \
                            " variable_id={}, unit_id={}" \
                .format(run_tuple[0], run_tuple[1], run_tuple[2], run_tuple[3], run_tuple[4])
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                connection.close()

    def insert_run(self, run_tuple):
        """
        Insert new run entry
        :param run_tuple: tuple like
        (tms_id[0], station_id[1], source_id[2], variable_id[3], unit_id[4])
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `run` (`id`, `station`, `source`, `variable`, `unit`) " \
                                "VALUES ( %s, %s, %s, %s, %s)"
                cursor.execute(sql_statement, run_tuple)

            connection.commit()
            return run_tuple[0]
        except Exception as ex:
            connection.rollback()
            error_message = "Insertion failed for run enty with tms_id={}, station_id={}, source_id={}," \
                            " variable_id={}, unit_id={}" \
                .format(run_tuple[0], run_tuple[1], run_tuple[2], run_tuple[3], run_tuple[4])
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                connection.close()
