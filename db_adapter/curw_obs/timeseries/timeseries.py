import pandas as pd
import hashlib
import json
import traceback
from pymysql import IntegrityError
from datetime import datetime, timedelta

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError, DuplicateEntryError
from db_adapter.curw_obs.station import StationEnum
from db_adapter.constants import COMMON_DATE_TIME_FORMAT


class Timeseries:
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def generate_timeseries_id(meta_data):
        # def generate_timeseries_id(meta_data: object) -> object:

        """
        Generate the event id for given metadata
        Only 'latitude', 'longitude', 'station_type', 'variable', 'unit', 'unit_type'
        are used to generate the id (i.e. hash value)

        :param meta_data: Dict with 'latitude', 'longitude', 'station_type', 'variable',
        'unit', 'unit_type' keys
        :return: str: sha256 hash value in hex format (length of 64 characters)
        """

        sha256 = hashlib.sha256()
        hash_data = {
                'latitude'    : '',
                'longitude'   : '',
                'station_type': '',
                'variable'    : '',
                'unit'        : '',
                'unit_type'   : ''
                }

        for key in hash_data.keys():
            hash_data[key] = meta_data[key]

        sha256.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        event_id = sha256.hexdigest()
        return event_id

    def get_timeseries_id_if_exists(self, meta_data):

        """
        Check whether a timeseries id exists in the database for a given set of meta data
        :param meta_data: Dict with 'latitude', 'longitude', 'station_type', 'variable',
        'unit', 'unit_type' keys
        :return: timeseries id if exist else raise DatabaseAdapterError
        """
        event_id = self.generate_timeseries_id(meta_data)

        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                sql_statement = "SELECT 1 FROM `run` WHERE `id`=%s"
                is_exist = cursor.execute(sql_statement, event_id)
            return event_id if is_exist > 0 else None
        except Exception as exception:
            error_message = "Retrieving timeseries id for metadata={} failed.".format(meta_data)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
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
        except Exception as exception:
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
        except Exception as exception:
            connection.rollback()
            error_message = "Data insertion to data table for tms id {}, upsert={} failed.".format(timeseries[0][0],
                    upsert)
            logger.error(error_message)
            traceback.print_exc()
            raise exception

        finally:
            if connection is not None:
                connection.close()

    # def insert_timeseries(self, timeseries, run_tuple):
    #
    #     """
    #     Insert new timeseries into the Run table and Data table, for given timeseries id
    #     :param tms_id:
    #     :param timeseries: list of [tms_id, time, value] lists
    #     :param run_tuple: tuples like
    #     (tms_id[0], run_name[1], start_date[2], end_date[3], station_id[4], variable_id[5], unit_id[6])
    #     :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
    #     """
    #
    #     connection = self.pool.connection()
    #     try:
    #
    #         with connection.cursor() as cursor:
    #             sql_statement = "INSERT INTO `run` (`id`, `run_name`, `start_date`, `end_date`, `station`, " \
    #                             "`variable`, `unit`) " \
    #                             "VALUES ( %s, %s, %s, %s, %s, %s, %s)"
    #             sql_values = run_tuple
    #             cursor.execute(sql_statement, sql_values)
    #
    #         connection.commit()
    #         self.insert_data(timeseries, True)
    #         return run_tuple[0]
    #     except Exception as exception:
    #         connection.rollback()
    #         error_message = "Insertion failed for timeseries with tms_id={}, run_name={}, station_id={}, " \
    #                         " variable_id={}, unit_id={}" \
    #             .format(run_tuple[0], run_tuple[1], run_tuple[4], run_tuple[5], run_tuple[6])
    #         logger.error(error_message)
    #         traceback.print_exc()
    #         raise exception
    #     finally:
    #         if connection is not None:
    #             connection.close()

    # def insert_run(self, run_tuple):
    #     """
    #     Insert new run entry
    #     :param run_tuple: tuple like
    #     (tms_id[0], run_name[1], start_date[2], end_date[3], station_id[4], variable_id[5], unit_id[6])
    #     :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
    #     """
    #
    #     connection = self.pool.connection()
    #     try:
    #
    #         with connection.cursor() as cursor:
    #             sql_statement = "INSERT INTO `run` (`id`, `run_name`, `start_date`, `end_date`, `station`, " \
    #                             "`variable`, `unit`) " \
    #                             "VALUES ( %s, %s, %s, %s, %s, %s, %s)"
    #             cursor.execute(sql_statement, run_tuple)
    #
    #         connection.commit()
    #         return run_tuple[0]
    #     except Exception as exception:
    #         connection.rollback()
    #         error_message = "Insertion failed for run enty with tms_id={}, run_name={}, station_id={}, " \
    #                         " variable_id={}, unit_id={}" \
    #             .format(run_tuple[0], run_tuple[1], run_tuple[4], run_tuple[5], run_tuple[6])
    #         logger.error(error_message)
    #         traceback.print_exc()
    #         raise exception
    #     finally:
    #         if connection is not None:
    #             connection.close()

    def insert_run(self, run_meta):
        """
        Insert new run entry
        :param run_meta: dictionary like
        {
            'tms_id'  : '',
            'run_name' : '',
            'station_id'  : '',
            'unit_id'     : '',
            'variable_id': ''
        }
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `run` (`id`, `station`, `variable`, `unit`) " \
                                "VALUES ( %s, %s, %s, %s)"
                cursor.execute(sql_statement, (run_meta.get('tms_id'), run_meta.get('station_id'),
                                               run_meta.get('variable_id'), run_meta.get('unit_id')))

            connection.commit()
            return run_meta.get('tms_id')
        except Exception as exception:
            connection.rollback()
            error_message = "Insertion failed for run entry with tms_id={}, station_id={}, " \
                            " variable_id={}, unit_id={}" \
                .format(run_meta.get('tms_id'), run_meta.get('station_id'),
                    run_meta.get('variable_id'), run_meta.get('unit_id'))
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_end_date(self, id_):
        """
        Retrieve end date
        :param id_: timeseries id
        :return: end_date
        """

        connection = self.pool.connection()

        try:
            with connection.cursor() as cursor:
                sql_statement = "SELECT `end_date` FROM `run` WHERE `id`=%s"
                row_count= cursor.execute(sql_statement, id_)
                if row_count > 0:
                    return cursor.fetchone()['end_date']
            return None
        except Exception as exception:
            error_message = "Retrieving end_date for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def update_end_date(self, id_, end_date):
        """
        Update end_date for inserted timeseries, if end date is latest date than the existing one
        :param id_: timeseries id
        :return: end_date if update is successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()

        if type(end_date) is str:
            end_date = datetime.strptime(end_date, COMMON_DATE_TIME_FORMAT)

        existing_end_date = None
        try:

            with connection.cursor() as cursor:
                sql_statement = "SELECT `end_date` FROM `run` WHERE `id`=%s"
                row_count= cursor.execute(sql_statement, id_)
                if row_count > 0:
                    existing_end_date = cursor.fetchone()['end_date']

            if existing_end_date is None or existing_end_date < end_date:
                with connection.cursor() as cursor:
                    sql_statement = "UPDATE `run` SET `end_date`=%s WHERE `id`=%s"
                    cursor.execute(sql_statement, (end_date, id_))

            connection.commit()
            return end_date
        except Exception as exception:
            connection.rollback()
            error_message = "Updating end_date for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def update_start_date(self, id_, start_date):
        """
            Update (very first obs date) start_date for inserted timeseries, if start_date is earlier date than the existing one
            :param id_: timeseries id
            :return: start_date if update is successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()

        if type(start_date) is str:
            start_date = datetime.strptime(start_date, COMMON_DATE_TIME_FORMAT)

        existing_start_date = None

        try:

            with connection.cursor() as cursor:
                sql_statement = "SELECT `start_date` FROM `run` WHERE `id`=%s"
                row_count = cursor.execute(sql_statement, id_)
                if row_count > 0:
                    existing_start_date = cursor.fetchone()['start_date']

            if existing_start_date is None or existing_start_date > start_date:
                with connection.cursor() as cursor:
                    sql_statement = "UPDATE `run` SET `start_date`=%s WHERE `id`=%s"
                    cursor.execute(sql_statement, (start_date, id_))
            connection.commit()
            return start_date
        except Exception as exception:
            connection.rollback()
            error_message = "Updating start_date for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()
