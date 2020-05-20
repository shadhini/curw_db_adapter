import pandas as pd
import hashlib
import json
import traceback
from pymysql import IntegrityError
from datetime import datetime, timedelta

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError, DuplicateEntryError
from db_adapter.constants import COMMON_DATE_TIME_FORMAT


class Timeseries:
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def generate_timeseries_id(meta_data):
        # def generate_timeseries_id(meta_data: object) -> object:

        """
        Generate the event id for given metadata
        Only 'sim_tag', 'latitude', 'longitude', 'model', 'version', 'variable', 'unit', 'unit_type'
        are used to generate the id (i.e. hash value)

        :param meta_data: Dict with 'sim_tag', 'latitude', 'longitude', 'model', 'version', 'variable',
        'unit', 'unit_type' keys
        :return: str: sha256 hash value in hex format (length of 64 characters)
        """

        sha256 = hashlib.sha256()
        hash_data = {
                'sim_tag'  : '',
                'latitude' : '',
                'longitude': '',
                'model'    : '',
                'version'  : '',
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
        :param meta_data: Dict with 'sim_tag', 'latitude', 'longitude', 'model', 'version', 'variable',
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

    def insert_formatted_data(self, timeseries, upsert=False):
        """
        Insert timeseries to Data table in the database
        :param timeseries: list of [tms_id, time, fgt, value] lists
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
                    sql_statement = "INSERT INTO `data` (`id`, `time`, `fgt`, `value`) VALUES (%s, %s, %s, %s) " \
                                    "ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"
                else:
                    sql_statement = "INSERT INTO `data` (`id`, `time`, `fgt`, `value`) VALUES (%s, %s, %s, %s)"
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

    def insert_data(self, timeseries, tms_id, fgt, upsert=False):
        """
        Insert timeseries to Data table in the database
        :param tms_id: hash value
        :param fgt: forecast generated time
        :param timeseries: list of [time, value] lists
        :param boolean upsert: If True, upsert existing values ON DUPLICATE KEY. Default is False.
        Ref: 1). https://stackoverflow.com/a/14383794/1461060
             2). https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
        :return: row count if insertion was successful, else raise DatabaseAdapterError
        """

        new_timeseries = []
        for t in [i for i in timeseries]:
            if len(t) > 1:
                # Insert EventId in front of timestamp, value list
                t.insert(0, tms_id)
                t.insert(2, fgt)
                new_timeseries.append(t)
            else:
                logger.warning('Invalid timeseries data:: %s', t)

        row_count = 0

        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                if upsert:
                    sql_statement = "INSERT INTO `data` (`id`, `time`, `fgt`, `value`) VALUES (%s, %s, %s, %s) " \
                                    "ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"
                else:
                    sql_statement = "INSERT INTO `data` (`id`, `time`, `fgt`, `value`) VALUES (%s, %s, %s, %s)"
                row_count = cursor.executemany(sql_statement, new_timeseries)
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

    def insert_timeseries(self, timeseries, run_tuple):

        """
        Insert new timeseries into the Run table and Data table, for given timeseries id
        :param tms_id:
        :param timeseries: list of [tms_id, time, fgt, value] lists
        :param run_tuple: tuples like
        (tms_id[0], sim_tag[1], start_date[2], end_date[3], station_id[4], source_id[5], variable_id[6], unit_id[7])
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `run` (`id`, `sim_tag`, `start_date`, `end_date`, `station`, `source`, " \
                                "`variable`, `unit`) " \
                                "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"
                sql_values = run_tuple
                cursor.execute(sql_statement, sql_values)

            connection.commit()
            self.insert_data(timeseries, True)
            return run_tuple[0]
        # except IntegrityError as ie:
        #     connection.rollback()
        #     if ie.args[0] == 1062:
        #         error_message = "Timeseries id {} already exists in the database".format(run_tuple[0])
        #         logger.info("Timeseries id {} already exists in the database".format(run_tuple[0]))
        #         print("Timeseries id {} already exists in the database".format(run_tuple[0]))
        #         raise DuplicateEntryError(error_message, ie)
        #     else:
        #         error_message = "Insertion failed for timeseries with tms_id={}, sim_tag={}, scheduled_date={}, " \
        #                         "station_id={}, source_id={}, variable_id={}, unit_id={}, fgt={}" \
        #             .format(run_tuple[0], run_tuple[1], run_tuple[9], run_tuple[4], run_tuple[5], run_tuple[6],
        #                 run_tuple[7], run_tuple[8])
        #         logger.error(error_message)
        #         traceback.print_exc()
        #         raise DatabaseAdapterError(error_message, ie)
        except Exception as exception:
            connection.rollback()
            error_message = "Insertion failed for timeseries with tms_id={}, sim_tag={}, station_id={}, source_id={}," \
                            " variable_id={}, unit_id={}" \
                .format(run_tuple[0], run_tuple[1], run_tuple[4], run_tuple[5], run_tuple[6], run_tuple[7])
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    # def insert_run(self, run_tuple):
    #     """
    #     Insert new run entry
    #     :param run_tuple: tuple like
    #     (tms_id[0], sim_tag[1], start_date[2], end_date[3], station_id[4], source_id[5], variable_id[6], unit_id[7])
    #     :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
    #     """
    #
    #     connection = self.pool.connection()
    #     try:
    #
    #         with connection.cursor() as cursor:
    #             sql_statement = "INSERT INTO `run` (`id`, `sim_tag`, `start_date`, `end_date`, `station`, `source`, " \
    #                             "`variable`, `unit`) " \
    #                             "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"
    #             cursor.execute(sql_statement, run_tuple)
    #
    #         connection.commit()
    #         return run_tuple[0]
    #     except Exception as exception:
    #         connection.rollback()
    #         error_message = "Insertion failed for run enty with tms_id={}, sim_tag={}, station_id={}, source_id={}," \
    #                         " variable_id={}, unit_id={}" \
    #             .format(run_tuple[0], run_tuple[1], run_tuple[4], run_tuple[5], run_tuple[6], run_tuple[7])
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
            'sim_tag' : '',
            'start_date': '',
            'end_date': '',
            'station_id'  : '',
            'source_id' : '',
            'unit_id'     : '',
            'variable_id': ''
        }
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `run` (`id`, `sim_tag`, `start_date`, `end_date`, `station`, `source`, " \
                                "`variable`, `unit`) " \
                                "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql_statement, (run_meta.get('tms_id'), run_meta.get('sim_tag'),
                                               run_meta.get('start_date'), run_meta.get('end_date'), run_meta.get('station_id'),
                                               run_meta.get('source_id'), run_meta.get('variable_id'), run_meta.get('unit_id')))

            connection.commit()
            return run_meta.get('tms_id')
        except Exception as exception:
            connection.rollback()
            error_message = "Insertion failed for run enty with tms_id={}, sim_tag={}, station_id={}, source_id={}," \
                            " variable_id={}, unit_id={}" \
                .format(run_meta.get('tms_id'), run_meta.get('sim_tag'), run_meta.get('station_id'),
                    run_meta.get('source_id'), run_meta.get('variable_id'), run_meta.get('unit_id'))
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def update_latest_fgt(self, id_, fgt):
        """
        Update fgt for inserted timeseries, if new fgt is latest date than the existing
        :param id_: timeseries id
        :return: scheduled data if update is successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()

        if type(fgt) is str:
            fgt = datetime.strptime(fgt, COMMON_DATE_TIME_FORMAT)

        existing_end_date = None
        try:

            with connection.cursor() as cursor:
                sql_statement = "SELECT `end_date` FROM `run` WHERE `id`=%s"
                row_count= cursor.execute(sql_statement, id_)
                if row_count > 0:
                    existing_end_date = cursor.fetchone()['end_date']

            if existing_end_date is None or existing_end_date < fgt:
                with connection.cursor() as cursor2:
                    sql_statement = "UPDATE `run` SET `end_date`=%s WHERE `id`=%s"
                    cursor2.execute(sql_statement, (fgt, id_))

            connection.commit()
            return
        except Exception as exception:
            connection.rollback()
            error_message = "Updating fgt for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_latest_fgt(self, id_):
        """
        Retrive latest fgt for given id
        :param id_: timeseries id
        :return:
        """

        connection = self.pool.connection()

        try:

            with connection.cursor() as cursor:
                sql_statement = "SELECT `end_date` FROM `run` WHERE `id`=%s"
                row_count= cursor.execute(sql_statement, id_)
                if row_count > 0:
                    return cursor.fetchone()['end_date']
        except Exception as exception:
            error_message = "Retrieving latest fgt for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_end_date(self, sim_tag, station_id, source_id, variable_id, unit_id):

        """
        Retrieve the latest fgt of fcst timeseries available for the given parameters
        :param sim_tag:
        :param station_id:
        :param source_id:
        :param variable_id:
        :param unit_id:
        :return:
        """

        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor1:
                sql_statement = "SELECT `end_date` FROM `run` WHERE `source`=%s AND `station`=%s " \
                                "AND `sim_tag`=%s AND `variable`=%s AND `unit`=%s;"
                is_exist = cursor1.execute(sql_statement, (source_id, station_id, sim_tag, variable_id, unit_id))
                if is_exist > 0:
                    return cursor1.fetchone()['end_date']
                else:
                    return None

        except Exception as exception:
            error_message = "Retrieving latest fgt failed."
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def update_start_date(self, id_, start_date, force=False):
        """
            Update (very first fgt) start_date for inserted timeseries, if new start_date is earlier than the existing
            :param id_: timeseries id
            :return: scheduled data if update is successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()

        if type(start_date) is str:
            start_date = datetime.strptime(start_date, COMMON_DATE_TIME_FORMAT)

        try:

            if not force:
                existing_start_date = None

                with connection.cursor() as cursor:
                    sql_statement = "SELECT `start_date` FROM `run` WHERE `id`=%s"
                    row_count= cursor.execute(sql_statement, id_)
                    if row_count > 0:
                        existing_start_date = cursor.fetchone()['start_date']

                if existing_start_date is None or existing_start_date > start_date:
                    with connection.cursor() as cursor2:
                        sql_statement = "UPDATE `run` SET `start_date`=%s WHERE `id`=%s"
                        cursor2.execute(sql_statement, (start_date, id_))

            else:
                with connection.cursor() as cursor3:
                    sql_statement = "UPDATE `run` SET `start_date`=%s WHERE `id`=%s"
                    cursor3.execute(sql_statement, (start_date, id_))

            connection.commit()
            return
        except Exception as exception:
            connection.rollback()
            error_message = "Updating start_date for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_latest_timeseries(self, sim_tag, station_id, source_id, variable_id, unit_id, start=None):

        """
        Retrieve the latest fcst timeseries available for the given parameters
        :param sim_tag:
        :param station_id:
        :param source_id:
        :param variable_id:
        :param unit_id:
        :param start: expected beginning of the timeseries
        :return: return list of lists with time, value pairs [[time, value], [time1, value2]]
        """

        meta_data = {}
        ts = []
        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor1:
                sql_statement = "SELECT `id`, `end_date` FROM `run` WHERE `source`=%s AND `station`=%s " \
                                "AND `sim_tag`=%s AND `variable`=%s AND `unit`=%s;"
                is_exist = cursor1.execute(sql_statement, (source_id, station_id, sim_tag, variable_id, unit_id))
                if is_exist > 0:
                    meta_data = cursor1.fetchone()
                else:
                    return None
            if start:
                with connection.cursor() as cursor2:
                    sql_statement = "SELECT `time`, `value` FROM `data` WHERE `id`=%s AND `fgt`=%s AND `time` >= %s;"
                    rows = cursor2.execute(sql_statement, (meta_data.get('id'), meta_data.get('end_date'), start))
                    if rows > 0:
                        results = cursor2.fetchall()
                        for result in results:
                            ts.append([result.get('time'), result.get('value')])
            else:
                with connection.cursor() as cursor2:
                    sql_statement = "SELECT `time`, `value` FROM `data` WHERE `id`=%s AND `fgt`=%s;"
                    rows = cursor2.execute(sql_statement, (meta_data.get('id'), meta_data.get('end_date')))
                    if rows > 0:
                        results = cursor2.fetchall()
                        for result in results:
                            ts.append([result.get('time'), result.get('value')])
            return ts

        except Exception as exception:
            error_message = "Retrieving latest timeseries failed."
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_nearest_timeseries(self, sim_tag, station_id, source_id, variable_id, unit_id, expected_fgt, start=None):

        """
        Retrieve the fcst timeseries nearest to the specified expected fgt and available for the given parameters.
        :param sim_tag:
        :param station_id:
        :param source_id:
        :param variable_id:
        :param unit_id:
        :param expected_fgt:
        :param start: expected beginning of the timeseries
        :return: return list of lists with time, value pairs [[time, value], [time1, value2]]
        """

        meta_data = {}
        ts = []
        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor1:
                sql_statement = "SELECT `id`, `end_date` FROM `run` WHERE `source`=%s AND `station`=%s " \
                                "AND `sim_tag`=%s AND `variable`=%s AND `unit`=%s;"
                is_exist = cursor1.execute(sql_statement, (source_id, station_id, sim_tag, variable_id, unit_id))
                if is_exist > 0:
                    meta_data = cursor1.fetchone()
                else:
                    return None

            with connection.cursor() as cursor3:
                cursor3.callproc('getNearestFGTs', (meta_data.get('id'), expected_fgt))
                fgt = cursor3.fetchone()['fgt']
                if fgt is None:
                    return None

            if start:
                with connection.cursor() as cursor2:
                    sql_statement = "SELECT `time`, `value` FROM `data` WHERE `id`=%s AND `fgt`=%s AND `time` >= %s;"
                    rows = cursor2.execute(sql_statement, (meta_data.get('id'), fgt, start))
                    if rows > 0:
                        results = cursor2.fetchall()
                        for result in results:
                            ts.append([result.get('time'), result.get('value')])
            else:
                with connection.cursor() as cursor2:
                    sql_statement = "SELECT `time`, `value` FROM `data` WHERE `id`=%s AND `fgt`=%s;"
                    rows = cursor2.execute(sql_statement, (meta_data.get('id'), fgt))
                    if rows > 0:
                        results = cursor2.fetchall()
                        for result in results:
                            ts.append([result.get('time'), result.get('value')])
            return ts

        except Exception as exception:
            error_message = "Retrieving latest timeseries failed."
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def delete_timeseries(self, id_, fgt):
        """
        Delete specific timeseries identified by hash id and a fgt
        :param id_: hash id
        :param fgt: fgt
        :return:
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "DELETE FROM `curw_fcst`.`data` WHERE `id`= %s AND `fgt`=%s ;"
                row_count = cursor.execute(sql_statement, (id_, fgt))

            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Deletion of timeseries with hash id {} failed".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def delete_all_by_hash_id(self, id_):
        """
        Delete all timeseries with different fgts but with the same hash id (same meta data)
        :param id_: hash id
        :return:
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "DELETE FROM `curw_fcst`.`run` WHERE `id`= %s ;"
                row_count = cursor.execute(sql_statement, id_)

            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Deletion of timeseries with hash id {} failed".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()
