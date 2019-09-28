import pandas as pd
import hashlib
import json
import traceback
from pymysql import IntegrityError

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError
from db_adapter.curw_sim.grids import GridInterpolationEnum


class Timeseries:
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def generate_timeseries_id(meta_data):
        # def generate_timeseries_id(meta_data: object) -> object:

        """
        Generate the event id for given metadata
        Only 'latitude', 'longitude', 'model', 'method'
        are used to generate the id (i.e. hash value)

        :param meta_data: Dict with 'latitude', 'longitude', 'model', 'method' keys
        :return: str: sha256 hash value in hex format (length of 64 characters)
        """

        sha256 = hashlib.sha256()
        hash_data = {
                'latitude' : '',
                'longitude': '',
                'model'    : '',
                'method'   : ''
                }

        for key in hash_data.keys():
            hash_data[key] = meta_data[key]

        sha256.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        event_id = sha256.hexdigest()
        return event_id

    def get_timeseries_id_if_exists(self, meta_data):

        """
        Check whether a timeseries id exists in the database for a given set of meta data
        :param meta_data: Dict with ''latitude', 'longitude', 'model', 'method' keys
        :return: timeseries id if exist else raise DatabaseAdapterError
        """
        event_id = self.generate_timeseries_id(meta_data)

        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                sql_statement = "SELECT 1 FROM `dis_run` WHERE `id`=%s"
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

    def get_timeseries_id(self, grid_id, method):

        """
        Check whether a timeseries id exists in the database dis_run table for a given grid_id and method
        :param grid_id: grid id (e.g.: flo2d_250_954)
        :param method: value interpolation method
        :return: timeseries id if exist else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                sql_statement = "SELECT `id` FROM `dis_run` WHERE `grid_id`=%s AND `method`=%s;"
                result = cursor.execute(sql_statement, (grid_id, method))
                if result > 0:
                    return cursor.fetchone()['id']
                else:
                    return None
        except Exception as exception:
            error_message = "Retrieving timeseries id for grid_id={} failed.".format(grid_id)
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
                sql_statement = "SELECT 1 FROM `dis_run` WHERE `id`=%s"
                is_exist = cursor.execute(sql_statement, id_)
            return False if is_exist > 0 is None else True
        except Exception as exception:
            error_message = "Check operation to find timeseries id {} in the dis_run table failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise False
        finally:
            if connection is not None:
                connection.close()

    def insert_data(self, timeseries, tms_id, upsert=False):
        """
        Insert timeseries to Data table in the database
        :param tms_id: hash value
        :param timeseries: list of [tms_id, time, value] lists
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
                new_timeseries.append(t)
            else:
                logger.warning('Invalid timeseries data:: %s', t)

        row_count = 0
        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                if upsert:
                    sql_statement = "INSERT INTO `dis_data` (`id`, `time`, `value`) VALUES (%s, %s, %s) " \
                                    "ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"
                else:
                    sql_statement = "INSERT INTO `dis_data` (`id`, `time`, `value`) VALUES (%s, %s, %s)"
                row_count = cursor.executemany(sql_statement, timeseries)
            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Data insertion to dis_data table for tms id {}, upsert={} failed.".format(timeseries[0][0],
                    upsert)
            logger.error(error_message)
            traceback.print_exc()
            raise exception

        finally:
            if connection is not None:
                connection.close()

    def insert_data_max(self, timeseries, tms_id, upsert=False):
        """
        Insert timeseries to DataMax table in the database
        :param tms_id: hash value
        :param timeseries: list of [tms_id, time, value] lists
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
                new_timeseries.append(t)
            else:
                logger.warning('Invalid timeseries data:: %s', t)

        row_count = 0
        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                if upsert:
                    sql_statement = "INSERT INTO `dis_data_max` (`id`, `time`, `value`) VALUES (%s, %s, %s) " \
                                    "ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"
                else:
                    sql_statement = "INSERT INTO `dis_data_max` (`id`, `time`, `value`) VALUES (%s, %s, %s)"
                row_count = cursor.executemany(sql_statement, timeseries)
            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Data insertion to dis_data table for tms id {}, upsert={} failed.".format(timeseries[0][0],
                    upsert)
            logger.error(error_message)
            traceback.print_exc()
            raise exception

        finally:
            if connection is not None:
                connection.close()

    def insert_data_min(self, timeseries, tms_id, upsert=False):
        """
        Insert timeseries to DataMin table in the database
        :param tms_id: hash value
        :param timeseries: list of [tms_id, time, value] lists
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
                new_timeseries.append(t)
            else:
                logger.warning('Invalid timeseries data:: %s', t)

        row_count = 0
        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                if upsert:
                    sql_statement = "INSERT INTO `dis_data_min` (`id`, `time`, `value`) VALUES (%s, %s, %s) " \
                                    "ON DUPLICATE KEY UPDATE `value`=VALUES(`value`)"
                else:
                    sql_statement = "INSERT INTO `dis_data_min` (`id`, `time`, `value`) VALUES (%s, %s, %s)"
                row_count = cursor.executemany(sql_statement, timeseries)
            connection.commit()
            return row_count
        except Exception as exception:
            connection.rollback()
            error_message = "Data insertion to dis_data table for tms id {}, upsert={} failed.".format(timeseries[0][0],
                    upsert)
            logger.error(error_message)
            traceback.print_exc()
            raise exception

        finally:
            if connection is not None:
                connection.close()

    def insert_run(self, meta_data):
        """
        Insert new dis_run entry
        :param meta_data: dictionary like
        meta_data = {
                'id'       : '',
                'latitude' : '',
                'longitude': '',
                'model'    : '',
                'method'   : '',
                'grid_id'  : '',
                'obs_end'  : ''
                }
           grid_id and obs_end keys are optional
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        if 'grid_id' in meta_data.keys() and 'obs_end' in meta_data.keys():
            sql_statement = "INSERT INTO `dis_run` (`id`, `latitude`, `longitude`, `model`, `method`, " \
                            "`grid_id`, `obs_end`) " \
                            "VALUES ( %s, %s, %s, %s, %s, %s, %s)"
            dis_run_tuple = (meta_data['id'], meta_data['latitude'], meta_data['longitude'], meta_data['model'],
                         meta_data['method'], meta_data['grid_id'], meta_data['obs_end'])

        elif 'grid_id' in meta_data.keys():
            sql_statement = "INSERT INTO `dis_run` (`id`, `latitude`, `longitude`, `model`, `method`, `grid_id`) " \
                            "VALUES ( %s, %s, %s, %s, %s, %s)"
            dis_run_tuple = (meta_data['id'], meta_data['latitude'], meta_data['longitude'], meta_data['model'],
                         meta_data['method'], meta_data['grid_id'])
        elif 'obs_end' in meta_data.keys():
            sql_statement = "INSERT INTO `dis_run` (`id`, `latitude`, `longitude`, `model`, `method`, `obs_end`) " \
                            "VALUES ( %s, %s, %s, %s, %s, %s)"
            dis_run_tuple = (meta_data['id'], meta_data['latitude'], meta_data['longitude'], meta_data['model'],
                         meta_data['method'], meta_data['obs_end'])
        else:
            sql_statement = "INSERT INTO `dis_run` (`id`, `latitude`, `longitude`, `model`, `method`) " \
                            "VALUES ( %s, %s, %s, %s, %s)"
            dis_run_tuple = (meta_data['id'], meta_data['latitude'], meta_data['longitude'], meta_data['model'],
                         meta_data['method'])

        connection = self.pool.connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql_statement, dis_run_tuple)

            connection.commit()
            return dis_run_tuple[0]
        except Exception as exception:
            connection.rollback()
            error_message = "Insertion failed for timeseries with tms_id={}, latitude={}, longitude={}, model={}," \
                            " method={}" \
                .format(dis_run_tuple[0], dis_run_tuple[1], dis_run_tuple[2], dis_run_tuple[3], dis_run_tuple[4])
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def update_latest_obs(self, id_, obs_end):
        """
        Update obs_end for inserted timeseries
        :param id_: timeseries id
        :param obs_end: end time of observations
        :return: True if update is successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "UPDATE `dis_run` SET `obs_end`=%s WHERE `id`=%s"
                cursor.execute(sql_statement, (obs_end, id_))
            connection.commit()
            return True
        except Exception as exception:
            connection.rollback()
            error_message = "Updating obs_end for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_obs_end(self, id_):
        """
        Retrieve obs_end for a given hash id
        :param id_:
        :return: obs_end if exists. None if doesn't exist, raise DatabaseAdapterError otherwise
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "SELECT `obs_end` FROM `dis_run` WHERE `id`=%s"
                result = cursor.execute(sql_statement, id_)
                if result > 0:
                    return cursor.fetchone()['obs_end']
                else:
                    return None
        except Exception as exception:
            error_message = "Retrieving obs_end for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def update_hash_id(self, existing_id, new_id):
        """
        Update hash id in dis_run table
        :param existing_id: existing hash id
        :param new_id: newly generated hash id
        :return: True if the update was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "UPDATE `dis_run` SET `id`=%s WHERE `id`=%s;"
                cursor.execute(sql_statement, (new_id, existing_id))
            connection.commit()
            return True
        except Exception as exception:
            connection.rollback()
            error_message = "Updating hash id {} to id={} failed.".format(existing_id, new_id)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_timeseries(self, id_, start_date, end_date):
        """
        Retrieve timeseries by id
        :param id_:
        :return: list of [time, value] pairs if id exists, else None
        """

        connection = self.pool.connection()
        ts = []
        try:

            with connection.cursor() as cursor:
                sql_statement = "SELECT `time`,`value` FROM `dis_data` WHERE `id`=%s AND `time` BETWEEN %s AND %s;"
                rows = cursor.execute(sql_statement, (id_, start_date, end_date))
                if rows > 0:
                    results = cursor.fetchall()
                    for result in results:
                        ts.append([result.get('time'), result.get('value')])
            return ts
        except Exception as exception:
            error_message = "Retrieving timeseries for id {} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def get_timeseries_end(self, id_):
        """
        Retrieve timeseries by id
        :param id_:
        :return: last timestamp if id exists, else None
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "SELECT max(`time`) AS `time` FROM `dis_data` WHERE `id`=%s ;"
                rows = cursor.execute(sql_statement, id_)
                if rows > 0:
                    return cursor.fetchone()['time']
                else:
                    return None
        except Exception as exception:
            error_message = "Retrieving timeseries end for id {} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()

    def update_grid_id(self, id_, grid_id):
        """
        Update gird id for inserted timeseries
        :param id_: timeseries id
        :param grid_id: link to the grid maps
        :return: True if update is successful, else raise DatabaseAdapterError
        """

        connection = self.pool.connection()
        try:

            with connection.cursor() as cursor:
                sql_statement = "UPDATE `dis_run` SET `grid_id`=%s WHERE `id`=%s"
                cursor.execute(sql_statement, (grid_id, id_))
            connection.commit()
            return True
        except Exception as exception:
            connection.rollback()
            error_message = "Updating grid_id for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise exception
        finally:
            if connection is not None:
                connection.close()
