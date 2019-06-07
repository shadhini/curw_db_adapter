import pandas as pd
import hashlib
import json
import traceback
from pymysql import IntegrityError

from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError, DuplicateEntryError


class Timeseries:
    def __init__(self, pool):
        self.pool = pool

    @staticmethod
    def generate_timeseries_id(meta_data: object) -> object:
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

        connection = self.pool.get_conn()
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
                self.pool.release(connection)

    def is_id_exists(self, id_):
        """
        Check whether a given timeseries id exists in the database
        :param id_:
        :return: True, if id is in the database, False otherwise
        """
        connection = self.pool.get_conn()
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
                self.pool.release(connection)

    def insert_data(self, timeseries, upsert=False):
        """
        Insert timeseries to Data table in the database
        :param tms_id: hash value
        :param timeseries: list of [tms_id, time, fgt, value] lists
        :param boolean upsert: If True, upsert existing values ON DUPLICATE KEY. Default is False.
        Ref: 1). https://stackoverflow.com/a/14383794/1461060
             2). https://chartio.com/resources/tutorials/how-to-insert-if-row-does-not-exist-upsert-in-mysql/
        :return: row count if insertion was successful, else raise DatabaseAdapterError
        """

        row_count = 0
        connection = self.pool.get_conn()
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
        except Exception as ex:
            connection.rollback()
            error_message = "Data insertion to data table for tms id {}, upsert={} failed.".format(timeseries[0][0],
                    upsert)
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)

        finally:
            if connection is not None:
                self.pool.release(connection)

    def insert_timeseries(self, timeseries, sim_tag, latitude, longitude,
                          model, version, variable, unit, unit_type, start_date, end_date, fgt):
        """
        Insert new timeseries into the Run table and Data table, this will generate the tieseries id from the given meta data
        :param timeseries: list of [time, value] lists
        :param sim_tag:
        :param latitude:
        :param longitude:
        :param model:
        :param version:
        :param variable:
        :param unit:
        :param unit_type: str value
        :param fgt:
        :return: str: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """
        tms_meta = {
                'sim_tag'  : sim_tag,
                'latitude' : latitude,
                'longitude': longitude,
                'model'    : model,
                'version'  : version,
                'variable' : variable,
                'unit'     : unit,
                'unit_type': unit_type
                }

        tms_id = Timeseries.get_timeseries_id_if_exists(tms_meta)

        connection = self.pool.get_conn()

        if tms_id is None:

            try:
                sql_statements = [
                        "SELECT `id` as `source_id` FROM `source` WHERE `model`=%s and `version`=%s",
                        "SELECT `id` as `station_id` FROM `station` WHERE `latitude`=%s and `longitude`=%s",
                        "SELECT `id` as `unit_id` FROM `unit` WHERE `unit`=%s and `type`=%s",
                        "SELECT `id` as `variable_id` FROM `variable` WHERE `variable`=%s"
                        ]

                station_id = None
                source_id = None
                variable_id = None
                unit_id = None

                with connection.cursor() as cursor1:
                    source_id = cursor1.execute(sql_statements[0], (model, version)).fetchone()
                with connection.cursor() as cursor2:
                    station_id = cursor2.execute(sql_statements[1], (latitude, longitude)).fetchone()
                with connection.cursor() as cursor3:
                    unit_id = cursor3.execute(sql_statements[2], (unit, unit_type)).fetchone()
                with connection.cursor() as cursor4:
                    variable_id = cursor4.execute(sql_statements[3], variable).fetchone()

                with connection.cursor() as cursor5:
                    sql_statement = "INSERT INTO `run` (`id`, `sim_tag`, `start_date`, `end_date`, `station`, `source`, " \
                                    "`variable`, `unit`) " \
                                    "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"
                    sql_values = (tms_id, sim_tag, start_date, end_date, station_id, source_id, variable_id, unit_id)
                    cursor5.execute(sql_statement, sql_values)

                connection.commit()

            except Exception as ex:
                connection.rollback()
                error_message = "Insertion to run table failed for timeseries with sim_tag={}, latitude={}, longitude={}," \
                                " model={}, version={}, variable={}, unit={}, unit_type={}, fgt={}" \
                    .format(sim_tag, latitude, longitude, model, version, variable, unit, unit_type, fgt)
                logger.error(error_message)
                traceback.print_exc()
                raise DatabaseAdapterError(error_message, ex)
            finally:
                if connection is not None:
                    self.pool.release(connection)

        try:
            new_timeseries = []
            for t in [i for i in timeseries]:
                if len(t) > 1:
                    # Insert EventId in front of timestamp, value list
                    t.insert(0, tms_id)
                    t.insert(2, fgt)
                    new_timeseries.append(t)
                else:
                    logger.warning('Invalid timeseries data:: %s', t)
            self.insert_data(new_timeseries, True)

            self.update_latest_fgt(id_=tms_id, fgt=fgt)

            return tms_id
        except Exception as ex:
            connection.rollback()
            error_message = "Insertion to data table failed for timeseries with sim_tag={}, latitude={}, longitude={}," \
                            " model={}, version={}, variable={}, unit={}, unit_type={}, fgt={}" \
                .format(sim_tag, latitude, longitude, model, version, variable, unit, unit_type, fgt)
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.release(connection)

    def insert_timeseries(self, timeseries, run_tuple):

        """
        Insert new timeseries into the Run table and Data table, for given timeseries id
        :param tms_id:
        :param timeseries: list of [tms_id, time, fgt, value] lists
        :param run_tuple: tuples like
        (tms_id[0], sim_tag[1], start_date[2], end_date[3], station_id[4], source_id[5], variable_id[6], unit_id[7])
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.get_conn()
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
        except Exception as ex:
            connection.rollback()
            error_message = "Insertion failed for timeseries with tms_id={}, sim_tag={}, station_id={}, source_id={}," \
                            " variable_id={}, unit_id={}" \
                .format(run_tuple[0], run_tuple[1], run_tuple[4], run_tuple[5], run_tuple[6], run_tuple[7])
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.release(connection)

    def insert_run(self, run_tuple):
        """
        Insert new run entry
        :param run_tuple: tuple like
        (tms_id[0], sim_tag[1], start_date[2], end_date[3], station_id[4], source_id[5], variable_id[6], unit_id[7])
        :return: timeseries id if insertion was successful, else raise DatabaseAdapterError
        """

        connection = self.pool.get_conn()
        try:

            with connection.cursor() as cursor:
                sql_statement = "INSERT INTO `run` (`id`, `sim_tag`, `start_date`, `end_date`, `station`, `source`, " \
                                "`variable`, `unit`) " \
                                "VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(sql_statement, run_tuple)

            connection.commit()
            return run_tuple[0]
        except Exception as ex:
            connection.rollback()
            error_message = "Insertion failed for run enty with tms_id={}, sim_tag={}, station_id={}, source_id={}," \
                            " variable_id={}, unit_id={}" \
                .format(run_tuple[0], run_tuple[1], run_tuple[4], run_tuple[5], run_tuple[6], run_tuple[7])
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.release(connection)

    def update_latest_fgt(self, id_, fgt):
        """
        Update fgt for inserted timeseries
        :param id_: timeseries id
        :return: scheduled data if update is sccessfull, else raise DatabaseAdapterError
        """

        connection = self.pool.get_conn()
        try:

            with connection.cursor() as cursor:
                sql_statement = "UPDATE `run` SET `end_date`=%s WHERE `id`=%s"
                cursor.execute(sql_statement, (fgt, id_))
            connection.commit()
            return
        except Exception as ex:
            connection.rollback()
            error_message = "Updating fgt for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.release(connection)

    def update_start_date(self, id_, start_date):
        """
            Update (very first fgt) start_date for inserted timeseries
            :param id_: timeseries id
            :return: scheduled data if update is sccessfull, else raise DatabaseAdapterError
        """

        connection = self.pool.get_conn()
        try:

            with connection.cursor() as cursor:
                sql_statement = "UPDATE `run` SET `start_date`=%s WHERE `id`=%s"
                cursor.execute(sql_statement, (start_date, id_))
            connection.commit()
            return
        except Exception as ex:
            connection.rollback()
            error_message = "Updating start_date for id={} failed.".format(id_)
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.release(connection)

    def get_latest_timeseries(self, sim_tag, station_id, source_id, variable_id, unit_id, start="2019-01-01 00:00:00"):

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
        connection = self.pool.get_conn()
        try:
            with connection.cursor() as cursor1:
                sql_statement = "SELECT `id`, `end_date` FROM `run` WHERE `source`=%s AND `station`=%s " \
                                "AND `sim_tag`=%s AND `variable`=%s AND `unit`=%s;"
                is_exist = cursor1.execute(sql_statement, (source_id, station_id, sim_tag, variable_id, unit_id))
                if is_exist > 0:
                    meta_data = cursor1.fetchone()
                else:
                    return None
            with connection.cursor() as cursor2:
                sql_statement = "SELECT `time`, `value` FROM `data` WHERE `id`=%s AND `fgt`=%s AND `time` > %s;"
                rows = cursor2.execute(sql_statement, (meta_data.get('id'), meta_data.get('end_date'), start))
                if rows > 0:
                    results = cursor2.fetchall()
                    for result in results:
                        ts.append([result.get('time'), result.get('value')])
            return ts

        except Exception as ex:
            error_message = "Retrieving latest timeseries failed."
            logger.error(error_message)
            traceback.print_exc()
            raise DatabaseAdapterError(error_message, ex)
        finally:
            if connection is not None:
                self.pool.release(connection)


    # def get_timeseries(self, timeseries_id, start_date, end_date):
    #     """
    #     Retrieves the timeseries corresponding to given id s.t.
    #     time is in between given start_date (inclusive) and end_date (exclusive).
    #
    #     :param timeseries_id: string timeseries id
    #     :param start_date: datetime object
    #     :param end_date: datetime object
    #     :return: array of [id, time, value]: pandas DataFrame
    #     """
    #
    #     if not isinstance(start_date, datetime) or \
    #             not isinstance(end_date, datetime):
    #         raise ValueError(
    #                 'start_date and/or end_date are not of datetime type.',
    #                 start_date, end_date)
    #
    #     session = self.Session()
    #     try:
    #         result = session.query(Data).filter(
    #                 Data.id==timeseries_id,
    #                 Data.time >= start_date, Data.time < end_date
    #                 ).all()
    #         timeseries = [[data_obj.time, data_obj.value] for data_obj in result]
    #         return pd.DataFrame(
    #                 data=timeseries,
    #                 columns=['time', 'value']) \
    #             .set_index(keys='time')
    #     finally:
    #         session.close()
    #
    # def update_timeseries(self, timeseries_id, timeseries, should_overwrite):
    #
    #     """
    #     Add timeseries to the Data table / Update timeseries in the Data table
    #     :param timeseries_id:
    #     :param timeseries: pandas DataFrame, with 'time' as index
    #     and 'value' as the column
    #     :param should_overwrite:
    #     :return:
    #     """
    #
    #     if not isinstance(timeseries, pd.DataFrame):
    #         raise ValueError(
    #                 'The "timeseries" shoud be a pandas Dataframe '
    #                 'containing (time, value) in rows')
    #
    #     session = self.Session()
    #     try:
    #         if should_overwrite:
    #             # update on conflict duplicate key.
    #             for index, row in timeseries.iterrows():
    #                 session.merge(
    #                         Data(id=timeseries_id, time=index.to_pydatetime(),
    #                                 value=float(row['value'])))
    #             session.commit()
    #             return True
    #
    #         else:
    #
    #             # The bulk save feature allows for a lower-latency INSERT/UPDATE
    #             # of rows at the expense of most other unit-of-work features.
    #             # Features such as object management, relationship handling,
    #             # and SQL clause support are silently omitted in favor of raw
    #             # INSERT/UPDATES of records.
    #             # raise IntegrityError on duplicate key.
    #             data_obj_list = []
    #             for index, row in timeseries.iterrows():
    #                 data_obj_list.append(
    #                         Data(id=timeseries_id, time=index.to_pydatetime(),
    #                                 value=float(row['value'])))
    #
    #             session.bulk_save_objects(data_obj_list)
    #             session.commit()
    #             return True
    #     finally:
    #         session.close()


    #
    #     self.meta_struct = {
    #         'station': '',
    #         'variable': '',
    #         'unit': '',
    #         'type': '',
    #         'source': '',
    #         'name': ''
    #     }
    #     self.meta_struct_keys = sorted(self.meta_struct.keys())
    #
    #     self.station_struct = {
    #         'id': '',
    #         'stationId': '',
    #         'name': '',
    #         'latitude': '',
    #         'longitude': '',
    #         'resolution': '',
    #         'description': ''
    #     }
    #     self.station_struct_keys = self.station_struct.keys()
    #
    #     self.source_struct = {
    #         'id': '',
    #         'source': '',
    #         'parameters': ''
    #     }
    #     self.source_struct_keys = self.source_struct.keys()
    #
    # def get_connection(self):
    #     pass
    #
    # def get_meta_struct(self):
    #     """Get the Meta Data Structure of hash value
    #     NOTE: start_date and end_date is not using for hashing
    #     """
    #     return self.meta_struct
    #
    # def get_station_struct(self):
    #     """Get the Station Data Structure
    #     """
    #     return self.station_struct
    #


    # def delete_timeseries(self, event_id):
    #     """Delete given timeseries from the database
    #
    #     :param string event_id: Hex Hash value that need to delete timeseries against
    #
    #     :return int: Affected row count.
    #     """
    #     row_count = 0
    #     connection = self.pool.get()
    #     try:
    #         with connection.cursor() as cursor:
    #             sql = [
    #                 "DELETE FROM `run` WHERE `id`=%s",
    #             ]
    #             '''
    #             "DELETE FROM `data` WHERE `id`=%s"
    #             NOTE: Since `data` table `id` foriegn key contain on `ON DELETE CASCADE`,
    #             when deleting entries on `run` table will automatically delete the records
    #             in `data` table
    #             '''
    #             row_count = cursor.execute(sql[0], event_id)
    #             return row_count
    #     except Exception as ex:
    #         error_message = 'Error in deleting timeseries of for event_id: %s.' % event_id
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
    # def get_event_ids(self, meta_query=None, opts=None):
    #     """Get event ids set according to given meta data
    #
    #     :param dict meta_query: Dict of Meta Query that use to search the hash
    #     event ids. It may contain any of following keys s.t.
    #     {
    #         'station': 'Hanwella', // Or a list ['Hanwella', 'Colombo']
    #         'variable': 'Precipitation',
    #         'unit': 'mm',
    #         'type': 'Forecast',
    #         'source': 'WRF',
    #         'name': 'Daily Forecast',
    #         'start_date': '2017-05-01 00:00:00',
    #         'end_date': '2017-05-03 23:00:00'
    #     }
    #
    #     :param dict opts: Dict of options for searching and handling data s.t.
    #     {
    #         'limit': 100,
    #         'skip': 0
    #     }
    #
    #     :return list: Return list of event objects which matches the given scenario
    #     """
    #     if opts is None:
    #         opts = {}
    #     if meta_query is None:
    #         meta_query = {}
    #     connection = self.pool.get()
    #     try:
    #         if not opts.get('limit'):
    #             opts['limit'] = 100
    #         if not opts.get('skip'):
    #             opts['skip'] = 0
    #
    #         with connection.cursor() as cursor:
    #             out_order = []
    #             sorted_keys = ['id'] + self.meta_struct_keys
    #             for key in sorted_keys:
    #                 out_order.append("`%s` as `%s`" % (key, key))
    #             out_order = ','.join(out_order)
    #
    #             sql = "SELECT %s FROM `run_view` " % out_order
    #             if meta_query:
    #                 sql += "WHERE "
    #                 cnt = 0
    #                 for key in meta_query:
    #                     if cnt:
    #                         sql += "AND "
    #
    #                     # TODO: Need to update start and end date of timeseries
    #                     if key is 'from':
    #                         # sql += "`%s`>=\"%s\" " % ('start_date', meta_query[key])
    #                         sql = sql[:-4]
    #                     elif key is 'to':
    #                         # sql += "`%s`<=\"%s\" " % ('start_date', meta_query[key])
    #                         sql = sql[:-4]
    #                     elif key is 'station' and isinstance(meta_query[key], list):
    #                         sql += "`%s` in (%s) " % (key, ','.join('\"%s\"' % x for x in meta_query[key]))
    #                     else:
    #                         sql += "`%s`=\"%s\" " % (key, meta_query[key])
    #                     cnt += 1
    #
    #             logging.debug('sql (get_event_ids):: %s', sql)
    #             cursor.execute(sql)
    #             events = cursor.fetchmany(opts.get('limit'))
    #             logging.debug('Events (get_event_ids):: %s', events)
    #             response = []
    #             for event in events:
    #                 meta_struct = dict(self.meta_struct)
    #                 for i, value in enumerate(sorted_keys):
    #                     meta_struct[sorted_keys[i]] = event[i]
    #                 response.append(meta_struct)
    #
    #             return response
    #
    #     except Exception as ex:
    #         error_message = 'Error in retrieving event_ids for meta query: %s.' % meta_query
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
    # def retrieve_timeseries(self, meta_query=None, opts=None):
    #     """Get timeseries
    #
    #     :param (dict | list) meta_query: If Meta Query is a Dict, then it'll use to search the hash
    #     event ids. It may contain any of following keys s.t.
    #     {
    #         'station': 'Hanwella',
    #         'variable': 'Precipitation',
    #         'unit': 'mm',
    #         'type': 'Forecast',
    #         'source': 'WRF',
    #         'name': 'Daily Forecast',
    #         'start_date': '2017-05-01 00:00:00',
    #         'end_date': '2017-05-03 23:00:00',
    #     }
    #     If the Meta Query is a List, then those will use to retrieve the timeseries.
    #     List may have following structure s.t.
    #         ['eventId1', 'eventId2', ...] // List of strings
    #     Or
    #         [{id: 'eventId1'}, {id: 'eventId2'}, ...] // List of Objects
    #
    #     :param dict opts: Dict of options for searching and handling data s.t.
    #     {
    #         'limit': 100,
    #         'skip': 0,
    #         'from': '2017-05-01 00:00:00',
    #         'to': '2017-05-06 23:00:00',
    #         'mode': Data.data | Data.processed_data, # Default is `Data.data`
    #     }
    #
    #     :return list: Return list of objects with the timeseries data for given matching events
    #     """
    #     if opts is None:
    #         opts = {}
    #     if meta_query is None:
    #         meta_query = []
    #
    #     data_table = opts.get('mode', Data.data)
    #     if isinstance(data_table, Data):
    #         data_table = data_table.value
    #     else:
    #         raise InvalidDataAdapterError("Provided Data type %s is invalid" % data_table)
    #
    #     connection = self.pool.get()
    #     try:
    #         if not opts.get('limit'):
    #             opts['limit'] = 100
    #         if not opts.get('skip'):
    #             opts['skip'] = 0
    #
    #         if isinstance(meta_query, dict):
    #             event_ids = self.get_event_ids(meta_query)
    #         else:
    #             event_ids = list(meta_query)
    #
    #         logging.debug('event_ids :: %s', event_ids)
    #         response = []
    #         for event in event_ids:
    #             with connection.cursor() as cursor:
    #                 if isinstance(event, dict):
    #                     event_id = event.get('id')
    #                 else:
    #                     event_id = event
    #                     event = {'id': event_id}
    #
    #                 sql = "SELECT `time`,`value` FROM `%s` WHERE `id`=\"%s\" " % (data_table, event_id)
    #
    #                 if opts.get('from'):
    #                     sql += "AND `%s`>=\"%s\" " % ('time', opts['from'])
    #                 if opts.get('to'):
    #                     sql += "AND `%s`<=\"%s\" " % ('time', opts['to'])
    #
    #                 logging.debug('sql (retrieve_timeseries):: %s', sql)
    #                 cursor.execute(sql)
    #                 timeseries = cursor.fetchall()
    #                 event['timeseries'] = [[time, value] for time, value in timeseries]
    #                 response.append(event)
    #
    #             return response
    #     except Exception as ex:
    #         error_message = 'Error in retrieving timeseries for meta query: %s.' % meta_query
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
    # def extract_grouped_time_series(self, event_id, start_date, end_date, group_operation):
    #     """
    #     Extract the grouped timeseries for the given event_id.
    #     :param event_id: timeseries id
    #     :param start_date: start datetime (early datetime) [exclusive]
    #     :param end_date: end datetime (late datetime) [inclusive]
    #     :param group_operation: aggregation time interval and value operation
    #     :return: timerseries, a list of list, [[datetime, value], [datetime, value], ...]
    #     """
    #     # Group operation should be of TimeseriesGroupOperation enum type.
    #     if not isinstance(group_operation, TimeseriesGroupOperation):
    #         raise InvalidDataAdapterError("Provided group_operation: %s is of not valid type" % group_operation)
    #
    #     # Validate start and end dates.
    #     # Should be in the COMMON_DATETIME_FORMAT('%Y-%m-%d %H:%M:%S'). Should be in string format.
    #     if not validate_common_datetime(start_date) or not validate_common_datetime(end_date):
    #         raise InvalidDataAdapterError("Provided start_date: %s or end_date: %s is no in the '%s' format"
    #                                       % (start_date, end_date, COMMON_DATETIME_FORMAT))
    #
    #     # Get the SQL Query.
    #     sql_query = get_query(group_operation, event_id, start_date, end_date)
    #     # Execute the SQL Query.
    #     connection = self.pool.get()
    #     try:
    #         with connection.cursor() as cursor:
    #             cursor.execute(sql_query)
    #             timeseries = cursor.fetchall()
    #             # Prepare the output time series.
    #             # If the retrieved data set is empty return empty list.
    #             return [[time, value] for time, value in timeseries]
    #     except Exception as ex:
    #         error_message = 'Error in retrieving grouped_time_series for meta data: (%s, %s, %s, %s).' \
    #                         % (event_id, start_date, end_date, group_operation)
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
    # def create_station(self, station=None):
    #     """Insert stations into the database
    #
    #      Station ids ranged as below;
    #     - 1 xx xxx - CUrW (stationId: curw_<SOMETHING>)
    #     - 2 xx xxx - Megapolis (stationId: megapolis_<SOMETHING>)
    #     - 3 xx xxx - Government (stationId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
    #     - 4 xx xxx - Public (stationId: pub_<SOMETHING>)
    #     - 8 xx xxx - Satellite (stationId: sat_<SOMETHING>)
    #
    #     Simulation models station ids ranged over 1’000’000 as below;
    #     - 1 1xx xxx - WRF (stationId: [;<prefix>_]wrf_<SOMETHING>)
    #     - 1 2xx xxx - FLO2D (stationId: [;<prefix>_]flo2d_<SOMETHING>)
    #     - 1 3xx xxx - MIKE (stationId: [;<prefix>_]mike_<SOMETHING>)
    #
    #     :param list/tuple   station: Station details in the form of list s.t.
    #     [<Station.CUrW>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>] Or
    #     (<ID>, <STATION_ID>, <NAME>, <LATITUDE>, <LONGITUDE>, <RESOLUTION>, <DESCRIPTION>)
    #     """
    #     if station is None:
    #         station = []
    #     row_count = 0
    #     connection = self.pool.get()
    #     try:
    #         with connection.cursor() as cursor1:
    #             if isinstance(station, tuple) and isinstance(station[0], Station):
    #                 sql = "SELECT max(id) FROM `station` WHERE %s <= id AND id < %s" \
    #                       % (station[0].value, station[0].value+Station.getRange(station[0]))
    #                 logging.debug(sql)
    #                 cursor1.execute(sql)
    #                 last_id = cursor1.fetchone()
    #                 station = list(station)
    #                 if last_id[0] is not None:
    #                     station[0] = last_id[0] + 1
    #                 else:
    #                     station[0] = station[0].value
    #             elif isinstance(station, list) and isinstance(station[0], Station):
    #                 sql = "SELECT max(id) FROM `station` WHERE %s <= id AND id < %s" % (station[0].value, station[0].value+Station.getRange(station[0]))
    #                 logging.debug(sql)
    #                 cursor1.execute(sql)
    #                 last_id = cursor1.fetchone()
    #                 if last_id[0] is not None:
    #                     station[0] = last_id[0] + 1
    #                 else:
    #                     station[0] = station[0].value
    #
    #         with connection.cursor() as cursor2:
    #             sql = "INSERT INTO `station` (`id`, `stationId`, `name`, `latitude`, `longitude`, `resolution`, `description`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    #
    #             logging.debug('Create Station: %s', station)
    #             row_count = cursor2.execute(sql, station)
    #             logging.debug('Created Station # %s', row_count)
    #
    #         return row_count
    #
    #     except Exception as ex:
    #         error_message = 'Error in creating station: %s' % station
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
    # def get_station(self, query={}):
    #     """
    #     Get matching station details for given query.
    #
    #     :param query Dict: Query to find the station. It may contain any of following keys s.t.
    #     {
    #         id: 100001, // Integer
    #         stationId: 'curw_hanwella',
    #         name: 'Hanwella'
    #     }
    #     :return Object: Details of matching station. If not found empty Object will be return.
    #     """
    #     response = None
    #     connection = self.pool.get()
    #     try:
    #         with connection.cursor() as cursor:
    #             out_put_order = []
    #             for key in self.station_struct_keys:
    #                 out_put_order.append("`%s` as `%s`" % (key, key))
    #             out_put_order = ','.join(out_put_order)
    #
    #             sql = "SELECT %s FROM `station` " % out_put_order
    #             if query:
    #                 sql += "WHERE "
    #                 cnt = 0
    #                 for key in query:
    #                     if cnt:
    #                         sql += "AND "
    #
    #                     sql += "`%s`=\"%s\" " % (key, query[key])
    #                     cnt += 1
    #
    #             logging.debug('sql (get_station):: %s', sql)
    #             cursor.execute(sql)
    #             station = cursor.fetchone()
    #             if station is not None:
    #                 response = {}
    #                 for i, value in enumerate(self.station_struct_keys):
    #                     response[value] = station[i]
    #                 logging.debug('station:: %s', response)
    #         return response
    #
    #     except Exception as ex:
    #         error_message = 'Error in retrieving station details for query: %s' % query
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
    # def delete_station(self, id=0, station_id=''):
    #     """Delete given station from the database
    #
    #     :param integer id: Station Id
    #     :param string station_id: Station Id
    #
    #     :return int: Affected row count.
    #     """
    #     row_count = 0
    #     connection = self.pool.get()
    #     try:
    #         with connection.cursor() as cursor:
    #             if id > 0:
    #                 sql = "DELETE FROM `station` WHERE `id`=%s"
    #                 row_count = cursor.execute(sql, (id))
    #             elif station_id:
    #                 sql = "DELETE FROM `station` WHERE `stationId`=%s"
    #
    #                 row_count = cursor.execute(sql, (station_id))
    #             else:
    #                 logging.warning('Unable to find station')
    #             return row_count
    #     except Exception as ex:
    #         error_message = 'Error in deleting station: id = %s, stationId = %s' % (id, station_id)
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
    # def get_stations(self, query={}):
    #     """
    #     Get matching stations details for given query.
    #
    #     :param query:
    #     :return:
    #     """
    #     return []
    #
    # def get_stations_in_area(self, query={}):
    #     """Get stations
    #
    #     :param dict query: Query for retrieve stations. It may contain any of following keys s.t.
    #     {
    #         latitude_lower: '',
    #         longitude_lower: '',
    #         latitude_upper: '',
    #         longitude_upper: '',
    #     }
    #     If the latitude and longitude of a particular area is provided,
    #     it'll look into all the stations which reside inside that area.
    #
    #     :return list: Return list of objects with the stations data which resign in given area.
    #     """
    #     connection = self.pool.get()
    #     try:
    #         with connection.cursor() as cursor:
    #             out_order = []
    #             sorted_keys = sorted(self.station_struct.keys())
    #             for key in sorted_keys:
    #                 out_order.append("`%s` as `%s`" % (key, key))
    #             out_order = ','.join(out_order)
    #
    #             sql = "SELECT %s FROM `station` " % (out_order)
    #             if query:
    #                 sql += "WHERE "
    #                 cnt = 0
    #                 for key in query:
    #                     if cnt:
    #                         sql += "AND "
    #
    #                     if key is 'latitude_lower':
    #                         sql += "`%s`>=\"%s\" " % ('latitude', query[key])
    #                     elif key is 'longitude_lower':
    #                         sql += "`%s`>=\"%s\" " % ('longitude', query[key])
    #                     elif key is 'latitude_upper':
    #                         sql += "`%s`<=\"%s\" " % ('latitude', query[key])
    #                     elif key is 'longitude_upper':
    #                         sql += "`%s`<=\"%s\" " % ('longitude', query[key])
    #                     cnt += 1
    #
    #             logging.debug('sql (get_stations):: %s', sql)
    #             cursor.execute(sql)
    #             stations = cursor.fetchall()
    #             response = []
    #             for station in stations:
    #                 station_struct = dict(self.station_struct)
    #                 for i, value in enumerate(sorted_keys):
    #                     station_struct[sorted_keys[i]] = station[i]
    #                 response.append(station_struct)
    #
    #             return response
    #     except Exception as ex:
    #         error_message = 'Error in getting station in area of: %s' % query
    #         # TODO logging and raising is considered as a cliche' and bad practice.
    #         logging.error(error_message)
    #         raise DatabaseAdapterError(error_message, ex)
    #     finally:
    #         if connection is not None:
    #             self.pool.put(connection)
    #
