import pandas as pd
import hashlib
import json
import traceback

from sqlalchemy.sql import select
from sqlalchemy import bindparam

# from sqlalchemy.ext.compiler import compiles
# from sqlalchemy.sql.expression import Insert

from datetime import datetime

from db_adapter.curw_fcst.models import Data, Run
from db_adapter.curw_fcst.station import get_station_id
from db_adapter.curw_fcst.source import get_source_id
from db_adapter.curw_fcst.variable import get_variable_id
from db_adapter.curw_fcst.unit import get_unit_id
from db_adapter.logger import logger

#
# @compiles(Insert)
# def append_string(insert, compiler, **kw):
#     s = compiler.visit_insert(insert, **kw)
#     if 'mysql_append_string' in insert.kwargs:
#         return s + " " + insert.kwargs['mysql_append_string']
#     return s


class Timeseries:
    def __init__(self, engine, session):
        self.engine = engine
        self.session = session

    @staticmethod
    def generate_timeseries_id(meta_data: object) -> object:

        """
        Generate the event id for given metadata
        Only 'sim_tag', 'scheduled_date', 'latitude', 'longitude', 'model',
        'version', 'variable', 'unit', 'unit_type'
        are used to generate the id (i.e. hash value)

        :param meta_data: Dict with 'sim_tag', 'scheduled_date', 'latitude',
        'longitude', 'model', 'version', 'variable', 'unit', 'unit_type' keys
        :return: str: sha256 hash value in hex format (length of 64 characters)
        """

        sha256 = hashlib.sha256()
        hash_data = {
                'sim_tag'       : '',
                'scheduled_date': '',
                'latitude'      : '',
                'longitude'     : '',
                'model'         : '',
                'version'       : '',
                'variable'      : '',
                'unit'          : '',
                'unit_type'     : ''
                }

        for key in hash_data.keys():
            hash_data[key] = meta_data[key]

        sha256.update(json.dumps(hash_data, sort_keys=True).encode("ascii"))
        event_id = sha256.hexdigest()
        return event_id

    def get_timeseries_id_if_exists(self, meta_data):

        """
        Check whether a timeseries id exists in the database for a given set of meta data
        :param meta_data: Dict with 'sim_tag', 'scheduled_date', 'latitude',
        'longitude', 'model', 'version', 'variable', 'unit', 'unit_type' keys
        :return: timeseries id if exist else None
        """

        conn = self.engine.connect()

        event_id = self.generate_timeseries_id(meta_data)
        try:
            run_row = conn.execute(select([Run.id]).where(Run.id==event_id)).fetchone()
            return None if run_row is None else run_row[Run.__table__.c.id]
        except Exception as e:
            logger.error("Exception occurred while retrieving timeseries id for metadata={}".format(meta_data))
            traceback.print_exc()
            return False
        finally:
            conn.close()

    def is_id_exists(self, id_):
        """
        Check whether a given timeseries id exists in the database
        :param id_:
        :return: True, if id is in the database, False otherwise
        """
        conn = self.engine.connect()

        try:
            tms_entry = conn.execute(select([Run.id]).where(Run.id == id_)).fetchone()
            return False if tms_entry is None else tms_entry[Run.__table__.c.id]
        except Exception as e:
            logger.error(
                    "Exception occurred while checking whether timeseries id {} exists in the run table".format(id_))
            traceback.print_exc()
            return False
        finally:
            conn.close()

    def insert_data(self, timeseries):
        """
        Insert timeseries to Data table in the database
        :param timeseries: list of python dictionaries with 'tms_id', 'time', 'value' keys
        e.g.:
        [
            {
                "tms_id": "00000ee01a1e41780098f5dc9564bde365c8e6688ebf443ee5c1a75ef56c3b06",
                "time": "2019-03-25 23:30:00",
                "value": 0.000
            },
            {
                "tms_id": "00000ee01a1e41780098f5dc9564bde365c8e6688ebf443ee5c1a75ef56c3b06",
                "time": "2019-03-25 23:45:00",
                "value": 0.020
            }
        ]

        :return: True if insertion is successful, else False
        """

        engine = self.engine

        connection = engine.connect()

        try:
            # engine.execute(Data.__table__.insert(mysql_append_string='ON DUPLICATE KEY UPDATE id=id'), timeseries)
            # connection.execute(Data.__table__.insert(), timeseries[0])
            connection.execute(Data.__table__.update().values(id=bindparam('id'), time=bindparam('time'), value=bindparam('value')), timeseries)
            return True
        except Exception as e:
            logger.error(
                    "Exception occurred while inserting data to data table for tms id {}".format(timeseries[0]['id']))
            traceback.print_exc()
            raise Exception("Incomplete Timeseries Insertion : tms_id{}".format(timeseries[0]['id']))
        finally:
            connection.close()
            return

    def insert_timeseries(self, timeseries, sim_tag, scheduled_date, latitude, longitude,
                          model, version, variable, unit, unit_type, fgt, start_date, end_date):
        """
        Insert new timeseries into the Run table and Data table, this will generate the tieseries id from the given meta data
        :param timeseries: list of [id, time, value] dictionaries
        :param sim_tag:
        :param scheduled_date:
        :param latitude:
        :param longitude:
        :param model:
        :param version:
        :param variable:
        :param unit:
        :param unit_type: str value
        :param fgt:
        :return: str: True if insertion was successful, else raise Exception
        """
        tms_meta = {
                'sim_tag'       : sim_tag,
                'scheduled_date': scheduled_date,
                'latitude'      : latitude,
                'longitude'     : longitude,
                'model'         : model,
                'version'       : version,
                'variable'      : variable,
                'unit'          : unit,
                'unit_type'     : unit_type
                }

        tms_id = Timeseries.generate_timeseries_id(tms_meta)

        station_id = get_station_id(self, latitude, longitude)
        source_id = get_source_id(self, model, version)
        variable_id = get_variable_id(self, variable)
        unit_id = get_unit_id(self, unit, unit_type)

        run = {
                'id'            : tms_id,
                'sim_tag'       : sim_tag,
                'start_date'    : start_date,
                'end_date'      : end_date,
                'station'       : station_id,
                'source'        : source_id,
                'variable'      : variable_id,
                'unit'          : unit_id,
                'fgt'           : None,
                'scheduled_date': scheduled_date
                }

        engine = self.engine

        connection = engine.connect()
        trans = connection.begin()
        try:
            connection.execute(Run.insert(), run)
            self.insert_data(timeseries)
            trans.commit()
            return True
        except:
            trans.rollback()
            logger.error(
                    "Exception occurred while inserting timeseries for sim_tag={}, scheduled_date={}, latitude={}, "
                    "longitude={}, model={}, version={}, variable={}, unit={}, unit_type={}, fgt={}"
                        .format(sim_tag, scheduled_date, latitude, longitude, model, version, variable, unit, unit_type,
                            fgt))
            traceback.print_exc()
            raise Exception("Incomplete Timeseries Insertion : tms_id{}".format(tms_id))
        finally:
            connection.close()
            return

    def insert_timeseries(self, timeseries, run):

        """
        Insert new timeseries into the Run table and Data table, for given timeseries id
        :param run: dictionary that contains each row's column in run table
        :param timeseries: list of dictionaries that contain each row's columns in data table
        :return: True if insertion was successful, else raise Exception
        """

        engine = self.engine

        connection = engine.connect()
        trans = connection.begin()
        try:
            connection.execute(Run.__table__.insert(), run)
            self.insert_data(timeseries)
            trans.commit()
            return True
        except:
            trans.rollback()
            logger.error("Exception occurred while inserting timeseries for tms_id={}, sim_tag={}, scheduled_date={}, "
                         "station_id={}, source_id={}, variable_id={}, unit_id={}"
                .format(run['id'], run['sim_tag'], run['scheduled_date'], run['station'], run['source'], ['variable'],
                    run['unit']))
            traceback.print_exc()
            raise Exception("Incomplete Timeseries Insertion : tms_id {}".format(run['id']))
        finally:
            connection.close()
            return

    def update_fgt(self, scheduled_date, fgt):
        """
        Update fgt for inserted timeseries
        :param scheduled_date:
        :return:
        """
        session = self.session

        try:
            session.query(Run) \
                .filter_by(scheduled_date=scheduled_date) \
                .update({ Run.fgt: fgt }, synchronize_session=False)
            session.commit()
            return True
        except Exception as e:
            logger.error("Exception occurred while updating fgt for scheduled_date={}"
                .format(scheduled_date))
            traceback.print_exc()
            return False
        finally:
            session.close()

    def get_timeseries(self, timeseries_id, start_date, end_date):
        """
        Retrieves the timeseries corresponding to given id s.t.
        time is in between given start_date (inclusive) and end_date (exclusive).

        :param timeseries_id: string timeseries id
        :param start_date: datetime object
        :param end_date: datetime object
        :return: array of [id, time, value]: pandas DataFrame
        """

        if not isinstance(start_date, datetime) or \
                not isinstance(end_date, datetime):
            raise ValueError(
                    'start_date and/or end_date are not of datetime type.',
                    start_date, end_date)

        session = self.Session()
        try:
            result = session.query(Data).filter(
                    Data.id==timeseries_id,
                    Data.time >= start_date, Data.time < end_date
                    ).all()
            timeseries = [[data_obj.time, data_obj.value] for data_obj in result]
            return pd.DataFrame(
                    data=timeseries,
                    columns=['time', 'value']) \
                .set_index(keys='time')
        finally:
            session.close()

    def update_timeseries(self, timeseries_id, timeseries, should_overwrite):

        """
        Add timeseries to the Data table / Update timeseries in the Data table
        :param timeseries_id:
        :param timeseries: pandas DataFrame, with 'time' as index
        and 'value' as the column
        :param should_overwrite:
        :return:
        """

        if not isinstance(timeseries, pd.DataFrame):
            raise ValueError(
                    'The "timeseries" shoud be a pandas Dataframe '
                    'containing (time, value) in rows')

        session = self.Session()
        try:
            if should_overwrite:
                # update on conflict duplicate key.
                for index, row in timeseries.iterrows():
                    session.merge(
                            Data(id=timeseries_id, time=index.to_pydatetime(),
                                    value=float(row['value'])))
                session.commit()
                return True

            else:

                # The bulk save feature allows for a lower-latency INSERT/UPDATE
                # of rows at the expense of most other unit-of-work features.
                # Features such as object management, relationship handling,
                # and SQL clause support are silently omitted in favor of raw
                # INSERT/UPDATES of records.
                # raise IntegrityError on duplicate key.
                data_obj_list = []
                for index, row in timeseries.iterrows():
                    data_obj_list.append(
                            Data(id=timeseries_id, time=index.to_pydatetime(),
                                    value=float(row['value'])))

                session.bulk_save_objects(data_obj_list)
                session.commit()
                return True
        finally:
            session.close()
