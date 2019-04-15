import pandas as pd
import hashlib
import json
import traceback

from datetime import datetime

from db_adapter.models import Data, Run
from db_adapter.station import get_station_id
from db_adapter.source import get_source_id
from db_adapter.variable import get_variable_id
from db_adapter.unit import get_unit_id


# to do: avoid hash collisions


class Timeseries:
    def __init__(self, session_creator):
        self.Session = session_creator

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

    def get_timeseries_id(self, meta_data):

        """
        :param meta_data: Dict with 'sim_tag', 'scheduled_date', 'latitude',
        'longitude', 'model', 'version', 'variable', 'unit', 'unit_type' keys
        :return: timeseries id if exist else None
        """

        session = self.Session()

        event_id = self.generate_timeseries_id(meta_data)
        try:
            run_row = session.query(Run).filter_by(id=event_id).first()
            return None if run_row is None else run_row.id
        except Exception as e:
            traceback.print_exc()
            return False
        finally:
            session.close()

    def insert_data(self, tms_id, timeseries):
        """
        Insert timeseries to Data table in the database
        :param tms_id: hash value
        :param timeseries: list of [time, value] lists
        :return: timeseries id if insertion was successful, else False
        """

        data_objects = []

        for item in range(len(timeseries)):
            data_objects.append(Data(id=tms_id, time=item[0], value=item[1]))

        session = self.Session()

        try:
            session.add_all(data_objects)
            session.commit()
            return tms_id
        except Exception as e:
            traceback.print_exc()
            return False
        finally:
            session.close()

    def insert_timeseries(self, timeseries, sim_tag, scheduled_date, latitude, longitude,
                          model, version, variable, unit, unit_type, fgt):
        """
        Insert new timeseries into the Run table and Data table, this will generate the tieseries id from the given data
        :param timeseries: list of [time, value] lists
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
        :return: str: timeseries id if insertion was successful, else False
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
        source_id = get_source_id(self,model, version)
        variable_id = get_variable_id(self, variable)
        unit_id = get_unit_id(self, unit, unit_type)

        run = Run(
                id=tms_id,
                sim_tag=sim_tag,
                start_date=timeseries[0][0],
                end_date=timeseries[-1][0],
                station=station_id,
                source=source_id,
                variable=variable_id,
                unit=unit_id,
                fgt=fgt,
                scheduled_date=scheduled_date
                )

        session = self.Session()

        try:
            session.add(run)
            session.commit()
            return self.insert_data(tms_id, timeseries)
        except Exception as e:
            traceback.print_exc()
            return False
        finally:
            session.close()

    def insert_timeseries_by_ids(self, tms_id, timeseries, sim_tag, scheduled_date, station_id, source_id, variable_id,
                          unit_id, fgt):

        """
        Insert new timeseries into the Run table and Data table, for given timeseries id
        :param tms_id:
        :param timeseries: list of [time, value] lists
        :param sim_tag:
        :param scheduled_date:
        :param station_id:
        :param source_id:
        :param variable_id:
        :param unit_id: str value
        :param fgt:
        :return: timeseries id if insertion was successful, else False
        """
        run = Run(
                id=tms_id,
                sim_tag=sim_tag,
                start_date=timeseries[0][0],
                end_date=timeseries[-1][0],
                station=station_id,
                source=source_id,
                variable=variable_id,
                unit=unit_id,
                fgt=fgt,
                scheduled_date=scheduled_date
                )

        session = self.Session()

        try:
            session.add(run)
            session.commit()
            return self.insert_data(tms_id, timeseries)
        except Exception as e:
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
