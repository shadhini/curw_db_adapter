import traceback
import csv
import pkg_resources
import json, collections
from datetime import datetime

from db_adapter.curw_obs.station.station_enum import StationEnum
from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError
from db_adapter.constants import COMMON_DATE_TIME_FORMAT

"""
Station JSON Object would looks like this 
e.g.:
    {
        'station_type': 'CUrW_WeatherStation',
        'name'        : 'IBATTARA2',
        'latitude'    : '6.535172',
        'longitude'   : '79.875435',
        'description' : '',
        'station_type': StationEnum.WRF
    }
"""


def get_station_by_id(pool, id_):
    """
    Retrieve station by id
    :param pool: database connection pool
    :param id_: station id
    :return: Station if the stations exists in the database, else None
    """

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `station` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            if row_count > 0:
                return cursor.fetchone()
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving station with station_id {} failed".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_station_id(pool, latitude, longitude, station_type) -> str:
    """
    Retrieve station id
    :param pool: database connection pool
    :param latitude:
    :param longitude:
    :param station_type: StationEnum: which defines the station type
    such as 'CUrW', 'WRF'
    :return: str: station id, if station exists in the db, else None
    """

    connection = pool.connection()
    try:

        initial_value = str(station_type.value)

        if len(initial_value)==6:
            pattern = "{}_____".format(initial_value[0])
        elif len(initial_value)==7:
            pattern = "{}{}_____".format(initial_value[0], initial_value[1])

        with connection.cursor() as cursor:
            sql_statement = "SELECT `id` FROM `station` WHERE `id` like %s and `latitude`=%s and `longitude`=%s and `station_type`=%s;"
            row_count = cursor.execute(sql_statement, (pattern, latitude, longitude, StationEnum.getTypeString(station_type)))
            if row_count > 0:
                return cursor.fetchone()['id']
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving station id: latitude={}, longitude={}, and station_type{} failed."\
            .format(latitude, longitude, station_type)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_station(pool, name, latitude, longitude, station_type, description=None):
    """
    Insert sources into the database

    StationEnum ids ranged as below;

        - 1 xx xxx - CUrW_WeatherStation (station_id: curw_<SOMETHING>)
        - 1 xx xxx - CUrW_WaterLevelGauge (station_id: curw_wl_<SOMETHING>)
        - 3 xx xxx - Megapolis (station_id: megapolis_<SOMETHING>)
        - 4 xx xxx - Government (station_id: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
        - 5 xx xxx - Satellite (station_id: sat_<SOMETHING>)

        - 2 xxx xxx - Public (station_id: pub_<SOMETHING>)

        Simulation models StationEnum ids ranged over 1’000’000 as below;
        - 1 1xx xxx - WRF (name: wrf_<SOMETHING>)
        - 1 2xx xxx - FLO2D 250(name: flo2d_250_<SOMETHING>)
        - 1 3xx xxx - FLO2D 150(name: flo2d_150_<SOMETHING>)
        - 1 4xx xxx - FLO2D 30(name: flo2d_30_<SOMETHING>)
        - 1 8xx xxx - MIKE11(name: mike_<SOMETHING>)

        Other;
        - 3 xxx xxx - Other (name/station_id: other_<SOMETHING>)

    :param pool: database connection pool
    :param name: string
    :param latitude: double
    :param longitude: double
    :param description: string
    :param station_type: StationEnum: which defines the station type
    such as 'CUrW'
    :return: True if the station is added into the 'Station' table, else False
    """

    initial_value = station_type.value
    range_ = StationEnum.getRange(station_type)

    connection = pool.connection()
    try:
        if get_station_id(pool=pool, latitude=latitude, longitude=longitude, station_type=station_type) is None:

            with connection.cursor() as cursor1:
                sql_statement = "SELECT `id` FROM `station` WHERE `id` BETWEEN %s and %s ORDER BY `id` DESC"
                row_count = cursor1.execute(sql_statement, (initial_value, initial_value + range_ - 1))
                if row_count > 0:
                    station_id = cursor1.fetchone()['id'] + 1
                else:
                    station_id = initial_value

            with connection.cursor() as cursor2:
                sql_statement = "INSERT INTO `station` (`id`, `station_type`, `name`, `latitude`, `longitude`, `description`) " \
                                "VALUES ( %s, %s, %s, %s, %s, %s)"

                row_count = cursor2.execute(sql_statement,
                        (station_id, StationEnum.getTypeString(station_type), name, latitude, longitude, description))
                connection.commit()
                return True if row_count > 0 else False
        else:
            logger.info("Station with latitude={} longitude={} and station_type={} already exists in the database"
                .format(latitude, longitude, station_type))
            return False
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of station: name={}, latitude={}, longitude={}, description={}, " \
                        "and station_type={} failed.".format(name, latitude, longitude, description, station_type)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def add_stations(stations, pool):
    """
    Add stations into Station table
    :param stations: list of json objects that define station attributes
    e.g.:
    {
        'name'        : 'wrf0_79.875435_6.535172',
        'latitude'    : '6.535172',
        'longitude'   : '79.875435',
        'description' : '',
        'station_type': StationEnum.WRF
    }
    :return:
    """

    for station in stations:

        print(add_station(pool=pool, name=station.get('name'), latitude=station.get('latitude'),
                longitude=station.get('longitude'), station_type=station.get('station_type'),
                description=station.get('description')))
        print(station.get('name'))


def delete_station(pool, latitude, longitude, station_type):
    """
    Delete station from Station table
    :param pool: database connection pool
    :param latitude:
    :param longitude:
    :param station_type: StationEnum: which defines the station type
    such as 'CUrW'
    :return: True if the deletion was successful, else False
    """

    connection = pool.connection()
    try:
        initial_value = str(station_type.value)

        if len(initial_value)==6:
            pattern = "{}_____".format(initial_value[0])
        elif len(initial_value)==7:
            pattern = "{}{}_____".format(initial_value[0], initial_value[1])

        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `station` WHERE `id` like %s and `latitude`=%s and `longitude`=%s and `station_type`=%s;"
            row_count = cursor.execute(sql_statement, (pattern, latitude, longitude, StationEnum.getTypeString(station_type)))
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of station in the database with latitude={}, "
                            "longitude={}, and station_type{}".format(latitude, longitude, station_type))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting station with latitude={}, longitude={}, and station_type{} failed."\
            .format(latitude, longitude, station_type)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def delete_station_by_id(pool, id_):
    """
    Delete station from Station table by id
    :param pool: database connection pool
    :param id_:
    :return: True if the deletion was successful, else False
    """

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "DELETE FROM `station` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            connection.commit()
            if row_count > 0:
                return True
            else:
                logger.info("There's no record of station in the database with the station id {}".format(id_))
                return False
    except Exception as exception:
        connection.rollback()
        error_message = "Deleting station with id {} failed.".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_description(pool, id_):
    """
    Retrieve station description for a given station id
    :param pool:
    :param id_: station id
    :return:
    """

    description = {}

    connection = pool.connection()
    try:

        with connection.cursor() as cursor:
            sql_statement = "SELECT `description` FROM `station` WHERE `id`=%s"
            row_count = cursor.execute(sql_statement, id_)
            if row_count > 0:
                result = cursor.fetchone()['description']
                if result is not None:
                    description = json.loads(result, object_pairs_hook=collections.OrderedDict)

        return description
    except Exception as exception:
        error_message = "Retrieving station description for id={} failed.".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def update_description(pool, id_, description, append=True):
    """
    Update description in case weather station iot device is changed
    "description" is a JSON object with timestamp as the key
    :param pool:
    :param id_: station id
    :param description: JSON: new station description
    :param append: If True, new description would be appended to the already existing description,
    otherwise description would be reset to the new  description
    :return: True if successful
    """
    existing_description = get_description(pool=pool, id_=id_)

    new_description = {}

    timestamp = (datetime.now()).strftime(COMMON_DATE_TIME_FORMAT)

    if append:
        new_description = existing_description

    new_description[timestamp] = description

    ordered_description = json.dumps(collections.OrderedDict(sorted(new_description.items())))

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "UPDATE `station` SET `description`=%s WHERE `id`=%s"
            cursor.execute(sql_statement, (ordered_description, id_))
        connection.commit()
        return True
    except Exception as exception:
        connection.rollback()
        error_message = "Updating station description for id={} failed.".format(id_)
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()
