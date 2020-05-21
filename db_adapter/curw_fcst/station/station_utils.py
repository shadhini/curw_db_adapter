import traceback
import csv
import pkg_resources

from db_adapter.curw_fcst.station.station_enum import StationEnum
from db_adapter.logger import logger
from db_adapter.exceptions import DatabaseAdapterError

"""
Station JSON Object would looks like this 
e.g.:
    {
        'name'        : '79.875435_6.535172',
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
            sql_statement = "SELECT `id` FROM `station` WHERE `id` like %s and `latitude`=%s and `longitude`=%s"
            row_count = cursor.execute(sql_statement, (pattern, latitude, longitude))
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


def add_station(pool, name, latitude, longitude, description, station_type):
    """
    Insert sources into the database

    Station ids ranged as below;
    - 1 xx xxx - CUrW (stationId: curw_<SOMETHING>)
    - 2 xx xxx - Megapolis (stationId: megapolis_<SOMETHING>)
    - 3 xx xxx - Government (stationId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
    - 4 xx xxx - Public (stationId: pub_<SOMETHING>)
    - 8 xx xxx - Satellite (stationId: sat_<SOMETHING>)

    Simulation models station ids ranged over 1’000’000 as below;
    - 1 1xx xxx - WRF (stationId: [;<prefix>_]wrf_<SOMETHING>)
    - 1 2xx xxx - FLO2D (stationId: [;<prefix>_]flo2d_<SOMETHING>)model
    - 1 3xx xxx - MIKE (stationId: [;<prefix>_]mike_<SOMETHING>)

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
                sql_statement = "INSERT INTO `station` (`id`, `name`, `latitude`, `longitude`, `description`) " \
                                "VALUES ( %s, %s, %s, %s, %s)"
                row_count = cursor2.execute(sql_statement, (station_id, name, latitude, longitude, description))
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
            sql_statement = "DELETE FROM `station` WHERE `id` like %s and `latitude`=%s and `longitude`=%s"
            row_count = cursor.execute(sql_statement, (pattern, latitude, longitude))
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


def add_wrf_stations(pool):

    """
    Add wrfv3 stations to the database
    :param pool:  database connection pool
    :param wrf_stations_list: tuple that contains values for (id, name, latitude, longitude, description)
    :return: True if the insertion is successful, else False
    """

    # resource_path = pkg_resources.resource_string(__name__, "wrfv3_stations.csv")
    with open('wrf_stations.csv', 'r') as f:
        data=[tuple(line) for line in csv.reader(f)][1:]

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "INSERT INTO `station` (`id`, `name`, `latitude`, `longitude`, `description`) " \
                                "VALUES ( %s, %s, %s, %s, %s)"
            row_count = cursor.executemany(sql_statement, data)
        connection.commit()
        return row_count
    except Exception as exception:
        connection.rollback()
        error_message = "Insertion of wrf stations failed."
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_wrf_stations(pool):

    """
    Retrieve ids of wrf_v3 stations, for each station name
    :param pool: database connection pool
    :return: dictionary with keys of type "<latitude>_<longitude>" and corresponding id as the value
    """

    wrfv3_stations = {}

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT `id`, `name` FROM `station` WHERE `id` like %s"
            row_count = cursor.execute(sql_statement, "11_____")
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    wrfv3_stations[dict.get("name")] = dict.get("id")
                return wrfv3_stations
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving wrf stations failed"
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_flo2d_output_stations(pool, flo2d_model):

    """
    Retrieve ids of wrf_v3 stations, for each station name
    :param flo2d_model: StationEnum describing the flo2d model
    :param pool: database connection pool
    :return: dictionary with keys of type "<cell_id>" and [list of station id, latitude, longitude and station name] as the value
    """

    flo2d_output_stations = {}

    id_pattern = '{}_____'.format((str(flo2d_model.value)).split('0')[0])

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `station` WHERE `id` like %s"
            row_count = cursor.execute(sql_statement, id_pattern)
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    station_name = dict.get("name")
                    flo2d_output_stations[station_name.split("_")[0]] = [dict.get("id"), dict.get("latitude"),
                                                                             dict.get("longitude"),
                                                                             '_'.join(station_name.split('_')[1:])]
                return flo2d_output_stations
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving flo2d output stations failed"
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_hechms_stations(pool):

    """
    Retrieve ids of hechms stations, for each station name
    :param pool: database connection pool
    :return: dictionary with station names as keys and list of station [name, latitude and longitude] as the value
    """

    hechms_stations = {}

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `station` WHERE `id` like %s"
            row_count = cursor.execute(sql_statement, "10_____")
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    hechms_stations[dict.get("name")] = [dict.get("id"), dict.get("latitude"), dict.get("longitude")]
                return hechms_stations
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving hechms stations failed"
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()


def get_mike_stations(pool):

    """
    Retrieve ids of mike11 stations, for each station name
    :param pool: database connection pool
    :return: dictionary with station names as keys and list of station [name, latitude and longitude] as the value
    """

    mike_stations = {}

    connection = pool.connection()
    try:
        with connection.cursor() as cursor:
            sql_statement = "SELECT * FROM `station` WHERE `id` like %s"
            row_count = cursor.execute(sql_statement, "18_____")
            if row_count > 0:
                results = cursor.fetchall()
                for dict in results:
                    mike_stations[dict.get("name")] = [dict.get("id"), dict.get("latitude"), dict.get("longitude")]
                return mike_stations
            else:
                return None
    except Exception as exception:
        error_message = "Retrieving mike stations failed"
        logger.error(error_message)
        traceback.print_exc()
        raise exception
    finally:
        if connection is not None:
            connection.close()