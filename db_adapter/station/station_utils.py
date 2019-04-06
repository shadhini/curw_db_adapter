from db_adapter.models import Station
from db_adapter.station.station_enum import StationEnum

"""
Station JSON Object would looks like this 
e.g.:
    {
        'name'        : 'wrf0_79.875435_6.535172',
        'latitude'    : '6.535172',
        'longitude'   : '79.875435',
        'description' : '',
        'station_type': StationEnum.WRF
    }
"""


def get_station_by_id(session, id_):
    """
    Retrieve station by id
    :param session: session made by sessionmaker for the database engine
    :param id_: station id
    :return: Station
    """
    try:
        station_row = session.query(Station).get(id_)
        return None if station_row is None else station_row
    finally:
        session.close()


def get_station_id(session, latitude, longitude, station_type) -> str:
    """
    Retrieve station id
    :param session: session made by sessionmaker for the database engine
    :param latitude:
    :param longitude:
    :param station_type: StationEnum: which defines the station type
    such as 'CUrW', 'WRF'
    :return: str: station id
    """

    initial_value = str(station_type.value)

    try:
        if len(initial_value)==6:
            pattern = "{}_____".format(initial_value[0])
        elif len(initial_value)==7:
            pattern = "{}{}_____".format(initial_value[0], initial_value[1])
        station_row = session.query(Station) \
            .filter(Station.id.like(pattern)) \
            .filter_by(latitude=latitude) \
            .filter_by(longitude=longitude) \
            .first()
        return None if station_row is None else station_row.id
    finally:
        session.close()


def add_station(session, name, latitude, longitude, description, station_type):
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

    :param session: session made by sessionmaker for the database engine
    :param name: string
    :param latitude: double
    :param longitude: double
    :param description: string
    :param station_type: StationEnum: which defines the station type
    such as 'CUrW'
    :return: True if the station is added into the 'Station' table
    """
    initial_value = station_type.value
    range_ = StationEnum.getRange(station_type)

    station = session.query(Station) \
        .filter(Station.id >= initial_value, Station.id <= initial_value + range_) \
        .order_by(Station.id.desc()) \
        .first()

    if station is not None:
        station_id = station.id + 1
    else:
        station_id = initial_value

    try:
        station = Station(
                id=station_id,
                name=name,
                latitude=latitude,
                longitude=longitude,
                description=description
                )

        session.add(station)
        session.commit()
    finally:
        session.close()


def add_stations(stations, session):
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

        print(add_station(session=session, name=station.get('name'), latitude=station.get('latitude'),
                longitude=station.get('longitude'), station_type=station.get('station_type'),
                description=station.get('description')))
        print(station.get('name'))


def delete_station(session, latitude, longitude, station_type):
    """
    Delete station from Station table
    :param session: session made by sessionmaker for the database engine
    :param latitude:
    :param longitude:
    :param station_type: StationEnum: which defines the station type
    such as 'CUrW'
    :return: True if the deletion was successful
    """

    id_ = get_station_id(session, latitude=latitude, longitude=longitude, station_type=station_type)

    try:
        if id_ is not None:
            delete_station_by_id(session, id_)
            session.commit()
            return True
        else:
            print("There's no record in the database with the station id ", id_)
            return False
    finally:
        session.close()


def delete_station_by_id(session, id_):
    """
    Delete station from Station table by id
    :param session: session made by sessionmaker for the database engine
    :param id_:
    :return: True if the deletion was successful
    """

    try:
        station = session.query(Station).get(id_)
        if station is not None:
            session.delete(station)
            session.commit()
            status = session.query(Station).filter_by(id=id_).count()
            return True if status==0 else False
        else:
            print("There's no record in the database with the station id ", id_)
            return False
    finally:
        session.close()
