from db_adapter.base import get_engine, get_sessionmaker
from db_adapter.constants import (
    CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_HOST, CURW_FCST_PORT,
    CURW_FCST_DATABASE,
    )
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL

from db_adapter.station import add_station


def add_stations(stations):

    engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_FCST_HOST, CURW_FCST_PORT, CURW_FCST_DATABASE,
            CURW_FCST_USERNAME, CURW_FCST_PASSWORD)

    Session = get_sessionmaker(engine=engine)  # Session is a class
    session = Session()

    for station in stations:

        print(add_station(session=session, name=station.get('name'), latitude=station.get('latitude'),
                longitude=station.get('longitude'), station_type=station.get('station_type'), description=station.get('description')))
        print(station.get('name'))

