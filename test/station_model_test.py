from db_adapter.base import get_engine, get_sessionmaker
from db_adapter.constants import (
    CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_HOST, CURW_FCST_PORT,
    CURW_FCST_DATABASE,
    )
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL

from db_adapter.curw_fcst.station import StationEnum, get_station_by_id, get_station_id, delete_station, add_stations

stations = [
        {
                'name'        : 'Attanagalla',
                'latitude'    : '7.111666667',
                'longitude'   : '80.14983333',
                'description' : '',
                'station_type': StationEnum.CUrW
                },
        {
                'name'        : 'Colombo',
                'latitude'    : '6.898158',
                'longitude'   : '79.8653',
                'description' : '',
                'station_type': StationEnum.CUrW
                },
        {
                'name'        : 'Canyon',
                'latitude'    : '6.871389',
                'longitude'   : '80.524444',
                'description' : '',
                'station_type': StationEnum.Government
                },
        {
                'name'        : 'Irrigation KUB mean',
                'latitude'    : '6.872778',
                'longitude'   : '80.564444',
                'description' : '',
                'station_type': StationEnum.Government
                },
        {
                'name'        : 'wrf0_79.848206_6.535172',
                'latitude'    : '6.872778',
                'longitude'   : '79.848206',
                'description' : '',
                'station_type': StationEnum.WRF
                },
        {
                'name'        : 'wrf0_79.875435_6.535172',
                'latitude'    : '6.535172',
                'longitude'   : '79.875435',
                'description' : '',
                'station_type': StationEnum.WRF
                }
        ]

engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_FCST_HOST, CURW_FCST_PORT, CURW_FCST_DATABASE,
            CURW_FCST_USERNAME, CURW_FCST_PASSWORD)

Session = get_sessionmaker(engine=engine)  # Session is a class
session = Session()


print("########### Add Stations ########################")
print(add_stations(stations=stations, session=session))


print("########### Get Stations by id ##################")
print("Id 300001:", get_station_by_id(session=session, id_="300001"))


print("########## Retrieve station id ##################")
print("latitude=6.872778, longitude=80.564444, station_type=StationEnum.Government:", get_station_id(session=session,
        latitude="6.872778", longitude="80.564444", station_type=StationEnum.Government))


print("######### Delete station by id #################")
# print("Id 100001 deleted status: ", delete_station_by_id(session=session, id_=100001))

print("######### Delete station with given latitude, longitude, station_type #######")
print("latitude=6.871389, longitude=80.524444, station_type=StationEnum.Government :",
        delete_station(session=session, latitude=6.871389, longitude=80.524444, station_type=StationEnum.Government))
