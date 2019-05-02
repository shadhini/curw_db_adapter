from db_adapter.base import get_Pool

from db_adapter.curw_fcst.station import StationEnum, add_stations, get_station_id, get_station_by_id, \
    delete_station_by_id, delete_station

USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"

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

pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)

print("########### Add Stations ########################")
print(add_stations(stations=stations, pool=pool))


print("########### Get Stations by id ##################")
print("Id 300001:", get_station_by_id(pool=pool, id_="300001"))


print("########## Retrieve station id ##################")
# print("latitude=6.872778, longitude=80.564444, station_type=StationEnum.Government:", get_station_id(pool=pool,
#         latitude="6.872778", longitude="80.564444", station_type=StationEnum.Government))
print("latitude=6.872778, longitude=80.564444, station_type=StationEnum.Government:", get_station_id(pool=pool,
        latitude="5.72296905517578", longitude="79.6031494140625", station_type=StationEnum.WRF))

print("######### Delete station by id #################")
print("Id 100001 deleted status: ", delete_station_by_id(pool=pool, id_=100001))

print("######### Delete station with given latitude, longitude, station_type #######")
print("latitude=6.871389, longitude=80.524444, station_type=StationEnum.Government :",
        delete_station(pool=pool, latitude=6.871389, longitude=80.524444, station_type=StationEnum.Government))
