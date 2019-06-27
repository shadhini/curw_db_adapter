from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_obs.station import StationEnum, add_stations, get_station_id, get_station_by_id, \
    delete_station_by_id, delete_station

USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "curw_obs"

stations = [
        {
                'name'        : 'Attanagalla',
                'latitude'    : '7.111666667',
                'longitude'   : '80.14983333',
                'description' : '',
                'station_type': StationEnum.CUrW_WeatherStation
                },
        {
                'name'        : 'Colombo',
                'latitude'    : '6.898158',
                'longitude'   : '79.8653',
                'description' : '',
                'station_type': StationEnum.CUrW_WaterLevelGauge
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


destroy_Pool(pool=pool)