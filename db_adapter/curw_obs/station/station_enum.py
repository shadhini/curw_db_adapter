from aenum import Enum, NoAlias


class StationEnum(Enum, settings=NoAlias):
    """
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
    """
    CUrW_WeatherStation = 100000
    CUrW_WaterLevelGauge = 100000
    Megapolis = 300000
    Government = 400000
    Satellite = 500000

    WRF = 1100000
    FLO2D_250 = 1200000
    FLO2D_150 = 1300000
    FLO2D_30 = 1400000
    MIKE11 = 1800000

    Public = 2000000

    Other = 3000000

    _nameToRange = {
            CUrW_WeatherStation : 200000,
            CUrW_WaterLevelGauge: 200000,
            Megapolis           : 100000,
            Government          : 100000,
            Satellite           : 500000,

            WRF                 : 100000,
            FLO2D_250           : 100000,
            FLO2D_150           : 100000,
            FLO2D_30            : 200000,
            MIKE11              : 100000,

            Public              : 1000000,

            Other               : 1000000
            }

    @staticmethod
    def getRange(name):
        _nameToRange = {
                StationEnum.CUrW_WeatherStation : 200000,
                StationEnum.CUrW_WaterLevelGauge: 200000,
                StationEnum.Megapolis           : 100000,
                StationEnum.Government          : 100000,
                StationEnum.Satellite           : 500000,

                StationEnum.WRF                 : 100000,
                StationEnum.FLO2D_250           : 100000,
                StationEnum.FLO2D_150           : 100000,
                StationEnum.FLO2D_30            : 200000,
                StationEnum.MIKE11              : 100000,

                StationEnum.Public              : 1000000,

                StationEnum.Other               : 1000000
                }
        return _nameToRange.get(name, 1000000)

    @staticmethod
    def getType(name):
        _nameToType = {
                'CUrW_WeatherStation' : StationEnum.CUrW_WeatherStation,
                'CUrW_WaterLevelGauge': StationEnum.CUrW_WaterLevelGauge,
                'Megapolis'           : StationEnum.Megapolis,
                'Government'          : StationEnum.Government,
                'Gov'                 : StationEnum.Government,
                'Satellite'           : StationEnum.Satellite,
                'Sat'                 : StationEnum.Satellite,

                'WRF'                 : StationEnum.WRF,
                'FLO2D_250'           : StationEnum.FLO2D_250,
                'FLO2D_150'           : StationEnum.FLO2D_150,
                'FLO2D_30'            : StationEnum.FLO2D_30,
                'MIKE11'              : StationEnum.MIKE11,

                'Public'              : StationEnum.Public,

                'Other'               : StationEnum.Other
                }

        return _nameToType.get(name, StationEnum.Other)

    @staticmethod
    def getTypeString(name):
        _nameToString = {
                StationEnum.CUrW_WeatherStation : 'CUrW_WeatherStation',
                StationEnum.CUrW_WaterLevelGauge: 'CUrW_WaterLevelGauge',
                StationEnum.Megapolis           : 'Megapolis',
                StationEnum.Government          : 'Government',
                StationEnum.Satellite           : 'Satellite',

                StationEnum.WRF                 : 'WRF',
                StationEnum.FLO2D_250           : 'FLO2D_250',
                StationEnum.FLO2D_150           : 'FLO2D_150',
                StationEnum.FLO2D_30            : 'FLO2D_30',
                StationEnum.MIKE11              : 'MIKE11',

                StationEnum.Public              : 'Public',

                StationEnum.Other               : 'Other'
                }

        return _nameToString.get(name, 'Other')
