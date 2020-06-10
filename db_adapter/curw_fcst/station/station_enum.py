from aenum import Enum, NoAlias


class StationEnum(Enum, settings=NoAlias):
    """
    StationEnum ids ranged as below;

        - 1 xx xxx - CUrW_WeatherStation (station_id: curw_<SOMETHING>)
        - 1 xx xxx - CUrW_WaterLevelGauge (station_id: curw_wl_<SOMETHING>)
        - 3 xx xxx - CUrW_CrossSection (station_id: megapolis_<SOMETHING>)
        - 4 xx xxx - Irrigation_Department (station_id: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
        - 5 xx xxx - Satellite (station_id: sat_<SOMETHING>)

        - 2 xxx xxx - Public (station_id: pub_<SOMETHING>)

        Simulation models StationEnum ids ranged over 1’000’000 as below;
        - 1 0xx xxx - HECHMS (name: hechms_<SOMETHING>)
        - 1 1xx xxx - WRF (name: wrf_<SOMETHING>)
        - 1 2xx xxx - FLO2D 250(name: flo2d_250_<SOMETHING>)
        - 1 3xx xxx - FLO2D 150(name: flo2d_150_<SOMETHING>)
        - 1 4xx xxx - FLO2D 150_v2(name: flo2d_150_v2_<SOMETHING>)
        - 1 5xx xxx - FLO2D 10(name: flo2d_10_<SOMETHING>)
        - 1 8xx xxx - MIKE11(name: mike_<SOMETHING>)

        Other;
        - 3 xxx xxx - Other (name/station_id: other_<SOMETHING>)
    """
    CUrW_WeatherStation = 100000
    CUrW_WaterLevelGauge = 100000
    CUrW_CrossSection = 300000
    Irrigation_Department = 400000
    Satellite = 500000

    HECHMS = 1000000
    WRF = 1100000
    FLO2D_250 = 1200000
    FLO2D_150 = 1300000
    FLO2D_150_v2 = 1400000
    FLO2D_10 = 1500000
    MIKE11 = 1800000

    Public = 2000000

    Other = 10000000

    _nameToRange = {
            CUrW_WeatherStation : 200000,
            CUrW_WaterLevelGauge: 200000,
            CUrW_CrossSection   : 100000,
            Irrigation_Department: 100000,
            Satellite           : 500000,

            HECHMS              : 100000,
            WRF                 : 100000,
            FLO2D_250           : 100000,
            FLO2D_150           : 100000,
            FLO2D_150_v2        : 100000,
            FLO2D_10            : 100000,
            MIKE11              : 100000,

            Public              : 8000000,

            Other               : 1000000
            }

    @staticmethod
    def getRange(name):
        _nameToRange = {
                StationEnum.CUrW_WeatherStation : 200000,
                StationEnum.CUrW_WaterLevelGauge: 200000,
                StationEnum.CUrW_CrossSection   : 100000,
                StationEnum.Irrigation_Department: 100000,
                StationEnum.Satellite           : 500000,

                StationEnum.HECHMS              : 100000,
                StationEnum.WRF                 : 100000,
                StationEnum.FLO2D_250           : 100000,
                StationEnum.FLO2D_150           : 100000,
                StationEnum.FLO2D_150_v2        : 100000,
                StationEnum.FLO2D_10            : 100000,
                StationEnum.MIKE11              : 100000,

                StationEnum.Public              : 8000000,

                StationEnum.Other               : 1000000
                }
        return _nameToRange.get(name, 1000000)

    @staticmethod
    def getType(name):
        _nameToType = {
                'CUrW_WeatherStation' : StationEnum.CUrW_WeatherStation,
                'CUrW_WaterLevelGauge': StationEnum.CUrW_WaterLevelGauge,
                'CUrW_CrossSection'   : StationEnum.CUrW_CrossSection,
                'Irrigation_Department': StationEnum.Irrigation_Department,
                'Satellite'           : StationEnum.Satellite,

                'HECHMS'              : StationEnum.HECHMS,
                'WRF'                 : StationEnum.WRF,
                'FLO2D_250'           : StationEnum.FLO2D_250,
                'FLO2D_150'           : StationEnum.FLO2D_150,
                'FLO2D_150_v2'        : StationEnum.FLO2D_150_v2,
                'FLO2D_10'            : StationEnum.FLO2D_10,
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
                StationEnum.CUrW_CrossSection   : 'CUrW_CrossSection',
                StationEnum.Irrigation_Department: 'Irrigation_Department',
                StationEnum.Satellite           : 'Satellite',

                StationEnum.HECHMS              : 'HECHMS',
                StationEnum.WRF                 : 'WRF',
                StationEnum.FLO2D_250           : 'FLO2D_250',
                StationEnum.FLO2D_150           : 'FLO2D_150',
                StationEnum.FLO2D_150_v2        : 'FLO2D_150_v2',
                StationEnum.FLO2D_10            : 'FLO2D_10',
                StationEnum.MIKE11              : 'MIKE11',

                StationEnum.Public              : 'Public',

                StationEnum.Other               : 'Other'
                }

        return _nameToString.get(name, 'Other')