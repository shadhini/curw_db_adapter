from enum import Enum


class StationEnum(Enum):
    """
    StationEnum ids ranged as below;

        - 1 xx xxx - CUrW (StationEnumId: curw_<SOMETHING>)
        - 2 xx xxx - Megapolis (StationEnumId: megapolis_<SOMETHING>)
        - 3 xx xxx - Government (StationEnumId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
        - 4 xx xxx - Public (StationEnumId: pub_<SOMETHING>)
        - 8 xx xxx - Satellite (StationEnumId: sat_<SOMETHING>)

        Simulation models StationEnum ids ranged over 1’000’000 as below;
        - 1 1xx xxx - WRF (StationEnumId: wrf_<SOMETHING>)
        - 1 2xx xxx - FLO2D (StationEnumId: flo2d_<SOMETHING>)

        Other;
        - 2 xxx xxx - Other (StationEnumId: other_<SOMETHING>)
    """
    CUrW = 100000
    Megapolis = 200000
    Government = 300000
    Gov = 300000
    Public = 400000
    Satellite = 800000
    Sat = 800000

    WRF = 1100000
    FLO2D_250 = 1200000
    FLO2D_150 = 1300000
    FLO2D_30 = 1400000
    MIKE11 = 1800000

    Other = 2000000

    _nameToRange = {
            CUrW      : 100000,
            Megapolis : 100000,
            Government: 100000,
            Gov       : 100000,
            Public    : 400000,
            Satellite : 200000,
            Sat       : 200000,

            WRF       : 100000,
            FLO2D_250 : 100000,
            FLO2D_150 : 100000,
            FLO2D_30  : 200000,
            MIKE11    : 100000,

            Other     : 1000000
            }

    @staticmethod
    def getRange(name):
        _nameToRange = {
                StationEnum.CUrW      : 100000,
                StationEnum.Megapolis : 100000,
                StationEnum.Government: 100000,
                StationEnum.Gov       : 100000,
                StationEnum.Public    : 400000,
                StationEnum.Satellite : 200000,
                StationEnum.Sat       : 200000,

                StationEnum.WRF       : 100000,
                StationEnum.FLO2D_250 : 100000,
                StationEnum.FLO2D_150 : 100000,
                StationEnum.FLO2D_30  : 200000,
                StationEnum.MIKE11    : 100000,

                StationEnum.Other     : 1000000
                }
        return _nameToRange.get(name, 1000000)

    @staticmethod
    def getType(name):
        _nameToType = {
                'CUrW'      : StationEnum.CUrW,
                'Megapolis' : StationEnum.Megapolis,
                'Government': StationEnum.Government,
                'Gov'       : StationEnum.Gov,
                'Public'    : StationEnum.Public,
                'Satellite' : StationEnum.Satellite,
                'Sat'       : StationEnum.Sat,

                'WRF'       : StationEnum.WRF,
                'FLO2D_250' : StationEnum.FLO2D_250,
                'FLO2D_150' : StationEnum.FLO2D_150,
                'FLO2D_30'  : StationEnum.FLO2D_30,
                'MIKE11'    : StationEnum.MIKE11,

                'Other'     : StationEnum.Other
                }

        return _nameToType.get(name, StationEnum.Other)
