from enum import Enum


class GridInterpolationEnum(Enum):
    MDPA = 'Minimum_Distance_Point_Approximation'
    TP = 'Thiessen_Polygon'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Minimum_Distance_Point_Approximation': GridInterpolationEnum.MDPA,
                'MDPA':                                 GridInterpolationEnum.MDPA,
                'Thiessen_Polygon':                     GridInterpolationEnum.TP,
                'TP':                                   GridInterpolationEnum.TP,
                'Other':                                GridInterpolationEnum.Other
                }

        return _nameToType.get(name, GridInterpolationEnum.Other)

    @staticmethod
    def getAbbreviation(name):
        _nameToAbbr = {
                GridInterpolationEnum.MDPA:     'MDPA',
                GridInterpolationEnum.TP:       'TP',
                GridInterpolationEnum.Other:    'Other'
                }

        return _nameToAbbr.get(name, 'Other')
