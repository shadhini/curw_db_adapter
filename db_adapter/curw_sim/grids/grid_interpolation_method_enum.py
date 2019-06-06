from enum import Enum


class GridInterpolationEnum(Enum):
    MDPA = 'Minimum_Distance_Point_Approximation'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Minimum_Distance_Point_Approximation': GridInterpolationEnum.MDPA,
                'MDPA'                                : GridInterpolationEnum.MDPA,
                'Other'                               : GridInterpolationEnum.Other
                }

        return _nameToType.get(name, GridInterpolationEnum.Other)

    @staticmethod
    def getAbbreviation(name):
        _nameToAbbr = {
                GridInterpolationEnum.MDPA : 'MDPA',
                GridInterpolationEnum.Other: 'Other'
                }

        return _nameToAbbr.get(name, 'Other')
