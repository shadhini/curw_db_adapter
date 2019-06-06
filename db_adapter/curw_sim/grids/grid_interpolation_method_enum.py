from enum import Enum


class GridInterpolationMethodEnum(Enum):
    MDPA = 'Minimum_Distance_Point_Approximation'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Minimum_Distance_Point_Approximation': GridInterpolationMethodEnum.MDPA,
                'MDPA'                                : GridInterpolationMethodEnum.MDPA,
                'Other'                               : GridInterpolationMethodEnum.Other
                }

        return _nameToType.get(name, GridInterpolationMethodEnum.Other)
