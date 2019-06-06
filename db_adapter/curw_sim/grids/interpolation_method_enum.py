from enum import Enum


class InterpolationMethodEnum(Enum):
    MDPA = 'Minimum_Distance_Point_Approximation'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Minimum_Distance_Point_Approximation': InterpolationMethodEnum.MDPA,
                'MDPA'                                : InterpolationMethodEnum.MDPA,
                'Other'                               : InterpolationMethodEnum.Other
                }

        return _nameToType.get(name, InterpolationMethodEnum.Other)
