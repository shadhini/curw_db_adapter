from enum import Enum


class GridInterpolationEnum(Enum):
    MDPA = 'MDPA'
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
                'Minimum_Distance_Point_Approximation': GridInterpolationEnum.MDPA.value,
                'MDPA'                                : GridInterpolationEnum.MDPA.value,
                'Other'                               : GridInterpolationEnum.Other.value
                }

        return _nameToAbbr.get(name,  GridInterpolationEnum.Other.value)

    @staticmethod
    def getMeaning(name):
        _nameToMeaning = {
                GridInterpolationEnum.MDPA : 'Minimum_Distance_Point_Approximation',
                GridInterpolationEnum.Other: 'Other'
                }

        return _nameToMeaning.get(name, 'Other')
