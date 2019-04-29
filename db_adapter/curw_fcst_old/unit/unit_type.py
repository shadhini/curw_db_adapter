from enum import Enum


class UnitType(Enum):
    Accumulative = 'Accumulative'
    Instantaneous = 'Instantaneous'
    Mean = 'Mean'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Accumulative' : UnitType.Accumulative,
                'Instantaneous': UnitType.Instantaneous,
                'Mean'         : UnitType.Mean,
                'Other'        : UnitType.Other
                }

        return _nameToType.get(name, UnitType.Other)
