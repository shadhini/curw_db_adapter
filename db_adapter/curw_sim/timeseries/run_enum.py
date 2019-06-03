from enum import Enum


class RunEnum(Enum):
    RunMean = 'Run_Mean'
    RunMax = 'Run_Max'
    RunMin = 'Run_Min'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Run_Mean': RunEnum.Accumulative,
                'Run_Max' : RunEnum.Instantaneous,
                'Run_Min' : RunEnum.Mean,
                'Other'   : RunEnum.Other
                }

        return _nameToType.get(name, RunEnum.Other)
