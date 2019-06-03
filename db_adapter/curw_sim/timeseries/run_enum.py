from enum import Enum


class RunEnum(Enum):
    RunMean = 'Run_Mean'
    RunMax = 'Run_Max'
    RunMin = 'Run_Min'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Run_Mean': RunEnum.RunMean,
                'Run_Max' : RunEnum.RunMax,
                'Run_Min' : RunEnum.RunMin,
                'Other'   : RunEnum.Other
                }

        return _nameToType.get(name, RunEnum.Other)
