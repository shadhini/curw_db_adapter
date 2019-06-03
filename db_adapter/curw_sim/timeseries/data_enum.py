from enum import Enum


class DataEnum(Enum):
    DataMean = 'Data_Mean'
    DataMax = 'Data_Max'
    DataMin = 'Data_Min'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Data_Mean': DataEnum.DataMean,
                'Data_Max' : DataEnum.DataMax,
                'Data_Min' : DataEnum.DataMin,
                'Other'   : DataEnum.Other
                }

        return _nameToType.get(name, DataEnum.Other)
