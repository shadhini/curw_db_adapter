# from enum import Enum
#
#
# class DataEnum(Enum):
#     DataMean = 'data'
#     DataMax = 'data_max'
#     DataMin = 'data_min'
#     Other = 'Other'
#
#     @staticmethod
#     def getType(name):
#         _nameToType = {
#                 'Data_Mean': DataEnum.DataMean,
#                 'Data'     : DataEnum.DataMean,
#                 'Data_Max' : DataEnum.DataMax,
#                 'Data_Min' : DataEnum.DataMin,
#                 'Other'    : DataEnum.Other
#                 }
#
#         return _nameToType.get(name, DataEnum.Other)
