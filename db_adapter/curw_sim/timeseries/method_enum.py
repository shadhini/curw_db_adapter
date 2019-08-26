from enum import Enum


class MethodEnum(Enum):
    MME = 'Multi_Model_Ensemble'
    SF = 'Statistical_Forecasting'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Multi_Model_Ensemble': MethodEnum.MME,
                'MME'                 : MethodEnum.MME,
                'Statistical_Forecasting': MethodEnum.SF,
                'SF'                  : MethodEnum.SF,
                'Other'               : MethodEnum.Other
                }

        return _nameToType.get(name, MethodEnum.Other)

    @staticmethod
    def getAbbreviation(name):
        _nameToAbbr = {
                MethodEnum.MME  : 'MME',
                MethodEnum.SF   : 'SF',
                MethodEnum.Other: 'Other'
                }

        return _nameToAbbr.get(name, 'Other')
