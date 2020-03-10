from enum import Enum


class MethodEnum(Enum):
    MME = 'Multi_Model_Ensemble'
    SF = 'Statistical_Forecasting'
    TSF = 'Time_Series_Forecasting'
    MGF = 'Mobile_Geographics_Forecasts'
    OBS = 'Observations'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
            'Multi_Model_Ensemble': MethodEnum.MME,
            'MME': MethodEnum.MME,
            'Statistical_Forecasting': MethodEnum.SF,
            'SF': MethodEnum.SF,
            'Time_Series_Forecasting': MethodEnum.TSF,
            'TSF': MethodEnum.TSF,
            'Mobile_Geographics_Forecasts': MethodEnum.MGF,
            'MGF': MethodEnum.MGF,
            'Observations': MethodEnum.OBS,
            'OBS': MethodEnum.OBS,
            'Other': MethodEnum.Other
        }

        return _nameToType.get(name, MethodEnum.Other)

    @staticmethod
    def getAbbreviation(name):
        _nameToAbbr = {
            MethodEnum.MME: 'MME',
            MethodEnum.SF: 'SF',
            MethodEnum.TSF: 'TSF',
            MethodEnum.MGF: 'MGF',
            MethodEnum.OBS: 'OBS',
            MethodEnum.Other: 'Other'
        }

        return _nameToAbbr.get(name, 'Other')
