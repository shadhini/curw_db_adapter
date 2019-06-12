from enum import Enum


class MethodEnum(Enum):
    MME = 'MME'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Multi_Model_Ensemble': MethodEnum.MME,
                'MME'                 : MethodEnum.MME,
                'Other'               : MethodEnum.Other
                }

        return _nameToType.get(name, MethodEnum.Other)

    # Abbreviation is included in the hash
    @staticmethod
    def getAbbreviation(name):
        _nameToAbbr = {
                'Multi_Model_Ensemble': MethodEnum.MME.value,
                'MME'                 : MethodEnum.MME.value,
                'Other'               : MethodEnum.Other.value
                }

        return _nameToAbbr.get(name, MethodEnum.Other.value)

    @staticmethod
    def getMeaning(name):
        _nameToMeaning = {
                MethodEnum.MME  : 'Multi_Model_Ensemble',
                MethodEnum.Other: 'Other'
                }

        return _nameToMeaning.get(name, 'Other')
