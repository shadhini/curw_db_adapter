from enum import Enum


class MethodEnum(Enum):
    MME = 'Multi_Model_Ensamble'
    Other = 'Other'

    @staticmethod
    def getType(name):
        _nameToType = {
                'Multi_Model_Ensamble': MethodEnum.MME,
                'MME'                 : MethodEnum.MME,
                'Other'               : MethodEnum.Other
                }

        return _nameToType.get(name, MethodEnum.Other)
