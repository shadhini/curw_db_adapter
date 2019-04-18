from db_adapter.base import get_engine, get_sessionmaker
from db_adapter.constants import (
    CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_HOST, CURW_FCST_PORT,
    CURW_FCST_DATABASE,
    )
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL

from db_adapter.curw_fcst.unit import UnitType, get_unit_id, delete_unit_by_id, delete_unit


units = [
        {
                'unit'     : 'mm',
                'unit_type': UnitType.Accumulative
                },
        {
                'unit'     : 'm3/s',
                'unit_type': UnitType.Instantaneous
                },
        {
                'unit'     : 'm',
                'unit_type': UnitType.Instantaneous
                },
        {
                'unit'     : 'count',
                'unit_type': UnitType.Accumulative
                },
        {
                'unit'     : 'W/m2',
                'unit_type': UnitType.Instantaneous
                }
        ]

engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_FCST_HOST, CURW_FCST_PORT, CURW_FCST_DATABASE,
        CURW_FCST_USERNAME, CURW_FCST_PASSWORD)

Session = get_sessionmaker(engine=engine)  # Session is a class
session = Session()

print("########### Add Units #################################")
# print(add_units(units=units, session=session))


print("########### Get Units by id ###########################")
# print("Id 3:", get_unit_by_id(session=session, id_="3"))


print("########## Retrieve unit id ###########################")
print("unit: count, unit_type: UnitType.Accumulative id:",
        get_unit_id(session=session, unit="count", unit_type=UnitType.Accumulative))


print("######### Delete unit by id ###########################")
print("Id 3 deleted status: ", delete_unit_by_id(session=session, id_=3))

print("######### Delete unit with given unit, unit_type #######")
print("unit: count, unit_type: UnitType.Accumulative   delete status :",
        delete_unit(session=session, unit="count", unit_type=UnitType.Accumulative))
