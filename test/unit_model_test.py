from db_adapter.base import get_Pool, destroy_Pool

from db_adapter.curw_fcst.unit import UnitType, add_units, get_unit_id, get_unit_by_id, delete_unit, delete_unit_by_id

USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"


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

pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)


print("########### Add Units #################################")
print(add_units(units=units, pool=pool))


print("########### Get Units by id ###########################")
print("Id 6:", get_unit_by_id(pool=pool, id_="6"))


print("########## Retrieve unit id ###########################")
print("unit: count, unit_type: UnitType.Accumulative id:",
        get_unit_id(pool=pool, unit="m", unit_type=UnitType.Instantaneous))


print("######### Delete unit by id ###########################")
print("Id 3 deleted status: ", delete_unit_by_id(pool=pool, id_=3))

print("######### Delete unit with given unit, unit_type #######")
print("unit: count, unit_type: UnitType.Accumulative   delete status :",
        delete_unit(pool=pool, unit="count", unit_type=UnitType.Accumulative))

destroy_Pool(pool)