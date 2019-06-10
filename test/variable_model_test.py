from db_adapter.base import get_Pool, destroy_Pool, destroy_Pool

from db_adapter.curw_fcst.variable import add_variables, get_variable_id, get_variable_by_id, delete_variable_by_id, delete_variable

USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"

variables = [
        {
                'variable': 'Discharge',
                },
        {
                'variable': 'Waterlevel',
                },
        {
                'variable': 'Temperature',
                },
        {
                'variable': 'Precipitation',
                },
        {
                'variable': 'WindSpeed',
                }
        ]

pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)


print("########### Add Variables #################################")
print(add_variables(variables=variables, pool=pool))


print("########### Get Variables by id ###########################")
print("Id 3:", get_variable_by_id(pool=pool, id_="3"))


print("########## Retrieve variable id ###########################")
print("variable: Precipitation,   id:",
        get_variable_id(pool=pool, variable="Precipitation"))

print("######### Delete variable by id ###########################")
print("Id 3 deleted status: ", delete_variable_by_id(pool=pool, id_=3))

print("######### Delete variable with given variable name #######")
print("variable: Precipitation,   delete status :",
        delete_variable(pool=pool, variable="Precipitation"))

# destroy_Pool(pool)
destroy_Pool(pool)
destroy_Pool(pool)
destroy_Pool(pool)
print("bdjhsvdhsvdjh")
exit(0)