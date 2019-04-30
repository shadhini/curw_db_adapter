from db_adapter.temp.base import get_engine, get_sessionmaker
from db_adapter.temp.constants import (
    CURW_FCST_USERNAME, CURW_FCST_PASSWORD, CURW_FCST_HOST, CURW_FCST_PORT,
    CURW_FCST_DATABASE,
    )
from db_adapter.temp.constants import DIALECT_MYSQL, DRIVER_PYMYSQL

from db_adapter.temp.curw_fcst import (
    get_variable_by_id, get_variable_id, add_variables, delete_variable_by_id,
    delete_variable,
    )


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

engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_FCST_HOST, CURW_FCST_PORT, CURW_FCST_DATABASE,
        CURW_FCST_USERNAME, CURW_FCST_PASSWORD)

Session = get_sessionmaker(engine=engine)  # Session is a class
session = Session()

print("########### Add Variables #################################")
print(add_variables(variables=variables, session=session))


print("########### Get Variables by id ###########################")
print("Id 3:", get_variable_by_id(session=session, id_="3"))


print("########## Retrieve variable id ###########################")
print("variable: Precipitation,   id:",
        get_variable_id(session=session, variable="Precipitation"))

print("######### Delete variable by id ###########################")
print("Id 3 deleted status: ", delete_variable_by_id(session=session, id_=3))

print("######### Delete variable with given variable name #######")
print("variable: Precipitation,   delete status :",
        delete_variable(session=session, variable="Precipitation"))
