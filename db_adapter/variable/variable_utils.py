# def get_variable_id(self, variable) -> str:
#     session = self.Session()
# 
#     try:
#         variable_row = session.query(Variable) \
#             .filter_by(variable=variable) \
#             .first()
#         return None if variable_row is None else variable_row.id
#     finally:
#         session.close()
# 
# 
# def add_variable(self, variable):
#     """
#     Insert variable into the database
#     :param variable: string
#     :return: True if the variable has been added to the database
#     """
# 
#     session = self.Session()
# 
#     try:
#         source = Variable(
#                 variable=variable
#                 )
# 
#         session.add(source)
#         session.commit()
# 
#         return True
# 
#     finally:
#         session.close()

from db_adapter.models import Variable

"""
Variable JSON Object would looks like this
e.g.:
    {
        'variable'     : 'mm',
    }
"""


def get_variable_by_id(session, id_):
    """
    Retrieve variable by id
    :param session: session made by sessionmaker for the database engine
    :param id_: variable id
    :return: Variable
    """

    try:
        variable_row = session.query(Variable).get(id_)
        return None if variable_row is None else variable_row
    finally:
        session.close()


def get_variable_id(session, variable) -> str:
    """
    Retrieve Variable id
    :param session: session made by sessionmaker for the database engine
    :param variable:
    :return: str: variable id
    """

    try:
        variable_row = session.query(Variable) \
            .filter_by(variable=variable) \
            .first()
        return None if variable_row is None else variable_row.id
    finally:
        session.close()


def add_variable(session, variable):
    """
    Insert variables into the database
    :param session: session made by sessionmaker for the database engine
    :param variable: string
    :return: True if the variable has been added to the "Variable" table of the database
    """

    try:
        variable = Variable(
                variable=variable,
                )

        session.add(variable)
        session.commit()

        return True

    finally:
        session.close()


def add_variables(variables, session):
    """
    Add variables into Variable table
    :param variables: list of json objects that define variable attributes
    e.g.:
    {
        'variable'     : 'mm',
    }
    :param session: session made by sessionmaker for the database engine
    :return:
    """

    for variable in variables:

        print(add_variable(session=session, variable=variable.get('variable')))
        print(variable.get('variable'))


def delete_variable(session, variable):
    """
    Delete variable from Variable table, given variable name
    :param session: session made by sessionmaker for the database engine
    :param variable: string
    :return: True if the deletion was successful
    """

    id_ = get_variable_id(session=session, variable=variable)

    try:
        if id_ is not None:
            delete_variable_by_id(session, id_)
            session.commit()
            return True
        else:
            print("There's no record in the database with the variable id ", id_)
            return False
    finally:
        session.close()


def delete_variable_by_id(session, id_):
    """
    Delete variable from Variable table by id
    :param session: session made by sessionmaker for the database engine
    :param id_:
    :return: True if the deletion was successful
    """

    try:
        variable = session.query(Variable).get(id_)
        if variable is not None:
            session.delete(variable)
            session.commit()
            status = session.query(Variable).filter_by(id=id_).count()
            return True if status==0 else False
        else:
            print("There's no record in the database with the variable id ", id_)
            return False
    finally:
        session.close()
