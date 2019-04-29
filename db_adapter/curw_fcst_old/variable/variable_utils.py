import traceback
from db_adapter.curw_fcst_old.models import Variable
from db_adapter.logger import logger

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
    :return: Variable if variable exists in the db, else None
    """

    try:
        variable_row = session.query(Variable).get(id_)
        return None if variable_row is None else variable_row
    except Exception as e:
        logger.error("Exception occurred while retrieving variable with id {}".format(id_))
        traceback.print_exc()
        return False
    finally:
        session.close()


def get_variable_id(session, variable) -> str:
    """
    Retrieve Variable id
    :param session: session made by sessionmaker for the database engine
    :param variable:
    :return: str: variable id if variable exists in the db, else None
    """

    try:
        variable_row = session.query(Variable) \
            .filter_by(variable=variable) \
            .first()
        return None if variable_row is None else variable_row.id
    except Exception as e:
        logger.error("Exception occurred while retrieving variable id: variable={}".format(variable))
        traceback.print_exc()
        return False
    finally:
        session.close()


def add_variable(session, variable):
    """
    Insert variables into the database
    :param session: session made by sessionmaker for the database engine
    :param variable: string
    :return: True if the variable has been added to the "Variable" table of the database, else False
    """

    try:
        variable = Variable(
                variable=variable,
                )

        session.add(variable)
        session.commit()

        return True
    except Exception as e:
        logger.error("Exception occurred while adding variable: variable={}".format(variable))
        traceback.print_exc()
        return False
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
    :return: True if the deletion was successful, else False
    """

    id_ = get_variable_id(session=session, variable=variable)

    try:
        if id_ is not None:
            return delete_variable_by_id(session, id_)
        else:
            logger.info("There's no record in the database with the variable id {}".format(id_))
            print("There's no record in the database with the variable id ", id_)
            return False
    finally:
        session.close()


def delete_variable_by_id(session, id_):
    """
    Delete variable from Variable table by id
    :param session: session made by sessionmaker for the database engine
    :param id_:
    :return: True if the deletion was successful, else False
    """

    try:
        variable = session.query(Variable).get(id_)
        if variable is not None:
            session.delete(variable)
            session.commit()
            status = session.query(Variable).filter_by(id=id_).count()
            return True if status==0 else False
        else:
            logger.info("There's no record in the database with the variable id {}".format(id_))
            print("There's no record in the database with the variable id ", id_)
            return False
    except Exception as e:
        logger.error("Exception occurred while deleting variable with id {}".format(id_))
        traceback.print_exc()
        return False
    finally:
        session.close()
