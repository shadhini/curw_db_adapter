import traceback
from db_adapter.models import Unit
from db_adapter.logger import logger

"""
Unit JSON Object would looks like this
e.g.:
    {
        'unit'     : 'mm',
        'unit_type': UnitType.Accumulative
    }
    {
        'unit'     : 'm3/s',
        'unit_type': UnitType.Instantaneous
    }
"""


def get_unit_by_id(session, id_):
    """
    Retrieve unit by id
    :param session: session made by sessionmaker for the database engine
    :param id_: unit id
    :return: Unit if unit exists in the db, else None
    """

    try:
        unit_row = session.query(Unit).get(id_)
        return None if unit_row is None else unit_row
    except Exception as e:
        logger.error("Exception occurred while retrieving unit with id {}".format(id_))
        traceback.print_exc()
        return False
    finally:
        session.close()


def get_unit_id(session, unit, unit_type) -> str:
    """
    Retrieve Unit id
    :param session: session made by sessionmaker for the database engine
    :param unit:
    :param unit_type: UnitType enum value. This value can be any of {Accumulative, Instantaneous, Mean} set
    :return: str: unit id if unit exists in the db, else None
    """

    try:
        unit_row = session.query(Unit) \
            .filter_by(unit=unit) \
            .filter_by(type=unit_type.value) \
            .first()
        return None if unit_row is None else unit_row.id
    except Exception as e:
        logger.error("Exception occurred while retrieving unit id: unit={} and unit_type={}".format(unit, unit_type))
        traceback.print_exc()
        return False
    finally:
        session.close()


def add_unit(session, unit, unit_type):
    """
    Insert units into the database
    :param session: session made by sessionmaker for the database engine
    :param unit: string
    :param unit_type: UnitType enum value. This value can be any of {Accumulative, Instantaneous, Mean} set
    :return: True if the unit has been added to the "Unit" table of the database, else False
    """

    try:
        unit = Unit(
                unit=unit,
                type=unit_type.value
                )

        session.add(unit)
        session.commit()

        return True
    except Exception as e:
        logger.error("Exception occurred while adding unit: unit={}, unit_type={}".format(unit, unit_type))
        traceback.print_exc()
        return False
    finally:
        session.close()


def add_units(units, session):
    """
    Add units into Unit table
    :param units: list of json objects that define unit attributes
    e.g.:
    {
        'unit'     : 'mm',
        'unit_type': UnitType.Accumulative
    }
    {
        'unit'     : 'm3/s',
        'unit_type': UnitType.Instantaneous
    }
    :param session: session made by sessionmaker for the database engine
    :return:
    """

    for unit in units:

        print(add_unit(session=session, unit=unit.get('unit'), unit_type=unit.get('unit_type')))
        print(unit.get('unit'))


def delete_unit(session, unit, unit_type):
    """
    Delete unit from Unit table, given unit and unit_type
    :param session: session made by sessionmaker for the database engine
    :param unit: string
    :param unit_type: UnitType enum value. This value can be any of {Accumulative, Instantaneous, Mean} set
    :return: True if the deletion was successful, else False
    """

    id_ = get_unit_id(session=session, unit=unit, unit_type=unit_type)

    try:
        if id_ is not None:
            return delete_unit_by_id(session, id_)
        else:
            logger.info("There's no record in the database with the unit id {}".format(id_))
            print("There's no record in the database with the unit id ", id_)
            return False
    finally:
        session.close()


def delete_unit_by_id(session, id_):
    """
    Delete unit from Unit table by id
    :param session: session made by sessionmaker for the database engine
    :param id_:
    :return: True if the deletion was successful, else False
    """

    try:
        unit = session.query(Unit).get(id_)
        if unit is not None:
            session.delete(unit)
            session.commit()
            status = session.query(Unit).filter_by(id=id_).count()
            return True if status==0 else False
        else:
            logger.info("There's no record in the database with the unit id {}".format(id_))
            print("There's no record in the database with the unit id ", id_)
            return False
    except Exception as e:
        logger.error("Exception occurred while deleting unit with id {}".format(id_))
        traceback.print_exc()
        return False
    finally:
        session.close()
