from db_adapter.models import Source
import traceback

"""
Source JSON Object would looks like this 
e.g.:
   {
        'model'     : 'wrfSE',
        'version'   : 'v3',
        'parameters': { }
    }
    {
        'model'     : 'OBS_WATER_LEVEL',
        'version'   : '',
        'parameters': {
                "CHANNEL_CELL_MAP"               : {
                        "594" : "Wellawatta", "1547": "Ingurukade", "3255": "Yakbedda", "3730": "Wellampitiya",
                        "7033": "Janakala Kendraya"
                        }, "FLOOD_PLAIN_CELL_MAP": { }
                }
    }
"""


def get_source_by_id(session, id_):
    """
    Retrieve source by id
    :param session: session made by sessionmaker for the database engine
    :param id_: source id
    :return: Source if source exists in the database, else None
    """

    try:
        source_row = session.query(Source).get(id_)
        return None if source_row is None else source_row
    except Exception as e:
        traceback.print_exc()
        return False
    finally:
        session.close()


def get_source_id(session, model, version) -> str:
    """
    Retrieve Source id
    :param session: session made by sessionmaker for the database engine
    :param model:
    :param version:
    :return: str: source id if source exists in the database, else None
    """

    try:
        source_row = session.query(Source) \
            .filter_by(model=model) \
            .filter_by(version=version) \
            .first()
        return None if source_row is None else source_row.id
    except Exception as e:
        traceback.print_exc()
        return False
    finally:
        session.close()


def add_source(session, model, version, parameters):
    """
    Insert sources into the database
    :param session: session made by sessionmaker for the database engine
    :param model: string
    :param version: string
    :param parameters: JSON
    :return: True if the source has been added to the "Source' table of the database, else False
    """

    try:
        source = Source(
                model=model,
                version=version,
                parameters=parameters
                )

        session.add(source)
        session.commit()

        return True
    except Exception as e:
        traceback.print_exc()
        return False
    finally:
        session.close()


def add_sources(sources, session):
    """
    Add sources into Source table
    :param sources: list of json objects that define source attributes
    e.g.:
   {
        'model'     : 'wrfSE',
        'version'   : 'v3',
        'parameters': { }
    }
    {
        'model'     : 'OBS_WATER_LEVEL',
        'version'   : '',
        'parameters': {
                "CHANNEL_CELL_MAP"               : {
                        "594" : "Wellawatta", "1547": "Ingurukade", "3255": "Yakbedda", "3730": "Wellampitiya",
                        "7033": "Janakala Kendraya"
                        }, "FLOOD_PLAIN_CELL_MAP": { }
                }
    }
    :return:
    """

    for source in sources:

        print(add_source(session=session, model=source.get('model'), version=source.get('version'),
                parameters=source.get('parameters')))
        print(source.get('model'))


def delete_source(session, model, version):
    """
    Delete source from Source table, given model and version
    :param session: session made by sessionmaker for the database engine
    :param model: str
    :param version: str
    :return: True if the deletion was successful, else False
    """

    id_ = get_source_id(session=session, model=model, version=version)

    try:
        if id_ is not None:
            return delete_source_by_id(session, id_)
        else:
            print("There's no record in the database with the source id ", id_)
            return False
    finally:
        session.close()


def delete_source_by_id(session, id_):
    """
    Delete source from Source table by id
    :param session: session made by sessionmaker for the database engine
    :param id_:
    :return: True if the deletion was successful, else False
    """

    try:
        source = session.query(Source).get(id_)
        if source is not None:
            session.delete(source)
            session.commit()
            status = session.query(Source).filter_by(id=id_).count()
            return True if status==0 else False
        else:
            print("There's no record in the database with the source id ", id_)
            return False
    except Exception as e:
        traceback.print_exc()
        return False
    finally:
        session.close()
