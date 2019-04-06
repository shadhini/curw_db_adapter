from db_adapter.models import Source


def get_source_by_id(session, id_):
    """
    Retrieve source by id
    :param session: session made by sessionmaker for the database engine
    :param id_: source id
    :return: Source
    """

    try:
        source_row = session.query(Source).get(id_)
        return None if source_row is None else source_row
    finally:
        session.close()


def get_source_id(session, model, version) -> str:

    """
    Retrieve Source id
    :param session: session made by sessionmaker for the database engine
    :param model:
    :param version:
    :return: str: source id
    """

    try:
        source_row = session.query(Source) \
            .filter_by(model=model) \
            .filter_by(version=version) \
            .first()
        return None if source_row is None else source_row.id
    finally:
        session.close()

def add_source(session, model, version, parameters):
    """
    Insert sources into the database
    :param model: string
    :param version: string
    :param parameters: JSON
    :return: True if the source has been added to the "Source' table of the database
    """

    session = session.Session()

    try:
        source = Source(
                model=model,
                version=version,
                parameters=parameters
                )

        session.add(source)
        session.commit()

        return True

    finally:
        session.close()



# def add_station(session, name, latitude, longitude, description, station_type):
#     """
#     Insert stations into the database
# 
#     Station ids ranged as below;
#     - 1 xx xxx - CUrW (stationId: curw_<SOMETHING>)
#     - 2 xx xxx - Megapolis (stationId: megapolis_<SOMETHING>)
#     - 3 xx xxx - Government (stationId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
#     - 4 xx xxx - Public (stationId: pub_<SOMETHING>)
#     - 8 xx xxx - Satellite (stationId: sat_<SOMETHING>)
# 
#     Simulation models station ids ranged over 1’000’000 as below;
#     - 1 1xx xxx - WRF (stationId: [;<prefix>_]wrf_<SOMETHING>)
#     - 1 2xx xxx - FLO2D (stationId: [;<prefix>_]flo2d_<SOMETHING>)model
#     - 1 3xx xxx - MIKE (stationId: [;<prefix>_]mike_<SOMETHING>)
# 
#     :param session: session made by sessionmaker for the database engine
#     :param name: string
#     :param latitude: double
#     :param longitude: double
#     :param description: string
#     :param station_type: StationEnum: which defines the station type
#     such as 'CUrW'
#     :return: True if the station is added into the 'Station' table
#     """
#     initial_value = station_type.value
#     range_ = StationEnum.getRange(station_type)
# 
#     station = session.query(Station) \
#         .filter(Station.id >= initial_value, Station.id <= initial_value + range_) \
#         .order_by(Station.id.desc()) \
#         .first()
# 
#     if station is not None:
#         station_id = station.id + 1
#     else:
#         station_id = initial_value
# 
#     try:
#         station = Station(
#                 id=station_id,
#                 name=name,
#                 latitude=latitude,
#                 longitude=longitude,
#                 description=description
#                 )
# 
#         session.add(station)
#         session.commit()
#     finally:
#         session.close()
# 
# 
# def delete_station(session, latitude, longitude, station_type):
#     """
#     Delete station from Station table
#     :param session: session made by sessionmaker for the database engine
#     :param latitude:
#     :param longitude:
#     :param station_type: StationEnum: which defines the station type
#     such as 'CUrW'
#     :return: True if the deletion was successful
#     """
# 
#     id_ = get_station_id(session, latitude=latitude, longitude=longitude, station_type=station_type)
# 
#     try:
#         if id_ is not None:
#             delete_station_by_id(session, id_)
#             session.commit()
#             return True
#         else:
#             return False
#     finally:
#         session.close()
# 
# 
# def delete_station_by_id(session, id_):
#     """
#     Delete station from Station table by id
#     :param session: session made by sessionmaker for the database engine
#     :param id_:
#     :return: True if the deletion was successful
#     """
# 
#     try:
#         station = session.query(Station).get(id_)
#         session.delete(station)
#         session.commit()
#         status = session.query(Station).filter_by(id=id_).count()
#         print("Count: ", status)
#         return True if status==0 else False
#     finally:
#         session.close()
