from db_adapter.models import Station
from db_adapter.station.station_enum import StationEnum

# print("getRange: " + str(StationEnum.getRange("CUrW")))
# print("getRange/getType: " + str(StationEnum.getRange(StationEnum.getType("CUrW"))))
# print("getType: " + str(StationEnum.getType("CUrW")))


class StationUtils:
    def __init__(self, session_creator):
        self.Session = session_creator

    def get_station_by_id(self, id_):
        """
        Retrieve station by id
        :param id_: station id
        :return:
        """
        session = self.Session()

        try:
            station_row = session.query(Station).get(id_)
            return None if station_row is None else station_row.id
        finally:
            session.close()

    def get_station_id(self, latitude, longitude, station_type) -> str:
        """
        Retrieve station id
        :param latitude:
        :param longitude:
        :param station_type: StationEnum: which defines the station type
        such as 'CUrW', 'WRF'
        :return:
        """

        session = self.Session()

        initial_value = station_type.value

        try:
            if len(initial_value)==6:
                station_row = session.query(Station) \
                    .filter_by(Station.id.like(initial_value[0] + "_____")) \
                    .filter_by(latitude=latitude) \
                    .filter_by(longitude=longitude) \
                    .first()
            elif len(initial_value)==7:
                station_row = session.query(Station) \
                    .filter_by(Station.id.like(initial_value[0] + initial_value[1] + "_____")) \
                    .filter_by(latitude=latitude) \
                    .filter_by(longitude=longitude) \
                    .first()
            return None if station_row is None else station_row.id
        finally:
            session.close()

    def add_station(self, name, latitude, longitude, description,
                    station_type):
        """
        Insert stations into the database

        Station ids ranged as below;
        - 1 xx xxx - CUrW (stationId: curw_<SOMETHING>)
        - 2 xx xxx - Megapolis (stationId: megapolis_<SOMETHING>)
        - 3 xx xxx - Government (stationId: gov_<SOMETHING>. May follow as gov_irr_<SOMETHING>)
        - 4 xx xxx - Public (stationId: pub_<SOMETHING>)
        - 8 xx xxx - Satellite (stationId: sat_<SOMETHING>)

        Simulation models station ids ranged over 1’000’000 as below;
        - 1 1xx xxx - WRF (stationId: [;<prefix>_]wrf_<SOMETHING>)
        - 1 2xx xxx - FLO2D (stationId: [;<prefix>_]flo2d_<SOMETHING>)model
        - 1 3xx xxx - MIKE (stationId: [;<prefix>_]mike_<SOMETHING>)

        :param name: string
        :param latitude: double
        :param longitude: double
        :param description: string
        :param station_type: StationEnum: which defines the station type
        such as 'CUrW'
        :return: True if the station is added into the 'Station' table
        """
        initial_value = station_type.value
        range_ = StationEnum.getRange(station_type)
        session = self.Session()

        try:
            station_id = session.query(Station) \
                .filter_by(
                    Station.id.between(initial_value, initial_value + range_)) \
                .order_by(Station.id.desc()) \
                .first()

            if station_id is not None:
                station_id = station_id + 1
            else:
                station_id = initial_value

            station = Station(
                    id=station_id,
                    name=name,
                    latitude=latitude,
                    longitude=longitude,
                    description=description
                    )

            session.add(station)
            session.commit()

            return True
        finally:
            session.close()

    def delete_station(self, latitude, longitude, station_type):
        """
        Delete station from Station table
        :param latitude:
        :param longitude:
        :param station_type: StationEnum: which defines the station type
        such as 'CUrW'
        :return:
        """

        session = self.Session()

        id_ = self.get_station_id(self, latitude=latitude, longitude=longitude, station_type=station_type)

        try:
            if id_ is not None:
                self.delete_station(self, id_)
            else:
                return False
        finally:
            session.close()

    def delete_station(self, id_):
        """
        Delete station from Station table by id
        :param id_:
        :return:
        """
        session = self.Session()

        try:
            station = session.query(Station).get(id_)
            session.delete(station)
            status = session.query(Station).filter_by(id=id_).count()
            return False if status!=0 else True
        finally:
            session.close()
