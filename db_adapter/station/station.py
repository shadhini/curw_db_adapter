from db_adapter.models import Station


class Station:
	def __init__(self, session_creator):
		self.Session = session_creator

	def get_station_id(self,  latitude, longitude) -> str:

		session = self.Session()

		try:
			station_row = session.query(Station)\
				.filter_by(latitude=latitude)\
				.filter_by(longitude=longitude)\
				.first()
			return None if station_row is None else station_row.id
		finally:
			session.close()
