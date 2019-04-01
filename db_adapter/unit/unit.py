from db_adapter.models import Unit


class Unit:
	def __init__(self, session_creator):
		self.Session = session_creator

	def get_unit_id(self,  unit, type) -> str:

		session = self.Session()

		try:
			unit_row = session.query(Unit)\
				.filter_by(unit=unit)\
				.filter_by(type=type)\
				.first()
			return None if unit_row is None else unit_row.id
		finally:
			session.close()
