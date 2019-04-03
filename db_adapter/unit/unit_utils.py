from db_adapter.models import Unit


class UnitUtils:
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

	def add_unit(self, unit, type_):
		"""
		Insert unit into the database
		:param unit: string
		:param type_: UnitType
		:return: True is the unit has been added to the database
		"""

		session = self.Session()

		try:
			source = Unit(
					unit=unit,
					type=type_.value
					)

			session.add(source)
			session.commit()

			return True

		finally:
			session.close()
