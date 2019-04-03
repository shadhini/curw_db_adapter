from db_adapter.models import Variable


class Variable:
	def __init__(self, session_creator):
		self.Session = session_creator

	def get_variable_id(self,  variable) -> str:

		session = self.Session()

		try:
			variable_row = session.query(Variable)\
				.filter_by(variable=variable)\
				.first()
			return None if variable_row is None else variable_row.id
		finally:
			session.close()

	def add_variable(self, variable):
		"""
		Insert variable into the database
		:param variable: string
		:return: True if the unit has been added to the database
		"""

		session = self.Session()

		try:
			source = Variable(
					variable=variable
					)

			session.add(source)
			session.commit()

			return True

		finally:
			session.close()
