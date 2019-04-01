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
