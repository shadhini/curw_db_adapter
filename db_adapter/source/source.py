from db_adapter.models import Source


class Source:
	def __init__(self, session_creator):
		self.Session = session_creator

	def get_source_id(self,  model, version) -> str:

		session = self.Session()

		try:
			source_row = session.query(Source)\
				.filter_by(model=model)\
				.filter_by(version=version)\
				.first()
			return None if source_row is None else source_row.id
		finally:
			session.close()
