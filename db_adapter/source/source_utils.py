from db_adapter.models import Source


class SourceUtils:
    def __init__(self, session_creator):
        self.Session = session_creator

    def get_source_by_id(self, id_):
        """
        Retrieve source by id
        :param id_: source id
        :return:
        """
        session = self.Session()

        try:
            source_row = session.query(Source).get(id_)
            return None if source_row is None else source_row.id
        finally:
            session.close()

    def get_source_id(self, model, version) -> str:

        session = self.Session()

        try:
            source_row = session.query(Source) \
                .filter_by(model=model) \
                .filter_by(version=version) \
                .first()
            return None if source_row is None else source_row.id
        finally:
            session.close()

    def add_source(self, model, version, parameters):
        """
        Insert sources into the database
        :param model: string
        :param version: string
        :param parameters: JSON
        :return:
        """

        session = self.Session()

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
