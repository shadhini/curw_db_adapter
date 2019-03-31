from db_adapter.base import get_engine, Base
from db_adapter.constants import USERNAME, PASSWORD, HOST, PORT, DATABASE


# connect to the MySQL engine
engine = get_engine(HOST, PORT, USERNAME, PASSWORD, DATABASE)

# create the schema using classes defined
Base.metadata.create_all(engine)

