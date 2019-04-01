from db_adapter.base import get_engine, Base
from db_adapter.constants import USERNAME, PASSWORD, HOST, PORT, DATABASE

# USERNAME = "root"
# PASSWORD = "cfcwm07"
# HOST = "35.230.102.148"
# PORT = 3306
# DATABASE = "test-schema"

# connect to the MySQL engine
engine = get_engine(HOST, PORT, USERNAME, PASSWORD, DATABASE)

# create the schema using classes defined
Base.metadata.create_all(engine)

