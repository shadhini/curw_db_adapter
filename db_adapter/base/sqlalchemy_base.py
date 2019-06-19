from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from db_adapter.logger import logger


def get_engine(dialect, driver, host, port, db, user, password):
    """ Connecting to database """

    db_url = "%s+%s://%s:%s@%s:%d/%s" % (dialect, driver, user, password, host, port, db)
    return create_engine(db_url, echo=False)


def get_sessionmaker(engine):
    return sessionmaker(bind=engine)

# ---- declarative_base ----
# allows us to create classes that include directives to describe the actual
# database table they will be mapped to.


# CurwFcstBase class for all the schema model classes of "curw-fcst" database
logger.info("Declaring an orm mapping for curw_fcst database.")
CurwFcstBase = declarative_base()

# CurwObsBase class for all the schema model classes of "curw-obs" database
logger.info("Declaring an orm mapping for curw_obs database.")
CurwObsBase = declarative_base()

# CurwSimBase class for all the schema model classes of "curw-sim" database
logger.info("Declaring an orm mapping for curw_sim database.")
CurwSimBase = declarative_base()