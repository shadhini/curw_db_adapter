from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

from db_adapter.logger import logger


@compiles(Insert)
def append_string(insert, compiler, **kw):
    s = compiler.visit_insert(insert, **kw)
    if 'mysql_appendstring' in insert.kwargs:
        return s + " " + insert.kwargs['mysql_appendstring']
    return s


def get_engine(dialect, driver, host, port, db, user, password):
    """ Connecting to database """

    db_url = "%s+%s://%s:%s@%s:%d/%s" % (dialect, driver, user, password, host, port, db)
    logger.info("Connecting to database.")
    return create_engine(db_url, echo=False)


def get_sessionmaker(engine):
    logger.info("Define a Session class which will serve as a factory for new Session objects.")
    return sessionmaker(bind=engine)

# # scoped_session is used for thread safety
# def get_sessionmaker(engine):
#     logger.info("Define a Session class which will serve as a factory for new Session objects.")
#     session = scoped_session(sessionmaker())
#
#     # removes the current Session associated with the thread
#     session.remove()
#
#     session.configure(bind=engine, autoflush=False, expire_on_commit=False)
#     # autoFLush: To sync the state of your application data with the state of the data in the databases
#
#     return session

# ---- declarative_base ----
# allows us to create classes that include directives to describe the actual
# database table they will be mapped to.


# CurwFcstBase class for all the schema model classes of "curw-fcst" database
logger.info("Declaring an orm mapping for curw_fcst database.")
CurwFcstBase = declarative_base()

# CurwObsBase class for all the schema model classes of "curw-obs" database
logger.info("Declaring an orm mapping for curw_obs database.")
CurwObsBase = declarative_base()
