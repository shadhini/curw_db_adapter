from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


def get_engine(host, port, user, password, db):

    """ Connecting to database """

    db_url = "mysql+pymysql://%s:%s@%s:%d/%s" % (user, password, host, port, db)
    return create_engine(db_url, echo=True)


def get_sessionmaker(engine):
    return sessionmaker(bind=engine)

# allows us to create classes that include directives to describe the actual
# database table they will be mapped to.


# Base class for all the schema model classes
Base = declarative_base()
