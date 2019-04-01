from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import paramiko


def get_engine(host, port, user, password, db):

    """ Connecting to database """

    db_url = "mysql+pymysql://%s:%s@%s:%d/%s" % (user, password, host, port, db)
    # "mysql+pymysql://sylvain:passwd@localhost/db?host=localhost?port=3306")
    return create_engine(db_url, echo=True)


def get_sessionmaker(engine):
    return sessionmaker(bind=engine)

# allows us to create classes that include directives to describe the actual
# database table they will be mapped to.


# Base class for all the schema model classes
Base = declarative_base()


# ssh = paramiko.SSHClient()
#
# ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
# ssh.connect('104.198.0.87', username='uwcc-admin',
#         key_filename='/home/shadhini/.ssh/uwcc-admin')
#
# stdin, stdout, stderr = ssh.exec_command('ls')
# print (stdout.readlines())
# ssh.close()