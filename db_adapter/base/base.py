from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


def get_engine(dialect, driver, host, port, db, user, password):
    """ Connecting to database """

    db_url = "%s+%s://%s:%s@%s:%d/%s" % (dialect, driver, user, password, host, port, db)
    return create_engine(db_url, echo=True)


def get_sessionmaker(engine):
    return sessionmaker(bind=engine)

# ---- declarative_base ----
# allows us to create classes that include directives to describe the actual
# database table they will be mapped to.


# CurwFcstBase class for all the schema model classes of "curw-fcst" database
CurwFcstBase = declarative_base()

# import paramiko
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
