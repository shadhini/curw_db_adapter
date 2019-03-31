from sqlalchemy import Column, ForeignKeyConstraint, ForeignKey
from sqlalchemy.dialects.mysql import DOUBLE, DATETIME, JSON, INTEGER, \
    VARCHAR, ENUM, DECIMAL
from sqlalchemy.orm import relationship


from db_adapter.base import Base


class Source(Base):
    __tablename__ = 'source'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(45), nullable=False)
    parameters = Column(JSON)

    def __repr__(self):
        return "<Source(name='%s', parameters='%s')>" \
               % (self.name, self.parameters)


class Station(Base):
    __tablename__ = 'station'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=True)
    name = Column(VARCHAR(45), nullable=False)
    latitude = Column(DOUBLE, nullable=False)
    longitude = Column(DOUBLE, nullable=False)
    resolution = Column(VARCHAR(50), nullable=False)
    description = Column(VARCHAR(255))

    def __repr__(self):
        return "<Station(name='%s', latitude='%r', longitude='%r', " \
               "resolution='%s',description='%s')>" \
               % (self.name, self.latitude, self.longitude, self.resolution,
                  self.description)


class Unit(Base):
    __tablename__ = 'unit'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=True)
    unit = Column(VARCHAR(10), nullable=False)
    type = Column(ENUM('Accumulative', 'Instantaneous', 'Mean'), nullable=False)

    def __repr__(self):
        return "<Unit(unit='%s', type='%s')>" % (self.unit, self.type)


class Variable(Base):
    __tablename__ = 'variable'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=True)
    variable = Column(VARCHAR(100), nullable=False)

    def __repr__(self):
        return "<Variable(variable='%s')>" % self.variable


class Run(Base):
    __tablename__ = 'run'

    id = Column(VARCHAR(64), nullable=False, primary_key=True, unique=True)
    sim_tag = Column(VARCHAR(100), nullable=False)
    start_date = Column(DATETIME)
    end_date = Column(DATETIME)
    station = Column(INTEGER(11), ForeignKey(Station.id), nullable=False)
    source = Column(INTEGER(11), ForeignKey(Source.id), nullable=False)
    variable = Column(INTEGER(11), ForeignKey(Variable.id), nullable=False)
    unit = Column(INTEGER(11), ForeignKey(Unit.id), nullable=False)
    fgt = Column(DATETIME, nullable=False)
    scheduled_date = Column(DATETIME, nullable=False)

    station_relationship = relationship('Station', foreign_keys='Run.station')
    source_relationship = relationship('Source', foreign_keys='Run.source')
    variable_relationship = relationship('Variable', foreign_keys='Run.variable')
    unit_relationship = relationship('Unit', foreign_keys='Run.unit')

    def __repr__(self):
        return "<Run(sim_tag='%s', station='%d', source='%d', variable='%d'," \
               " unit='%d', fgt='%r', scheduled_date='%r')>" \
               % (self.sim_tag, self.station, self.source, self.variable,
                  self.unit, self.fgt, self.scheduled_date)


class Data(Base):
    __tablename__='data'

    id = Column(VARCHAR(64), ForeignKey(Run.id), nullable=False, primary_key=True)
    time = Column(DATETIME, nullable=False, primary_key=True)
    value = Column(DECIMAL(8, 3), nullable=False)
    fgt = Column(DATETIME, nullable=False)  # to be removed

    id_relationship = relationship('Run', foreign_keys='Data.id')

    def __repr__(self):
        return "<Date(time='%s', value='%r', fgt='%r')>" \
               % (self.time, self.value, self.fgt)


