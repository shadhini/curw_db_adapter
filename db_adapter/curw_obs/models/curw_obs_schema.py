from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import DOUBLE, DATETIME, JSON, INTEGER, \
    VARCHAR, ENUM, DECIMAL
from sqlalchemy.orm import relationship


from db_adapter.base import CurwObsBase


class Source(CurwObsBase):
    __tablename__ = 'source'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=True)
    source = Column(VARCHAR(25), nullable=False)
    parameters = Column(JSON, nullable=True)

    run_relationship = relationship("Run", back_populates="source_relationship", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Source(id='%s', model='%s', parameters='%s')>" \
               % (self.id, self.model, self.parameters)


class Station(CurwObsBase):
    __tablename__ = 'station'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=False)
    name = Column(VARCHAR(45), nullable=False)
    latitude = Column(DOUBLE, nullable=False)
    longitude = Column(DOUBLE, nullable=False)

    description = Column(VARCHAR(255))

    run_relationship = relationship("Run", back_populates="station_relationship", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Station(id='%s', name='%s', latitude='%r', longitude='%r', " \
               "description='%s')>" \
               % (self.id, self.name, self.latitude, self.longitude, self.description)


class Unit(CurwObsBase):
    __tablename__ = 'unit'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=True)
    unit = Column(VARCHAR(10), nullable=False)
    type = Column(ENUM('Accumulative', 'Instantaneous', 'Mean'), nullable=False)

    run_relationship = relationship("Run", back_populates="unit_relationship", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Unit(id='%s', unit='%s', type='%s')>" \
               % (self.id, self.unit, self.type)


class Variable(CurwObsBase):
    __tablename__ = 'variable'

    id = Column(INTEGER(11), nullable=False, primary_key=True, autoincrement=True)
    variable = Column(VARCHAR(100), nullable=False)

    run_relationship = relationship("Run", back_populates="variable_relationship", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Variable(id='%s', variable='%s')>" % (self.id, self.variable)


class Run(CurwObsBase):
    __tablename__ = 'run'

    id = Column(VARCHAR(64), nullable=False, primary_key=True, unique=True)
    sim_tag = Column(VARCHAR(100), nullable=True)
    station = Column(INTEGER(11), ForeignKey(Station.id), nullable=False)
    source = Column(INTEGER(11), ForeignKey(Source.id), nullable=False)
    variable = Column(INTEGER(11), ForeignKey(Variable.id), nullable=False)
    unit = Column(INTEGER(11), ForeignKey(Unit.id), nullable=False)

    station_relationship = relationship('Station', foreign_keys='Run.station', back_populates="run_relationship")
    source_relationship = relationship('Source', foreign_keys='Run.source', back_populates="run_relationship")
    variable_relationship = relationship('Variable', foreign_keys='Run.variable', back_populates="run_relationship")
    unit_relationship = relationship('Unit', foreign_keys='Run.unit', back_populates="run_relationship")

    data_relationship = relationship("Data", back_populates="id_relationship", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Run(id='%s', sim_tag='%s', station='%d', source='%d', " \
               "variable='%d', unit='%d')>" \
               % (self.id, self.sim_tag, self.station, self.source,
                  self.variable, self.unit)


class Data(CurwObsBase):
    __tablename__='data'

    id = Column(VARCHAR(64), ForeignKey(Run.id), nullable=False, primary_key=True)
    time = Column(DATETIME, nullable=False, primary_key=True)
    value = Column(DECIMAL(8, 3), nullable=False)

    id_relationship = relationship('Run', foreign_keys='Data.id', back_populates="data_relationship")

    def __repr__(self):
        return "<Date(id='%s', time='%s', value='%r')>" \
               % (self.id, self.time, self.value)


