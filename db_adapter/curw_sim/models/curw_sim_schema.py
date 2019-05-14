from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import DOUBLE, DATETIME, VARCHAR, DECIMAL
from sqlalchemy.orm import relationship


from db_adapter.base import CurwSimBase


class Run(CurwSimBase):
    __tablename__ = 'run'

    id = Column(VARCHAR(64), nullable=False, primary_key=True, unique=True)
    latitude = Column(DOUBLE, nullable=False)
    longitude = Column(DOUBLE, nullable=False)
    model = Column(VARCHAR(25), nullable=False)
    method = Column(VARCHAR(100), nullable=False)

    data_relationship = relationship("Data", back_populates="id_relationship", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Run(id='%s', latitude='%s', longitude='%s', model='%s', method='%s')>" \
               % (self.id, self.latitude, self.longitude, self.model, self.method)


class Data(CurwSimBase):
    __tablename__='data'

    id = Column(VARCHAR(64), ForeignKey(Run.id), nullable=False, primary_key=True)
    time = Column(DATETIME, nullable=False, primary_key=True)
    value = Column(DECIMAL(8, 3), nullable=False)

    id_relationship = relationship('Run', foreign_keys='Data.id', back_populates="data_relationship")

    def __repr__(self):
        return "<Date(id='%s', time='%s', value='%r')>" \
               % (self.id, self.time, self.value)


