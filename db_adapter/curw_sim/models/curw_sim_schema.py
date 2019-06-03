from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import DOUBLE, DATETIME, VARCHAR, DECIMAL, INTEGER
from sqlalchemy.orm import relationship


from db_adapter.base import CurwSimBase


class Run(CurwSimBase):
    __tablename__ = 'run'

    id = Column(VARCHAR(64), nullable=False, primary_key=True, unique=True)
    latitude = Column(DOUBLE, nullable=False)
    longitude = Column(DOUBLE, nullable=False)
    model = Column(VARCHAR(25), nullable=False)
    method = Column(VARCHAR(100), nullable=False)
    grid_id = Column(INTEGER(11), nullable=False)

    data_relationship = relationship("Data", back_populates="id_relationship", cascade="all, delete, delete-orphan")

    grid_relationship = relationship('Grid_Map', foreign_keys='Run.grid_id', back_populates="run_relationship")

    def __repr__(self):
        return "<Run_Mean(id='%s', latitude='%s', longitude='%s', model='%s', method='%s')>" \
               % (self.id, self.latitude, self.longitude, self.model, self.method)

#
# class Run_Max(CurwSimBase):
#     __tablename__ = 'run_max'
#
#     id = Column(VARCHAR(64), nullable=False, primary_key=True, unique=True)
#     latitude = Column(DOUBLE, nullable=False)
#     longitude = Column(DOUBLE, nullable=False)
#     model = Column(VARCHAR(25), nullable=False)
#     method = Column(VARCHAR(100), nullable=False)
#     grid_id = Column(INTEGER(11), nullable=False)
#
#     data_relationship = relationship("Data", back_populates="max_id_relationship", cascade="all, delete, delete-orphan")
#
#     min_grid_relationship = relationship('Grid_Map', foreign_keys='Run_Max.grid_id', back_populates="run_max_relationship")
#
#     def __repr__(self):
#         return "<Run_Max(id='%s', latitude='%s', longitude='%s', model='%s', method='%s')>" \
#                % (self.id, self.latitude, self.longitude, self.model, self.method)
#
#
# class Run_Min(CurwSimBase):
#     __tablename__ = 'run_min'
#
#     id = Column(VARCHAR(64), nullable=False, primary_key=True, unique=True)
#     latitude = Column(DOUBLE, nullable=False)
#     longitude = Column(DOUBLE, nullable=False)
#     model = Column(VARCHAR(25), nullable=False)
#     method = Column(VARCHAR(100), nullable=False)
#     grid_id = Column(INTEGER(11), nullable=False)
#
#     data_relationship = relationship("Data", back_populates="min_id_relationship", cascade="all, delete, delete-orphan")
#
#     max_grid_relationship = relationship('Grid_Map', foreign_keys='Run_Min.grid_id', back_populates="run_min_relationship")
#
#     def __repr__(self):
#         return "<Run_Min(id='%s', latitude='%s', longitude='%s', model='%s', method='%s')>" \
#                % (self.id, self.latitude, self.longitude, self.model, self.method)


class Data(CurwSimBase):
    __tablename__='data'

    id = Column(VARCHAR(64), ForeignKey(Run.id), nullable=False, primary_key=True)
    time = Column(DATETIME, nullable=False, primary_key=True)
    value = Column(DECIMAL(8, 3), nullable=False)

    id_relationship = relationship('Run', foreign_keys='Data.id', back_populates="data_relationship")
    # max_id_relationship = relationship('Run_Max', foreign_keys='Data.id', back_populates="data_relationship")
    # min_id_relationship = relationship('Run_Min', foreign_keys='Data.id', back_populates="data_relationship")

    def __repr__(self):
        return "<Date(id='%s', time='%s', value='%r')>" \
               % (self.id, self.time, self.value)


# for flo2d
class Grid_Map(CurwSimBase):
    __tablename__ = 'grid_map'

    grid_id = Column(VARCHAR(45), nullable=False, primary_key=True, autoincrement=False)
    obs1 = Column(INTEGER(11), nullable=False)
    obs2 = Column(INTEGER(11), nullable=False)
    obs3 = Column(INTEGER(11), nullable=False)
    fcst = Column(INTEGER(11), nullable=False)

    run_relationship = relationship("Run", back_populates="grid_relationship", cascade="all, delete, delete-orphan")
    # run_max_relationship = relationship("Run_Max", back_populates="max_grid_relationship", cascade="all, delete, delete-orphan")
    # run_min_relationship = relationship("Run_Min", back_populates="min_grid_relationship", cascade="all, delete, delete-orphan")

    def __repr__(self):
        return "<Grid_Map(grid_id='%s', obs1='%d', obs2='%d', obs3='%d', fcst='%d')>" \
               % (self.grid_id, self.obs1, self.obs2, self.obs3, self.fcst)
