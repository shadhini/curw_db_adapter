from db_adapter.base import get_engine, CurwSimBase
from db_adapter.constants import (
    CURW_SIM_USERNAME, CURW_SIM_PASSWORD, CURW_SIM_HOST, CURW_SIM_PORT,
    CURW_SIM_DATABASE,
    )
from db_adapter.constants import DIALECT_MYSQL, DRIVER_PYMYSQL
from db_adapter.curw_sim.models import Run, Run_Max, Run_Min, Data, Grid_Map

from db_adapter.logger import logger


def create_curw_sim_db():

    logger.info("Creating curw_sim schema.")
    # connect to the MySQL engine

    engine = get_engine(DIALECT_MYSQL, DRIVER_PYMYSQL, CURW_SIM_HOST, CURW_SIM_PORT, CURW_SIM_DATABASE,
            CURW_SIM_USERNAME, CURW_SIM_PASSWORD)

    # create the schema using classes defined
    CurwSimBase.metadata.create_all(engine)

    logger.info("curw_sim schema generated.")


