import traceback
from datetime import datetime, timedelta

from db_adapter.curw_sim.constants import FLO2D_250
from db_adapter.curw_sim.grids import GridInterpolationEnum
from db_adapter.curw_sim.timeseries import MethodEnum
from db_adapter.curw_sim.flo2d import update_rainfall_obs


if __name__=="__main__":
    try:
        method = MethodEnum.getAbbreviation(MethodEnum.MME)
        grid_interpolation = GridInterpolationEnum.getAbbreviation(GridInterpolationEnum.MDPA)

        print("{} : ####### Insert obs rainfall for FLO2D 250 grids".format(datetime.now()))
        update_rainfall_obs(flo2d_model=FLO2D_250, method=method, grid_interpolation=grid_interpolation)

    except Exception as e:
        traceback.print_exc()
    finally:
        print("{} : ####### obs rainfall insertion process finished for {} #######".format(datetime.now(), FLO2D_250))
