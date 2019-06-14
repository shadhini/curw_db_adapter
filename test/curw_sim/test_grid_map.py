from db_adapter.base import get_Pool, destroy_Pool
from db_adapter.curw_sim.grids import add_flo2d_grid_mappings, add_obs_to_d03_grid_mappings_for_rainfall

from db_adapter.logger import logger


USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"

print(" Add flo2d grid mappings")

pool = get_Pool(host=HOST, port=PORT, user=USERNAME, password=PASSWORD, db=DATABASE)


# add_flo2d_grid_mappings(pool=pool, flo2d_model='flo2d_250')
#
# flo2d_grid_mapping = get_flo2d_grid_mappings(pool=pool, flo2d_model='flo2d_250')
#
# print('flo2d_250_5693', flo2d_grid_mapping['flo2d_250_5693'])


print("add obs grids to grid maps")


destroy_Pool(pool)
