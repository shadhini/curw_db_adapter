from db_adapter.logger import logger
import json


def read_attribute_from_config_file(attribute, config):
    """
    :param attribute: key name of the config json file
    :param config: loaded json file
    :return:
    """
    if attribute in config and (config[attribute]!=""):
        return config[attribute]
    else:
        logger.error("{} not specified in config file.".format(attribute))
        exit(1)


config = json.loads(open('db_adapter_config.json').read())

DIALECT_MYSQL = "mysql"
DRIVER_PYMYSQL = "pymysql"

# ---------- CUrW Fcst Database --------------
CURW_FCST_USERNAME = read_attribute_from_config_file('CURW_FCST_USERNAME', config)
CURW_FCST_PASSWORD = read_attribute_from_config_file('CURW_FCST_PASSWORD', config)
CURW_FCST_HOST = read_attribute_from_config_file('CURW_FCST_HOST', config)
CURW_FCST_PORT = read_attribute_from_config_file('CURW_FCST_PORT', config)
CURW_FCST_DATABASE = read_attribute_from_config_file('CURW_FCST_DATABASE', config)

# ---------- CUrW Obs Database --------------
CURW_OBS_USERNAME = read_attribute_from_config_file('CURW_OBS_USERNAME', config)
CURW_OBS_PASSWORD = read_attribute_from_config_file('CURW_OBS_PASSWORD', config)
CURW_OBS_HOST = read_attribute_from_config_file('CURW_OBS_HOST', config)
CURW_OBS_PORT = read_attribute_from_config_file('CURW_OBS_PORT', config)
CURW_OBS_DATABASE = read_attribute_from_config_file('CURW_OBS_DATABASE', config)

# ---------- CUrW Obs Database --------------
CURW_SIM_USERNAME = read_attribute_from_config_file('CURW_SIM_USERNAME', config)
CURW_SIM_PASSWORD = read_attribute_from_config_file('CURW_SIM_PASSWORD', config)
CURW_SIM_HOST = read_attribute_from_config_file('CURW_SIM_HOST', config)
CURW_SIM_PORT = read_attribute_from_config_file('CURW_SIM_PORT', config)
CURW_SIM_DATABASE = read_attribute_from_config_file('CURW_SIM_DATABASE', config)

# ----------- Test Database -----------------
USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"