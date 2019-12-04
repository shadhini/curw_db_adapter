from db_adapter.logger import logger
import json

DB_CONFIG_FILE_PATH = 'db_adapter_config.json'

DIALECT_MYSQL = "mysql"
DRIVER_PYMYSQL = "pymysql"

# ----------- Test Database -----------------
USERNAME = "root"
PASSWORD = "password"
HOST = "127.0.0.1"
PORT = 3306
DATABASE = "test_schema"

# ---------- CUrW Fcst Database --------------FileNotFoundError
CURW_FCST_USERNAME = ''
CURW_FCST_PASSWORD = ''
CURW_FCST_HOST = ''
CURW_FCST_PORT = ''
CURW_FCST_DATABASE = ''

# ---------- CUrW Obs Database --------------
CURW_OBS_USERNAME = ''
CURW_OBS_PASSWORD = ''
CURW_OBS_HOST = ''
CURW_OBS_PORT = ''
CURW_OBS_DATABASE = ''

# ---------- CUrW Obs Database --------------
CURW_SIM_USERNAME = ''
CURW_SIM_PASSWORD = ''
CURW_SIM_HOST = ''
CURW_SIM_PORT = ''
CURW_SIM_DATABASE = ''


def set_variables():

    global CURW_FCST_USERNAME
    global CURW_FCST_PASSWORD
    global CURW_FCST_HOST
    global CURW_FCST_PORT
    global CURW_FCST_DATABASE

    global CURW_OBS_USERNAME
    global CURW_OBS_PASSWORD
    global CURW_OBS_HOST
    global CURW_OBS_PORT
    global CURW_OBS_DATABASE

    global CURW_SIM_USERNAME
    global CURW_SIM_PASSWORD
    global CURW_SIM_HOST
    global CURW_SIM_PORT
    global CURW_SIM_DATABASE

    config = json.loads(open(DB_CONFIG_FILE_PATH).read())

    # ---------- CUrW Fcst Database --------------FileNotFoundError
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


def set_db_config_file_path(db_config_file_path):
    global DB_CONFIG_FILE_PATH

    DB_CONFIG_FILE_PATH = db_config_file_path

    set_variables()


try:
    set_variables()

except FileNotFoundError:
    logger.warning("db_adapter_config.json file does not exists in the calling directory path !!!")


