import logging
import logging.config
import yaml
import os

this_dir, this_filename = os.path.split(__file__)
logger_config_file_path = os.path.join(this_dir, "logger_config.yaml")


with open(logger_config_file_path, 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

