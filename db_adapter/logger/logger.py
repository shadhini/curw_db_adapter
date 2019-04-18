import logging
import logging.config
import yaml
import json

db_config = json.loads(open('db_config.json').read())

with open('{}/db_adapter/logger/config.yaml'.format(db_config["HOME"]), 'r') as f:
    config = yaml.safe_load(f.read())
    logging.config.dictConfig(config)

logger = logging.getLogger(__name__)

