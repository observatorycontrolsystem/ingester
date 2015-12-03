import logging
from logging.config import dictConfig
import json


def getLogger():
    try:
        config = json.loads(open('log_conf.json').read())
        dictConfig(config)
    except:
        logging.basicConfig()
        logging.warn('Falling back to basic logger')
    return logging.getLogger('ingester')
