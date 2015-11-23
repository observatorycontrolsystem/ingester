import json
import logging
from logging.config import dictConfig
from time import sleep
import random

try:
    config = json.loads(open('log_conf.json').read())
    dictConfig(config)
except:
    logging.basicConfig()
logger = logging.getLogger('ingester')

HEADER_BLACKLIST = ['HISTORY', '']


class Ingester(object):
    def __init__(self, api_root, s3_bucket):
        self.api_root = api_root
        self.s3_bucket = s3_bucket

    def ingest(self, path):
        logger.info('ingesting {0}'.format(path))
        sleep(random.randint(2, 20))
        logger.info('task {} complete'.format(path))
