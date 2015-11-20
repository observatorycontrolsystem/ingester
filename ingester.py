import json
import logging
from logging.config import dictConfig
from astropy.io import fits

try:
    config = json.loads(open('log_conf.json').read())
    dictConfig(config)
except:
    logging.basicConfig()
logger = logging.getLogger('ingester')


class Ingester(object):
    def __init__(self, api_root, s3_bucket):
        self.api_root = api_root
        self.s3_bucket = s3_bucket

    def ingest(self, path):
        logger.info('ingesting {0}'.format(path))

    @classmethod
    def fits_to_dict(clazz, path):
        hdulist = fits.open(path)
        return dict(hdulist[0].header)
