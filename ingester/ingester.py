import os
from .utils.s3 import filename_to_s3_key
import boto3
import logging

logger = logging.getLogger('ingester')


class Ingester(object):
    def __init__(self, path, bucket):
        self.path = path
        self.bucket = bucket

    def ingest(self):
        logger.info('ingesting {0}'.format(self.path))
        filename = os.path.basename(self.path)
        with open(self.path, 'rb') as f:
            data = f.read()
        key, version = self.upload_to_s3(filename, data)

        logger.info('finished ingesting {0} version {1}'.format(key, version))

    def upload_to_s3(self, filename, data):
        key = filename_to_s3_key(filename)
        content_disposition = 'attachment; filename={}'.format(filename)
        content_type = 'image/fits'
        s3 = boto3.resource('s3')
        response = s3.Object(self.bucket, key).put(
            Body=data,
            ContentDisposition=content_disposition,
            ContentType=content_type
        )
        return key, response['VersionId']
