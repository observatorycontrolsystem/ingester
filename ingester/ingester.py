import os
from .utils.s3 import get_client, filename_to_s3_key
import logging

logger = logging.getLogger('ingester')


class Ingester(object):
    def __init__(self, path, access_key, secret_key, region, bucket):
        self.path = path
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
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
        client = get_client(
            self.access_key,
            self.secret_key,
            self.region
        )
        response = client.put_object(
            Body=data,
            Key=key,
            Bucket=self.bucket,
            ContentDisposition=content_disposition,
            ContentType=content_type
        )
        return key, response['VersionId']
