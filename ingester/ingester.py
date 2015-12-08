import os
from .utils.s3 import filename_to_s3_key
from .utils.fits import fits_to_dict, remove_headers, missing_keys
from .exceptions import DoNotRetryError, BackoffRetryError
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
import boto3
import logging


logger = logging.getLogger('ingester')


class Ingester(object):
    def __init__(self, path, bucket, required_headers=[], blacklist_headers=[]):
        self.path = path
        self.bucket = bucket
        self.required_headers = required_headers
        self.blacklist_headers = blacklist_headers

    def ingest(self):
        logger.info('ingesting {0}'.format(self.path))
        filename = os.path.basename(self.path)
        try:
            with open(self.path, 'rb') as f:
                fits_dict = fits_to_dict(self.path)
                fits_dict = remove_headers(fits_dict, self.blacklist_headers)
                missing_headers = missing_keys(fits_dict, self.required_headers)
                if missing_headers:
                    raise DoNotRetryError('Fits file missing headers! {0}'.format(missing_headers))
                f.seek(0)  # astropy has read the file, so rewind before giving it to s3
                key, version = self.upload_to_s3(filename, f)
        except FileNotFoundError as exc:
            raise DoNotRetryError(exc)
        logger.info('finished ingesting {0} version {1}'.format(key, version))

    def upload_to_s3(self, filename, f):
        key = filename_to_s3_key(filename)
        content_disposition = 'attachment; filename={}'.format(filename)
        content_type = 'image/fits'
        try:
            s3 = boto3.resource('s3')
            response = s3.Object(self.bucket, key).put(
                Body=f,
                ContentDisposition=content_disposition,
                ContentType=content_type
            )
        except (ConnectionError, EndpointConnectionError, ConnectionClosedError) as exc:
            raise BackoffRetryError(exc)
        return key, response['VersionId']
