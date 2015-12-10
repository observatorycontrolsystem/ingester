import os
import boto3
import requests
import logging
from ingester.utils.s3 import filename_to_s3_key
from ingester.utils.fits import fits_to_dict, remove_headers, missing_keys
from ingester.exceptions import DoNotRetryError, BackoffRetryError
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError


logger = logging.getLogger('ingester')


class Ingester(object):
    def __init__(self, path, bucket, api_root, required_headers=None, blacklist_headers=None):
        self.path = path
        self.bucket = bucket
        self.api_root = api_root
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        logger.info('ingesting {0}'.format(self.path))
        filename = os.path.basename(self.path)
        try:
            with open(self.path, 'rb') as f:
                fits_dict = self.get_fits_dictionary(f)
                f.seek(0)  # return to beginning of file
                version = self.upload_to_s3(filename, f)
                self.call_api(fits_dict, version)
        except FileNotFoundError as exc:
            raise DoNotRetryError(exc)
        logger.info('finished ingesting {0} version {1}'.format(self.path, version))

    def get_fits_dictionary(self, f):
        fits_dict = fits_to_dict(f)
        fits_dict = remove_headers(fits_dict, self.blacklist_headers)
        missing_headers = missing_keys(fits_dict, self.required_headers)
        if missing_headers:
            raise DoNotRetryError('Fits file missing headers! {0}'.format(missing_headers))
        return fits_dict

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
        return response['VersionId']

    def call_api(self, fits_dict, version):
        requests.post(self.api_root, data={'fits': fits_dict, 'version': version})
