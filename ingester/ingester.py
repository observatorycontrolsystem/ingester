import os
import boto3
import requests
from ingester.utils.s3 import filename_to_s3_key, strip_quotes_from_etag, filename_to_content_type
from ingester.exceptions import DoNotRetryError, BackoffRetryError
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
from ingester.utils.fits import (fits_to_dict, remove_headers, missing_keys, wcs_corners_from_dict,
                                 get_meta_file_from_targz, add_required_headers, normalize_related)


class Ingester(object):
    def __init__(self, path, bucket, api_root, auth_token, required_headers=None, blacklist_headers=None):
        self.path = path
        self.bucket = bucket
        self.api_root = api_root
        self.auth_token = auth_token
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        self.filename = os.path.basename(self.path)
        try:
            f = open(self.path, 'rb')
        except FileNotFoundError as exc:
            raise DoNotRetryError(exc)
        with f:
            fits_dict = self.get_fits_dictionary(f)
            f.seek(0)
            version = self.upload_to_s3(f)
        area = wcs_corners_from_dict(fits_dict)
        self.call_api(fits_dict, version, area)

    def get_fits_dictionary(self, f):
        if self.filename.endswith('tar.gz'):
            f = get_meta_file_from_targz(f)
        fits_dict = fits_to_dict(f)
        fits_dict = add_required_headers(self.filename, fits_dict)
        fits_dict = remove_headers(fits_dict, self.blacklist_headers)
        fits_dict = normalize_related(fits_dict)
        missing_headers = missing_keys(fits_dict, self.required_headers)
        if missing_headers:
            raise DoNotRetryError('Fits file missing headers! {0}'.format(missing_headers))
        return fits_dict

    def upload_to_s3(self, f):
        key = filename_to_s3_key(self.filename)
        content_disposition = 'attachment; filename={}'.format(self.filename)
        content_type = filename_to_content_type(self.filename)
        try:
            s3 = boto3.resource('s3')
            response = s3.Object(self.bucket, key).put(
                Body=f,
                ContentDisposition=content_disposition,
                ContentType=content_type
            )
        except (ConnectionError, EndpointConnectionError, ConnectionClosedError) as exc:
            raise BackoffRetryError(exc)
        md5 = strip_quotes_from_etag(response['ETag'])
        key = response['VersionId']
        return {'key': key, 'md5':  md5}

    def call_api(self, fits_dict, version, area):
        fits_dict['version_set'] = [version]
        fits_dict['filename'] = self.filename
        fits_dict['area'] = area
        headers = {'Authorization': 'Token {}'.format(self.auth_token)}
        try:
            requests.post(self.api_root, json=fits_dict, headers=headers).raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise BackoffRetryError(exc)
        except requests.exceptions.HTTPError as exc:
            raise DoNotRetryError(exc)
