import os
import boto3
import requests
from ingester.utils.s3 import filename_to_s3_key, strip_quotes_from_etag
from ingester.exceptions import DoNotRetryError, BackoffRetryError
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
from ingester.utils.fits import (fits_to_dict, remove_headers, missing_keys,
                                 wcs_corners_from_dict, reduction_level,
                                 related_for_catalog)


class Ingester(object):
    def __init__(self, path, bucket, api_root, required_headers=None, blacklist_headers=None):
        self.path = path
        self.bucket = bucket
        self.api_root = api_root
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        filename = os.path.basename(self.path)
        try:
            f = open(self.path, 'rb')
        except FileNotFoundError as exc:
            raise DoNotRetryError(exc)
        with f:
            fits_dict = self.get_fits_dictionary(f)
            fits_dict = self.add_required_headers(filename, fits_dict)
            area = self.get_area(fits_dict)
            f.seek(0)  # return to beginning of file
            version = self.upload_to_s3(filename, f)
        self.call_api(fits_dict, version, filename, area)

    def get_fits_dictionary(self, f):
        fits_dict = fits_to_dict(f)
        fits_dict = remove_headers(fits_dict, self.blacklist_headers)
        missing_headers = missing_keys(fits_dict, self.required_headers)
        if missing_headers:
            raise DoNotRetryError('Fits file missing headers! {0}'.format(missing_headers))
        return fits_dict

    def add_required_headers(self, filename, fits_dict):
        # TODO: Remove this function entirely. We need these for now
        # because the pipeline does not write them as headers
        if not fits_dict.get('RLEVEL'):
            rlevel = reduction_level(filename)
            fits_dict['RLEVEL'] = rlevel
        if filename.endswith('_cat.fits') and not fits_dict.get('L1IDCAT'):
            l1idcat = related_for_catalog(filename)
            fits_dict['L1IDCAT'] = l1idcat
        return fits_dict

    def get_area(self, fits_dict):
        if fits_dict.get('CD1_1') is None:
            # This file doesn't have wcs information
            return None
        else:
            return wcs_corners_from_dict(fits_dict)

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
        md5 = strip_quotes_from_etag(response['ETag'])
        key = response['VersionId']
        return {'key': key, 'md5':  md5}

    def call_api(self, fits_dict, version, filename, area):
        fits_dict['version_set'] = [version]
        fits_dict['filename'] = filename
        fits_dict['area'] = area
        try:
            requests.post(self.api_root, json=fits_dict).raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise BackoffRetryError(exc)
        except requests.exceptions.HTTPError as exc:
            raise DoNotRetryError(exc)
