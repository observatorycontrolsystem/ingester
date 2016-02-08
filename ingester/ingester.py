import boto3
import requests
from ingester.utils.s3 import (basename_to_s3_key, strip_quotes_from_etag,
                               extension_to_content_type, get_md5)
from ingester.exceptions import DoNotRetryError, BackoffRetryError, NonFatalDoNotRetryError
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
from ingester.utils.fits import (fits_to_dict, wcs_corners_from_dict, normalize_null_values,
                                 get_basename_and_extension, get_meta_file_from_targz,
                                 add_required_headers, normalize_related)


class Ingester(object):
    """ Ingester - ingest a single file into the archive
    A single instance of this class is responsbile for parsing a fits file,
    uploading the data to s3, and making a call to the archive api.
    """

    def __init__(self, path, bucket, api_root, auth_token, required_headers=None, blacklist_headers=None):
        self.path = path
        self.bucket = bucket
        self.api_root = api_root
        self.auth_token = auth_token
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        self.basename, self.extension = get_basename_and_extension(self.path)
        try:
            f = open(self.path, 'rb')
        except FileNotFoundError as exc:
            raise DoNotRetryError(exc)
        with f:
            self.md5 = get_md5(f)
            self.check_for_existing_version()
            f.seek(0)
            fits_dict = self.get_fits_dictionary(f)
            f.seek(0)
            version = self.upload_to_s3(f)
        area = wcs_corners_from_dict(fits_dict)
        self.call_api(fits_dict, version, area)

    def check_for_existing_version(self):
        response = requests.get(
            '{0}versions/?md5={1}'.format(self.api_root, self.md5),
            headers={'Authorization': 'Token {}'.format(self.auth_token)}
        ).json()
        if response['count'] > 0:
            raise NonFatalDoNotRetryError('Version with this md5 already exists')

    def get_fits_dictionary(self, f):
        if self.extension == '.tar.gz':
            f = get_meta_file_from_targz(f)
        fits_dict = fits_to_dict(f, self.required_headers, self.blacklist_headers)
        fits_dict = add_required_headers(self.basename, self.extension, fits_dict)
        fits_dict = normalize_related(fits_dict)
        fits_dict = normalize_null_values(fits_dict)
        return fits_dict

    def upload_to_s3(self, f):
        key = basename_to_s3_key(self.basename)
        content_disposition = 'attachment; filename={0}{1}'.format(self.basename, self.extension)
        content_type = extension_to_content_type(self.extension)
        try:
            s3 = boto3.resource('s3')
            response = s3.Object(self.bucket, key).put(
                Body=f,
                ContentDisposition=content_disposition,
                ContentType=content_type
            )
        except (ConnectionError, EndpointConnectionError, ConnectionClosedError) as exc:
            raise BackoffRetryError(exc)
        s3_md5 = strip_quotes_from_etag(response['ETag'])
        if s3_md5 != self.md5:
            raise BackoffRetryError('S3 md5 did not match ours')
        key = response['VersionId']
        return {'key': key, 'md5':  self.md5, 'extension': self.extension}

    def call_api(self, fits_dict, version, area):
        fits_dict['version_set'] = [version]
        fits_dict['basename'] = self.basename
        fits_dict['area'] = area
        try:
            requests.post(
                self.api_root + 'frames/',
                json=fits_dict,
                headers={'Authorization': 'Token {}'.format(self.auth_token)}
            ).raise_for_status()
        except requests.exceptions.ConnectionError as exc:
            raise BackoffRetryError(exc)
        except requests.exceptions.HTTPError as exc:
            raise DoNotRetryError(exc)
