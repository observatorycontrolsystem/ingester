import boto3
import requests
from ingester.fits import FitsDict
from ingester.utils.s3 import basename_to_s3_key, strip_quotes_from_etag, extension_to_content_type
from ingester.exceptions import BackoffRetryError
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
from ingester.utils.fits import get_basename_and_extension, wcs_corners_from_dict, get_md5, get_fits_from_path


class Ingester(object):
    """ Ingester - ingest a single file into the archive
    A single instance of this class is responsbile for parsing a fits file,
    uploading the data to s3, and making a call to the archive api.
    """

    def __init__(self, path, bucket, archive_service, required_headers=None, blacklist_headers=None):
        self.path = path
        self.bucket = bucket
        self.archive_service = archive_service
        self.required_headers = required_headers if required_headers else []
        self.blacklist_headers = blacklist_headers if blacklist_headers else []

    def ingest(self):
        self.basename, self.extension = get_basename_and_extension(self.path)

        self.md5 = get_md5(self.path)
        self.archive_service.check_for_existing_version(self.md5)
        fits_dict = FitsDict(self.path, self.required_headers, self.blacklist_headers).as_dict()
        version = self.upload_to_s3()
        fits_dict['area'] = wcs_corners_from_dict(fits_dict)
        fits_dict['version_set'] = [version]
        fits_dict['basename'] = self.basename
        self.archive_service.post_frame(fits_dict)

    def upload_to_s3(self):
        key = basename_to_s3_key(self.basename)
        content_disposition = 'attachment; filename={0}{1}'.format(self.basename, self.extension)
        content_type = extension_to_content_type(self.extension)
        try:
            s3 = boto3.resource('s3')
            response = s3.Object(self.bucket, key).put(
                Body=get_fits_from_path(self.path),
                ContentDisposition=content_disposition,
                ContentType=content_type
            )
        except (requests.exceptions.ConnectionError,
                EndpointConnectionError, ConnectionClosedError) as exc:
            raise BackoffRetryError(exc)
        s3_md5 = strip_quotes_from_etag(response['ETag'])
        if s3_md5 != self.md5:
            raise BackoffRetryError('S3 md5 did not match ours')
        key = response['VersionId']
        return {'key': key, 'md5':  self.md5, 'extension': self.extension}
