import boto3
import requests
from io import BytesIO
from ingester.utils.s3 import (basename_to_s3_key, strip_quotes_from_etag,
                               extension_to_content_type, get_md5)
from ingester.exceptions import BackoffRetryError, RetryError
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError, ClientError
from ingester.utils.fits import (fits_to_dict, wcs_corners_from_dict, normalize_null_values,
                                 get_basename_and_extension, get_meta_file_from_targz,
                                 add_required_headers, normalize_related)


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
        try:
            f = self.get_image_data()
        except (FileNotFoundError, ClientError) as exc:
            raise RetryError(exc)

        with f:
            self.md5 = get_md5(f)
            self.archive_service.check_for_existing_version(self.md5)
            f.seek(0)
            fits_dict = self.get_fits_dictionary(f)
            f.seek(0)
            version = self.upload_to_s3(f)

        area = wcs_corners_from_dict(fits_dict)
        fits_dict['version_set'] = [version]
        fits_dict['basename'] = self.basename
        fits_dict['area'] = area
        self.archive_service.post_frame(fits_dict)

    def get_image_data(self):
        protocal_preface = 's3://'
        if self.path.startswith(protocal_preface):
            s3 = boto3.resource('s3')
            plist = self.path[len(protocal_preface):].split('/')
            bucket = plist[0]
            key = '/'.join(plist[1:])
            o = s3.Object(key=key, bucket_name=bucket)
            return BytesIO(o.get()['Body'].read())
        else:
            return open(self.path, 'rb')

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
        except (requests.exceptions.ConnectionError,
                EndpointConnectionError, ConnectionClosedError) as exc:
            raise BackoffRetryError(exc)
        s3_md5 = strip_quotes_from_etag(response['ETag'])
        if s3_md5 != self.md5:
            raise BackoffRetryError('S3 md5 did not match ours')
        key = response['VersionId']
        return {'key': key, 'md5':  self.md5, 'extension': self.extension}
