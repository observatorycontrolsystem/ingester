import hashlib
import logging
from io import BytesIO
from datetime import datetime

from lco_ingester.utils.fits import get_storage_class
from opentsdb_python_metrics.metric_wrappers import metric_timer, SendMetricMixin
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError

import requests
import boto3

from lco_ingester.exceptions import BackoffRetryError

logger = logging.getLogger('lco_ingester')


class S3Service(SendMetricMixin):
    def __init__(self, bucket):
        self.bucket = bucket

    def file_to_s3_key(self, file, fits_dict):
        site = fits_dict.get('SITEID')
        instrument = fits_dict.get('INSTRUME')
        date_obs = fits_dict.get('DATE-OBS')
        if site.lower() == 'sor':
            # SOR files don't have the day_obs in their filename, so use the DATE_OBS field:
            day_obs = date_obs.split('T')[0].replace('-', '')
        else:
            day_obs = file.basename.split('-')[2]
        return '/'.join((site, instrument, day_obs, file.basename)) + file.extension

    def extension_to_content_type(self, extension):
        content_types = {
            '.fits': 'image/fits',
            '.tar.gz': 'application/x-tar',
        }
        return content_types.get(extension, '')

    def strip_quotes_from_etag(self, etag):
        """
        Amazon returns the md5 sum of the uploaded
        file in the 'ETag' header wrapped in quotes
        """
        if etag.startswith('"') and etag.endswith('"'):
            return etag[1:-1]

    @metric_timer('ingester.upload_file')
    def upload_file(self, file, fits_dict):
        storage_class = get_storage_class(fits_dict)
        start_time = datetime.utcnow()
        s3 = boto3.resource('s3')
        key = self.file_to_s3_key(file, fits_dict)
        content_disposition = 'attachment; filename={0}{1}'.format(file.basename, file.extension)
        content_type = self.extension_to_content_type(file.extension)
        try:
            response = s3.Object(self.bucket, key).put(
                Body=file.get_from_start(),
                ContentDisposition=content_disposition,
                ContentType=content_type,
                StorageClass=storage_class,
            )
        except (requests.exceptions.ConnectionError,
                EndpointConnectionError, ConnectionClosedError) as exc:
            raise BackoffRetryError(exc)
        s3_md5 = self.strip_quotes_from_etag(response['ETag'])
        key = response['VersionId']
        logger.info('Ingester uploaded file to s3', extra={
            'tags': {
                'filename': '{}{}'.format(file.basename, file.extension),
                'key': key,
                'storage_class': storage_class,
            }
        })
        # Record metric for the bytes transferred / time to upload
        upload_time = datetime.utcnow() - start_time
        bytes_per_second = len(file) / upload_time.total_seconds()
        self.send_metric('ingester.s3_upload_bytes_per_second', bytes_per_second)
        # TODO: Remove 'migrated': True from the return dict when the s3 migration is complete
        return {'key': key, 'md5': s3_md5, 'extension': file.extension, 'migrated': True}

    @staticmethod
    def get_file(path):
        """
        Get a file from s3.

        Path is of the pattern `s3://{bucket}/{s3_key}`

        :param path: Path to file in s3
        :return: File-like object
        """
        s3 = boto3.resource('s3')
        protocol_preface = 's3://'
        plist = path[len(protocol_preface):].split('/')
        bucket = plist[0]
        key = '/'.join(plist[1:])
        o = s3.Object(key=key, bucket_name=bucket).get()
        filename = o['ContentDisposition'].split('filename=')[1]
        fileobj = BytesIO(o['Body'].read())
        fileobj.name = filename
        return fileobj
