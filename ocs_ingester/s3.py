import logging
from io import BytesIO
from datetime import datetime

from opentsdb_python_metrics.metric_wrappers import SendMetricMixin
from ocs_ingester.utils.fits import get_storage_class, get_dayobs

from botocore.exceptions import EndpointConnectionError, ConnectionClosedError

import requests
import boto3

from ocs_ingester.exceptions import BackoffRetryError
from ocs_ingester.utils import metrics
from ocs_ingester.settings import settings

logger = logging.getLogger('ocs_ingester')


class S3Service(SendMetricMixin):
    def __init__(self, bucket):
        self.bucket = bucket

    @staticmethod
    def is_bpm_file(filename, fits_dict):
        ''' Checks if file is a bad pixel mask using several rules for various naming schemes '''
        if fits_dict.get('OBSTYPE') == 'BPM' or fits_dict.get('EXTNAME') == 'BPM':
            return True
        filename = filename.replace('_', '-')
        if filename.startswith('bpm-') or '-bpm-' in filename or filename.endswith('-bpm'):
            return True
        return False

    def file_to_s3_key(self, file, fits_dict):
        ''' Creates s3 path name based on the filename and certain fits headers '''
        site = fits_dict.get('SITEID')
        instrument = fits_dict.get('INSTRUME')
        day_obs = get_dayobs(fits_dict)
        data_type = 'raw' if fits_dict.get('RLEVEL', 0) == 0 else 'processed'
        if self.is_bpm_file(file.basename, fits_dict):
            # Files with bpm in the name, or BPM OBSTYPE or EXTNAME headers are placed in the instrument/bpm/ dir
            return '/'.join((site, instrument, 'bpm', file.basename)) + file.extension
        else:
            # All other files go in instrument/daydir/datatype/ dir
            return '/'.join((site, instrument, day_obs, data_type, file.basename)) + file.extension

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

    @metrics.method_timer('ingester.upload_file')
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
        self.send_metric(
            metric_name='ingester.s3_upload_bytes_per_second',
            value=bytes_per_second,
            asynchronous=settings.SUBMIT_METRICS_ASYNCHRONOUSLY,
            **settings.EXTRA_METRICS_TAGS
        )
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
