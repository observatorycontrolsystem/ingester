import os
import hashlib
import logging
from io import BytesIO
from datetime import datetime

from opentsdb_python_metrics.metric_wrappers import metric_timer, SendMetricMixin
from botocore.exceptions import EndpointConnectionError, ConnectionClosedError
import requests
import boto3

from lco_ingester.utils.fits import get_basename_and_extension, reset_file
from lco_ingester.exceptions import BackoffRetryError

logger = logging.getLogger('lco_ingester')


class S3Service(SendMetricMixin):
    def __init__(self, bucket):
        self.bucket = bucket

    def basename_to_s3_key(self, basename):
        return '/'.join((hashlib.sha1(basename.encode('utf-8')).hexdigest()[0:4], basename))

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
    def upload_file(self, fileobj, path, storage_class):
        start_time = datetime.utcnow()
        s3 = boto3.resource('s3')
        basename, extension = get_basename_and_extension(path)
        key = self.basename_to_s3_key(basename)
        content_disposition = 'attachment; filename={0}{1}'.format(basename, extension)
        content_type = self.extension_to_content_type(extension)
        try:
            # boto reads the fileobj but does not reset the position
            with reset_file(fileobj):
                response = s3.Object(self.bucket, key).put(
                    Body=fileobj,
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
                'filename': '{}{}'.format(basename, extension),
                'key': key,
                'storage_class': storage_class,
            }
        })
        # Record metric for the bytes transfered / time to upload
        upload_time = datetime.utcnow() - start_time
        fileobj.seek(0, os.SEEK_END)
        bytes_per_second = fileobj.tell() / upload_time.total_seconds()
        fileobj.seek(0)
        self.send_metric('lco_ingester.s3_upload_bytes_per_second', bytes_per_second)
        return {'key': key, 'md5': s3_md5, 'extension': extension}

    def get_file(self, path):
        s3 = boto3.resource('s3')
        protocol_preface = 's3://'
        plist = path[len(protocol_preface):].split('/')
        bucket = plist[0]
        key = '/'.join(plist[1:])
        o = s3.Object(key=key, bucket_name=bucket)
        return BytesIO(o.get()['Body'].read())
