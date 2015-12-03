import unittest
import os
import boto3
import settings
from moto import mock_s3
from unittest.mock import patch
from ingester import Ingester
from tasks import do_ingest
from utils.s3 import filename_to_s3_key
from utils.fits import fits_to_dict


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/coj1m011-kb05-20150219-0125-e90.fits'
)


@mock_s3
def create_bucket():
    test_bucket = 'testbucket'
    settings.BUCKET = test_bucket
    s3 = boto3.resource('s3')
    s3.create_bucket(Bucket=test_bucket)
    bucket_versioning = s3.BucketVersioning(test_bucket)
    bucket_versioning.enable()
    return test_bucket


@mock_s3
@patch('tasks.collect_queue_length_metric')
class TestCelery(unittest.TestCase):
    def setUp(self):
        create_bucket()
        settings.CELERY_ALWAYS_EAGER = True

    def test_task_success(self, mock):
        self.assertTrue(do_ingest.delay(FITS_PATH).successful())

    def test_task_failure(self, mock):
        result = do_ingest.delay('/pathdoesnot/exit.fits')
        self.assertIs(result.result.__class__, FileNotFoundError)
        self.assertTrue(result.failed())


@mock_s3
class TestS3(unittest.TestCase):
    def setUp(self):
        test_bucket_name = create_bucket()
        s3 = boto3.resource('s3')
        self.bucket = s3.Bucket(test_bucket_name)

    def get_bucket_keys(self):
        return [k.key for k in self.bucket.objects.all()]

    def test_ingest_good_file(self):
        ingester = Ingester(FITS_PATH)
        ingester.ingest()
        self.assertIn(filename_to_s3_key(FITS_PATH), self.get_bucket_keys())

    def test_ingest_missing_file(self):
        badpath = '/doesnot/exist.fits'
        ingester = Ingester(badpath)
        with self.assertRaises(FileNotFoundError):
            ingester.ingest()
        self.assertNotIn(filename_to_s3_key(badpath), self.get_bucket_keys())


class TestUtils(unittest.TestCase):
    def test_fits_to_dict(self):
        result = fits_to_dict(FITS_PATH, settings.HEADER_BLACKLIST)
        for header in settings.HEADER_BLACKLIST:
            self.assertNotIn(
                header,
                result.keys()
            ),
        self.assertEqual(
            'coj1m011-kb05-20150219-0125-e00.fits',
            result['ORIGNAME']
        )
