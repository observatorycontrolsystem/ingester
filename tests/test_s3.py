import unittest
import os
import boto3
import settings
from moto import mock_s3
from ingester import Ingester
from utils.s3 import filename_to_s3_key


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/coj1m011-kb05-20150219-0125-e90.fits'
)

test_bucket = 'testbucket'


@mock_s3
class TestS3(unittest.TestCase):
    def setUp(self):
        settings.BUCKET = test_bucket
        s3 = boto3.resource('s3')
        s3.create_bucket(Bucket=test_bucket)
        bucket_versioning = s3.BucketVersioning(test_bucket)
        bucket_versioning.enable()
        self.bucket = s3.Bucket(test_bucket)

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
