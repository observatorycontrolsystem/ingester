import os
from unittest.mock import patch
import unittest

from lco_ingester.s3 import S3Service
from lco_ingester.utils.fits import File

FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/'
)
FITS_FILE = os.path.join(
    FITS_PATH,
    'coj1m011-kb05-20150219-0125-e90.fits.fz'
)


def mocked_s3_object(*args, **kwargs):
    class MockS3Object:
        class Object:
            def __init__(self, *args, **kwargs):
                pass

            def put(self, *args, **kwargs):
                return {'ETag': '"fakemd5"', 'VersionId': 'fakeversion'}

            def get(self, *args, **kwargs):
                return {'Body': open(FITS_FILE, 'rb'), 'ContentDisposition': 'attachment: filename=thing.fits.fz'}

    return MockS3Object()


class TestS3(unittest.TestCase):
    def setUp(self):
        self.s3 = S3Service('')

    def test_basename_to_hash(self):
        fits_dict = {'SITEID': 'tst', 'INSTRUME': 'inst01', 'DATE-OBS': '2019-10-11T00:11:22.123'}
        self.assertEqual(
            'tst/inst01/20191011/somefilename-inst01-20191011-0011.fits.fz',
            self.s3.file_to_s3_key('somefilename-inst01-20191011-0011.fits.fz', fits_dict)
        )

    def test_extension_to_content_type(self):
        self.assertEqual('image/fits', self.s3.extension_to_content_type('.fits'))
        self.assertEqual('application/x-tar', self.s3.extension_to_content_type('.tar.gz'))
        self.assertEqual('', self.s3.extension_to_content_type('.png'))

    def test_strip_quotes_from_etag(self):
        self.assertEqual('fakemd5', self.s3.strip_quotes_from_etag('"fakemd5"'))
        self.assertIsNone(self.s3.strip_quotes_from_etag('"wrong'))

    @patch('boto3.resource', side_effect=mocked_s3_object)
    def test_upload_file(self, s3_mock):
        fits_dict = {'SITEID': 'tst', 'INSTRUME': 'inst01', 'DATE-OBS': '2019-10-11T00:11:22.123'}
        with open(FITS_FILE, 'rb') as fileobj:
            self.s3.upload_file(File(fileobj), fits_dict)
        self.assertTrue(s3_mock.called)

    @patch('boto3.resource', side_effect=mocked_s3_object)
    def test_get_file(self, s3_mock):
        fileobj = self.s3.get_file('s3://somebucket/thing')
        self.assertTrue(s3_mock.called)
        self.assertEqual(fileobj.name, 'thing.fits.fz')
