from unittest.mock import patch, MagicMock
import opentsdb_python_metrics.metric_wrappers
import unittest
import os
import tarfile
import dateutil
import hashlib

from ingester.ingester import Ingester
from ingester.exceptions import DoNotRetryError, RetryError
import settings

opentsdb_python_metrics.metric_wrappers.test_mode = True


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/'
)
FITS_FILE = os.path.join(
    FITS_PATH,
    'coj1m011-kb05-20150219-0125-e90.fits.fz'
)
CAT_FILE = os.path.join(
    FITS_PATH,
    'cpt1m010-kb70-20151219-0073-e10_cat.fits.fz'
)
SPECTRO_FILE = os.path.join(
    FITS_PATH,
    'KEY2014A-002_0000483537_ftn_20160119_57407.tar.gz'
)


def mocked_s3_object(*args, **kwargs):
    class MockS3Object:
        class Object:
            def __init__(self, *args, **kwargs):
                pass

            def put(self, *args, **kwargs):
                return {'ETag': '"fakemd5"', 'VersionId': 'fakeversion'}

            def get(self, *args, **kwargs):
                return {'Body': open(FITS_FILE, 'rb')}

    return MockS3Object()


def mock_hashlib_md5(*args, **kwargs):
    class MockHash:
        def __init__(self):
            pass

        def hexdigest(self):
            return 'fakemd5'

    return MockHash()


@patch('boto3.resource', side_effect=mocked_s3_object)
class TestIngester(unittest.TestCase):
    def setUp(self):
        hashlib.md5 = MagicMock(side_effect=mock_hashlib_md5)
        fits_files = [os.path.join(FITS_PATH, f) for f in os.listdir(FITS_PATH)]
        self.archive_mock = MagicMock()
        self.ingesters = [
            Ingester(
                path, 'testbucket', archive_service=self.archive_mock,
                required_headers=settings.REQUIRED_HEADERS,
                blacklist_headers=settings.HEADER_BLACKLIST,
                )
            for path in fits_files
        ]

    def create_ingester_for_path(self, path=FITS_FILE):
        ingester = Ingester(
            path,
            'test_bucket',
            archive_service=self.archive_mock,
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        return ingester

    def test_ingest_file(self, s3_mock):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertTrue(s3_mock.called)
            self.assertTrue(self.archive_mock.post_frame.called)

    def test_missing_file(self, s3_mock):
        ingester = self.create_ingester_for_path('/path/doesnot/exist.fits.fz')
        with self.assertRaises(RetryError):
            ingester.ingest()
        self.assertFalse(s3_mock.called)
        self.assertFalse(self.archive_mock.post_frame.called)

    def test_required(self, s3_mock):
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            archive_service=self.archive_mock,
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=['fooheader']
        )
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()
        self.assertFalse(s3_mock.called)
        self.assertFalse(self.archive_mock.post_frame.called)

    def test_get_area(self, s3_mock):
        ingester = self.create_ingester_for_path(FITS_FILE)
        ingester.ingest()
        self.assertEqual('Polygon', self.archive_mock.post_frame.call_args[0][0]['area']['type'])
        ingester = self.create_ingester_for_path(CAT_FILE)
        ingester.ingest()
        self.assertIsNone(self.archive_mock.post_frame.call_args[0][0]['area'])

    def test_blacklist(self, s3_mock):
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            archive_service=self.archive_mock,
            blacklist_headers=['DAY-OBS', '', 'COMMENT', 'HISTORY'],
            required_headers=settings.REQUIRED_HEADERS
        )
        ingester.ingest()
        self.assertNotIn('DAY-OBS', self.archive_mock.post_frame.call_args[0][0].keys())

    def test_reduction_level(self, s3_mock):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertIn('RLEVEL', self.archive_mock.post_frame.call_args[0][0].keys())

    def test_related(self, s3_mock):
        ingester = self.create_ingester_for_path(FITS_FILE)
        ingester.ingest()
        self.assertEqual(
            'bias_kb05_20150219_bin2x2',
            self.archive_mock.post_frame.call_args[0][0]['L1IDBIAS']
        )
        self.assertEqual(
            'dark_kb05_20150219_bin2x2',
            self.archive_mock.post_frame.call_args[0][0]['L1IDDARK']
        )
        self.assertEqual(
            'flat_kb05_20150219_SKYFLAT_bin2x2_V',
            self.archive_mock.post_frame.call_args[0][0]['L1IDFLAT']
        )

    def test_catalog_related(self, s3_mock):
        ingester = self.create_ingester_for_path(CAT_FILE)
        ingester.ingest()
        self.assertEqual(
            'cpt1m010-kb70-20151219-0073-e10',
            self.archive_mock.post_frame.call_args[0][0]['L1IDCAT']
        )

    def test_spectograph(self, s3_mock):
        ingester = self.create_ingester_for_path(SPECTRO_FILE)
        ingester.ingest()
        self.assertEqual(90, self.archive_mock.post_frame.call_args[0][0]['RLEVEL'])
        self.assertTrue(dateutil.parser.parse(self.archive_mock.post_frame.call_args[0][0]['L1PUBDAT']))

    def test_spectrograph_missing_meta(self, s3_mock):
        tarfile.TarFile.getnames = MagicMock(return_value=[''])
        ingester = self.create_ingester_for_path(SPECTRO_FILE)
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()

    def test_empty_string_for_na(self, s3_mock):
        ingester = self.create_ingester_for_path(
            os.path.join(FITS_PATH, 'coj1m011-fl08-20151216-0049-b00.fits')
        )
        ingester.ingest()
        self.assertFalse(self.archive_mock.post_frame.call_args[0][0]['OBJECT'])
        self.assertTrue(self.archive_mock.post_frame.call_args[0][0]['DATE-OBS'])

    def test_s3_get(self, s3_mock):
        ingester = self.create_ingester_for_path('s3://testbucket/testfile.fits')
        ingester.ingest()
        self.assertTrue(s3_mock.called)
        self.assertTrue(self.archive_mock.post_frame.called)

    def test_reqnum_null_or_int(self, s3_mock):
        for ingester in self.ingesters:
            ingester.ingest()
            reqnum = self.archive_mock.post_frame.call_args[0][0]['REQNUM']
            try:
                self.assertIsNone(reqnum)
            except AssertionError:
                self.assertGreater(int(reqnum), -1)
