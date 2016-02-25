import unittest
import os
import tarfile
from unittest.mock import patch, MagicMock
from ingester.ingester import Ingester
from ingester.exceptions import DoNotRetryError, NonFatalDoNotRetryError
import settings
import opentsdb_python_metrics.metric_wrappers
import dateutil
import requests
import hashlib
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


def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data):
            self.json_data = json_data

        def json(self):
            return self.json_data

        def raise_for_status(self):
            return None

    if args[0].startswith('http://return1/'):
        return MockResponse({'count': 1})

    return MockResponse({'count': 0})


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
@patch('requests.post')
class TestIngester(unittest.TestCase):
    def setUp(self):
        hashlib.md5 = MagicMock(side_effect=mock_hashlib_md5)
        requests.get = MagicMock(side_effect=mocked_requests_get)
        fits_files = [os.path.join(FITS_PATH, f) for f in os.listdir(FITS_PATH)]
        self.ingesters = [
            Ingester(
                path, 'testbucket', 'http://testendpoint', auth_token='',
                required_headers=settings.REQUIRED_HEADERS,
                blacklist_headers=settings.HEADER_BLACKLIST,
                )
            for path in fits_files
        ]

    def create_ingester_for_path(self, path=FITS_FILE):
        ingester = Ingester(
            path,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        return ingester

    def test_ingest_file(self, requests_mock, s3_mock):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertTrue(s3_mock.called)
            self.assertTrue(requests_mock.called)

    def test_missing_file(self, requests_mock, s3_mock):
        ingester = self.create_ingester_for_path('/path/doesnot/exist.fits.fz')
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()
        self.assertFalse(s3_mock.called)
        self.assertFalse(requests_mock.called)

    def test_required(self, requests_mock, s3_mock):
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=['fooheader']
        )
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()
        self.assertFalse(s3_mock.called)
        self.assertFalse(requests_mock.called)

    def test_get_area(self, requests_mock, s3_mock):
        ingester = self.create_ingester_for_path(FITS_FILE)
        ingester.ingest()
        self.assertEqual('Polygon', requests_mock.call_args[1]['json']['area']['type'])
        ingester = self.create_ingester_for_path(CAT_FILE)
        ingester.ingest()
        self.assertIsNone(requests_mock.call_args[1]['json']['area'])

    def test_blacklist(self, requests_mock, s3_mock):
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=['DAY-OBS', '', 'COMMENT', 'HISTORY'],
            required_headers=settings.REQUIRED_HEADERS
        )
        ingester.ingest()
        self.assertNotIn('DAY-OBS', requests_mock.call_args[1]['json'].keys())

    def test_reduction_level(self, requests_mock, s3_mock):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertIn('RLEVEL', requests_mock.call_args[1]['json'].keys())

    def test_related(self, requests_mock, s3_mock):
        ingester = self.create_ingester_for_path(FITS_FILE)
        ingester.ingest()
        self.assertEqual(
            'bias_kb05_20150219_bin2x2',
            requests_mock.call_args[1]['json']['L1IDBIAS']
        )
        self.assertEqual(
            'dark_kb05_20150219_bin2x2',
            requests_mock.call_args[1]['json']['L1IDDARK']
        )
        self.assertEqual(
            'flat_kb05_20150219_SKYFLAT_bin2x2_V',
            requests_mock.call_args[1]['json']['L1IDFLAT']
        )

    def test_catalog_related(self, requests_mock, s3_mock):
        ingester = self.create_ingester_for_path(CAT_FILE)
        ingester.ingest()
        self.assertEqual(
            'cpt1m010-kb70-20151219-0073-e10',
            requests_mock.call_args[1]['json']['L1IDCAT']
        )

    def test_spectograph(self, requests_mock, s3_mock):
        ingester = self.create_ingester_for_path(SPECTRO_FILE)
        ingester.ingest()
        self.assertEqual(90, requests_mock.call_args[1]['json']['RLEVEL'])
        self.assertTrue(dateutil.parser.parse(requests_mock.call_args[1]['json']['L1PUBDAT']))

    def test_spectrograph_missing_meta(self, requests_mock, s3_mock):
        tarfile.TarFile.getnames = MagicMock(return_value=[''])
        ingester = self.create_ingester_for_path(SPECTRO_FILE)
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()

    def test_empty_string_for_na(self, requests_mock, s3_mock):
        ingester = self.create_ingester_for_path(
            os.path.join(FITS_PATH, 'coj1m011-fl08-20151216-0049-b00.fits')
        )
        ingester.ingest()
        self.assertFalse(requests_mock.call_args[1]['json']['OBJECT'])
        self.assertTrue(requests_mock.call_args[1]['json']['DATE-OBS'])

    def test_existing_version(self, requests_mock, s3_mock):
        requests.get = MagicMock(side_effect=mocked_requests_get)
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            'http://return1/',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        with self.assertRaises(NonFatalDoNotRetryError):
            ingester.ingest()

    def test_s3_get(self, requests_mock, s3_mock):
        ingester = self.create_ingester_for_path('s3://testbucket/testfile.fits')
        ingester.ingest()
        self.assertTrue(s3_mock.called)
        self.assertTrue(requests_mock.called)
