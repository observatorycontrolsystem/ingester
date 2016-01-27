import unittest
import os
import tarfile
from unittest.mock import patch, MagicMock
from ingester.ingester import Ingester
from ingester.exceptions import DoNotRetryError
import settings
import opentsdb_python_metrics.metric_wrappers
import dateutil
opentsdb_python_metrics.metric_wrappers.test_mode = True


FITS_PATH = os.path.join(
    os.path.dirname(__file__),
    'fits/'
)
FITS_FILE = os.path.join(
    os.path.dirname(__file__),
    'fits/coj1m011-kb05-20150219-0125-e90.fits.fz'
)
CAT_FILE = os.path.join(
    os.path.dirname(__file__),
    'fits/cpt1m010-kb70-20151219-0073-e10_cat.fits.fz'
)
SPECTRO_FILE = os.path.join(
    os.path.dirname(__file__),
    'fits/KEY2014A-002_0000483537_ftn_20160119_57407.tar.gz'
)


test_bucket = 'testbucket'
blacklist_headers = ['', 'COMMENT', 'HISTORY']


@patch('boto3.resource')
@patch('requests.post')
class TestIngester(unittest.TestCase):
    def setUp(self):
        fits_files = [os.path.join(FITS_PATH, f) for f in os.listdir(FITS_PATH)]
        self.ingesters = [
            Ingester(
                path, 'testbucket', 'http://testendpoint', auth_token='',
                required_headers=settings.REQUIRED_HEADERS,
                blacklist_headers=settings.HEADER_BLACKLIST,
                )
            for path in fits_files
        ]

    def test_ingest_file(self, requests_mock, s3_mock):
        for ingester in self.ingesters:
            ingester.ingest()
            self.assertTrue(s3_mock.called)
            self.assertTrue(requests_mock.called)

    def test_missing_file(self, requests_mock, s3_mock):
        ingester = Ingester(
            '/path/does/not/exist.fits.fz', 'testbucket', 'http://testendpoint', auth_token='',
            required_headers=settings.REQUIRED_HEADERS,
            blacklist_headers=settings.HEADER_BLACKLIST,
        )
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
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        ingester.ingest()
        self.assertTrue(s3_mock.called)
        self.assertTrue(requests_mock.called)

    def test_get_area(self, requests_mock, s3_mock):
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        ingester.ingest()
        self.assertEqual('Polygon', requests_mock.call_args[1]['json']['area']['type'])
        ingester = Ingester(
            CAT_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
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
        ingester = Ingester(
            FITS_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
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
        ingester = Ingester(
            CAT_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        ingester.ingest()
        self.assertEqual(
            'cpt1m010-kb70-20151219-0073-e10',
            requests_mock.call_args[1]['json']['L1IDCAT']
        )

    def test_spectograph(self, requests_mock, s3_mock):
        ingester = Ingester(
            SPECTRO_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        ingester.ingest()
        self.assertEqual(90, requests_mock.call_args[1]['json']['RLEVEL'])
        self.assertTrue(dateutil.parser.parse(requests_mock.call_args[1]['json']['L1PUBDAT']))

    def test_spectrograph_missing_meta(self, requests_mock, s3_mock):
        tarfile.TarFile.getnames = MagicMock(return_value=[''])
        ingester = Ingester(
            SPECTRO_FILE,
            'test_bucket',
            'http://testendpoint',
            auth_token='',
            blacklist_headers=settings.HEADER_BLACKLIST,
            required_headers=settings.REQUIRED_HEADERS
        )
        with self.assertRaises(DoNotRetryError):
            ingester.ingest()
